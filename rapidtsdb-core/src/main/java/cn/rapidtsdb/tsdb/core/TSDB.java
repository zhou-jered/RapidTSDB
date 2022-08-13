package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.calculate.CalculatorFactory;
import cn.rapidtsdb.tsdb.calculate.DownSampler;
import cn.rapidtsdb.tsdb.common.TimeUtils;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.persistent.*;
import cn.rapidtsdb.tsdb.core.pojo.TSEngineQuery;
import cn.rapidtsdb.tsdb.core.pojo.TSEngineQueryResult;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.tasks.BlockCompressTask;
import cn.rapidtsdb.tsdb.tasks.TwoHoursTriggerTask;
import lombok.extern.log4j.Log4j2;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TSDB main class
 */
@Log4j2
public class TSDB implements Initializer, Closer {

    // components
    private AbstractTSBlockManager blockManager;
    private AppendOnlyLogManager appendOnlyLogManager;
    private IMetricsKeyManager IMetricsKeyManager;
    private TSDBCheckPointManager checkPointManager;
    private ManagedThreadPool globalExecutor = ManagedThreadPool.getInstance();

    public static final int DB_STATE_INIT = 0;
    public static final int DB_STATE_RUNNING = 1;
    public static final int DB_STATE_CLOSED = 2;
    private AtomicInteger dbState = new AtomicInteger(DB_STATE_INIT);

    TSDBConfig config;

    TooOldWriteQueue tooOldWriteQueue = new TooOldWriteQueue();

    public TSDB() {
        this.config = TSDBConfig.getConfigInstance();
        IMetricsKeyManager = MetricsKeyManagerFactory.getInstance();
        blockManager = new TSBlockManager(config);
        appendOnlyLogManager = new AppendOnlyLogManager();
        checkPointManager = TSDBCheckPointManager.getInstance();
    }


    /**
     * recover metric key index
     * recover metric memory data
     * warmup memory cache
     * init time scheduled task
     */
    @Override
    public void init() {
        appendOnlyLogManager.init();
        IMetricsKeyManager.init();
        blockManager.init();
        checkPointManager.init();
        recoveryDBData();
        initScheduleTimeTask();
        dbState.set(DB_STATE_RUNNING);
    }


    @Override
    public void close() {
        log.info("Closing TSDB");
        dbState.set(DB_STATE_CLOSED);
        IMetricsKeyManager.close();
        blockManager.close();
        checkPointManager.savePoint(appendOnlyLogManager.getLogIndex());
        appendOnlyLogManager.close();
        globalExecutor.close();
        log.info("TSDB Close completed");
    }


    public void writeMetric(String metric, double val) throws Exception {
        writeMetric(metric, val, TimeUtils.currentTimestamp());
    }

    public void writeMetric(String metric, double val, long timestamp) {
        Integer mIdx = IMetricsKeyManager.getMetricsIndex(metric, true);
        writeMetricInternal(mIdx, timestamp, val);
        appendOnlyLogManager.appendLog(mIdx, timestamp, val);
    }


    private void writeMetricInternal(int mid, long timestamp, double val) {
        TSBlock tsBlock = blockManager.getTargetWriteBlock(mid, timestamp);
        if (tsBlock != null) {
            tsBlock.appendDataPoint(timestamp, val);
        } else if (TimeUtils.currentSeconds() - timestamp / 1000 < config.getMaxAllowedDelaySeconds()) {
            tooOldWriteQueue.write(mid, timestamp, val);
        }
    }

    public TSEngineQueryResult queryTimeSeriesData(TSEngineQuery query) {
        long startNano = System.nanoTime();
        int mid = IMetricsKeyManager.getMetricsIndex(query.getMetric(), false);
        if (mid <= 0) {
            return TSEngineQueryResult.builder()
                    .dps(new TreeMap<>())
                    .scanCostNanos(0)
                    .scannerPointNumber(0)
                    .build();
        }
        List<TSBlock> blocks = blockManager.getBlockWithTimeRange(mid, query.getStartTimestamp(), query.getEndTimestamp());
        SortedMap<Long, Double> dps = new TreeMap<>();
        for (TSBlock b : blocks) {
            dps.putAll(b.getDataPoints());
        }
        int totalScanPointNUmber = dps.size();
        dps = dps.subMap(query.getStartTimestamp(), query.getEndTimestamp() + 1);
        DownSampler downSampler = CalculatorFactory.getDownSample(query.getDownSampler());
        if (downSampler != null) {
            dps = downSampler.downSample(dps);
        }
        long costNano = System.nanoTime() - startNano;
        TSEngineQueryResult engineQueryResult = TSEngineQueryResult.builder()
                .dps(dps)
                .scanCostNanos(costNano)
                .scannerPointNumber(totalScanPointNUmber)
                .build();
        return engineQueryResult;
    }


    public void triggerBlockPersist() {
        final long aolLogIdx = appendOnlyLogManager.getLogIndex();
        blockManager.triggerRoundCheck((data) -> {
            checkPointManager.savePoint(aolLogIdx);
            return null;
        });
    }

    private void recoveryDBData() {
        tryRecoveyMemoryData();
        checkAOLog();
    }

    private void checkAOLog() {
        long aolIdx = appendOnlyLogManager.getLogIndex();
        long checkpoint = checkPointManager.getSavedPoint();
        if (checkpoint == aolIdx) {
            return;
        }
        if (checkpoint < aolIdx) {
            AOLog[] logs = appendOnlyLogManager.recoverLog(checkPointManager.getSavedPoint());
            if (logs != null) {
                for (AOLog log : logs) {
                    int mid = log.getMetricsId();
                    long time = log.getTimestamp();
                    double val = log.getVal();
                    writeMetricInternal(mid, time, val);
                }
            }
        }
    }

    private void tryRecoveyMemoryData() {
        Set<String> allMetrics = IMetricsKeyManager.getAllMetrics();
        List<Integer> allMids = new ArrayList<>(allMetrics.size());
        allMetrics.forEach(m -> allMids.add(IMetricsKeyManager.getMetricsIndex(m, false)));
        blockManager.tryRecoveryMemoryData(allMids);
    }

    private void initScheduleTimeTask() {

        long currentSeconds = TimeUtils.currentSeconds();
        long triggerInitDelay = TimeUnit.HOURS.toSeconds(2) - currentSeconds % TimeUnit.HOURS.toSeconds(2);
        TwoHoursTriggerTask twoHoursTriggerTask = new TwoHoursTriggerTask(this);
        log.info("schedule 2 hour trigger task, current: {}, initDelay:{},", currentSeconds, triggerInitDelay);
        globalExecutor.scheduledExecutor().scheduleAtFixedRate(twoHoursTriggerTask, triggerInitDelay, TimeUnit.HOURS.toSeconds(2), TimeUnit.SECONDS);
        log.info("initScheduleTimeTask with initDelay: {} min {} secs", triggerInitDelay/60, triggerInitDelay%60);

        scheduleCompressTask();
    }

    private void scheduleCompressTask() {
        BlockCompressTask blockCompressTask = new BlockCompressTask();
        globalExecutor.scheduledExecutor().scheduleAtFixedRate(blockCompressTask, 12, 12, TimeUnit.HOURS);
    }


}
