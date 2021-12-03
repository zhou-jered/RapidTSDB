package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.TSDBBridge;
import cn.rapidtsdb.tsdb.calculate.CalculatorFactory;
import cn.rapidtsdb.tsdb.calculate.DownSampler;
import cn.rapidtsdb.tsdb.common.TimeUtils;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.persistent.AOLog;
import cn.rapidtsdb.tsdb.core.persistent.AppendOnlyLogManager;
import cn.rapidtsdb.tsdb.core.persistent.MetricsKeyManager;
import cn.rapidtsdb.tsdb.core.persistent.TSDBCheckPointManager;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.obj.WriteMetricResult;
import cn.rapidtsdb.tsdb.tasks.BlockCompressTask;
import cn.rapidtsdb.tsdb.tasks.TwoHoursTriggerTask;
import cn.rapidtsdb.tsdb.utils.TSDataUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * TSDB main class
 */
@Log4j2
public class TSDB implements Initializer, Closer {

    // components
    private AbstractTSBlockManager blockManager;
    private AppendOnlyLogManager appendOnlyLogManager;
    private MetricsKeyManager metricsKeyManager;
    private TSDBCheckPointManager checkPointManager;
    private ManagedThreadPool globalExecutor = ManagedThreadPool.getInstance();

    public static final int DB_STATE_INIT = 0;
    public static final int DB_STATE_RUNNING = 1;
    public static final int DB_STATE_CLOSED = 2;
    private int dbState = DB_STATE_INIT;

    TSDBConfig config;

    TooOldWriteQueue tooOldWriteQueue = new TooOldWriteQueue();

    public TSDB() {
        this.config = TSDBConfig.getConfigInstance();
        metricsKeyManager = MetricsKeyManager.getInstance();
        blockManager = new TSBlockManager(config);
        appendOnlyLogManager = new AppendOnlyLogManager();
        checkPointManager = TSDBCheckPointManager.getInstance();
        TSDBBridge.regist(this);
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
        metricsKeyManager.init();
        blockManager.init();
        recoveryDBData();
        initScheduleTimeTask();
        dbState = DB_STATE_RUNNING;
    }


    @Override
    public void close() {
        log.info("Closing TSDB");
        dbState = DB_STATE_CLOSED;
        appendOnlyLogManager.close();
        metricsKeyManager.close();
        blockManager.close();
        checkPointManager.savePoint(appendOnlyLogManager.getLogIndex());
        globalExecutor.close();
        log.info("TSDB Close completed");
    }


    public WriteMetricResult writeMetric(String metric, double val) throws Exception {
        return writeMetric(metric, val, TimeUtils.currentTimestamp());
    }

    public WriteMetricResult writeMetric(String metric, double val, long timestamp) {
        if (StringUtils.isBlank(metric)) {
            return WriteMetricResult.FAILED_METRIC_EMPTY;
        }
        if (dbState != DB_STATE_RUNNING) {
            return WriteMetricResult.DB_STATE_NOT_RUNNING;
        }
        Integer mIdx = metricsKeyManager.getMetricsIndex(metric);
        WriteMetricResult internalWResult = writeMetricInternal(mIdx, timestamp, val);
        if (internalWResult.isSuccess()) {
            appendOnlyLogManager.appendLog(mIdx, timestamp, val);
        }
        return internalWResult;
    }


    private WriteMetricResult writeMetricInternal(int mid, long timestamp, double val) {
        TSBlock tsBlock = blockManager.getCurrentWriteBlock(mid, timestamp);
        if (tsBlock != null) {
            tsBlock.appendDataPoint(timestamp, val);
            return WriteMetricResult.SUCCESS;
        } else if (TimeUtils.currentSeconds() - timestamp / 1000 < config.getMaxAllowedDelaySeconds()) {
            tooOldWriteQueue.write(mid, timestamp, val);
        }
        return WriteMetricResult.FAILED_TIME_FAILED;
    }

    public List<TSDataPoint> queryTimeSeriesData(SimpleDataQuery query) {
        int mid = metricsKeyManager.getMetricsIndex(query.getMetric());
        List<TSBlock> blocks = blockManager.getBlockWithTimeRange(mid, query.getStartTime(), query.getEndTime());
        List<TSDataPoint> dps = new ArrayList<>();
        for (TSBlock b : blocks) {
            dps.addAll(b.getDataPoints());
        }
        int left = TSDataUtils.binarySearchByTimestamp(dps, query.getStartTime(), true);
        int right = TSDataUtils.binarySearchByTimestamp(dps, query.getEndTime(), false);
        List<TSDataPoint> queriedResult = dps.subList(left, right);
        return queriedResult;
    }

    public List<TSDataPoint> queryTimeSeriesData(SimpleDataQuery query, String downsampler) {
        DownSampler downSampler = CalculatorFactory.getDownSample(downsampler);
        List<TSDataPoint> dps = queryTimeSeriesData(query);
        return downSampler.downSample(dps);
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
        Set<String> allMetrics = metricsKeyManager.getAllMetrics();
        List<Integer> allMids = new ArrayList<>(allMetrics.size());
        allMetrics.forEach(m -> allMids.add(metricsKeyManager.getMetricsIndex(m)));
        blockManager.tryRecoveryMemoryData(allMids);
    }

    private void initScheduleTimeTask() {
        log.info("schedule 2 hour trigger task");
        long currentSeconds = TimeUtils.currentSeconds();
        long triggerInitDelay = TimeUnit.HOURS.toSeconds(2) - currentSeconds % TimeUnit.HOURS.toSeconds(2);
        TwoHoursTriggerTask twoHoursTriggerTask = new TwoHoursTriggerTask(this);
        globalExecutor.scheduledExecutor().scheduleAtFixedRate(twoHoursTriggerTask, triggerInitDelay, TimeUnit.HOURS.toSeconds(2), TimeUnit.SECONDS);
        log.info("initScheduleTimeTask with initDelay:{}", triggerInitDelay);

        scheduleCompressTask();
    }

    private void scheduleCompressTask() {
        BlockCompressTask blockCompressTask = new BlockCompressTask();
        globalExecutor.scheduledExecutor().scheduleAtFixedRate(blockCompressTask, 12, 12, TimeUnit.HOURS);
    }


}
