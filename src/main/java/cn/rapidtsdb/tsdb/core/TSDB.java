package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.persistent.AOLog;
import cn.rapidtsdb.tsdb.core.persistent.AppendOnlyLogManager;
import cn.rapidtsdb.tsdb.core.persistent.MetricsKeyManager;
import cn.rapidtsdb.tsdb.core.persistent.TSDBCheckPointManager;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.obj.WriteMetricResult;
import cn.rapidtsdb.tsdb.query.TSQuery;
import cn.rapidtsdb.tsdb.tasks.TwoHoursTriggerTask;
import cn.rapidtsdb.tsdb.utils.TimeUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;

import java.util.List;
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


    TSDBConfig config;

    private static final int TIME_HEADER64 = 1 << 9; // -63 ~ 64
    private static final int TIME_HEADER256 = (1 << 12) | (1 << 11);  // -255 ~ 256
    private static final int TIME_HEADER2048 = (1 << 16) | (1 << 15) | (1 << 14); //-2047 ~ 2048
    private static final int TIME_HEADER_FULL_BITS = 0b1111; // full bits


    public TSDB() {
        this.config = TSDBConfig.getConfigInstance();
        metricsKeyManager = MetricsKeyManager.getInstance();
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
        initMemDb();
        initScheduleTimeTask();
    }


    @Override
    public void close() {
        log.info("Closing TSDB");
        appendOnlyLogManager.close();
        log.info("TSDB Close completed");
    }


    public synchronized WriteMetricResult writeMetric(String metric, double val) throws Exception {
        return writeMetric(metric, val, TimeUtils.currentTimestamp());
    }

    public synchronized WriteMetricResult writeMetric(String metric, double val, long timestamp) {
        if (StringUtils.isBlank(metric)) {
            return WriteMetricResult.FAILED_METRIC_EMPTY;
        }
        Integer mIdx = metricsKeyManager.getMetricsIndex(metric);
        writeMetricInternal(mIdx, timestamp, val);
        appendOnlyLogManager.appendLog(mIdx, timestamp, val);
        return WriteMetricResult.FAILED_TIME_EXPIRED;
    }

    private synchronized WriteMetricResult writeMetricInternal(int mid, long timestamp, double val) {
        TSBlock tsBlock = blockManager.getCurrentWriteBlock(mid, timestamp);
        if (tsBlock != null) {
            tsBlock.appendDataPoint(timestamp, val);
            return WriteMetricResult.SUCCESS;
        }
        return WriteMetricResult.FAILED_TIME_EXPIRED;
    }

    public List<TSDataPoint> queryTimeSeriesData(TSQuery query) {
        return null;
    }

    public synchronized void triggerBlockPersist() {
        long aolLogIdx = appendOnlyLogManager.getLogIndex();
        blockManager.triggerRoundCheck(() -> {
            checkPointManager.savePoint(aolLogIdx);
        });
    }

    private void initMemDb() {
        long aolIdx = appendOnlyLogManager.getLogIndex();
        long cpIdx = checkPointManager.getSavedPoint();
        if (cpIdx < aolIdx) {
            AOLog[] logs = appendOnlyLogManager.recoverLog(checkPointManager.getSavedPoint());
            for (AOLog log : logs) {
                int mid = log.getMetricsIdx();
                long time = log.getTimestamp();
                double val = log.getVal();
                writeMetricInternal(mid, time, val);
            }
        }
    }

    private void initScheduleTimeTask() {
        log.info("schedule 2 hour trigger task");
        long currentSeconds = TimeUtils.currentTimestamp();
        long triggerInitDelay = TimeUnit.HOURS.toSeconds(2) - currentSeconds % TimeUnit.HOURS.toSeconds(2);
        TwoHoursTriggerTask twoHoursTriggerTask = new TwoHoursTriggerTask(this);
        globalExecutor.scheduledExecutor().scheduleAtFixedRate(twoHoursTriggerTask, triggerInitDelay, 2, TimeUnit.HOURS);
        log.info("initScheduleTimeTask with initDelay:{}", triggerInitDelay);
    }


}
