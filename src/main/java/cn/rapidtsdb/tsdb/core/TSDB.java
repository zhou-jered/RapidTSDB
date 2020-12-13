package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.persistent.AppendOnlyLogManager;
import cn.rapidtsdb.tsdb.core.persistent.MetricsKeyManager;
import cn.rapidtsdb.tsdb.exectors.GlobalExecutorHolder;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.obj.WriteMetricResult;
import cn.rapidtsdb.tsdb.query.TSQuery;
import cn.rapidtsdb.tsdb.utils.TimeUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private GlobalExecutorHolder globalExecutor = GlobalExecutorHolder.getInstance();


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
        long currentMills = TimeUtils.currentMills();
        long initDelay = currentMills - currentMills % TimeUnit.HOURS.toMillis(2);
        initDelay = Math.max(initDelay, TimeUnit.MINUTES.toMillis(30));
        globalExecutor.scheduledExecutor().scheduleAtFixedRate(() -> blockManager.triggerPersist(), initDelay, TimeUnit.HOURS.toMillis(2), TimeUnit.HOURS);
    }


    @Override
    public void close() {
        log.info("Closing TSDB");
        appendOnlyLogManager.close();
        log.info("TSDB Close completed");
    }


    public WriteMetricResult writeMetric(String metric, double val) throws Exception {
        return writeMetric(metric, val, TimeUtils.currentTimestamp());
    }

    public WriteMetricResult writeMetric(String metric, double val, long timestamp) {
        if (StringUtils.isBlank(metric)) {
            return WriteMetricResult.FAILED_METRIC_EMPTY;
        }
        Integer mIdx = metricsKeyManager.getMetricsIndex(metric);
        TSBlock tsBlock = blockManager.getCurrentWriteBlock(mIdx, timestamp);
        if (tsBlock != null) {
            tsBlock.appendDataPoint(timestamp, val);

            appendOnlyLogManager.appendLog(mIdx, timestamp, val);

            return WriteMetricResult.SUCCESS;
        }
        return WriteMetricResult.FAILED_TIME_EXPIRED;
    }

    public List<TSDataPoint> queryTimeSeriesData(TSQuery query) {
        return null;
    }

    private void initMemDb() {
        appendOnlyLogManager.recoverLog();
    }

    private void initScheduleTimeTask() {
        log.info("schedule 2 hour trigger task");
        long currentSeconds = TimeUtils.currentTimestamp();
        long triggerInitDelay = TimeUnit.HOURS.toSeconds(2) - currentSeconds % TimeUnit.HOURS.toSeconds(2);
        log.info("initScheduleTimeTask with initDelay:{}", triggerInitDelay);

    }


    public static class DoubleXor {
        public static DoubleXorResult doubleXor(Double pre, Double current) {
            long preBits = Double.doubleToLongBits(pre);
            long currentBits = Double.doubleToLongBits(current);
            long rawResult = preBits ^ currentBits;
            DoubleXorResult result = fromRawResult(rawResult);
            return result;
        }

        public static DoubleXorResult fromRawResult(long rawResult) {
            DoubleXorResult result = new DoubleXorResult();
            if (rawResult != 0) {
                result.setRawResult(rawResult);
                byte left0 = (byte) (63 - bitsRightZeroTable.get(Long.highestOneBit(rawResult)));
                byte right0 = bitsRightZeroTable.get(Long.lowestOneBit(rawResult));
                result.setLeft0(left0);
                result.setRight0(right0);
            } else {
                result.setLeft0((byte) 32);
                result.setRight0((byte) 32);
                result.setZero(true);
            }
            return result;
        }


        private static Map<Long, Byte> bitsRightZeroTable = new HashMap<>(128);

        static {
            bitsRightZeroTable.put(0L, (byte) 0);
            long n = 1;
            for (byte i = 0; i < 64; i++) {
                bitsRightZeroTable.put(n << i, i);
            }
        }
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DoubleXorResult {
        private long rawResult;
        private byte left0;
        private byte right0;
        boolean zero = false;

        public long getMeaningLongBits() {
            return rawResult << left0;
        }

        public byte getMeaningBitsLength() {
            return (byte) (64 - left0 - right0);
        }

        public boolean inSubRange(DoubleXorResult other) {
            return other.left0 >= this.left0 && other.right0 >= this.right0;
        }
    }


}
