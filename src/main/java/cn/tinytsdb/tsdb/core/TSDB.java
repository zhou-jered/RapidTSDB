package cn.tinytsdb.tsdb.core;

import cn.tinytsdb.tsdb.config.TSDBConfig;
import cn.tinytsdb.tsdb.core.persistent.AppendOnlyLogManager;
import cn.tinytsdb.tsdb.core.persistent.MetricsKeyManager;
import cn.tinytsdb.tsdb.exectors.GlobalExecutorHolder;
import cn.tinytsdb.tsdb.lifecycle.Closer;
import cn.tinytsdb.tsdb.lifecycle.Initializer;
import cn.tinytsdb.tsdb.query.TSQuery;
import cn.tinytsdb.tsdb.utils.TimeUtils;
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
    private TSBlockManager blockManager;
    private AppendOnlyLogManager appendOnlyLogManager;
    private MetricsKeyManager metricsKeyManager;
    private GlobalExecutorHolder commonExecutor = GlobalExecutorHolder.getInstance();


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
    }


    @Override
    public void close() {
        appendOnlyLogManager.close();
    }


    public void writeMetric(String metric, double val) throws Exception {
        writeMetric(metric, val, TimeUtils.currentTimestamp());
    }

    public void writeMetric(String metric, double val, long timestamp) throws Exception {
        if (StringUtils.isBlank(metric)) {
            throw new Exception("metric can not be blank");
        }
        Integer mIdx = metricsKeyManager.getMetricsIndex(metric);
        appendOnlyLogManager.appendLog(mIdx, timestamp, val);
        TSBlock tsBlock = blockManager.getCurrentWriteBlock(mIdx, timestamp);
        tsBlock.appendDataPoint(timestamp, val);

        if (tsBlock.inBlock(timestamp)) {
            tsBlock.appendDataPoint(timestamp, val);
        } else if (tsBlock.getBaseTime() < timestamp) {

        } else {
            // todo find old block to write in
        }

    }

    /**
     * update non current time range blocks,
     * may disk io involved
     *
     * @param metric
     * @param val
     * @param timestamp
     */
    public void updatePastMetric(String metric, double val, long timestamp) {
        Integer mIdx = metricsKeyManager.getMetricsIndex(metric);
        TSBlock elderBlock = blockManager.searchHistoryBlock(mIdx, timestamp);

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
