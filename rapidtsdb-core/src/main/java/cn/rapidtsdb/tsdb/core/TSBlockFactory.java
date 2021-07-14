package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.utils.TimeUtils;

import java.util.concurrent.TimeUnit;

public class TSBlockFactory {
    public static final int BLOCK_SIZE_SECONDS = (int) TimeUnit.HOURS.toSeconds(2);

    public static TSBlock newTSBlock(int metricId, long timestamp) {
        TSBlock tsBlock = null;
        long secondsTimestamp = timestamp / 1000;
        long secondsBasetime = TimeUtils.getBlockBaseTimeSeconds(secondsTimestamp);
        long blockBasetime = secondsBasetime;
        tsBlock = new TSBlock(blockBasetime, BLOCK_SIZE_SECONDS);
        return tsBlock;
    }

    /**
     * return an empty block which copy the
     * configuration from configuredBlock
     *
     * @param configuredBlock
     * @return
     */
    public static TSBlock newEmptyBlock(TSBlock configuredBlock) {
        TSBlock block = new TSBlock(configuredBlock.getBaseTime(), BLOCK_SIZE_SECONDS);
        return block;
    }

}
