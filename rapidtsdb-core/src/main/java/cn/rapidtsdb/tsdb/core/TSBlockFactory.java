package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.common.TimeUtils;

import java.util.concurrent.TimeUnit;

public class TSBlockFactory {
    public static final int BLOCK_SIZE_MILLSECONDS = (int) TimeUnit.HOURS.toMillis(2);

    public static TSBlock newTSBlock(int metricId, long timestamp) {
        TSBlock tsBlock = null;
        long blockBasetime = TimeUtils.getBlockBaseTime(timestamp);
        tsBlock = new TSBlock(blockBasetime, BLOCK_SIZE_MILLSECONDS);
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
        TSBlock block = new TSBlock(configuredBlock.getBaseTime(), BLOCK_SIZE_MILLSECONDS);
        return block;
    }

}
