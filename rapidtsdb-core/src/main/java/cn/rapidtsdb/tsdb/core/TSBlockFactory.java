package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.config.MetricConfig;
import cn.rapidtsdb.tsdb.utils.TimeUtils;

import java.util.concurrent.TimeUnit;

import static cn.rapidtsdb.tsdb.core.AbstractTSBlockManager.TIME_UNIT_ADAPTOR_SECONDS;

public class TSBlockFactory {
    public static final int BLOCK_SIZE_SECONDS = (int) TimeUnit.HOURS.toSeconds(2);

    public static TSBlock newTSBlock(int metricId, long timestamp) {
        TSBlock tsBlock = null;
        MetricConfig mc = MetricConfig.getMetricConfig(metricId);
        TimeUtils.TimeUnitAdaptor timeUnitAdaptor = TimeUtils.TimeUnitAdaptorFactory.getTimeAdaptor(mc.getTimeUnit());
        long secondsTimestamp = TIME_UNIT_ADAPTOR_SECONDS.adapt(timestamp);
        long secondsBasetime = secondsTimestamp - (secondsTimestamp % BLOCK_SIZE_SECONDS);
        long blockBasetime = timeUnitAdaptor.adapt(secondsBasetime);
        tsBlock = new TSBlock(blockBasetime, BLOCK_SIZE_SECONDS, timeUnitAdaptor);
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
        TSBlock block = new TSBlock(configuredBlock.getBaseTime(), BLOCK_SIZE_SECONDS, configuredBlock.getTimeUnitAdapter());
        return block;
    }

}
