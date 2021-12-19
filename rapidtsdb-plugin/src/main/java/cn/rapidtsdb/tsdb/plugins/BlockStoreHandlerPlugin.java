package cn.rapidtsdb.tsdb.plugins;

import cn.rapidtsdb.tsdb.plugins.func.ConfigurablePlugin;
import cn.rapidtsdb.tsdb.plugins.func.NameablePlugin;
import cn.rapidtsdb.tsdb.plugins.func.PreparablePlugin;
import cn.rapidtsdb.tsdb.plugins.polo.BlockPojo;

import java.util.Map;
import java.util.stream.Stream;

public interface BlockStoreHandlerPlugin extends NameablePlugin, PreparablePlugin, ConfigurablePlugin {

    void storeBlock(int metricId, long baseTime, byte[] data);

    byte[] getBlockData(int metricId, long baseTime);

    BlockPojo[] multiGetBlock(int metricId, long fromBasetime, long toBaseTime);

    BlockPojo[] multiGetBlock(int[] metricIds, long basetime);

    Map<Integer, BlockPojo[]> multiGetBlock(int[] metricIds, long fromBasetime, long toBasetime);

    Stream<BlockPojo> scanBlocks(int metricId, long fromBasetime, long toBasetime);

    Stream<Map<Integer, BlockPojo>> crossScanBlocks(int metricId, long fromBasetime, long toBasetime);
}
