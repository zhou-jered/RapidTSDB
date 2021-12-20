package cn.rapidtsdb.tsdb.plugins;

import cn.rapidtsdb.tsdb.plugins.func.ConfigurablePlugin;
import cn.rapidtsdb.tsdb.plugins.func.NameablePlugin;
import cn.rapidtsdb.tsdb.plugins.func.PreparablePlugin;
import cn.rapidtsdb.tsdb.plugins.polo.BlockPojo;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public interface BlockStoreHandlerPlugin extends NameablePlugin, PreparablePlugin, ConfigurablePlugin {

    boolean blockExists(int metricid, long basetime);

    void storeBlock(int metricId, long baseTime, byte[] data);

    byte[] getBlockData(int metricId, long baseTime);

    BlockPojo[] multiGetBlock(int metricId, Iterator<Long> basetimeIter);

    BlockPojo[] multiGetBlock(int[] metricIds, long basetime);

    Map<Integer, BlockPojo[]> multiGetBlock(int[] metricIds, Iterator<Long> basetimeScanner);

    Stream<BlockPojo> scanBlocks(int metricId, Iterator<Long> basetimeIter);

    Stream<Map<Integer, BlockPojo>> crossScanBlocks(int[] metricId, Iterator<Long> basetimeScanner);
}
