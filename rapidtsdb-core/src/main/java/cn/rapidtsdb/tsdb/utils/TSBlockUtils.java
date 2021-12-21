package cn.rapidtsdb.tsdb.utils;

import cn.rapidtsdb.tsdb.core.TSBlock;
import cn.rapidtsdb.tsdb.core.TSBlockFactory;
import cn.rapidtsdb.tsdb.core.TSBlockSnapshot;
import cn.rapidtsdb.tsdb.core.io.TSBlockDeserializer;
import lombok.extern.log4j.Log4j2;

import java.util.Map;

@Log4j2
public class TSBlockUtils {
    public static TSBlock mergeStoredBlockWithMemoryBlock(TSBlockDeserializer.TSBlockAndMeta storedBlock, TSBlockSnapshot memoryBlock) {
        TSBlock preBlock = storedBlock.getData();
        TSBlock newBlock = memoryBlock.getTsBlock();
        return mergeBlocks(preBlock, newBlock);
    }

    public static TSBlock mergeBlocks(TSBlock preBlock, TSBlock newBlock) {
        if (preBlock.getBaseTime() != newBlock.getBaseTime()) {
            log.error("Can not merge Block with different Basetime, trying to merge {} with {}",
                    preBlock.getBaseTime(), newBlock.getBaseTime());
            throw new RuntimeException(String.format("Can not merge Block with different Basetime, trying to merge %s with %s",
                    preBlock.getBaseTime(), newBlock.getBaseTime()));
        }
        Map<Long, Double> dps1 = preBlock.getDataPoints();
        Map<Long, Double> dps2 = newBlock.getDataPoints();
        dps1.putAll(dps2);
        preBlock.rewriteBytesData(dps1);
        return preBlock;
    }

    public static TSBlock orderedMassiveMerge(TSBlock olderBlock, TSBlock newerBlock) {
        if (olderBlock == null) {
            return newerBlock;
        }
        if (newerBlock == null) {
            return olderBlock;
        }

        Map<Long, Double> newerDps = newerBlock.getDataPoints();
        Map<Long, Double> olderDps = olderBlock.getDataPoints();
        olderDps.putAll(newerDps);

        TSBlock block = TSBlockFactory.newEmptyBlock(olderBlock);
        newerBlock.getDataPoints().forEach((k, v) -> block.appendDataPoint(k, v));

        return block;
    }
}
