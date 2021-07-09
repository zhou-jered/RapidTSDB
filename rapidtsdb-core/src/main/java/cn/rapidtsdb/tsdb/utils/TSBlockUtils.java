package cn.rapidtsdb.tsdb.utils;

import cn.rapidtsdb.tsdb.core.TSBlock;
import cn.rapidtsdb.tsdb.core.TSBlockFactory;
import cn.rapidtsdb.tsdb.core.TSBlockSnapshot;
import cn.rapidtsdb.tsdb.core.TSDataPoint;
import cn.rapidtsdb.tsdb.core.io.TSBlockDeserializer;
import lombok.extern.log4j.Log4j2;

import java.util.LinkedList;
import java.util.List;

@Log4j2
public class TSBlockUtils {
    public static TSBlock mergeStoredBlockWithMemoryBlock(TSBlockDeserializer.TSBlockAndMeta storedBlock, TSBlockSnapshot memoryBlock) {
        TSBlock preBlock = storedBlock.getData();
        TSBlock newBlock = memoryBlock.getTsBlock();
        if (preBlock.getBaseTime() != newBlock.getBaseTime()) {
            log.error("Can not merge Block with different Basetime, trying to merge {} with {}",
                    preBlock.getBaseTime(), newBlock.getBaseTime());
            throw new RuntimeException(String.format("Can not merge Block with different Basetime, trying to merge %s with %s",
                    preBlock.getBaseTime(), newBlock.getBaseTime()));
        }
        List<TSDataPoint> dps = newBlock.getDataPoints();
        if (dps != null) {
            for (TSDataPoint dp : dps) {
                preBlock.appendDataPoint(dp.getTimestamp(), dp.getValue());
            }
        }
        preBlock.rewriteBytesData();
        return preBlock;
    }

    public static TSBlock orderedMassiveMerge(TSBlock olderBlock, TSBlock newerBlock) {
        if (olderBlock == null) {
            return newerBlock;
        }
        if (newerBlock == null) {
            return olderBlock;
        }

        List<TSDataPoint> newerDps = newerBlock.getDataPoints();
        List<TSDataPoint> olderDps = olderBlock.getDataPoints();
        List<TSDataPoint> mergedDps = new LinkedList<>();

        int oidx = 0;
        int nidx = 0;
        while (oidx < olderDps.size() && nidx < newerDps.size()) {
            TSDataPoint oldDp = olderDps.get(oidx);
            TSDataPoint newDp = newerDps.get(nidx);
            if (oldDp.getTimestamp() == newDp.getTimestamp()) {
                mergedDps.add(newDp);
                oidx++;
                nidx++;
            } else if (oldDp.getTimestamp() < newDp.getTimestamp()) {
                mergedDps.add(oldDp);
                oidx++;
            } else {
                mergedDps.add(newDp);
                nidx++;
            }
        }
        TSBlock block = TSBlockFactory.newEmptyBlock(olderBlock);
        for (TSDataPoint dp : mergedDps) {
            block.appendDataPoint(dp.getTimestamp(), dp.getValue());
        }
        return block;
    }
}
