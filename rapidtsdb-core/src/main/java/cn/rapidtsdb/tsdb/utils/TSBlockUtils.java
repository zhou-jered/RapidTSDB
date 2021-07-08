package cn.rapidtsdb.tsdb.utils;

import cn.rapidtsdb.tsdb.core.TSBlock;
import cn.rapidtsdb.tsdb.core.TSBlockSnapshot;
import cn.rapidtsdb.tsdb.core.TSDataPoint;
import cn.rapidtsdb.tsdb.core.io.TSBlockDeserializer;
import lombok.extern.log4j.Log4j2;

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
}
