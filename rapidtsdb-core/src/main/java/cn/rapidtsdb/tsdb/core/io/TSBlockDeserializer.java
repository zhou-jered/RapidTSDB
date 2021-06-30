package cn.rapidtsdb.tsdb.core.io;

import cn.rapidtsdb.tsdb.core.TSBlock;
import cn.rapidtsdb.tsdb.core.TSBlockFactory;
import cn.rapidtsdb.tsdb.core.TSBlockMeta;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.io.InputStream;

public class TSBlockDeserializer {

    public TSBlockAndMeta deserializeFromStream(InputStream inputStream) throws IOException {
        final int availableBytes = inputStream.available();
        byte[] bs = new byte[availableBytes];
        int readBytes = inputStream.read(bs);
        if (readBytes != availableBytes) {
            throw new RuntimeException("Read TSBlock Serilized byte error, expect " + availableBytes + ", but " + readBytes);
        }
        return deserializeFromBytes(bs);
    }

    public TSBlockAndMeta deserializeFromBytes(byte[] bytes) {
        TSBlockMeta blockMeta = TSBlockMeta.fromSeries(bytes);
        TSBlock data = TSBlockFactory.newTSBlock(blockMeta.getMetricId(), blockMeta.getBaseTime());

        final int timeBytesOffset = TSBlockMeta.SERIES_LEN;
        data.recoveryTimeBytes(bytes, timeBytesOffset, blockMeta.getTimeBitsLen());
        int valueBytesOffset = timeBytesOffset + blockMeta.getTimeBitsLen() / 8;
        if (blockMeta.getTimeBitsLen() % 8 != 0) {
            valueBytesOffset += 1;
        }
        data.recovertValueBytes(bytes, valueBytesOffset, blockMeta.getValuesBitsLen());
        return new TSBlockAndMeta(blockMeta, data);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TSBlockAndMeta {
        private TSBlockMeta meta;
        private TSBlock data;
    }

}
