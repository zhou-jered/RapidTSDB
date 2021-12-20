package cn.rapidtsdb.tsdb.core.io;

import cn.rapidtsdb.tsdb.core.AbstractTSBlockManager;
import cn.rapidtsdb.tsdb.core.TSBlockMeta;
import cn.rapidtsdb.tsdb.core.TSBlockSnapshot;
import cn.rapidtsdb.tsdb.core.TSBytes;

import java.io.IOException;
import java.io.OutputStream;

public class TSBlockSerializer {


    public void serializeToStream(int metricId, TSBlockSnapshot snapshot, OutputStream outputStream) throws IOException {
        TSBlockMeta blockMeta = AbstractTSBlockManager.createTSBlockMeta(snapshot, metricId);
        TSBytes timeBytes = snapshot.getTsBlock().getTime();
        TSBytes valBytes = snapshot.getTsBlock().getValues();

        outputStream.write(blockMeta.series());
        outputStream.write(timeBytes.getData(), 0, snapshot.getTimeBytesLength());
        outputStream.write(valBytes.getData(), 0, snapshot.getValuesBytesLength());
        outputStream.flush();
    }

    public byte[] serializedToBytes(int metricId, TSBlockSnapshot snapshot) {
        TSBlockMeta blockMeta = AbstractTSBlockManager.createTSBlockMeta(snapshot, metricId);
        byte[] metaSeries = blockMeta.series();
        byte[] series = new byte[metaSeries.length + snapshot.getTimeBytesLength() + snapshot.getValuesBytesLength()];
        int copyOffset = 0;
        System.arraycopy(metaSeries, 0, series, copyOffset, metaSeries.length);
        copyOffset += metaSeries.length;
        System.arraycopy(snapshot.getTsBlock().getTime().getData(), 0, series, copyOffset, snapshot.getTimeBytesLength());
        copyOffset += snapshot.getTimeBytesLength();
        System.arraycopy(snapshot.getTsBlock().getValues().getData(), 0, series,
                copyOffset, snapshot.getValuesBytesLength());
        return series;
    }

}
