package cn.rapidtsdb.tsdb.core.io;

import cn.rapidtsdb.tsdb.core.TSBlockSnapshot;
import cn.rapidtsdb.tsdb.core.TSBytes;

import java.io.IOException;
import java.io.OutputStream;

public class TSBlockSerializer {

    public void serializeToStream(TSBlockSnapshot snapshot, OutputStream outputStream) throws IOException {
        TSBytes timeBytes = snapshot.getTsBlock().getTime();
        TSBytes valBytes = snapshot.getTsBlock().getValues();
        outputStream.write(timeBytes.getData(), 0, snapshot.getTimeBytesLength());
        outputStream.write(valBytes.getData(), 0, snapshot.getValuesBytesLength());
        outputStream.flush();
    }

    public byte[] serializedToBytes(TSBlockSnapshot snapshot) {
        byte[] series = new byte[snapshot.getTimeBytesLength() + snapshot.getValuesBytesLength()];
        System.arraycopy(snapshot.getTsBlock().getTime(), 0, series, 0, snapshot.getTimeBytesLength());
        System.arraycopy(snapshot.getTsBlock().getValues(), 0, series,
                snapshot.getTimeBytesLength(), snapshot.getValuesBytesLength());
        return series;
    }

}
