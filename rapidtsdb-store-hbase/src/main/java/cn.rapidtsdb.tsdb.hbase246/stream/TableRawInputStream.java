package cn.rapidtsdb.tsdb.hbase246.stream;

import java.io.IOException;
import java.io.InputStream;

public class TableRawInputStream extends InputStream implements StreamEventAware {



    @Override
    public int read() throws IOException {
        return 0;
    }

    @Override
    public void onFlush() {

    }

    @Override
    public void onClose() {

    }
}
