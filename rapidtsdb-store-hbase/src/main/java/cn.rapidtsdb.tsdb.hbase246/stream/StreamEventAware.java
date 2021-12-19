package cn.rapidtsdb.tsdb.hbase246.stream;

import java.io.IOException;

public interface StreamEventAware {
    void onFlush() throws IOException;

    void onClose() throws IOException;
}
