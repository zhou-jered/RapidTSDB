package cn.rapidtsdb.tsdb.hbase246.stream;

import cn.rapidtsdb.tsdb.hbase246.HbaseDefine;
import cn.rapidtsdb.tsdb.hbase246.buffer.DynamicByteBuffer;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.OutputStream;

@Log4j2
public class TableRowOutputStream extends OutputStream implements StreamEventAware {

    private static final int INIT_BUFFER_SIZE = 128 * 1024; // 128k
    private static final int MAXIMUM_WRITABLE_BYTES = 10 * 1024 * 1024; // 10m

    private Table table;
    private byte[] row;
    private DynamicByteBuffer innerBuffer;

    public TableRowOutputStream(Table table, byte[] row) {
        this.table = table;
        this.row = row;
        innerBuffer = new DynamicByteBuffer(INIT_BUFFER_SIZE, MAXIMUM_WRITABLE_BYTES);
    }

    @Override
    public void write(int b) throws IOException {
        innerBuffer.append(b);
    }

    @Override
    public void flush() throws IOException {
        onFlush();
    }

    @Override
    public void close() throws IOException {
        onClose();
    }

    @Override
    public void onFlush() {
        doWriteTable();
    }

    @Override
    public void onClose() {
        doWriteTable();
    }

    private void doWriteTable() {
        Put put = new Put(row);
        put.addColumn(HbaseDefine.CF_DATA_BYTES, new byte[0], innerBuffer.copyOfUsedBytes());
        try {
            table.put(put);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }


}
