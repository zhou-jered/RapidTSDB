package cn.rapidtsdb.tsdb.hbase246;

import cn.rapidtsdb.tsdb.plugins.StoreHandlerPlugin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HbaseStoreHandler implements StoreHandlerPlugin {

    private Connection connection;

    public HbaseStoreHandler() {
        
    }

    @Override
    public String getScheme() {
        return "hbase";
    }

    @Override
    public boolean fileExisted(String filePath) {
        return false;
    }

    @Override
    public boolean fileWriteable(String filePath) {
        return false;
    }

    @Override
    public boolean fileReadable(String filePath) {
        return false;
    }

    @Override
    public InputStream openFileInputStream(String filePath) throws IOException {
        return null;
    }

    @Override
    public OutputStream openFileOutputStream(String filePath) throws IOException {
        return null;
    }

    @Override
    public OutputStream openFileAppendStream(String filePath) throws IOException {
        return null;
    }

    @Override
    public File getFile(String filePath) {
        return null;
    }

    @Override
    public long getFileSize(String filePath) {
        return 0;
    }

    @Override
    public boolean createDirectory(String filePath) {
        return false;
    }

    @Override
    public boolean createFile(String filePath) {
        return false;
    }

    @Override
    public boolean removeFile(String filePath, boolean force) {
        return false;
    }
}
