package cn.tinytsdb.tsdb.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * handle file io
 */
public interface StoreHandler {

    boolean fileExisted(String filePath);

    boolean fileWriteable(String filePath);

    boolean fileReadable(String filePath);

    InputStream getFileInputStream(String filePath) throws IOException;

    OutputStream getFileOutputStream(String filePath) throws IOException;

    OutputStream getFileAppendStream(String filePath) throws IOException;

    long getFileSize(String filePath);

    boolean createDirectory(String filePath);

    boolean createFile(String filePath);

    boolean removeFile(String filePath, boolean force);

}
