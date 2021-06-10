package cn.rapidtsdb.tsdb.store;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * handle file io, note you need handle io lock by yourself
 */
public interface StoreHandler {

    boolean fileExisted(String filePath);

    boolean fileWriteable(String filePath);

    boolean fileReadable(String filePath);

    InputStream openFileInputStream(String filePath) throws IOException;

    OutputStream openFileOutputStream(String filePath) throws IOException;

    OutputStream openFileAppendStream(String filePath) throws IOException;

    File getFile(String filePath);

    long getFileSize(String filePath);

    boolean createDirectory(String filePath);

    boolean createFile(String filePath);

    boolean removeFile(String filePath, boolean force);

}
