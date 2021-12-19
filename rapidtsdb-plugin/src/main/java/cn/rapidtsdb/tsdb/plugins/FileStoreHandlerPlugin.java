package cn.rapidtsdb.tsdb.plugins;

import cn.rapidtsdb.tsdb.plugins.func.ConfigurablePlugin;
import cn.rapidtsdb.tsdb.plugins.func.NameablePlugin;
import cn.rapidtsdb.tsdb.plugins.func.PreparablePlugin;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * abstracting the persistence storage into file notation.
 * <p>
 * handle file io, note you need handle io lock by yourself
 */
public interface FileStoreHandlerPlugin extends NameablePlugin, PreparablePlugin, ConfigurablePlugin {

    /**
     * return the handler scheme
     * such as file, s3, hadoop, hdfs
     *
     * @return
     */
    String getScheme();

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
