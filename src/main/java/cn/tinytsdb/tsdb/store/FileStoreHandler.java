package cn.tinytsdb.tsdb.store;

import cn.tinytsdb.tsdb.config.TSDBConfig;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


@Log4j2
public class FileStoreHandler implements StoreHandler {

    TSDBConfig tsdbConfig;
    private String baseDir;

    public FileStoreHandler(TSDBConfig tsdbConfig) {
        this.tsdbConfig = tsdbConfig;
        String configDataDir = tsdbConfig.getDataDir();
        if(StringUtils.isBlank(configDataDir)) {
            baseDir="";
        } else {
            if(configDataDir.endsWith("/")) {
                baseDir = configDataDir;
            } else {
                baseDir =  configDataDir+"/";
            }
        }
    }

    @Override
    public boolean fileExisted(String filePath) {
        return new File(baseDir+filePath).exists();
    }

    @Override
    public boolean fileWriteable(String filePath) {
        return new File(baseDir+filePath).canWrite();
    }

    @Override
    public boolean fileReadable(String filePath) {
        return new File(baseDir+filePath).canRead();
    }

    @Override
    public InputStream getFileInputStream(String filePath) throws IOException {
        return new FileInputStream(baseDir+filePath);
    }

    @Override
    public OutputStream getFileOutputStream(String filePath) throws IOException {
        return new FileOutputStream(baseDir+filePath);
    }

    @Override
    public OutputStream getFileAppendStream(String filePath) throws IOException {
        return new FileOutputStream(baseDir+filePath, true);
    }

    @Override
    public long getFileSize(String filePath) {
        return new File(baseDir+filePath).length();
    }

    @Override
    public boolean createDirectory(String filePath) {
        return new File(baseDir+filePath).mkdirs();
    }

    @Override
    public boolean createFile(String filePath) {
        try {
            return new File(baseDir+filePath).createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean removeFile(String filePath, boolean force) {
        File file = new File(baseDir+filePath);
        if(file.isDirectory() && file.list().length !=0 && !force) {
            return false;
        }
        return file.delete();
    }
}
