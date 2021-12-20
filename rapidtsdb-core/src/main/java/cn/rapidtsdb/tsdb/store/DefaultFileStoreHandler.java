package cn.rapidtsdb.tsdb.store;

import cn.rapidtsdb.tsdb.plugins.FileStoreHandlerPlugin;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;


@Log4j2
public class DefaultFileStoreHandler implements FileStoreHandlerPlugin {

    private String baseDir;

    @Override
    public String getName() {
        return "default-file-store";
    }

    @Override
    public void prepare() {

    }

    @Override
    public String getInterestedPrefix() {
        return "dataPath";
    }

    @Override
    public void config(Map<String, String> subConfig) {
        String configDataDir = subConfig.get("dataPath");
        if (StringUtils.isBlank(configDataDir)) {
            baseDir = "";
        } else {
            if (configDataDir.endsWith("/")) {
                baseDir = configDataDir;
            } else {
                baseDir = configDataDir + "/";
            }
        }
        File baseDirFile = new File(baseDir);
        if (!baseDirFile.exists()) {
            boolean mkDirSuccess = baseDirFile.mkdirs();
            if (!mkDirSuccess) {
                throw new RuntimeException("Can not create data directory:" + baseDir + ", check the file permission please.");
            }
        }
        if (!baseDirFile.canWrite()) {
            boolean setWriteAbleSuccess = baseDirFile.setWritable(true);
            if (!setWriteAbleSuccess) {
                throw new RuntimeException("Can not write the data directory, check the file permission please.");
            }
        }
    }

    @Override
    public String getScheme() {
        return "file";
    }

    public DefaultFileStoreHandler() {


    }

    @Override
    public boolean fileExisted(String filePath) {
        return new File(baseDir + filePath).exists();
    }

    @Override
    public boolean fileWriteable(String filePath) {
        return new File(baseDir + filePath).canWrite();
    }

    @Override
    public boolean fileReadable(String filePath) {
        return new File(baseDir + filePath).canRead();
    }

    @Override
    public InputStream openFileInputStream(String filePath) throws IOException {
        return new FileInputStream(baseDir + filePath);
    }

    @Override
    public OutputStream openFileOutputStream(String filePath) throws IOException {
        ensureDirs(baseDir + filePath);
        return new FileOutputStream(baseDir + filePath);
    }

    @Override
    public OutputStream openFileAppendStream(String filePath) throws IOException {
        ensureDirs(baseDir + filePath);
        return new FileOutputStream(baseDir + filePath, true);
    }

    @Override
    public File getFile(String filePath) {
        return new File(baseDir + filePath);
    }

    @Override
    public long getFileSize(String filePath) {
        return new File(baseDir + filePath).length();
    }

    @Override
    public boolean createDirectory(String filePath) {
        return new File(baseDir + filePath).mkdirs();
    }

    @Override
    public boolean createFile(String filePath) {
        try {
            return new File(baseDir + filePath).createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean removeFile(String filePath, boolean force) {
        File file = new File(baseDir + filePath);
        if (file.isDirectory() && file.list().length != 0 && !force) {
            return false;
        }
        return file.delete();
    }

    private void ensureDirs(String filepath) {
        int partIdx = filepath.lastIndexOf('/');
        if (partIdx > 0) {
            String dirPart = filepath.substring(0, partIdx);
            new File(dirPart).mkdirs();
        }
    }

}
