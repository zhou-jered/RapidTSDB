package cn.rapidtsdb.tsdb.core.persistent;

import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.plugins.FileStoreHandlerPlugin;
import cn.rapidtsdb.tsdb.plugins.PluginManager;
import lombok.extern.log4j.Log4j2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Log4j2
public class TSDBCheckPointManager implements Initializer {

    private FileStoreHandlerPlugin storeHandler;
    private final String checkPointFilename = "ckFile.checkpoint";
    private static TSDBCheckPointManager INSTANCE;

    private TSDBCheckPointManager() {
    }

    @Override
    public void init() {
        storeHandler = PluginManager.getPlugin(FileStoreHandlerPlugin.class);
    }

    public void savePoint(long point) {
        try {
            OutputStream outputStream = storeHandler.openFileOutputStream(checkPointFilename);
            DataOutputStream dos = new DataOutputStream(outputStream);
            dos.writeLong(point);
            dos.flush();
            dos.close();
        } catch (IOException e) {
            e.printStackTrace();
            log.error("SavePoint Exception", e);
        }
    }

    public long getSavedPoint() {
        if (!storeHandler.fileExisted(checkPointFilename)) {
            return 0;
        }
        try {
            InputStream inputStream = storeHandler.openFileInputStream(checkPointFilename);
            DataInputStream dis = new DataInputStream(inputStream);
            long point = dis.readLong();
            dis.close();
            return point;
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Read Saved Point Exception", e);
        }
        return 0;
    }

    public static TSDBCheckPointManager getInstance() {
        if (INSTANCE == null) {
            synchronized (TSDBCheckPointManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new TSDBCheckPointManager();
                }
            }
        }
        return INSTANCE;
    }

}
