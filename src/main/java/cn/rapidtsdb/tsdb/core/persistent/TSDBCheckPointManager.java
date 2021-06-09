package cn.rapidtsdb.tsdb.core.persistent;

import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.store.StoreHandler;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;

public class TSDBCheckPointManager implements Initializer {

    private final StoreHandler storeHandler = StoreHandlerFactory.getStoreHandler();
    private final String checkPointFilename = "ckFile.checkpoint";
    private static TSDBCheckPointManager INSTANCE;

    private TSDBCheckPointManager() {
    }

    @Override
    public void init() {

    }

    public void savePoint(long point) {

    }

    public static TSDBCheckPointManager getInstance() {
        if (INSTANCE == null) {
            synchronized (TSDBCheckPointManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new TSDBCheckPointManager();
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }
}
