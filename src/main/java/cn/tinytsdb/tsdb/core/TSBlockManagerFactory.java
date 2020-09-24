package cn.tinytsdb.tsdb.core;

import cn.tinytsdb.tsdb.config.TSDBConfig;

public class TSBlockManagerFactory {

    private static AbstractTSBlockManager INSTANCE;

    public static AbstractTSBlockManager getBlockManager() {
        TSDBConfig config = TSDBConfig.getConfigInstance();
        if(INSTANCE == null) {
            synchronized (config) {
                if(INSTANCE == null) {
                    INSTANCE = new TSBlockManager(config);
                }
            }
        }
        return INSTANCE;
    }

}
