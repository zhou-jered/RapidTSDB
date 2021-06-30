package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.config.TSDBConfig;

public class TSBlockManagerProvider {

    private static AbstractTSBlockManager INSTANCE;

    public static AbstractTSBlockManager getBlockManagerInstance() {
        TSDBConfig config = TSDBConfig.getConfigInstance();
        if (INSTANCE == null) {
            synchronized (config) {
                if (INSTANCE == null) {
                    INSTANCE = new TSBlockManager(config);
                }
            }
        }
        return INSTANCE;
    }

}
