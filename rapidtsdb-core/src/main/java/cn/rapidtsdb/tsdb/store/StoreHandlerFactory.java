package cn.rapidtsdb.tsdb.store;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.plugins.StoreHandlerPlugin;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;

public class StoreHandlerFactory {

    private static TSDBConfig tsdbConfig;
    private static final String STORE_SCHEMA_FILE = "file";
    private static final String STORE_SCHEMA_HBASE = "hbase";
    private static final String STORE_SCHEMA_HDFS = "hdfs";
    private static final String STORE_SCHEMA_CUSTOM = "custom";
    private static StoreHandlerPlugin storeHandlerInstance = null;

    static {
        tsdbConfig = TSDBConfig.getConfigInstance();
        configStoreHandler();
    }

    public static StoreHandlerPlugin getStoreHandler() {
        return storeHandlerInstance;
    }


    private static void configStoreHandler() {
        final String configSchema = tsdbConfig.getStoreScheme();
        String storeImplClass = tsdbConfig.getStoreHandlerImplClass();
        switch (configSchema) {
            case STORE_SCHEMA_FILE:
                storeHandlerInstance = new FileStoreHandler(tsdbConfig);
                break;
            case STORE_SCHEMA_CUSTOM:
                if (StringUtils.isNotBlank(storeImplClass)) {
                    try {
                        Class clazz = Class.forName(storeImplClass);
                        if (ClassUtils.isAssignable(clazz, StoreHandlerPlugin.class)) {
                            storeHandlerInstance = (StoreHandlerPlugin) clazz.newInstance();
                        }
                    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
                break;
            default:
                throw new RuntimeException("UnSupported Store Schema " + configSchema);

        }

    }

}
