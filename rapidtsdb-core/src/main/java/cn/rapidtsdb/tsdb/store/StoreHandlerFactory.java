package cn.rapidtsdb.tsdb.store;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.plugins.PluginManager;
import cn.rapidtsdb.tsdb.plugins.StoreHandlerPlugin;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

@Log4j2
public class StoreHandlerFactory {

    private static TSDBConfig tsdbConfig;
    private static final String STORE_SCHEMA_FILE = "file";
    private static final String STORE_SCHEMA_HBASE = "hbase";
    private static final String STORE_SCHEMA_HDFS = "hdfs";
    private static final String STORE_SCHEMA_CUSTOM = "custom";
    private static StoreHandlerPlugin storeHandlerInstance = null;

    static {
        tsdbConfig = TSDBConfig.getConfigInstance();
        tryUsingPlugin();
        if (storeHandlerInstance == null) {
            configStoreHandler();
        }
        if (storeHandlerInstance == null) {
            storeHandlerInstance = new FileStoreHandler(tsdbConfig);
        }
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
                    Class clazz = getClassByNameSilently(storeImplClass);
                    if (ClassUtils.isAssignable(clazz, StoreHandlerPlugin.class)) {
                        try {
                            storeHandlerInstance = (StoreHandlerPlugin) clazz.getConstructor(null).newInstance();
                        } catch (InstantiationException e) {
                            e.printStackTrace();
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        } catch (NoSuchMethodException e) {
                            e.printStackTrace();
                        }
                    }
                }
                break;
            default:
                throw new RuntimeException("No StoreHandler Find By Config");

        }
    }

    private static void tryUsingPlugin() {
        final String configSchema = tsdbConfig.getStoreScheme();
        String storeImplClass = tsdbConfig.getStoreHandlerImplClass();
        List<StoreHandlerPlugin> plugins = PluginManager.getPlugin(StoreHandlerPlugin.class);
        StoreHandlerPlugin schemeMatchedPlugin = null;
        if (plugins != null && plugins.size() > 0) {
            for (StoreHandlerPlugin storePlugin : plugins) {
                if (configSchema != null && configSchema.trim().length() > 0) {
                    if (configSchema.trim().equals(storePlugin.getScheme())) {
                        schemeMatchedPlugin = storePlugin;
                        Class configImplClass = getClassByNameSilently(storeImplClass);
                        if (configImplClass.isAssignableFrom(storePlugin.getClass())) {
                            log.info("Using Plugin StoreHandler");
                            storeHandlerInstance = schemeMatchedPlugin;
                        }
                    }
                }
            }
        }
        if (storeHandlerInstance == null && schemeMatchedPlugin != null) {
            storeHandlerInstance = schemeMatchedPlugin;
        }
    }

    private static Class getClassByNameSilently(String className) {
        try {
            Class clazz = Class.forName(className);
            return clazz;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

}
