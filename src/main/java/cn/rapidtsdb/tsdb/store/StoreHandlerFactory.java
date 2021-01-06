package cn.rapidtsdb.tsdb.store;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;

public class StoreHandlerFactory {

    private static TSDBConfig tsdbConfig;

    static {
        tsdbConfig = TSDBConfig.getConfigInstance();
    }

    public static StoreHandler getStoreHandler() {
        String storeImplClass = tsdbConfig.getStoreHandlerImplClass();
        if (StringUtils.isNotBlank(storeImplClass)) {
            try {

                Class clazz = Class.forName(storeImplClass);
                if (ClassUtils.isAssignable(clazz, StoreHandler.class)) {
                    return (StoreHandler) clazz.newInstance();
                }
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return new FileStoreHandler(tsdbConfig);
    }


    private static String handleImplClass = null;

    public static void setHandleImplClass(String implClass) {
        try {
            Class clazz = Class.forName(implClass);
            if (!ClassUtils.isAssignable(clazz, StoreHandler.class)) {
                throw new Exception(implClass + " is not a StoreHandler Class");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        handleImplClass = implClass;
    }

}
