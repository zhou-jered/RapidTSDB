package cn.rapidtsdb.tsdb.plugins;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PluginManager {

    private static Class[] plugins = new Class[]{ConnectionAuthPlugin.class, StoreHandlerPlugin.class};
    private static Map<String, List> pluginRegistry = new ConcurrentHashMap<>();

    static {
        loadPlugins();
    }

    private static synchronized void loadPlugins() {
        pluginRegistry.clear();
        for (Class clz : plugins) {
            ServiceLoader serviceLoader = ServiceLoader.load(ConnectionAuthPlugin.class);
            final String pluginName = clz.getCanonicalName();
            pluginRegistry.put(pluginName, new ArrayList());
            Iterator<ConnectionAuthPlugin> authPluginIterator = serviceLoader.iterator();
            while (authPluginIterator.hasNext()) {
                pluginRegistry.get(pluginName).add(authPluginIterator.next());
            }
        }
    }

    public static <T> List<T> getPlugin(Class<T> pluginClazz) {
        final String pname = pluginClazz.getCanonicalName();
        List<T> result = pluginRegistry.get(pname);
        return result;
    }

}

