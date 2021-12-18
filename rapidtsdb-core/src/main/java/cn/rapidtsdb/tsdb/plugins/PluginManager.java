package cn.rapidtsdb.tsdb.plugins;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class PluginManager {

    private static Class[] plugins = new Class[]{ConnectionAuthPlugin.class, StoreHandlerPlugin.class,
            ConfigProcessPlugin.class};
    private static Map<String, List> pluginRegistry = new ConcurrentHashMap<>();
    private static AtomicBoolean inited = new AtomicBoolean(false);

    static {
        loadPlugins();
    }

    public static synchronized void loadPlugins() {
        if (inited.compareAndSet(false, true)) {
            pluginRegistry.clear();
            for (Class clz : plugins) {
                ServiceLoader serviceLoader = ServiceLoader.load(clz);
                final String pluginName = clz.getCanonicalName();
                pluginRegistry.put(pluginName, new ArrayList());
                Iterator pluginIterator = serviceLoader.iterator();
                while (pluginIterator.hasNext()) {
                    pluginRegistry.get(pluginName).add(pluginIterator.next());
                }
            }
        }
    }

    public static <T> T getPlugin(Class<T> pluginClazz) {
        final String pname = pluginClazz.getCanonicalName();
        List<T> result = pluginRegistry.get(pname);
        if (result != null && result.size() > 0) {
            return result.get(0);
        }
        return null;
    }

    public static <T> List<T> getPlugins(Class<T> pluginClazz) {
        final String pname = pluginClazz.getCanonicalName();
        List<T> result = pluginRegistry.get(pname);
        return result;
    }

}

