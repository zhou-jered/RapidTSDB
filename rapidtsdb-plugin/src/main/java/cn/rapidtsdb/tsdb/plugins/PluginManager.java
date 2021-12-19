package cn.rapidtsdb.tsdb.plugins;

import cn.rapidtsdb.tsdb.plugins.func.ConfigurablePlugin;
import cn.rapidtsdb.tsdb.plugins.func.PreparablePlugin;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public class PluginManager {

    private static Class[] plugins = new Class[]{ConnectionAuthPlugin.class, FileStoreHandlerPlugin.class,
            BlockStoreHandlerPlugin.class};
    private static Map<String, List> pluginRegistry = new ConcurrentHashMap<>();
    static final int CREATE = 0;
    static final int REGISTED = 1;
    static final int CONFIGED = 3;
    static final int PREPARED = 4;
    private static AtomicInteger state = new AtomicInteger(CREATE);

    static {
        loadPlugins();
    }

    public static synchronized void loadPlugins() {
        if (state.compareAndSet(CREATE, REGISTED)) {
            pluginRegistry.clear();
            for (Class clz : plugins) {
                ServiceLoader serviceLoader = ServiceLoader.load(clz);
                final String pluginName = clz.getCanonicalName();
                pluginRegistry.put(pluginName, new ArrayList());
                Iterator pluginIterator = serviceLoader.iterator();
                while (pluginIterator.hasNext()) {
                    Object plugin = pluginIterator.next();
                    pluginRegistry.get(pluginName).add(plugin);
                    log.info("add plugin: {} : {}", clz.getSimpleName(), plugin.getClass().getName());
                }
            }
        }
    }

    public static synchronized void configPlugins(Map<String, String> globalConfig) {
        if (state.compareAndSet(REGISTED, CONFIGED)) {
            Iterator<List> valIter = pluginRegistry.values().iterator();
            while (valIter.hasNext()) {
                List pluginList = valIter.next();
                for (Object plugin : pluginList) {
                    if (plugin instanceof ConfigurablePlugin) {
                        ConfigurablePlugin configurablePlugin = (ConfigurablePlugin) plugin;
                        String prefix = configurablePlugin.getInterestedPrefix();
                        if (StringUtils.isNotBlank(prefix)) {
                            Map<String, String> pluginInterestedConfig = filterByPrefix(globalConfig, prefix);
                            configurablePlugin.config(pluginInterestedConfig);
                        }
                    }
                }
            }
        } else {
            throw new RuntimeException("Plugin not init");
        }
    }

    public static synchronized void preparePlugin() {
        if (state.compareAndSet(CONFIGED, PREPARED)) {
            pluginRegistry.values().forEach(list -> list.forEach(p -> {
                if (p instanceof PreparablePlugin) {
                    ((PreparablePlugin) p).prepare();
                }
            }));
        }
    }

    public static <T> T getPlugin(Class<T> pluginClazz) {
        if (state.get() != PREPARED) {
            throw new RuntimeException("plugin not prepared");
        }
        final String pname = pluginClazz.getCanonicalName();
        List<T> result = pluginRegistry.get(pname);
        if (result != null && result.size() > 0) {
            return result.get(0);
        }
        return null;
    }

    public static <T> List<T> getPlugins(Class<T> pluginClazz) {
        if (state.get() != PREPARED) {
            throw new RuntimeException("plugin not prepared");
        }
        final String pname = pluginClazz.getCanonicalName();
        List<T> result = pluginRegistry.get(pname);
        return result;
    }

    private static Map<String, String> filterByPrefix(Map<String, String> parentMap, String prefix) {
        Map<String, String> subMap = new HashMap<>();
        for (String k : parentMap.keySet()) {
            if (k.startsWith(prefix)) {
                subMap.put(k, parentMap.get(k));
            }
        }
        return subMap;
    }

}

