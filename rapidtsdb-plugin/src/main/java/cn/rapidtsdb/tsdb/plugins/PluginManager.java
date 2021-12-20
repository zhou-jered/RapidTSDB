package cn.rapidtsdb.tsdb.plugins;

import cn.rapidtsdb.tsdb.plugins.func.ConfigurablePlugin;
import cn.rapidtsdb.tsdb.plugins.func.NameablePlugin;
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
    private static Map<Class, String> pluginSpecifierMap;
    private static Map<String, List> pluginRegistry = new HashMap<>();
    private static Map<String, String> pluginSpecNameMap = new HashMap<>();
    static final int CREATE = 0;
    static final int REGISTED = 1;
    static final int CONFIGED = 3;
    static final int PREPARED = 4;
    private static AtomicInteger state = new AtomicInteger(CREATE);

    static {
        pluginSpecifierMap = new ConcurrentHashMap<>();
        pluginSpecifierMap.put(ConnectionAuthPlugin.class, "auth.plugin");
        pluginSpecifierMap.put(FileStoreHandlerPlugin.class, "fileStore.plugin");
        pluginSpecifierMap.put(BlockStoreHandlerPlugin.class, "blockStore.plugin");
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
            rememberPluginSpecConfig(globalConfig);
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
            for (Class pluginClz : plugins) {
                String rk = pluginClz.getCanonicalName();
                String specName = pluginSpecNameMap.get(rk);
                List pluginObjList = pluginRegistry.get(rk);
                if (StringUtils.isNotBlank(specName)) {
                    Object namedPlugin = null;
                    for (Object pl : pluginObjList) {
                        if (pl instanceof NameablePlugin) {
                            if (((NameablePlugin) pl).getName().equals(specName)) {
                                namedPlugin = pl;
                                break;
                            }
                        }
                    }
                    if (namedPlugin == null) {
                        String msg = String.format("Can not find Plugin:%s of name:%s", rk, specName);
                        throw new RuntimeException(msg);
                    } else if (namedPlugin instanceof PreparablePlugin) {
                        ((PreparablePlugin) namedPlugin).prepare();
                    }
                } else {
                    pluginObjList.forEach(p -> {
                        if (p instanceof PreparablePlugin) {
                            ((PreparablePlugin) p).prepare();
                        }
                    });
                }
            }

        }
    }

    public static <T> T getPlugin(Class<T> pluginClazz) {
        if (state.get() != PREPARED) {
            throw new RuntimeException("plugin not prepared");
        }
        final String pname = pluginClazz.getCanonicalName();
        List<T> candidatePlugins = pluginRegistry.get(pname);
        String pluginSpecName = pluginSpecNameMap.get(pluginClazz.getCanonicalName());
        if (StringUtils.isNotBlank(pluginSpecName)) {
            for (Object plugin : candidatePlugins) {
                if (plugin instanceof NameablePlugin && ((NameablePlugin) plugin).getName().equals(pluginSpecName)) {
                    return (T) plugin;
                }
            }
        } else if (candidatePlugins != null && candidatePlugins.size() > 0) {
            return candidatePlugins.get(0);
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

    private static void rememberPluginSpecConfig(Map<String, String> globalConfig) {
        for (Class pluginClz : plugins) {
            String specK = pluginSpecifierMap.get(pluginClz);
            if (globalConfig.containsKey(specK)) {
                pluginSpecNameMap.put(pluginClz.getCanonicalName(), globalConfig.get(specK));
            }
        }
    }

}

