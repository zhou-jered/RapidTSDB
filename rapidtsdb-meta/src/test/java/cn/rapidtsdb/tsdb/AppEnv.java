package cn.rapidtsdb.tsdb;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.plugins.PluginManager;

import java.util.HashMap;
import java.util.Map;

public class AppEnv {

    static boolean prepared = false;

    public static void prepareOnce(Map<String, String> config, boolean foreceRePrepare) {
        if (prepared && !foreceRePrepare) {
            return;
        }
        prepared = true;
        Map<String, String> globalconfig = getDefaultConfig();
        if (config != null) {
            globalconfig.putAll(config);
        }
        TSDBConfig.init(globalconfig);
        PluginManager.loadPlugins();
        PluginManager.configPlugins(globalconfig);
        PluginManager.preparePlugin();
    }

    static Map<String, String> getDefaultConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("dataPath", "/tmp/data/tsdb-test");
        config.put("print.banner", "yes");
        return config;
    }

}
