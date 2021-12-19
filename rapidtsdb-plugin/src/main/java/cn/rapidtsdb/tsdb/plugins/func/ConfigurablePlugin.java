package cn.rapidtsdb.tsdb.plugins.func;

import java.util.Map;

public interface ConfigurablePlugin {
    String getInterestedPrefix();

    /**
     * the global config will send config items with prefix of getInterestedPrefix()
     */
    void config(Map<String, String> subConfig);
}
