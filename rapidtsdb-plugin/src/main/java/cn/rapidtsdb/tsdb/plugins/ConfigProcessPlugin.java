package cn.rapidtsdb.tsdb.plugins;

import java.util.Map;

public interface ConfigProcessPlugin {

    String getConfigPrefix();

    void process(Map<String, String> config);

}
