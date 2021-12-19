package cn.rapidtsdb.tsdb.plugins;

import cn.rapidtsdb.tsdb.plugins.func.ConfigurablePlugin;
import cn.rapidtsdb.tsdb.plugins.func.NameablePlugin;
import cn.rapidtsdb.tsdb.plugins.func.PreparablePlugin;

import java.util.Map;

public interface ConnectionAuthPlugin extends NameablePlugin, PreparablePlugin, ConfigurablePlugin {

    boolean hasReadPermission(String authType, int version, Map<String, String> authParams);

    boolean hasWritePermission(String authType, int version, Map<String, String> authParams);

    boolean hasAdminPermission(String authType, int version, Map<String, String> authParams);

    int getPermissions(String authType, int version, Map<String, String> authParams);
}
