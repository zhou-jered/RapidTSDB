package cn.rapidtsdb.tsdb.server.defaults;

import cn.rapidtsdb.tsdb.plugins.ConnectionAuthPlugin;
import cn.rapidtsdb.tsdb.plugins.Permissions;

import java.util.Map;
import java.util.Set;

public class DefaultAuthPlugins implements ConnectionAuthPlugin {
    @Override
    public boolean hasReadPermission(String authType, int version, Map<String, String> authParams) {
        return false;
    }

    @Override
    public boolean hasWritePermission(String authType, int version, Map<String, String> authParams) {
        return false;
    }

    @Override
    public boolean hasAdminPermission(String authType, int version, Map<String, String> authParams) {
        return false;
    }

    @Override
    public Set<Permissions> getPermissions(String authType, int version, Map<String, String> authParams) {
        return null;
    }
}
