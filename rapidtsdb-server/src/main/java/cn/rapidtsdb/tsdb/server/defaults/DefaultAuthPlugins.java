package cn.rapidtsdb.tsdb.server.defaults;

import cn.rapidtsdb.tsdb.plugins.ConnectionAuthPlugin;
import cn.rapidtsdb.tsdb.plugins.Permissions;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DefaultAuthPlugins implements ConnectionAuthPlugin {
    @Override
    public boolean hasReadPermission(String authType, int version, Map<String, String> authParams) {
        return true;
    }

    @Override
    public boolean hasWritePermission(String authType, int version, Map<String, String> authParams) {
        return true;
    }

    @Override
    public boolean hasAdminPermission(String authType, int version, Map<String, String> authParams) {
        return true;
    }

    @Override
    public Set<Permissions> getPermissions(String authType, int version, Map<String, String> authParams) {
        Set<Permissions> pers = new HashSet<>();
        pers.add(Permissions.READ);
        pers.add(Permissions.WRITE);
        return pers;
    }
}
