package cn.rapidtsdb.tsdb.server.defaults;

import cn.rapidtsdb.tsdb.plugins.ConnectionAuthPlugin;
import cn.rapidtsdb.tsdb.protocol.OperationPermissionMasks;

import java.util.Map;

/**
 * read credentials from config files
 */
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
    public int getPermissions(String authType, int version, Map<String, String> authParams) {
        return OperationPermissionMasks.RW_PERMISSION;
    }
}
