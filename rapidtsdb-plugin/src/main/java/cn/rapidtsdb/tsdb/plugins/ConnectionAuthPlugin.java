package cn.rapidtsdb.tsdb.plugins;

import java.util.Map;

public interface ConnectionAuthPlugin {

    boolean hasReadPermission(String authType, int version, Map<String, String> authParams);

    boolean hasWritePermission(String authType, int version, Map<String, String> authParams);

    boolean hasAdminPermission(String authType, int version, Map<String, String> authParams);

    int getPermissions(String authType, int version, Map<String, String> authParams);
}
