package cn.rapidtsdb.tsdb.plugins;

import java.util.Map;
import java.util.Set;

public interface ConnectionAuthPlugin {

    boolean hasReadPermission(String authType, int version, Map<String, String> authParams);

    boolean hasWritePermission(String authType, int version, Map<String, String> authParams);

    boolean hasAdminPermission(String authType, int version, Map<String, String> authParams);

    Set<Permissions> getPermissions(String authType, int version, Map<String, String> authParams);
}
