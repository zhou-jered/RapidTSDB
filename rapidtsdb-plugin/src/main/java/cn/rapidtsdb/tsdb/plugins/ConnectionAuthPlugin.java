package cn.rapidtsdb.tsdb.plugins;

import java.util.Map;
import java.util.Set;

public interface ConnectionAuthPlugin {

    boolean hasReadPermission(Map<String, String> authParams);

    boolean hasWritePermission(Map<String, String> authParams);

    boolean hasAdminPermission(Map<String, String> authParams);

    Set<Permissions> getPermissions(Map<String, String> authParams);
}
