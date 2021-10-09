package cn.rapidtsdb.tsdb.plugins;

import java.util.Map;

public interface ConnectionAuthPlugin {

    boolean hasReadPermission(Map<String, String> authParams);

    boolean hasWritePermission(Map<String, String> authParams);

    boolean hasAdminPermission(Map<String, String> authParams);

}
