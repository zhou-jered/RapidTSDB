package cn.rapidtsdb.tsdb.utils;

import java.util.Collection;
import java.util.Map;

public class CollectionUtils {
    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.size() == 0;
    }

    public static boolean isEmpty(Map map) {
        return map == null || map.size() == 0;
    }

    public static boolean isNotEmpty(Collection collection) {
        return !isEmpty(collection);
    }

    public static boolean isNotEmpty(Map map) {
        return !isEmpty(map);
    }
}
