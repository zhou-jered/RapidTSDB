package cn.rapidtsdb.tsdb.plugins;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Handle the KV style persistence,
 * provider more flexible and sufficient data organization ability.
 */
public interface KVStoreHandlerPlugin {

    /**
     * return the schema name
     * @return
     */
    String schema();

    /**
     * return the decription of the implementation
     * @return
     */
    String describeImpl();

    int getMaxKeyBytesLength();

    int getMaxValuesBytesLength();

    boolean set(byte[] key, byte[] val);

    byte[] setOnAbsent(byte[] key, byte[] val);

    byte[] setOnPresent(byte[] key, byte[] val);

    boolean appendValue(byte[] key, byte[] suffixVal);

    byte[] get(byte[] key);

    byte[] delete(byte[] key);

    Map<byte[], byte[]> batchGet(Collection<byte[]> keys);

    Stream<byte[]> scan(byte[] startInclusiveKey, byte[] endExclusiveKey);

}
