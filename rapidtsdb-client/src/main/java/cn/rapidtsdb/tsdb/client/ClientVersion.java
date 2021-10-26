package cn.rapidtsdb.tsdb.client;

public class ClientVersion {
    public static final int MIN_VERSION = 1;
    public static final int MAX_VERSION = 1;

    public static boolean versionSupport(int version) {
        return MIN_VERSION <= version && version <= MAX_VERSION;
    }


}
