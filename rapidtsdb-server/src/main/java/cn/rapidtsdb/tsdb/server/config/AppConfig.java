package cn.rapidtsdb.tsdb.server.config;

public class AppConfig {


    private final static int currentVersion = 1;
    private final static int minimumVersion=1;
    private static boolean debug = true;


    public static boolean isDebug() {
        return debug;
    }

    public static void setDebug(boolean debug) {
        AppConfig.debug = debug;
    }


    public static int getCurrentVersion() {
        return currentVersion;
    }

    public static int getMinimumVersion() {
        return minimumVersion;
    }
}
