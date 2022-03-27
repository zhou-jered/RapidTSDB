package cn.rapidtsdb.tsdb.core.persistent;

public class MetricsKeyManagerFactory {

    private static IMetricsKeyManager metricsKeyManager = null;

    public static IMetricsKeyManager getInstance() {
        if (metricsKeyManager != null) {
            return metricsKeyManager;
        } else {
            synchronized (MetricsKeyManagerFactory.class) {
                if (metricsKeyManager == null) {
                    metricsKeyManager = new MetricsKeyFileManager();
                }
            }
            return metricsKeyManager;
        }
    }


}
