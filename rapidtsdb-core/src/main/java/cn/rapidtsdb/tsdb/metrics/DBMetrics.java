package cn.rapidtsdb.tsdb.metrics;

public class DBMetrics {
    private DBMetrics() {

    }

    private static DBMetrics INSTANCE = null;

    public static DBMetrics getInstance() {
        if (INSTANCE == null) {
            synchronized (DBMetrics.class) {
                if (INSTANCE == null) {
                    INSTANCE = new DBMetrics();
                }
            }
        }
        return INSTANCE;
    }

    public void reportMetrics(String metricsName, double val) {

    }

    public void event(String eventName) {

    }
}
