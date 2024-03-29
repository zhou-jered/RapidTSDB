package cn.rapidtsdb.tsdb.metrics;

import lombok.extern.log4j.Log4j2;

@Log4j2
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
        String prefix = "DB_INTERNAL_";
        try {
            //todo
        } catch (Exception e) {
            log.error(e);
        }
    }

    public void event(String eventName) {

    }
}
