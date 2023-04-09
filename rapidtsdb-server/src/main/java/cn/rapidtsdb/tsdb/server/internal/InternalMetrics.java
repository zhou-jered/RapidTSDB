package cn.rapidtsdb.tsdb.server.internal;

import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;

public class InternalMetrics implements Initializer {

    public static final InternalMetrics INTERNAL_METRICS = new InternalMetrics();

    private TSDB tsdb;

    @Override
    public void init() {
    }

    public static InternalMetrics getInternalMetrics() {
        return INTERNAL_METRICS;
    }

    public void report(long time, String metric, double val) {
        String m = "tsdb.internal." + metric;
        tsdb.writeMetric(m, val, time);
    }
}
