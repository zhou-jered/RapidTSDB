package cn.rapidtsdb.tsdb.server.context;

import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.meta.MetricTransformer;

public class ExecuteContext {
    private static final ExecuteContext CONTEXT = new ExecuteContext();
    private TSDB database;

    private MetricTransformer metricTransformer;

}
