package cn.rapidtsdb.tsdb;

public abstract class TSDBDataOperationTask extends TsdbRunnableTask {
    public abstract int getMetricId();
}
