package cn.rapidtsdb.tsdb;

public abstract class TSDBDataOperationTask extends TsdbRunnableTask {
    protected TSDBTaskCallback callback;

    public abstract int getMetricId();

    public TSDBDataOperationTask(TSDBTaskCallback callback) {
        this.callback = callback;
    }


}
