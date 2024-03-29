package cn.rapidtsdb.tsdb;

public abstract class TSDBDataOperationTask extends TSDBRetryableTask {
    protected TSDBTaskCallback callback;

    public abstract int getMetricId();

    public TSDBDataOperationTask(TSDBTaskCallback callback) {
        this.callback = callback;
    }


}
