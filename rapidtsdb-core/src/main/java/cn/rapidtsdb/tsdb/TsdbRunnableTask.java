package cn.rapidtsdb.tsdb;

public abstract class TsdbRunnableTask implements Runnable {
    private int retryCount = 0;

    public void markRetry() {
        retryCount++;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public abstract int getRetryLimit();

    public abstract String getTaskName();
}
