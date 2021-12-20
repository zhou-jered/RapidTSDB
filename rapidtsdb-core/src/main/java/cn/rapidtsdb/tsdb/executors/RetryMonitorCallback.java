package cn.rapidtsdb.tsdb.executors;

import cn.rapidtsdb.tsdb.TSDBRetryableTask;
import cn.rapidtsdb.tsdb.TSDBTaskCallback;

import java.util.concurrent.atomic.AtomicInteger;

public class RetryMonitorCallback implements TSDBTaskCallback{
    TSDBTaskCallback allCompleteCallback;
    private int subTaskNumber = 0;
    private AtomicInteger successTaskNumber = new AtomicInteger(0);

    public RetryMonitorCallback(TSDBTaskCallback allCompleteCallback, int subTaskNumber) {
        this.allCompleteCallback = allCompleteCallback;
        this.subTaskNumber = subTaskNumber;
    }

    @Override
    public Object onSuccess(Object data) {
        final int finishedTaskNumber = successTaskNumber.incrementAndGet();
        if (finishedTaskNumber == subTaskNumber) {
            allCompleteCallback.onSuccess(subTaskNumber);
        }
        return null;
    }


    @Override
    public void onFailed(TSDBRetryableTask task, Object data) {
        if (task.getRetryCount() < task.getRetryLimit()) {
            task.markRetry();
            ManagedThreadPool.getInstance().submitFailedTask(task);
        } else {
            allCompleteCallback.onFailed(task, data);
        }
    }

    @Override
    public void onException(TSDBRetryableTask task, Object data, Throwable exception) {
        if (task.getRetryCount() < task.getRetryLimit()) {
            task.markRetry();
            ManagedThreadPool.getInstance().submitFailedTask(task);
        } else {
            allCompleteCallback.onException(task, data, exception);
        }
    }
}
