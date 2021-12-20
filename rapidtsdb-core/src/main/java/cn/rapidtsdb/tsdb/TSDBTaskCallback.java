package cn.rapidtsdb.tsdb;

public interface TSDBTaskCallback<T, R> {

    R onSuccess(T data);

    default void onFailed(TSDBRetryableTask task, T data) {
        task.markRetry();
    }


    default void onException(TSDBRetryableTask task, T data, Throwable exception) {

    }

}
