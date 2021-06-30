package cn.rapidtsdb.tsdb;

public interface TSDBTaskCallback<T, R> {

    R onSuccess(T data);

    default void onFailed(TsdbRunnableTask task, T data) {
        task.markRetry();
    }


    default void onException(TsdbRunnableTask task, T data, Throwable exception) {

    }

}
