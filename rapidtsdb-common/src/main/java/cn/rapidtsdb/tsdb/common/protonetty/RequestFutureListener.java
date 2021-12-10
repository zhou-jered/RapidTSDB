package cn.rapidtsdb.tsdb.common.protonetty;

public interface RequestFutureListener<T> {
    public void onResult(T result);
}
