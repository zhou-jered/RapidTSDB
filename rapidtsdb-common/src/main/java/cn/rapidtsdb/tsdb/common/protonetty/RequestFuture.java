package cn.rapidtsdb.tsdb.common.protonetty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class RequestFuture<T> {
    private int reqId;
    private AtomicReference<T> result = new AtomicReference<>(null);
    private Object lock = new Object();
    private List<RequestFutureListener> listeners;

    public RequestFuture(int reqId) {
        this.reqId = reqId;
    }

    public void setResult(T result) {
        synchronized (lock) {
            T optionalOrigin = this.result.get();
            if (optionalOrigin != null) {
                throw new RuntimeException("Result already set, the origin value is " + optionalOrigin);
            }
            this.result.set(result);
            if (listeners != null) {
                listeners.forEach(l -> {
                    l.onResult(result);
                });
            }
        }
    }

    public T get() {
        return result.get();
    }

    public synchronized void addListener(RequestFutureListener<T> listener) {
        if (listeners == null) {
            listeners = new ArrayList<>();
        }
        listeners.add(listener);
    }

    public T block() {
        return null;
    }
}
