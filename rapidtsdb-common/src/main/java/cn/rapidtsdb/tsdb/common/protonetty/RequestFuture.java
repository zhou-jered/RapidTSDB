package cn.rapidtsdb.tsdb.common.protonetty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public class RequestFuture<T> implements Future {
    private int reqId;
    private AtomicReference<T> result = new AtomicReference<>(null);
    private List<RequestFutureListener> listeners;

    private WaitNode waitNodes;

    public RequestFuture(int reqId) {
        this.reqId = reqId;
    }

    public void setResult(T result) {
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

        WaitNode q = waitNodes;
        while (q != null) {
            LockSupport.unpark(q.thread);
            q = q.next;
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
        T t = get();
        if (t == null) {
            WaitNode newNode = new WaitNode();
            newNode.next = waitNodes;
            waitNodes = newNode;
            LockSupport.park();
            return get();
        } else {
            return t;
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return get() != null;
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
    }

    final static class WaitNode {
        Thread thread;
        WaitNode next;

        public WaitNode() {
            thread = Thread.currentThread();
        }
    }
}
