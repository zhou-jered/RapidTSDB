package cn.rapidtsdb.tsdb.client.handler.v1;

import cn.rapidtsdb.tsdb.client.exceptions.RequestException;
import lombok.Getter;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;


public class MsgExchange<R, T> {
    @Getter
    private int exchangeId;
    private R request;
    private T response;
    @Getter
    private Throwable executionException;
    private AtomicReference<WaitNode> waitNodes = new AtomicReference<>();
    private AtomicInteger state = new AtomicInteger(0);
    private static final int EXCHANGING = 0;
    private static final int COMPLETED = 2;
    private static final int EXCEPTIONAL = 3;
    private static final int CANCELLED = 4;


    public MsgExchange(int exchangeId, R request) {
        this.exchangeId = exchangeId;
        this.request = request;
    }

    public R getRequest() {
        return request;
    }

    public void setResult(T response) {
        if (state.compareAndSet(EXCHANGING, COMPLETED)) {
            this.response = response;
            finishedCompletion();
        }
    }

    public void setException(Throwable throwable) {
        if (state.compareAndSet(EXCHANGING, EXCEPTIONAL)) {
            this.executionException = throwable;
            finishedCompletion();
        }
    }

    public boolean cancel() {
        if (state.get() < COMPLETED) {
            return false;
        }
        boolean cancelSuccess = state.compareAndSet(EXCHANGING, CANCELLED);
        if (cancelSuccess) {
            finishedCompletion();
            return true;
        }
        return cancelSuccess;
    }

    public T get() {
        if (state.get() == EXCHANGING) {
            awaitDone(false, 0L);
        }
        return report();
    }

    public T get(long timeout, TimeUnit unit) throws TimeoutException {
        if (unit == null) {
            throw new NullPointerException();
        }
        if (state.get() == EXCHANGING) {
            awaitDone(true, unit.toNanos(timeout));
            if (state.get() == EXCHANGING) {
                throw new TimeoutException();
            }
        }
        return report();
    }


    private T report() {
        if (state.get() == COMPLETED) {
            return response;
        } else if (state.get() == CANCELLED) {
            throw new CancellationException();
        } else if (state.get() == EXCEPTIONAL) {
            throw new RequestException(executionException);
        } else {
            throw new RuntimeException("should not be here.");
        }
    }

    private WaitNode awaitDone(boolean timed, long waitNanos) {
        WaitNode newNode = new WaitNode();
        for (; ; ) {
            WaitNode q = waitNodes.get();
            newNode.next = q;
            if (waitNodes.compareAndSet(q, newNode)) {
                if (timed) {
                    LockSupport.parkNanos(waitNanos);
                } else {
                    LockSupport.park(this);
                }
                break;
            }
        }
        return newNode;
    }

    private void finishedCompletion() {
        WaitNode q;
        for (; ; ) {
            q = waitNodes.get();
            if (waitNodes.compareAndSet(q, null)) {
                break;
            }
        }
        while (q != null) {
            WaitNode next = q.next;
            LockSupport.unpark(q.thread);
            q.next = null;
            q = next;
        }
    }


    final static class WaitNode {
        volatile Thread thread;
        volatile WaitNode next;

        WaitNode() {
            thread = Thread.currentThread();
        }
    }

}
