package cn.rapidtsdb.tsdb.client.handler.v1;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;


public class MsgExchange<R, T> {
    @Getter
    private int exchangeId;
    @Getter
    private R request;
    @Getter
    private T response;
    @Setter
    @Getter
    private Exception executionException;
    @Getter
    private boolean success;
    @Getter
    private boolean cancelled;
    private AtomicReference<WaitNode> waitNodes;


    public MsgExchange(int exchangeId, R request) {
        this.exchangeId = exchangeId;
        this.request = request;
    }

    public void setResult(T response) {
        this.response = response;

    }

    public void cancel() {
        cancelled = true;
    }

    public T block() throws InterruptedException {
        LockSupport.park();
        LockSupport.unpark(Thread.currentThread());
        return null;
    }

    private void finishedCompletion() {
        if (waitNodes.get() != null) {

        }
    }

    private void addWaitNode() {
        WaitNode newNode = new WaitNode();
        while (true) {
            WaitNode originNode = waitNodes.get();
            newNode.setNext(originNode);
            if (waitNodes.compareAndSet(originNode, newNode)) {
                break;
            }
        }
    }

    private void removeWaiters() {

    }

    final static class WaitNode {
        volatile Thread thread;
        volatile AtomicReference<WaitNode> next = new AtomicReference<>();

        WaitNode() {
            thread = Thread.currentThread();
        }

        void setNext(WaitNode nextNode) {
            next.set(nextNode);
        }
    }

}
