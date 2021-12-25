package cn.rapidtsdb.tsdb.lifecycle;

public class LifeCycleState {
    public static final int CREATE = 0;
    public static final int INITIALIZING = 1;
    public static final int ACTIVE = 2;
    public static final int CLOSING = 3;
    public static final int CLOSED = 4;
}
