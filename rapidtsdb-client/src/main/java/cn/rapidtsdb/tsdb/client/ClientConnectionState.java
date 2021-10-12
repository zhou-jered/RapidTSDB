package cn.rapidtsdb.tsdb.client;

public enum ClientConnectionState {
    NONE,
    INIT,
    WAIT_AUTH,
    ACTIVE,
    CLOSED
}
