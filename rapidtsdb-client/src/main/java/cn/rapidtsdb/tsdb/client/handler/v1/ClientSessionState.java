package cn.rapidtsdb.tsdb.client.handler.v1;

public enum ClientSessionState {
    INIT,
    PENDING_AUTH,
    ACTIVE,
    SHUTDOWN, // wait close
    CLOSED
}
