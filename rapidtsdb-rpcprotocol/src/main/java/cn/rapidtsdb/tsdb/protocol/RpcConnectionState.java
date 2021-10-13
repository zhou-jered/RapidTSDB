package cn.rapidtsdb.tsdb.protocol;

public enum RpcConnectionState {
    INIT, // negotiate protocol version
    WAITING_AUTH, //
    READ_ONLY,
    WRITE_ONLY,
    READ_WRITE,
    IMPORT_DATA,
    ADMIN,
    UNAVAILABLE
}
