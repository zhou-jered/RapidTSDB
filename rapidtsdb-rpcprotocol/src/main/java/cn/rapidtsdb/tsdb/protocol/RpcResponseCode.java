package cn.rapidtsdb.tsdb.protocol;

public class RpcResponseCode {

    public static final byte SUCCESS = 0x0;
    public static final byte PROTOCOL_ERROR = (byte) 0xff;
    public static final byte SERVER_INTERNAL_ERROR = (byte) 0xee;
    public static final byte CLIENT_VERSION_LOW = 0x10;
    public static final byte CLIENT_VERSION_HIGH = 0x20;
    public static final byte SERVER_REFUSED = 0x30;
    public static final byte UNSUPPORTED_DOWN_SAMPLER = 0x31;
    public static final byte UNSUPPORTED_AGGREGATOR = 0x32;


    public static final byte AUTH_FAILED = 0x40;

}
