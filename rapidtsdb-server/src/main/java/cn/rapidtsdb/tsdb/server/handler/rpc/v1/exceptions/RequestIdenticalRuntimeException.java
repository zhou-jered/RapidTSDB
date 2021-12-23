package cn.rapidtsdb.tsdb.server.handler.rpc.v1.exceptions;

import lombok.Data;

@Data
public class RequestIdenticalRuntimeException extends RuntimeException {
    private int reqId;

    public RequestIdenticalRuntimeException(String message, int reqId) {
        super(message);
        this.reqId = reqId;
    }
}
