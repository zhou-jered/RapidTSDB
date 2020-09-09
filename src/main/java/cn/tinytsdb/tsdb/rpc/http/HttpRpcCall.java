package cn.tinytsdb.tsdb.rpc.http;

import lombok.Data;

import java.util.Map;

@Data
public class HttpRpcCall {
    protected String headerLine;
    protected Map<String, String> headers;
    protected Object body;
}
