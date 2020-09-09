package cn.tinytsdb.tsdb.rpc.http;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HttpRpcResult {
    protected String headerLine;
    protected Map<String, String> headers;
    protected Object body;
}
