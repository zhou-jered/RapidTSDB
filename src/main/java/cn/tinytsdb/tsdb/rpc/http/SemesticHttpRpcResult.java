package cn.tinytsdb.tsdb.rpc.http;


public class SemesticHttpRpcResult extends HttpRpcResult {

    String getContentType() {
        return headers.get("content-type");
    }


}
