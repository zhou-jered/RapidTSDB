package cn.rapidtsdb.tsdb.server.http;


public class SemesticHttpRpcResult extends HttpRpcResult {

    String getContentType() {
        return headers.get("content-type");
    }


}
