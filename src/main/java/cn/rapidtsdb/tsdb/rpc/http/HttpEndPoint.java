package cn.rapidtsdb.tsdb.rpc.http;

public interface HttpEndPoint {

    String[] getSupportedMediaType();

    String[] getSupportedHttpMethod();

    String getUrl();



}
