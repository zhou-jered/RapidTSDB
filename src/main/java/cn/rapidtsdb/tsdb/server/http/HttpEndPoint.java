package cn.rapidtsdb.tsdb.server.http;

public interface HttpEndPoint {

    String[] getSupportedMediaType();

    String[] getSupportedHttpMethod();

    String getUrl();



}
