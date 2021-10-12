package cn.rapidtsdb.tsdb.client;

public class TSDBClientFactory {
    public static TSDBClient getTSDBClient(TSDBClientConfig clientConfig) {
        DefaultTSDBClient defaultTSDBClient = new DefaultTSDBClient(clientConfig);
        defaultTSDBClient.connect();
        defaultTSDBClient.auth();
        return defaultTSDBClient;
    }
}
