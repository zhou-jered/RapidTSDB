package cn.rapidtsdb.tsdb.client;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class TSDBClientFactory {
    public static TSDBClient getTSDBClient(TSDBClientConfig clientConfig) {
        DefaultTSDBClient defaultTSDBClient = new DefaultTSDBClient(clientConfig);
        defaultTSDBClient.connect();
        log.debug("auth type:{}", clientConfig.getAuthType());
        defaultTSDBClient.auth(clientConfig.getAuthType(), clientConfig.getAuthParams());
        return defaultTSDBClient;
    }
}
