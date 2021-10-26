package cn.rapidtsdb.tsdb.client.main;

import cn.rapidtsdb.tsdb.client.ClientConfigHolder;
import cn.rapidtsdb.tsdb.client.TSDBClient;
import cn.rapidtsdb.tsdb.client.TSDBClientConfig;
import cn.rapidtsdb.tsdb.client.TSDBClientFactory;

public class ClientMainApplication {
    public static void main(String[] args) {
        TSDBClient client = TSDBClientFactory.getTSDBClient(getConfig());
        client.writeMetric("com.host.cpu.usage", System.currentTimeMillis(), 1.0);
    }

    private static TSDBClientConfig getConfig() {
        TSDBClientConfig.TSDBClientConfigBuilder configBuilder = new TSDBClientConfig.TSDBClientConfigBuilder();
        TSDBClientConfig config = configBuilder.serverBootstrap("127.0.0.1:9100")
                .authType("token")
                .authCredentials("hellotsdb".getBytes())
                .protocolVersion(1)
                .build();
        ClientConfigHolder.setConfiguration(config);
        return config;
    }
}
