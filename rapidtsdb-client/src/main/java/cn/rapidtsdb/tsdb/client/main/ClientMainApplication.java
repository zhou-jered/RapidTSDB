package cn.rapidtsdb.tsdb.client.main;

import cn.rapidtsdb.tsdb.client.TSDBClientConfig;

public class ClientMainApplication {
    public static void main(String[] args) {

    }

    private static TSDBClientConfig getConfig() {
        TSDBClientConfig.TSDBClientConfigBuilder configBuilder = new TSDBClientConfig.TSDBClientConfigBuilder();
        TSDBClientConfig config = configBuilder.serverBootstrap("127.0.0.1:9100")
                .authType("token")
                .authCredentials("hellotsdb".getBytes())
                .protocolVersion("1.0")
                .build();
        return config;
    }
}
