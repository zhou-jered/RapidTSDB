package cn.rapidtsdb.tsdb.client;

public class BaseFlowTest {

    public static void main(String[] args) {
        TSDBClientConfig config = TSDBClientConfig.newBuilder()
                .serverBootstrap("127.0.0.1:9100")
                .build();
        TSDBClient tsdbClient = TSDBClientFactory.getTSDBClient(config);

        tsdbClient.writeMetric("me.local", 1.3);
        tsdbClient.close();

    }

}
