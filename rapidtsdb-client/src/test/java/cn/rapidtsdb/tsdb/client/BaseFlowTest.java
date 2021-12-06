package cn.rapidtsdb.tsdb.client;

import java.util.HashMap;
import java.util.Map;

public class BaseFlowTest {

    public static void main(String[] args) {

        Map<String, String> authParams = new HashMap<>();
        authParams.put("token", "12345");

        TSDBClientConfig config = TSDBClientConfig.newBuilder()
                .serverBootstrap("127.0.0.1:9100")
                .authParams(authParams)
                .build();
        TSDBClient tsdbClient = TSDBClientFactory.getTSDBClient(config);
        tsdbClient.writeMetric("me.local", 1.3);
        tsdbClient.close();

    }

}
