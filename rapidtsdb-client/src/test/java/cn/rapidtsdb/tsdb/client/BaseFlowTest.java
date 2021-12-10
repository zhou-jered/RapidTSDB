package cn.rapidtsdb.tsdb.client;

import cn.rapidtsdb.tsdb.protocol.constants.AuthTypes;
import lombok.extern.log4j.Log4j2;

import java.util.HashMap;
import java.util.Map;

@Log4j2
public class BaseFlowTest {

    public static void main(String[] args) {

        Map<String, String> authParams = new HashMap<>();
        authParams.put("token", "12345");

        TSDBClientConfig config = TSDBClientConfig.newBuilder()
                .serverBootstrap("127.0.0.1:9100")
                .authType(AuthTypes.AUTH_TYPE_TOKEN)
                .authParams(authParams)
                .build();
        TSDBClient tsdbClient = TSDBClientFactory.getTSDBClient(config);
        WriteMetricResult writeMetricResult = tsdbClient.writeMetric("me.local", 1.3);
        System.out.println(writeMetricResult);
//        tsdbClient.close()
        log.info(tsdbClient);
    }

}
