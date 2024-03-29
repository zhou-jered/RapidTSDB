package cn.rapidtsdb.tsdb.client;

import cn.rapidtsdb.tsdb.object.TSQueryResult;
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
        long tp = 223;
        Map<String, String> tags = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            WriteMetricResult writeMetricResult = tsdbClient.writeMetric("me.test1", tp + i, 1.3 + i, tags);

            System.out.println(writeMetricResult);
        }

        System.out.println("start read");
        TSQueryResult queryResult = tsdbClient.readMetrics("me.test1", tp - 100, tp + 12, tags, "sum");
        System.out.println("read" + queryResult.getDps());
        System.out.println("Result:" + queryResult);

        tsdbClient.close();
        log.info(tsdbClient);
    }

}
