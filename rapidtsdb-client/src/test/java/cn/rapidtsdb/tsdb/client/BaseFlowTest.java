package cn.rapidtsdb.tsdb.client;

import cn.rapidtsdb.tsdb.object.TSDataPoint;
import cn.rapidtsdb.tsdb.protocol.constants.AuthTypes;
import lombok.extern.log4j.Log4j2;

import java.util.HashMap;
import java.util.List;
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
        long tp = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            WriteMetricResult writeMetricResult = tsdbClient.writeMetric("me.loc/…………中文al", tp + i, 1.3 + i);
            System.out.println(writeMetricResult);
        }

        System.out.println("start read");
        List<TSDataPoint> dps = tsdbClient.readMetrics("me.local", tp - 100, tp + 5);
        System.out.println("read" + dps);

        tsdbClient.close();
        log.info(tsdbClient);
    }

}
