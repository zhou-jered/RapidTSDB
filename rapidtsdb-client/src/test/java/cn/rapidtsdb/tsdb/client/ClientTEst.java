package cn.rapidtsdb.tsdb.client;

import lombok.extern.log4j.Log4j2;
import org.junit.Test;

@Log4j2
public class ClientTEst {
    @Test
    public void testClient() {
        TSDBClientConfig config = TSDBClientConfig.TSDBClientConfigBuilder.newBuilder()
                .serverBootstrap("127.0.0.1:80")
                .build();
        DefaultTSDBClient defaultTSDBClient = new DefaultTSDBClient(config);
        defaultTSDBClient.connect(true, 1200);
        System.out.println("done");
        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();

        }
        System.out.println("exit");
    }
}
