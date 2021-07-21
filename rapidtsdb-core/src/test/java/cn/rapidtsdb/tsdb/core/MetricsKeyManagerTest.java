package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.TSDBConfigTester;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.persistent.MetricsKeyManager;
import lombok.extern.log4j.Log4j2;
import org.junit.Before;
import org.junit.Test;

@Log4j2
public class MetricsKeyManagerTest {

    final String metric = "nihao";

    @Before
    public void setUp() throws Exception {
        TSDBConfigTester.init();
        TSDBConfig tsdbConfig = TSDBConfig.getConfigInstance();
        tsdbConfig.setConfigVal("dataPath", "/tmp/data/tsdb/test");
    }

    @Test
    public void testWriteMetric() {
        MetricsKeyManager metricsKeyManager = MetricsKeyManager.getInstance();
        metricsKeyManager.init();
        int mid = metricsKeyManager.getMetricsIndex(metric);
        System.out.println(metric + " mid: " + mid);
        int mid2 = metricsKeyManager.getMetricsIndex(metric);
        System.out.println(metric + " mid: " + mid2);
        metricsKeyManager.close();

    }

    @Test
    public void testReadMetric() {
        MetricsKeyManager mkManager = MetricsKeyManager.getInstance();
        mkManager.init();
        int mid = mkManager.getMetricsIndex(metric);
        System.out.println(metric + " mid: " + mid);
    }

}
