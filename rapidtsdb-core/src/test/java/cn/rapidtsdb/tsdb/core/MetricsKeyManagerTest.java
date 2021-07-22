package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.TSDBConfigTester;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.persistent.MetricsKeyManager;
import com.google.common.collect.Lists;
import lombok.extern.log4j.Log4j2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

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

    @Test
    public void testScanMetrics() {
        String[] data = new String[]{"aabcc", "aab", "abce", "bbyq", "aaff", "aaqqq", "aacca", "ppl"};
        MetricsKeyManager mkManager = MetricsKeyManager.getInstance();
        mkManager.init();
        for (String str : data) {
            int idx = mkManager.getMetricsIndex(str);
            System.out.println(str + " : " + idx);
        }
        List<String> scanMetricsResult = mkManager.scanMetrics("aa");
        Assert.assertNotNull(scanMetricsResult);
        Assert.assertEquals(5, scanMetricsResult.size());
        System.out.println(scanMetricsResult);
    }


    @Test
    public void testScanMetricsWithFilter() {
        String[] data = new String[]{"aabcc", "aab", "abce", "bbyq", "aaff", "aaqqq", "aacca", "ppl"};
        MetricsKeyManager mkManager = MetricsKeyManager.getInstance();
        mkManager.init();
        for (String str : data) {
            int idx = mkManager.getMetricsIndex(str);
            System.out.println(str + " : " + idx);
        }
        List<String> scanMetricsResult = mkManager.scanMetrics("aa", Lists.newArrayList("b"));
        Assert.assertNotNull(scanMetricsResult);
        Assert.assertEquals(2, scanMetricsResult.size());
        System.out.println(scanMetricsResult);
    }
}
