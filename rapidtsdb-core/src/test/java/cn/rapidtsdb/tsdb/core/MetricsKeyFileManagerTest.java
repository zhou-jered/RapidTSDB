package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.TSDBConfigTester;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.persistent.IMetricsKeyManager;
import cn.rapidtsdb.tsdb.core.persistent.MetricsKeyManagerFactory;
import cn.rapidtsdb.tsdb.plugins.PluginManager;
import com.google.common.collect.Lists;
import lombok.extern.log4j.Log4j2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

@Log4j2
public class MetricsKeyFileManagerTest {

    final String metric = "nihao";

    @Before
    public void setUp() throws Exception {

        System.out.println("runnging setup");
        TSDBConfigTester.init();
        TSDBConfig tsdbConfig = TSDBConfig.getConfigInstance();
        tsdbConfig.setConfigVal("dataPath", "/tmp/data/tsdb/test");
        PluginManager.loadPlugins();
        PluginManager.configPlugins(TSDBConfig.getConfigInstance().getRawConfig());
        PluginManager.preparePlugin();

    }

    @Test
    public void testWriteMetric() {
        IMetricsKeyManager IMetricsKeyManager = MetricsKeyManagerFactory.getInstance();
        IMetricsKeyManager.init();
        int mid = IMetricsKeyManager.getMetricsIndex(metric, true);
        System.out.println(metric + " mid: " + mid);
        int mid2 = IMetricsKeyManager.getMetricsIndex(metric, true);
        System.out.println(metric + " mid: " + mid2);
        IMetricsKeyManager.close();

    }

    @Test
    public void testReadMetric() {
        IMetricsKeyManager mkManager = MetricsKeyManagerFactory.getInstance();
        mkManager.init();
        int mid = mkManager.getMetricsIndex(metric,true);
        System.out.println(metric + " mid: " + mid);
    }

    @Test
    public void testScanMetrics() {
        String[] data = new String[]{"aabcc", "aab", "abce", "bbyq", "aaff", "aaqqq", "aacca", "ppl"};
        IMetricsKeyManager mkManager = MetricsKeyManagerFactory.getInstance();
        mkManager.init();
        for (String str : data) {
            int idx = mkManager.getMetricsIndex(str,true);
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
        IMetricsKeyManager mkManager = MetricsKeyManagerFactory.getInstance();
        mkManager.init();
        for (String str : data) {
            int idx = mkManager.getMetricsIndex(str,true);
            System.out.println(str + " : " + idx);
        }
        List<String> scanMetricsResult = mkManager.scanMetrics("aa", Lists.newArrayList("b"));
        Assert.assertNotNull(scanMetricsResult);
        Assert.assertEquals(2, scanMetricsResult.size());
        System.out.println(scanMetricsResult);
    }
}
