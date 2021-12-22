package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.AppEnv;
import cn.rapidtsdb.tsdb.meta.exception.IllegalCharsException;
import cn.rapidtsdb.tsdb.object.BizMetric;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MetricTransfromerTest {

    static MetricTransformer metricTransformer;

    @Before
    public void setUp() throws Exception {
        AppEnv.prepareOnce(null, false);
        metricTransformer = new MetricTransformer();
        metricTransformer.init();
    }

    @After
    public void tearDown() throws Exception {
        metricTransformer.close();
    }

    @Test
    public void testNoTAg() {
        BizMetric bizMetric = new BizMetric("Helo.com.sun.me", null);
        String intenral = null;
        try {
            intenral = metricTransformer.toInternalMetric(bizMetric);
        } catch (IllegalCharsException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(bizMetric.getMetric(), intenral);
        System.out.println(intenral);
    }


    @Test
    public void testHasTag() {
        Map<String, String> tags = new HashMap<>();
        tags.put("host", "192.168.0.1");
        tags.put("zone", "Asiz");
        tags.put("biz", "data");
        BizMetric bizMetric = new BizMetric("Helo.com.sun.me", tags);
        String intenral = null;
        try {
            intenral = metricTransformer.toInternalMetric(bizMetric);
            System.out.println(intenral);
        } catch (IllegalCharsException e) {
            e.printStackTrace();
        }
        Assert.assertNotEquals(bizMetric.getMetric(), intenral);

        BizMetric backBizMetrc = metricTransformer.toBizMetric(intenral);
        System.out.println(backBizMetrc);
        Assert.assertEquals(bizMetric, backBizMetrc);
    }

    @Test
    public void testBackDisk() {
        BizMetric bizMetric = metricTransformer.toBizMetric("Helo.com.sun.me/2^3/4^5/6^7");
        System.out.println(bizMetric);
    }
}
