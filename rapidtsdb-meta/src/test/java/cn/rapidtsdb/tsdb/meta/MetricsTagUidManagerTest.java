package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.AppEnv;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

@Log4j2
public class MetricsTagUidManagerTest {

    static MetricsTagUidManager uidManager;
    static boolean classSetup = false;

    @Before
    public void setUp() throws Exception {
        if (classSetup) {
            return;
        }
        log.info("Class Setup");
//        classSetup = true;
        AppEnv.prepareOnce(null, false);
        uidManager = MetricsTagUidManager.getInstance();
        uidManager.init();
    }

    @After
    public void tearDown() throws Exception {
        uidManager.close();
    }

    @Test
    public void testNormal() {
        String tag = "HelloWorld";
        int idx = uidManager.getTagIndex(tag);
        log.info("tag idx: {}", idx);
        String retTag = uidManager.getTagByIndex(idx);
        Assert.assertEquals(tag, retTag);
    }

    @Test
    public void testSetGet() {
        int idx = uidManager.getTagIndex("HelloWorld");
        Assert.assertTrue(idx > 0);
        String[] tags = new String[]{"a", "Workd", "C++", "best", "Spring"};
        for (String tag : tags) {
            int tagIdx = uidManager.getTagIndex(tag);
            String backTag = uidManager.getTagByIndex(tagIdx);
            Assert.assertEquals(tag, backTag);
        }
    }

    @Test
    public void testMemeoryUseage() {
        final int testSize = 100000;
        Runtime runtime = Runtime.getRuntime();
        long free = runtime.freeMemory();
        long max = runtime.maxMemory();
        long total = runtime.totalMemory();
        for (int i = 0; i < testSize; i++) {
            String rstr = RandomStringUtils.randomAlphanumeric(20);
            int idx = uidManager.getTagIndex(rstr);
            if (i % 100 == 0) {
                System.out.println(rstr + ":" + idx);
            }
        }
        long afterFree = runtime.freeMemory();
        long afterMax = runtime.maxMemory();
        long afterTotal = runtime.totalMemory();
        System.out.println(String.format("free:%s, max:%s, total:%s", free, max, total));
        System.out.println(String.format("After free:%s, max:%s, total:%s", afterFree, afterMax, afterTotal));
        long freeLess = free - total + (afterTotal - total);
        System.out.println("use memeory:" + freeLess + " " + (freeLess / 1024) + "k");
    }

    @Test
    public void testSeriesAndDeSeries() {
        final int testSize = 100000;
        Runtime runtime = Runtime.getRuntime();
        long used = runtime.totalMemory() - runtime.freeMemory();
        Map<String, Integer> keepRight = new HashMap<>();
        for (int i = 0; i < testSize; i++) {
            String rstr = RandomStringUtils.randomAlphanumeric(20);
            int idx = uidManager.getTagIndex(rstr);
            keepRight.put(rstr, idx);
        }
        long afterUsed = runtime.totalMemory() - runtime.freeMemory();
        afterUsed = afterUsed - used;
        System.out.println("used:" + afterUsed + " " + (afterUsed / 1024) + "KB  " + (afterUsed / 1024 / 1024) + "MB");
        System.out.println("actual str number:" + keepRight.size());
        System.out.println("Node Number:" + uidManager.nodeNumber());
        long start = System.nanoTime();
        uidManager.close();
        long cost = System.nanoTime() - start;
        System.out.println("close use:" + cost / 1e6 + "ms" + " " + (cost / 6e7) + "s");
        start = System.nanoTime();
        uidManager.forceInit();
        cost = System.nanoTime() - start;
        System.out.println("init use:" + cost / 1e6 + "ms" + " " + (cost / 6e7) + "s");

        int sucCnt = 0;
        int faileCnd = 0;
        for (String s : keepRight.keySet()) {
            int idx = uidManager.getTagIndex(s);
            if (idx != keepRight.get(s)) {
                faileCnd++;
                if (faileCnd < 10) {
                    System.out.print(idx + ":" + keepRight.get(s) + "  ");
                }

            } else {
                sucCnt++;
            }
        }
        System.out.println("fail:" + faileCnd + " suc:" + sucCnt);
        Assert.assertEquals(testSize, sucCnt);
    }
}
