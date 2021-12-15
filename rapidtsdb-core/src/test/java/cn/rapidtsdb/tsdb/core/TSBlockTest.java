package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.exception.BlockDataMissMatchException;
import cn.rapidtsdb.tsdb.common.TimeUtils;
import cn.rapidtsdb.tsdb.object.TSDataPoint;
import com.google.common.collect.Lists;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class TSBlockTest {

    @Test
    public void testBlock() throws BlockDataMissMatchException {
        int dpsNum = 120 * 60;
        long baseTime = TimeUtils.currentTimestamp();
        baseTime -= baseTime % (120 * 60);
        TSBlock tsBlock = new TSBlock(baseTime, 120 * 60);

        List<Double> values = Lists.newArrayList();
        for (int i = 0; i < dpsNum; i++) {
            double val = ((double) ((int) (Math.random() * 10000))) / 100;
            tsBlock.appendDataPoint(baseTime + i, val);
            values.add(val);
        }

        List<TSDataPoint> dps = tsBlock.getDataPoints();
        System.out.println(dps.size());
        for (int i = 0; i < dpsNum; i++) {
            Assert.assertEquals(baseTime + i, dps.get(i).getTimestamp());
            Assert.assertTrue(values.get(i) - dps.get(i).getValue() < 0.000001);
            if (i % 1000 == 0) {
                System.out.println("sample : " + values.get(i) + " : " + dps.get(i).getValue());
            }
        }
        System.out.println("total mem: " + tsBlock.getMemoryActualUsed());
        System.out.println("No compressed mem: " + dpsNum * (16) / 1024);
    }

    TSBlock tsBlock = new TSBlock(1, 7200);
    private Method handleMethod;

    @Before
    public void setUp() throws Exception {
        String handleMethodName = "handleDuplicateDatapoint";
        try {
            handleMethod = TSBlock.class.getDeclaredMethod(handleMethodName, ArrayList.class);
            handleMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testHandleNormal() {
        ArrayList<TSDataPoint> dps = new ArrayList<>(120);
        long current = System.currentTimeMillis();
        for (int i = 0; i < 120; i++) {
            TSDataPoint dp = new TSDataPoint(current + i, RandomUtils.nextDouble());
            dps.add(dp);
        }
        try {
            handleMethod.invoke(tsBlock, dps);
            assertDps(dps, current);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testHandleDisordered() {
        ArrayList<TSDataPoint> dps = new ArrayList<>(120);
        long current = System.currentTimeMillis();
        for (int i = 0; i < 120; i++) {
            TSDataPoint dp = new TSDataPoint(current + i, RandomUtils.nextDouble());
            dps.add(dp);
        }
        for (int i = 0; i < 1000; i++) {
            int left = RandomUtils.nextInt() % 120;
            int right = RandomUtils.nextInt() % 120;
            TSDataPoint tmp = dps.get(left);
            dps.set(left, dps.get(right));
            dps.set(right, tmp);
        }
        try {
            handleMethod.invoke(tsBlock, dps);
            assertDps(dps, current);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testHandleDuplicated() {
        ArrayList<TSDataPoint> dps = new ArrayList<>(120);
        long current = System.currentTimeMillis();
        for (int i = 0; i < 120; i++) {
            TSDataPoint dp = new TSDataPoint(current + i, RandomUtils.nextDouble());
            dps.add(dp);
            if (RandomUtils.nextBoolean()) {
                TSDataPoint dup = new TSDataPoint(current + i, RandomUtils.nextDouble());
                dps.add(dup);
            }
        }
        for (int i = 0; i < 10000; i++) {
            int left = RandomUtils.nextInt() % 120;
            int right = RandomUtils.nextInt() % 120;
            TSDataPoint tmp = dps.get(left);
            dps.set(left, dps.get(right));
            dps.set(right, tmp);
        }

        try {
            handleMethod.invoke(tsBlock, dps);
            assertDps(dps, current);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testHandleDisOrderedAndDuplicated() {
        ArrayList<TSDataPoint> dps = new ArrayList<>(120);
        long current = System.currentTimeMillis();
        for (int i = 0; i < 120; i++) {
            TSDataPoint dp = new TSDataPoint(current + i, RandomUtils.nextDouble());
            dps.add(dp);
            if (RandomUtils.nextBoolean()) {
                TSDataPoint dup = new TSDataPoint(current + i, RandomUtils.nextDouble());
                dps.add(dup);
            }
        }

        try {
            System.out.println("before size:" + dps.size());
            handleMethod.invoke(tsBlock, dps);
            System.out.println("after size:" + dps.size());
            assertDps(dps, current);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

    }

    private void assertDps(ArrayList<TSDataPoint> dps, long current) {
        Assert.assertEquals(120, dps.size());
        for (int i = 0; i < 120; i++) {
            Assert.assertEquals(current + i, dps.get(i).getTimestamp());
        }
    }

}
