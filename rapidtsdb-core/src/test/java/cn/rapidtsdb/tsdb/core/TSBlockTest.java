package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.common.TimeUtils;
import cn.rapidtsdb.tsdb.exception.BlockDataMissMatchException;
import cn.rapidtsdb.tsdb.object.TSDataPoint;
import com.google.common.collect.Lists;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TSBlockTest {


    @Test
    public void testNoData() {
        long baseTime = TimeUtils.getBlockBaseTime(System.currentTimeMillis());
        TSBlock tsBlock = new TSBlock(baseTime, 120 * 60);
//        tsBlock.appendDataPoint(baseTime+10, 3.1);
        TreeMap<Long, Double> dps = tsBlock.getDataPoints();
        System.out.println(dps);
        Assert.assertEquals(0, dps.size());
    }

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

        Map<Long, Double> dps = tsBlock.getDataPoints();
        System.out.println(dps.size());
        for (int i = 0; i < dpsNum; i++) {

            Assert.assertTrue(values.get(i) - dps.get(baseTime + i) < 0.000001);
            if (i % 1000 == 0) {
                System.out.println("sample : " + values.get(i) + " : " + dps.get(i + baseTime));
            }
        }
        System.out.println("total mem: " + tsBlock.getMemoryActualUsed());
        System.out.println("No compressed mem: " + dpsNum * (16) / 1024);
    }

    TSBlock tsBlock = new TSBlock(1, 7200);





}
