package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.common.TimeUtils;
import cn.rapidtsdb.tsdb.core.io.TSBlockDeserializer;
import cn.rapidtsdb.tsdb.core.persistent.TSBlockPersister;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PersisterTest {

    long testingTime = 0;

    @Before
    public void setUp() throws Exception {
        testingTime = System.currentTimeMillis();

    }

    @Test
    public void test0_BlockPersist() {

        long basetime = testingTime;
        basetime = TimeUtils.getBlockBaseTime(basetime );
        TSBlock tsBlock = TSBlockFactory.newTSBlock(1, basetime);
        for (int i = 0; i < 1000; i++) {
            double val = ((int) (Math.random() * 1000)) * 1.0 / 100;
            tsBlock.appendDataPoint(basetime + i, val);
            if (i % 123 == 0) {
                System.out.println(i + ":" + val);
            }
        }

        TSBlockPersister persister = TSBlockPersister.getINSTANCE();
        Map<Integer, TSBlock> memoryBlock = new HashMap<>();
        memoryBlock.put(1, tsBlock);
        persister.persistTSBlockSync(memoryBlock);


    }

    @Test
    public void test1_Read() {
        long basetime = testingTime / 1000;
        TSBlockPersister blockPersister = TSBlockPersister.getINSTANCE();
        TSBlock tsBlock = blockPersister.getTSBlock(1, basetime);
        Assert.assertNotNull(tsBlock);
        Map<Long, Double> dps = tsBlock.getDataPoints();
        Assert.assertEquals(1000, dps.size());
        for (int i = 0; i < 1000; i++) {
            if (i % 123 == 0) {
                System.out.println("recoiver:  " + i + ":" + dps.get(i));
            }
        }
        System.out.println(dps);
    }

    @Test
    public void test3_testMeta() {
        TSBlockDeserializer blockReader = new TSBlockDeserializer();
        try {
            String tp = String.valueOf(TimeUtils.getBlockBaseTime(testingTime / 1000));

            TSBlockDeserializer.TSBlockAndMeta blockAndMeta = blockReader.deserializeFromStream(new FileInputStream("/tmp/data/tsdb/1/T1:" + tp + ".data"));
            System.out.println(blockAndMeta.getMeta());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
