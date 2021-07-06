package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.core.io.TSBlockDeserializer;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Arrays;
@Log4j2
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BlockMetaSeriesTest {

    TSBlockMeta blockMeta;
    private byte[] series;

    @Before
    public void setUp() throws Exception {
        log.info("Setting tSblock meta");
        blockMeta = new TSBlockMeta();
        blockMeta.setBaseTime(10003);
        blockMeta.setDpsSize(102003);
        MessageDigest digest = MessageDigest.getInstance("md5");
        byte[] md5 = digest.digest("asf".getBytes());
        blockMeta.setMd5Checksum(md5);
        blockMeta.setTimeBitsLen(993);
        blockMeta.setValuesBitsLen(10044);
        blockMeta.setMetricId(1033);
        System.out.println(blockMeta);
    }

    @Test
    public void test0_series() {
        byte[] series = blockMeta.series();
        Assert.assertEquals(TSBlockMeta.SERIES_LEN, series.length);
        System.out.println("series:" + Arrays.toString(series));
        this.series = series;
    }

    @Test
    public void test1_deries() {
        this.series = this.blockMeta.series();
        System.out.println("deries from:" + Arrays.toString(this.series));
        TSBlockMeta newMeta = TSBlockMeta.fromSeries(this.series);
        System.out.println(newMeta);
        Assert.assertTrue(blockMeta.equals(newMeta));
    }

    @Test
    public void test4() {
        String f = "/tmp/data/tsdb/3/T3:0.data";
        File file = new File(f);
        if(file.exists()==false) {
            return;
        }
        try {
            byte[] bs = IOUtils.toByteArray(new FileInputStream(file));
            TSBlockDeserializer deserializer = new TSBlockDeserializer();
            TSBlockDeserializer.TSBlockAndMeta blockAndMeta = deserializer.deserializeFromBytes(bs);
            System.out.println(blockAndMeta.getMeta());
            System.out.println(blockAndMeta.getData().getDataPoints());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
