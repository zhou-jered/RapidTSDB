package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.TSDBConfigTester;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.persistent.AOLog;
import cn.rapidtsdb.tsdb.core.persistent.AppendOnlyLogManager;
import lombok.extern.log4j.Log4j2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

@Log4j2
public class AppendOnlyLogManagerTest {


    @Before
    public void setUp() throws Exception {

        Process process = Runtime.getRuntime().exec("rm -rf /tmp/data/tsdb/test/aol");
        int exitCOde = process.waitFor();
        System.out.println("delete file: " + exitCOde);
        TSDBConfigTester.init();
        String testingDir = "/tmp/data/tsdb/test/aol/";
        File file = new File(testingDir);
        if (file.exists()) {
            file.delete();
        }
        file.mkdirs();
        TSDBConfig.getConfigInstance().setConfigVal("dataPath", testingDir);
    }


    @Test
    public void testRolling() throws InterruptedException {
        AppendOnlyLogManager appendOnlyLogManager = new AppendOnlyLogManager();
        appendOnlyLogManager.init();

        final int testLength = 3;
        final int writeLeng = testLength + 3;
        appendOnlyLogManager.setMAX_WRITE_LENGTH(testLength);

        long baseTime = 12000910;
        for (int i = 0; i < writeLeng; i++) {
            double val = Math.round(Math.random() * 10000) * 1.0 / 100;
            AOLog aoLog = new AOLog(i, baseTime + i, val);
            appendOnlyLogManager.appendLog(aoLog);
        }
        long t = System.currentTimeMillis();
        appendOnlyLogManager.close(120000);
        long cost = System.currentTimeMillis() - t;
        log.debug("close using {}", cost);
        appendOnlyLogManager.init();
        long logIdx = appendOnlyLogManager.getLogIndex();
        Assert.assertEquals(writeLeng, logIdx);
        AOLog[] logs = appendOnlyLogManager.recoverLog(3);
        Assert.assertNotNull(logs);
        Assert.assertEquals(testLength, logs.length);
        if (logs != null) {
            System.out.println(Arrays.toString(logs));
        }

    }
}
