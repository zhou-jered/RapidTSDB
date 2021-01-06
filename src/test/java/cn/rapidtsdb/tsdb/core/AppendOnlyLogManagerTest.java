package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.RapidTSDBApplication;
import cn.rapidtsdb.tsdb.core.persistent.AppendOnlyLogManager;
import org.junit.Before;
import org.junit.Test;

public class AppendOnlyLogManagerTest {


    private RapidTSDBApplication application;

    @Before
    public void setUp() throws Exception {
        RapidTSDBApplication.main(new String[0]);
    }

    @Test
    public void testLifeCycle() {
        AppendOnlyLogManager appendOnlyLogManager = new AppendOnlyLogManager();
        appendOnlyLogManager.init();

        appendOnlyLogManager.recoverLog();
       // appendOnlyLogManager.close();
    }
}
