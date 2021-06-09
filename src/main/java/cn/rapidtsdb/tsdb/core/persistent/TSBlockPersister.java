package cn.rapidtsdb.tsdb.core.persistent;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.TSBlock;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.Map;

/**
 * maintain the metricId with the metric data,
 * no metric name knowledge here
 */
@Log4j2
public class TSBlockPersister implements Initializer, Closer {

    private TSDBConfig tsdbConfig = TSDBConfig.getConfigInstance();
    private AppendOnlyLogManager appendOnlyLogManager = AppendOnlyLogManager.getInstance();
    private TSDBCheckPointManager tsdbCheckPointManager = TSDBCheckPointManager.getInstance();
    private static TSBlockPersister INSTANCE = null;

    private TSBlockPersister() {
    }

    @Override
    public void init() {

    }

    @Override
    public void close() {

    }

    public void persistTSBlock(Map<Integer, TSBlock> tsBlocks) {

    }

    public TSBlock getTSBlock(Integer metricId, long timeSeconds) {
        return null;
    }

    public ArrayList<TSBlock> getTSBlocks(Integer metricId, long timeSecondsStart, long timeSecondsEnd) {
        return null;
    }

    public static TSBlockPersister getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (TSBlockPersister.class) {
                if(INSTANCE == null) {
                    INSTANCE = new TSBlockPersister();
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }
}
