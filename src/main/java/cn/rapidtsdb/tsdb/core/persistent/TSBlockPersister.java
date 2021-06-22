package cn.rapidtsdb.tsdb.core.persistent;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.TSBlock;
import cn.rapidtsdb.tsdb.core.TSBlockManager;
import cn.rapidtsdb.tsdb.core.TSBlockMeta;
import cn.rapidtsdb.tsdb.core.TSBlockSnapshot;
import cn.rapidtsdb.tsdb.core.persistent.file.FileLocation;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.store.StoreHandler;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
    private StoreHandler storeHandler;

    private TSBlockPersister() {
    }

    @Override
    public void init() {
        storeHandler = StoreHandlerFactory.getStoreHandler();
    }

    @Override
    public void close() {

    }

    /**
     * Create or Merge Block
     *
     * @param tsBlocks
     */
    public void persistTSBlockSync(Map<Integer, TSBlock> tsBlocks) {

    }

    public void persistTSBlockAsync(Map<Integer, TSBlock> tsBlocks) {
        Map<Integer, TSBlock> writingBlocks = new HashMap<>(tsBlocks);
        for (Integer metricId : writingBlocks.keySet()) {
            TSBlockSnapshot blockSnapshot = writingBlocks.get(metricId).snapshot();
            TSBlockMeta tsBlockMeta = TSBlockManager.createTSBlockMeta(blockSnapshot, metricId);
            FileLocation fileLocation = TSBlockManager.FilenameStrategy.getTodayFileLocation(metricId,
                    tsBlockMeta.getBaseTime());

        }
    }

    public TSBlock getTSBlock(Integer metricId, long timeSeconds) {
        return null;
    }

    public ArrayList<TSBlock> getTSBlocks(Integer metricId, long timeSecondsStart, long timeSecondsEnd) {
        return null;
    }

    public Iterator<TSBlock> getTSBlockIter(Integer metricId, long timeSecondsStart, Long timeSecondsEnd) {
        return null;
    }

    public static TSBlockPersister getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (TSBlockPersister.class) {
                if (INSTANCE == null) {
                    INSTANCE = new TSBlockPersister();
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }
}
