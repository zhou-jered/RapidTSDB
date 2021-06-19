package cn.rapidtsdb.tsdb.core.persistent;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.TSBlock;
import cn.rapidtsdb.tsdb.core.TSBlockManager;
import cn.rapidtsdb.tsdb.core.TSBlockMeta;
import cn.rapidtsdb.tsdb.core.TSBlockSnapshot;
import cn.rapidtsdb.tsdb.core.io.IOLock;
import cn.rapidtsdb.tsdb.core.io.TSBlockSerializer;
import cn.rapidtsdb.tsdb.core.persistent.file.FileLocation;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.store.StoreHandler;
import cn.rapidtsdb.tsdb.utils.TimeUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static cn.rapidtsdb.tsdb.core.AbstractTSBlockManager.createTSBlockMeta;

/**
 * The Logical TSBlock Persister
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

    /**
     * Create or Merge Block
     *
     * @param tsBlocks
     */
    public void persistTSBlockSync(Map<Integer, TSBlock> tsBlocks) {

    }

    public void persistTSBlockAsync(Map<Integer, TSBlock> tsBlocks) {

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


    /**
     * file structure
     * Today HotData: {metricid}/{timebased_hourlyIndex)}.mdata
     * Daily Data : {metricid}/day/{yyyy-MM-dd}.mdata
     * 30 Day Data: {metricid}/mon/{yyyy-MM}.data
     * 3000 Day Data: {metricid}/history/{yyyy-MM}.data
     */
    public static class FilenameStrategy {

        public static FileLocation getTodayFileLocation(int metric, long baseTimeSeconds) {
            return new FileLocation(getTodayDirectory(metric, baseTimeSeconds), getTodayBlockFilename(metric, baseTimeSeconds));
        }

        public static FileLocation getDailyFileLocation(int metric, long baseTimeSeconds) {
            return new FileLocation(getDailyBlockFilename(metric, baseTimeSeconds), getDailyBlockFilename(metric, baseTimeSeconds));
        }

        public static FileLocation getMonthlyFileLocation(int metric, long baseTimeSeconds) {
            return new FileLocation(getMonthlyDirectory(metric, baseTimeSeconds), getMonthBlockFilename(metric, baseTimeSeconds));
        }

        /**
         * Hot data
         *
         * @param metric
         * @param baseTimeSecconds
         * @return
         */
        public static String getTodayDirectory(int metric, long baseTimeSecconds) {
            return String.valueOf(metric);
        }

        public static String getDailyDirectory(int metric, long baseTimeSeconds) {
            return metric + "/day";
        }

        public static String getMonthlyDirectory(int metric, long baseTimeSeconds) {
            return metric + "/mon";
        }


        public static String getTodayBlockFilename(int metricId, long baseTimeSeconds) {
            return "T" + metricId + ":" + baseTimeSeconds + ".data";
        }

        public static String getDailyBlockFilename(int metricId, long timeSeconds) {
            timeSeconds = TimeUtils.truncateDaySeconds(timeSeconds);
            return "D" + metricId + ":" + timeSeconds + ".data";
        }

        public static String getMonthBlockFilename(int metricId, long timeSeconds) {
            timeSeconds = TimeUtils.truncateMonthSeconds(timeSeconds);
            return "M" + metricId + ":" + timeSeconds + ".data";
        }

        public static String getYearBlockFilename(int metricId, long timeSeconds) {
            timeSeconds = TimeUtils.truncateYearSeconds(timeSeconds);
            return "Y" + metricId + ":" + timeSeconds + ".data";
        }
    }

    /**
     * Just write A TSBlock into a single file
     * todo move this implementation to under layer,
     * block manager do not need to know about the persist implementation
     */
    static class SimpleTSBlockStoreTask implements Runnable {
        private final static Logger log = LogManager.getLogger("asf");
        private int metricId;
        private FileLocation fileLocation;
        private TSBlockSnapshot snapshot;
        private StoreHandler storeHandler;
        private static final TSBlockSerializer blockWriter = new TSBlockSerializer();

        public SimpleTSBlockStoreTask(int metricId, TSBlockSnapshot snapshot, StoreHandler storeHandler) {
            this.metricId = metricId;
            this.fileLocation = FilenameStrategy.getTodayFileLocation(metricId, snapshot.getTsBlock().getBaseTime());
            this.snapshot = snapshot;
            this.storeHandler = storeHandler;
        }

        @Override
        public void run() {
            TSBlockMeta blockMeta = createTSBlockMeta(snapshot, metricId);
            if (log.isDebugEnabled()) {
                log.debug("Store {}, dpsSize:{}, md5:{}, file:{}", blockMeta.getBaseTime(), blockMeta.getDpsSize(), blockMeta.getMd5Checksum(), fileLocation);
            }
            final String fullFilename = fileLocation.getPathWithFilename();
            if (storeHandler.fileExisted(fullFilename)) {
                log.warn("Store {}, file already Existed. Overrided", blockMeta.getSimpleInfo());
            }
            try {
                log.debug("{} start write:{}", metricId, fileLocation);
                OutputStream outputStream = storeHandler.openFileOutputStream(fullFilename);
                Lock metricLock = IOLock.getMetricLock(metricId);

                try {
                    int retry = 0;
                    while (retry++ < 10) {
                        if (metricLock.tryLock(3, TimeUnit.SECONDS)) {
                            outputStream.write(blockMeta.series());
                            blockWriter.serializeToStream(snapshot, outputStream);
                            snapshot.getTsBlock().markVersionClear(snapshot.getDataVersion());
                            break;
                        } else {
                            log.warn("metric IOLock failed: {}", metricId);
                        }
                    }
                } catch (InterruptedException e) {
                } finally {
                    outputStream.close();
                    metricLock.unlock();
                }
                log.debug("{} write :{}, completed", metricId, fileLocation);

            } catch (IOException e) {
                e.printStackTrace();
                log.error("Write File {} Exception", fileLocation.getPathWithFilename(), e);
                ManagedThreadPool.getInstance().submitFailedTask(this);
            }
        }
    }


}
