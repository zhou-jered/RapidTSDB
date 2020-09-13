package cn.tinytsdb.tsdb.core;

import cn.tinytsdb.tsdb.config.MetricConfig;
import cn.tinytsdb.tsdb.config.TSDBConfig;
import cn.tinytsdb.tsdb.core.persistent.Persistently;
import cn.tinytsdb.tsdb.core.persistent.file.FileLocation;
import cn.tinytsdb.tsdb.exectors.GlobalExecutorHolder;
import cn.tinytsdb.tsdb.lifecycle.Closer;
import cn.tinytsdb.tsdb.lifecycle.Initializer;
import cn.tinytsdb.tsdb.store.StoreHandler;
import cn.tinytsdb.tsdb.store.StoreHandlerFactory;
import cn.tinytsdb.tsdb.utils.TimeUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * TSBlock Logical Manager, File Implementation
 * response for @TSBlock store, search, compress
 */
@Log4j2
public class TSBlockManager  extends AbstractTSBlockManager implements Persistently, Initializer, Closer {

    private final static String METRIC_LOCATION_SEPARATOR_FILE = "mp.idx";

    TSDBConfig tsdbConfig;

    StoreHandler storeHandler;

    private static final int BLOCK_SIZE_SECONDS = 2 * 60;

    private Map<Integer, TSBlock> currentBlockCache = new ConcurrentHashMap<>(10240);
    private Map<Integer, int[]> metricLocationSeparator = new ConcurrentHashMap<>(10240);

    GlobalExecutorHolder executorHolder = GlobalExecutorHolder.getInstance();
    ThreadPoolExecutor ioExecutor = executorHolder.ioExecutor();

    public TSBlockManager(TSDBConfig tsdbConfig) {
        this.tsdbConfig = tsdbConfig;
        storeHandler = StoreHandlerFactory.getStoreHandler();

    }

    @Override
    public void close() {

    }

    @Override
    public void init() {
        if (storeHandler.fileExisted(METRIC_LOCATION_SEPARATOR_FILE)) {
            try {
                DataInputStream dataInputStream = new DataInputStream(storeHandler.getFileInputStream(METRIC_LOCATION_SEPARATOR_FILE));
                dataInputStream.readInt();
            } catch (IOException e) {
                e.printStackTrace();
                log.error("Read {} file Exception", METRIC_LOCATION_SEPARATOR_FILE, e);
            }
        }
    }

    public TSBlock getCurrentWriteBlock(int metricId, long timestamp) {
        TSBlock currentBlock = currentBlockCache.get(metricId);
        if (currentBlock == null) {
            currentBlock = newTSBlock(metricId, timestamp);
            TSBlock existedBlocks = currentBlockCache.putIfAbsent(metricId, currentBlock);
            if (existedBlocks == null) {
                return currentBlock;
            } else {
                return existedBlocks;
            }
        }
        if (currentBlock.inBlock(timestamp)) {
            return currentBlock;
        }

        // A new Round Come here
        if (currentBlock.afterBlock(timestamp)) {

            TSBlock expiredBlock = currentBlock;
            // persist expired current block todo


            currentBlock = newTSBlock(metricId, timestamp);
            currentBlockCache.put(metricId, currentBlock);
            return currentBlock;
        }

        return searchHistoryBlock(metricId, timestamp);

    }

    public TSBlock searchHistoryBlock(int metricId, long timestamp) {
        return null;
    }

    public TSBlock newTSBlock(int metricId, long timestamp) {
        TSBlock tsBlock = null;
        MetricConfig mc = MetricConfig.getMetricConfig(metricId);
        TimeUtils.TimeUnitAdaptor timeUnitAdaptor = TimeUtils.TimeUnitAdaptorFactory.getTimeAdaptor(mc.getTimeUnit());
        long secondsTimestamp = TIME_UNIT_ADAPTOR_SECONDS.adapt(timestamp);
        long secondsBasetime = secondsTimestamp - (secondsTimestamp % BLOCK_SIZE_SECONDS);
        long blockBasetime = timeUnitAdaptor.adapt(secondsBasetime);
        tsBlock = new TSBlock(blockBasetime, BLOCK_SIZE_SECONDS, timeUnitAdaptor);
        return tsBlock;
    }


    /**
     * persist a TSBlock
     *
     * @param tsBlock
     */
    public void persistTSBlock(int metricId, TSBlock tsBlock) {
        TSBlockMeta blockMeta = createTSBlockMeta(tsBlock);
        long baseTime = tsBlock.getBaseTime();
        baseTime = TIME_UNIT_ADAPTOR_SECONDS.adapt(baseTime);
        long currentSeconds = TimeUtils.currentTimestamp();
        long todayBase = TimeUtils.truncateDaySeconds(currentSeconds);
        if (baseTime >= todayBase) {

        }

    }

    @Override
    public void persistTSBlockSync(int metricId, TSBlock tsBlock) {
        persistTSBlock(metricId, tsBlock);
    }

    @Override
    public void persistTSBlockAsync(int metricId, TSBlock tsBlock) {

    }

    public List<TSBlock> getBlockWithTimeRange(int metricId, long start, long end) {
        return null;
    }

    public Iterator<TSBlock> getBlockStreamByTimeRange(int metricId, long start, long end) {
        return null;
    }


    static class SimpleTSBlockStoreTask implements Runnable {
        private final static Logger log = LogManager.getLogger("asf");
        private FileLocation fileLocation;
        private TSBlock tsBlock;
        private StoreHandler storeHandler;

        public SimpleTSBlockStoreTask(FileLocation fileLocation, TSBlock tsBlock, StoreHandler storeHandler) {
            this.fileLocation = fileLocation;
            this.tsBlock = tsBlock;
            this.storeHandler = storeHandler;
        }

        @Override
        public void run() {
            TSBlockMeta blockMeta = createTSBlockMeta(tsBlock);
            if(log.isDebugEnabled()) {
                log.debug("Store {}, dpsSize:{}, md5:{}, file:{}", blockMeta.getBaseTime(), blockMeta.getDpsSize(), blockMeta.getMd5Checksum(), fileLocation);
            }
            final String fullFilename = fileLocation.getPathWithFilename();
            if(storeHandler.fileExisted(fullFilename)) {
                log.warn("Store {}, file already Existed. Overrided", blockMeta.getSimpleInfo());
            }
            try {
                OutputStream outputStream = storeHandler.getFileOutputStream(fullFilename);
                TSBytes timeBytes = tsBlock.getTime();
                TSBytes valBytes = tsBlock.getValues();

                outputStream.write(blockMeta.series());

            } catch (IOException e) {
                e.printStackTrace();
                log.error("Write File {} Exception", fileLocation.getPathWithFilename(), e);
                GlobalExecutorHolder.getInstance().submitFailedTask(this);
            }


        }
    }

    static class OldTsBlockUpdateTask implements Runnable {

        private int metricId;
        private TSBlock tsBlock;

        public OldTsBlockUpdateTask(int metricId, TSBlock tsBlock) {
            this.metricId = metricId;
            this.tsBlock = tsBlock;
        }

        @Override
        public void run() {

        }
    }

    private static class FilenameStrategy {
        public static String getTodayBlockFilename(int metricId, long baseTimeSeconds) {
            return "T" + metricId + ":" + baseTimeSeconds + ".data";
        }

        public static String getDailyBlockFilename(int metricId, long timeSeconds) {
            timeSeconds = TimeUtils.truncateDaySeconds(timeSeconds);
            return "D"+metricId+":"+timeSeconds+".data";
        }

        public static String getMonthBlockFilename(int metricId, long timeSeconds) {
            timeSeconds = TimeUtils.truncateMonthSeconds(timeSeconds);
            return "M"+metricId+":"+timeSeconds+".data";
        }

        public static String getYearBlockFilename(int metricId, long timeSeconds) {
            timeSeconds = TimeUtils.truncateYearSeconds(timeSeconds);
            return "Y"+metricId+":"+ timeSeconds+".data";
        }
    }

}
