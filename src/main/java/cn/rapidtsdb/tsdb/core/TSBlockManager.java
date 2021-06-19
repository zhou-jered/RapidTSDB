package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.config.MetricConfig;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.io.TSBlockSerializer;
import cn.rapidtsdb.tsdb.core.persistent.TSBlockPersister;
import cn.rapidtsdb.tsdb.core.persistent.file.FileLocation;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.store.StoreHandler;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import cn.rapidtsdb.tsdb.tasks.ClearDirtyBlockTask;
import cn.rapidtsdb.tsdb.utils.TimeUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TSBlock Logical Manager, File Implementation
 * response for @TSBlock store, search, compress。
 * <p>
 * 启动过程:
 * 1. 恢复检查
 * 1.1 恢复内存数据，从AOL恢复。
 */
@Log4j2
public class TSBlockManager extends AbstractTSBlockManager implements Initializer, Closer {


    private TSDBConfig tsdbConfig;

    private StoreHandler storeHandler;

    private TSBlockPersister blockPersister;


    public static final int BLOCK_SIZE_SECONDS = (int) TimeUnit.HOURS.toSeconds(2);

    private AtomicReference<Map<Integer, TSBlock>> currentBlockCacheRef = new AtomicReference<>();
    private AtomicReference<Map<Integer, TSBlock>> preRoundBlockRef = new AtomicReference<>();
    private AtomicReference<Map<Integer, TSBlock>> forwardRoundBlockRef = new AtomicReference<>();


    /**
     * ???
     * 我也忘了这是啥？
     */
    private Map<Integer, int[]> metricLocationSeparator = new ConcurrentHashMap<>(10240);

    ManagedThreadPool globalExecutor = ManagedThreadPool.getInstance();
    ThreadPoolExecutor ioExecutor = globalExecutor.ioExecutor();

    TSBlockManager(TSDBConfig tsdbConfig) {
        this.tsdbConfig = tsdbConfig;
        storeHandler = StoreHandlerFactory.getStoreHandler();
        blockPersister = TSBlockPersister.getINSTANCE();
    }

    @Override
    public void close() {
        flushMemoryBlock();
    }

    @Override
    public void init() {
        dirtyBlocksRef.set(new HashSet<>());
        preRoundBlockRef.set(newTSMap());
        currentBlockCacheRef.set(newTSMap());
        forwardRoundBlockRef.set(newTSMap());

    }

    public TSBlock getCurrentWriteBlock(int metricId, long timestamp) {

        Map<Integer, TSBlock> currentBlockCache = currentBlockCacheRef.get();
        TSBlock currentBlock = currentBlockCache.get(metricId);
        if (currentBlock == null) {
            currentBlock = newTSBlock(metricId, timestamp);
            TSBlock existedBlocks = currentBlockCache.putIfAbsent(metricId, currentBlock);
            if (existedBlocks != null) {
                currentBlock = existedBlocks;
            }
        }

        if (currentBlock.inBlock(timestamp)) {
            return currentBlock;
        }

        if (currentBlock.afterBlock(timestamp)) {
            TSBlock forwardBlock = forwardRoundBlockRef.get().get(metricId);
            if (forwardBlock != null) {
                return forwardBlock;
            }
            forwardBlock = newTSBlock(metricId, timestamp);

            if (currentBlock.isNextAfjacentBlock(forwardBlock)) {
                forwardBlock = forwardRoundBlockRef.get().putIfAbsent(metricId, forwardBlock);
                return forwardBlock;
            } else {
                log.error("Refused to Write too much ahead of time data, metricId:{}, write time:{}, currentTime:{}",
                        metricId,
                        timestamp, currentBlock.getBaseTime());
                return null;
            }
        } else {
            if (tsdbConfig.getAllowOverwrite()) {
                //Warning, if Code running here, Too Much Memory could be used.
                TSBlock preBlock = preRoundBlockRef.get().get(metricId);
                if (preBlock == null || !preBlock.inBlock(timestamp)) {
                    preBlock = newTSBlock(metricId, timestamp);
                }
                markDirtyBlock(preBlock);
                return preBlock;
            }

        }
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


    @Override
    public void triggerRoundCheck(Runnable completedCallback) {
        preRoundBlockRef.set(currentBlockCacheRef.get());
        blockPersister.persistTSBlockSync(currentBlockCacheRef.get());
        currentBlockCacheRef.set(forwardRoundBlockRef.get());
        forwardRoundBlockRef.set(newTSMap());
        blockPersister.persistTSBlockAsync(preRoundBlockRef.get());
        Set<TSBlock> dirtyBlock = dirtyBlocksRef.get();
        dirtyBlocksRef.set(new HashSet<>());
        ioExecutor.submit(new ClearDirtyBlockTask(dirtyBlock, blockPersister));
    }

    public List<TSBlock> getBlockWithTimeRange(int metricId, long start, long end) {
        List<TSBlock> tsBlocks = blockPersister.getTSBlocks(metricId, start, end);
        return tsBlocks;
    }

    public Iterator<TSBlock> getBlockStreamByTimeRange(int metricId, long start, long end) {
        return null;
    }

    private void flushMemoryBlock() {
        blockPersister.persistTSBlockSync(currentBlockCacheRef.get());
    }

    /**
     * this most important data struct of the time series data
     *
     * @return
     */
    public Map<Integer, TSBlock> newTSMap() {
        return new ConcurrentHashMap<Integer, TSBlock>();
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
//                            snapshot.getTsBlock().markPersist();
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

    /**
     * write tsblock into a compressed file.
     * file seek invovled
     */
    static class OldTsBlockUpdateTask implements Runnable {

        private int metricId;
        private TSBlockSnapshot blockSnapshot;

        public OldTsBlockUpdateTask(int metricId, TSBlockSnapshot blockSnapshot) {
            this.metricId = metricId;
            this.blockSnapshot = blockSnapshot;
        }

        @Override
        public void run() {

        }
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


    private static class IOLock {
        private static ConcurrentHashMap<Integer, Lock> mLocks = new ConcurrentHashMap<>();

        public static Lock getMetricLock(int metric) {
            if (!mLocks.contains(metric)) {
                mLocks.putIfAbsent(metric, new ReentrantLock());
            }
            return mLocks.get(metric);
        }
    }

}
