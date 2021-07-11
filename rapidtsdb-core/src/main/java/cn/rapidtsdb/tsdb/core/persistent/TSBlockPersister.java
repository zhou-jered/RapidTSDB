package cn.rapidtsdb.tsdb.core.persistent;

import cn.rapidtsdb.tsdb.TSDBTaskCallback;
import cn.rapidtsdb.tsdb.TsdbRunnableTask;
import cn.rapidtsdb.tsdb.core.TSBlock;
import cn.rapidtsdb.tsdb.core.TSBlockMeta;
import cn.rapidtsdb.tsdb.core.TSBlockSnapshot;
import cn.rapidtsdb.tsdb.core.io.IOLock;
import cn.rapidtsdb.tsdb.core.io.TSBlockDeserializer;
import cn.rapidtsdb.tsdb.core.io.TSBlockDeserializer.TSBlockAndMeta;
import cn.rapidtsdb.tsdb.core.io.TSBlockSerializer;
import cn.rapidtsdb.tsdb.core.persistent.file.FileLocation;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.plugins.StoreHandlerPlugin;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import cn.rapidtsdb.tsdb.utils.TSBlockUtils;
import cn.rapidtsdb.tsdb.utils.TimeUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import static cn.rapidtsdb.tsdb.core.AbstractTSBlockManager.createTSBlockMeta;

/**
 * The Logical TSBlock Persister
 * maintain the metricId with the metric data,
 * no metric name knowledge here
 */
@Log4j2
public class TSBlockPersister implements Initializer, Closer {

    private ThreadPoolExecutor ioExecutor = ManagedThreadPool.getInstance().ioExecutor();
    private static TSBlockPersister INSTANCE = null;
    private StoreHandlerPlugin storeHandler;
    private TSBlockDeserializer blockReader = new TSBlockDeserializer();

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
        if (tsBlocks != null) {
            Map<Integer, TSBlock> writingBlocks = new HashMap<>(tsBlocks);
            for (Integer metricId : writingBlocks.keySet()) {
                TSBlockSnapshot blockSnapshot = writingBlocks.get(metricId).snapshot();
                SimpleTSBlockStoreTask simpleTSBlockStoreTask = new SimpleTSBlockStoreTask(metricId, blockSnapshot, storeHandler, null);
                simpleTSBlockStoreTask.run();
            }
        }
    }

    public void persistTSBlockAsync(Map<Integer, TSBlock> tsBlocks, TSDBTaskCallback completeCallback) {
        SingleTaskPersistCallback persisterTaskCallback = new SingleTaskPersistCallback(completeCallback, tsBlocks.size());
        Map<Integer, TSBlock> writingBlocks = new HashMap<>(tsBlocks);
        for (Integer metricId : writingBlocks.keySet()) {
            TSBlockSnapshot blockSnapshot = writingBlocks.get(metricId).snapshot();
            SimpleTSBlockStoreTask simpleTSBlockStoreTask = new SimpleTSBlockStoreTask(metricId, blockSnapshot, storeHandler, persisterTaskCallback);
            ioExecutor.submit(simpleTSBlockStoreTask);
        }
    }

    public TSBlock getTSBlock(Integer metricId, long timeSeconds) {
        long blockBaseTime = TimeUtils.getBlockBaseTimeSeconds(timeSeconds);
        FileLocation fl = FilenameStrategy.getTodayFileLocation(metricId, blockBaseTime);
        if (!storeHandler.fileExisted(fl.getPathWithFilename())) {
            fl = FilenameStrategy.getDailyFileLocation(metricId, blockBaseTime);
            //if request data is in today range, check for quick return
            if (Math.abs(TimeUtils.currentSeconds() - timeSeconds) < TimeUnit.DAYS.toSeconds(1)) {
                return null;
            }
        }
        if (!storeHandler.fileExisted(fl.getPathWithFilename())) {
            fl = FilenameStrategy.getMonthlyFileLocation(metricId, blockBaseTime);
        }
        if (!storeHandler.fileExisted(fl.getPathWithFilename())) {
            //todo, yearly block may be too large to be held in memory, using a stream with downsampler to handle it
            fl = FilenameStrategy.getyearlyFileLocation(metricId, blockBaseTime);
        }

        if (storeHandler.fileExisted(fl.getPathWithFilename())) {
            try {
                InputStream inputStream = storeHandler.openFileInputStream(fl.getPathWithFilename());
                TSBlockAndMeta blockAndMeta = blockReader.deserializeFromStream(inputStream);
                return blockAndMeta.getData();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    public ArrayList<TSBlock> getTSBlocks(Integer metricId, long timeSecondsStart, long timeSecondsEnd) {
        ArrayList<TSBlock> blocks = new ArrayList<>((int) ((timeSecondsEnd - timeSecondsStart) / TimeUnit.HOURS.toSeconds(2) + 1));
        long baseTime = TimeUtils.getBlockBaseTimeSeconds(timeSecondsStart);
        for (long t = baseTime; t <= timeSecondsEnd; t += TimeUnit.HOURS.toSeconds(2)) {
            TSBlock b = getTSBlock(metricId, t);
            if (b != null) {
                blocks.add(b);
            }
        }
        return blocks;
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


    private static class SingleTaskPersistCallback implements TSDBTaskCallback {
        TSDBTaskCallback allCompleteCallback;
        private int subTaskNumber = 0;
        private AtomicInteger successTaskNumber = new AtomicInteger(0);

        public SingleTaskPersistCallback(TSDBTaskCallback allCompleteCallback, int subTaskNumber) {
            this.allCompleteCallback = allCompleteCallback;
            this.subTaskNumber = subTaskNumber;
        }

        @Override
        public Object onSuccess(Object data) {
            final int finishedTaskNumber = successTaskNumber.incrementAndGet();
            if (finishedTaskNumber == subTaskNumber) {
                allCompleteCallback.onSuccess(subTaskNumber);
            }
            return null;
        }


        @Override
        public void onFailed(TsdbRunnableTask task, Object data) {
            if (task.getRetryCount() < task.getRetryLimit()) {
                task.markRetry();
                ManagedThreadPool.getInstance().submitFailedTask(task);
            } else {
                allCompleteCallback.onFailed(task, data);
            }
        }

        @Override
        public void onException(TsdbRunnableTask task, Object data, Throwable exception) {
            if (task.getRetryCount() < task.getRetryLimit()) {
                task.markRetry();
                ManagedThreadPool.getInstance().submitFailedTask(task);
            } else {
                allCompleteCallback.onException(task, data, exception);
            }
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
            return new FileLocation(getDailyDirectory(metric, baseTimeSeconds), getDailyBlockFilename(metric, baseTimeSeconds));
        }

        public static FileLocation getMonthlyFileLocation(int metric, long baseTimeSeconds) {
            return new FileLocation(getMonthlyDirectory(metric, baseTimeSeconds), getMonthBlockFilename(metric, baseTimeSeconds));
        }

        public static FileLocation getyearlyFileLocation(int metric, long baseTimeSeconds) {
            return new FileLocation(getYearyDirectory(metric, baseTimeSeconds), getYearBlockFilename(metric, baseTimeSeconds));
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

        public static String getYearyDirectory(int metric, long baseTimeSeconds) {
            return metric + "/year";
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
    static class SimpleTSBlockStoreTask extends TsdbRunnableTask {
        private final static Logger log = LogManager.getLogger("SimpleTSBlockStoreTask");
        private int metricId;
        private FileLocation fileLocation;
        private TSBlockSnapshot snapshot;
        private StoreHandlerPlugin storeHandler;
        private TSDBTaskCallback<TSBlockAndMeta, Void> completeCallback;
        private static final TSBlockSerializer blockWriter = new TSBlockSerializer();


        @Override
        public String getTaskName() {
            return String.format("SimpleTSBlockStoreTask:%s:%s", metricId, snapshot.getTsBlock().getBaseTime());
        }

        public SimpleTSBlockStoreTask(int metricId, TSBlockSnapshot snapshot, StoreHandlerPlugin storeHandler, TSDBTaskCallback completeCallback) {
            this.metricId = metricId;
            this.fileLocation = FilenameStrategy.getTodayFileLocation(metricId, snapshot.getTsBlock().getBaseTime());
            this.snapshot = snapshot;
            this.storeHandler = storeHandler;
            this.completeCallback = completeCallback;
        }

        @Override
        public void run() {
            TSBlockMeta blockMeta = createTSBlockMeta(snapshot, metricId);
            TSBlockAndMeta taskData = new TSBlockAndMeta(blockMeta, snapshot.getTsBlock());
            if (log.isDebugEnabled()) {
                log.debug("Store {}, dpsSize:{}, md5:{}, file:{}", blockMeta.getBaseTime(), blockMeta.getDpsSize(), blockMeta.getMd5Checksum(), fileLocation);
            }
            final String fullFilename = fileLocation.getPathWithFilename();
            if (storeHandler.fileExisted(fullFilename)) {
                log.warn("Store {}, file already Existed. Overrided", blockMeta.getSimpleInfo());
                try {
                    InputStream existsBlockIP = storeHandler.openFileInputStream(fullFilename);
                    TSBlockDeserializer blockReader = new TSBlockDeserializer();
                    TSBlockAndMeta existedBlock = blockReader.deserializeFromStream(existsBlockIP);
                    TSBlock mergedBlock = TSBlockUtils.mergeStoredBlockWithMemoryBlock(existedBlock, snapshot);
                    TSBlockSnapshot mergedSnapshot = mergedBlock.snapshot();
                    blockMeta = createTSBlockMeta(mergedSnapshot, metricId);
                    taskData = new TSBlockAndMeta(blockMeta, mergedBlock);
                    log.info("Merge TSBlock", fullFilename);
                } catch (IOException e) {
                    log.error("Read " + fullFilename + " EX", e);
                    log.error("skip block merge");
                }

            }
            try {
                log.debug("{} start write:{}", metricId, fileLocation);

                Lock metricLock = IOLock.getMetricLock(metricId);

                try (OutputStream outputStream = storeHandler.openFileOutputStream(fullFilename);) {
                    if (metricLock.tryLock(3, TimeUnit.SECONDS)) {
                        blockWriter.serializeToStream(blockMeta.getMetricId(), snapshot, outputStream);
                        snapshot.getTsBlock().markVersionClear(snapshot.getDataVersion());
                        if (completeCallback != null) {
                            completeCallback.onSuccess(taskData);
                        }
                        log.debug("{} write :{}, completed", metricId, fileLocation);
                    } else {
                        log.warn("metric IOLock failed: {}", metricId);
                        if (completeCallback != null) {
                            completeCallback.onFailed(this, taskData);
                        }
                    }
                } catch (InterruptedException e) {
                    if (completeCallback != null) {
                        completeCallback.onException(this, taskData, e);
                    }
                } finally {

                    metricLock.unlock();
                }

            } catch (IOException e) {
                e.printStackTrace();
                log.error("Write File {} Exception", fileLocation.getPathWithFilename(), e);
                if (completeCallback != null) {
                    TSBlockAndMeta failedData = new TSBlockAndMeta(blockMeta, snapshot.getTsBlock());
                    completeCallback.onException(this, failedData, e);
                }
            }
        }


        @Override
        public int getRetryLimit() {
            return 10;
        }
    }


}
