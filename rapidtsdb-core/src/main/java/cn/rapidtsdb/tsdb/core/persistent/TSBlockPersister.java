package cn.rapidtsdb.tsdb.core.persistent;

import cn.rapidtsdb.tsdb.TSDBTaskCallback;
import cn.rapidtsdb.tsdb.common.TimeUtils;
import cn.rapidtsdb.tsdb.core.QuickBlockDetector;
import cn.rapidtsdb.tsdb.core.TSBlock;
import cn.rapidtsdb.tsdb.core.TSBlockSnapshot;
import cn.rapidtsdb.tsdb.core.io.IOLock;
import cn.rapidtsdb.tsdb.core.io.TSBlockDeserializer;
import cn.rapidtsdb.tsdb.core.io.TSBlockDeserializer.TSBlockAndMeta;
import cn.rapidtsdb.tsdb.core.io.TSBlockSerializer;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.plugins.BlockStoreHandlerPlugin;
import cn.rapidtsdb.tsdb.plugins.FileStoreHandlerPlugin;
import cn.rapidtsdb.tsdb.plugins.PluginManager;
import cn.rapidtsdb.tsdb.tools.BlockBaseTimeScanner;
import cn.rapidtsdb.tsdb.utils.TSBlockUtils;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;

/**
 * The Logical TSBlock Persister
 * maintain the metricId with the metric data,
 * no metric name knowledge here
 */
@Log4j2
public class TSBlockPersister implements Initializer, Closer {

    private ThreadPoolExecutor ioExecutor = ManagedThreadPool.getInstance().ioExecutor();
    private static TSBlockPersister INSTANCE = null;
    private FileStoreHandlerPlugin fileStoreHandler;
    private BlockStoreHandlerPlugin blockStoreHandler;
    private TSBlockDeserializer blockReader = new TSBlockDeserializer();
    private TSBlockSerializer blockWriter = new TSBlockSerializer();
    private QuickBlockDetector quickBlockDetector = new QuickBlockDetector();

    private TSBlockPersister() {

    }

    @Override
    public void init() {
        fileStoreHandler = PluginManager.getPlugin(FileStoreHandlerPlugin.class);
        blockStoreHandler = PluginManager.getPlugin(BlockStoreHandlerPlugin.class);
        quickBlockDetector.init();
    }

    @Override
    public void close() {
        quickBlockDetector.close();
    }

    /**
     * Create or Merge Block
     *
     * @param tsBlocks
     */
    public void persistTSBlockSync(Map<Integer, TSBlock> tsBlocks) {
        if (tsBlocks != null) {
            setQuickBlockFinder(tsBlocks);
            Map<Integer, TSBlock> writingBlocks = new HashMap<>(tsBlocks);
            for (Integer metricId : writingBlocks.keySet()) {
                TSBlockSnapshot blockSnapshot = writingBlocks.get(metricId).snapshot();
                byte[] existed = blockStoreHandler.getBlockData(metricId, blockSnapshot.getTsBlock().getBaseTime());
                if (existed != null) {
                    TSBlockAndMeta blockAndMeta = blockReader.deserializeFromBytes(existed);
                    TSBlock mergedBlock = TSBlockUtils.mergeStoredBlockWithMemoryBlock(blockAndMeta, blockSnapshot);
                    blockSnapshot = mergedBlock.snapshot();
                }
                byte[] data = blockWriter.serializedToBytes(metricId, blockSnapshot);
                Lock ioLock = IOLock.getMetricLock(metricId);
                try {
                    ioLock.lockInterruptibly();
                    blockStoreHandler.storeBlock(metricId, blockSnapshot.getTsBlock().getBaseTime(), data);
                } catch (InterruptedException e) {
                    log.error(e);
                    log.error("write data {} , {} failed, cause {}", metricId, blockSnapshot.getTsBlock().getBaseTime(),
                            e.getClass().getSimpleName());
                } finally {
                    ioLock.unlock();
                }

            }
        }
    }

    public void persistTSBlockAsync(Map<Integer, TSBlock> tsBlocks, TSDBTaskCallback completeCallback) {
        setQuickBlockFinder(tsBlocks);
        ManagedThreadPool.getInstance().ioExecutor()
                .submit(() -> {
                    persistTSBlockSync(tsBlocks);
                });
    }

    public TSBlock getTSBlock(Integer metricId, long timestamp) {
        long blockBaseTime = TimeUtils.getBlockBaseTime(timestamp);
        if (!quickBlockDetector.blockMayExist(metricId, blockBaseTime)) {
            return null;
        }
        byte[] blockBytes = blockStoreHandler.getBlockData(metricId, blockBaseTime);
        if (blockBytes != null) {
            TSBlockAndMeta blockAndMeta = blockReader.deserializeFromBytes(blockBytes);
            return blockAndMeta.getData();
        }
        return null;

    }

    /**
     * need improve todo
     *
     * @param metricId
     * @param startTimestamp
     * @param endTimestamp
     * @return
     */
    public ArrayList<TSBlock> getTSBlocks(Integer metricId, long startTimestamp, long endTimestamp) {
        BlockBaseTimeScanner timeScanner = new BlockBaseTimeScanner(startTimestamp, endTimestamp);
        ArrayList<TSBlock> blocks = new ArrayList<>(timeScanner.size());
        long t;
        for (; timeScanner.hasNext(); ) {
            t = timeScanner.next();
            if (quickBlockDetector.blockMayExist(metricId, t)) {
                TSBlock b = getTSBlock(metricId, t);
                if (b != null) {
                    blocks.add(b);
                }
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
                }
            }
        }
        return INSTANCE;
    }

    private void setQuickBlockFinder(Map<Integer, TSBlock> blocks) {
        Map<Integer, Long> quickIndex = new HashMap<>();
        blocks.forEach((k, v) -> {
            quickIndex.put(k, v.getBaseTime());
        });
        ManagedThreadPool.getInstance().ioExecutor()
                .submit(() -> {
                    quickIndex.forEach((metricId, basetime) -> {
                        quickBlockDetector.rememberBlock(metricId, basetime);
                    });
                });
    }


}
