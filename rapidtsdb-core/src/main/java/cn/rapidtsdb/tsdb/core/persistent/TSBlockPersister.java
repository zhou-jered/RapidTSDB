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
import cn.rapidtsdb.tsdb.lifecycle.LifeCycleState;
import cn.rapidtsdb.tsdb.plugins.BlockStoreHandlerPlugin;
import cn.rapidtsdb.tsdb.plugins.PluginManager;
import cn.rapidtsdb.tsdb.tools.BlockBaseTimeScanner;
import cn.rapidtsdb.tsdb.utils.CollectionUtils;
import cn.rapidtsdb.tsdb.utils.TSBlockUtils;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import static cn.rapidtsdb.tsdb.lifecycle.LifeCycleState.CLOSED;
import static cn.rapidtsdb.tsdb.lifecycle.LifeCycleState.CREATE;

/**
 * The Logical TSBlock Persister
 * maintain the metricId with the metric data,
 * no metric name knowledge here
 */
@Log4j2
public class TSBlockPersister implements Initializer, Closer {

    private static TSBlockPersister INSTANCE = null;
    private BlockStoreHandlerPlugin blockStoreHandler;
    private TSBlockDeserializer blockReader = new TSBlockDeserializer();
    private TSBlockSerializer blockWriter = new TSBlockSerializer();
    private QuickBlockDetector quickBlockDetector = new QuickBlockDetector();
    private AtomicInteger state = new AtomicInteger(CREATE);

    private TSBlockPersister() {

    }

    @Override
    public void init() {
        if (state.compareAndSet(CREATE, LifeCycleState.INITIALIZING)) {
            blockStoreHandler = PluginManager.getPlugin(BlockStoreHandlerPlugin.class);
            quickBlockDetector.init();
            state.set(LifeCycleState.ACTIVE);
        }
    }

    @Override
    public void close() {
        if (state.get() != CLOSED) {
            quickBlockDetector.close();
            state.set(CLOSED);
        }
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
                persistInternal(metricId, writingBlocks.get(metricId));
            }
        }
    }

    public void persistBlocksSync(int metricId, Collection<TSBlock> blocks) {
        if (CollectionUtils.isEmpty(blocks)) {
            return;
        }
        for (TSBlock block : blocks) {
            persistInternal(metricId, block);
        }
    }

    public void persistBlocksAsync(int metricId, Collection<TSBlock> blocks) {
        ManagedThreadPool.getInstance().ioExecutor().submit(() -> {
            persistBlocksSync(metricId, blocks);
        });
    }

    public void persistTSBlockAsync(Map<Integer, TSBlock> tsBlocks, TSDBTaskCallback completeCallback) {

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
        throw new UnsupportedOperationException("Call RD");
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


    private void persistInternal(int metricId, TSBlock block) {
        TSBlockSnapshot blockSnapshot = block.snapshot();
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
            long blockBaseTime = blockSnapshot.getTsBlock().getBaseTime();
            blockStoreHandler.storeBlock(metricId, blockBaseTime, data);
            quickBlockDetector.rememberBlock(metricId, blockBaseTime);
        } catch (InterruptedException e) {
            log.error(e);
            log.error("write data {} , {} failed, cause {}", metricId, blockSnapshot.getTsBlock().getBaseTime(),
                    e.getClass().getSimpleName());
        } finally {
            ioLock.unlock();
        }
    }


}
