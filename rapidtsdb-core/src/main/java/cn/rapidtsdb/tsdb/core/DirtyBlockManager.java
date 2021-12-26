package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.common.TimeUtils;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.persistent.TSBlockPersister;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.tasks.ClearDirtyBlockTask;
import cn.rapidtsdb.tsdb.utils.TSBlockUtils;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import static cn.rapidtsdb.tsdb.core.TSBlockFactory.newTSBlock;

@Log4j2
public class DirtyBlockManager implements Initializer, Closer {

    /**
     * metricId -> { basetime -> TsBlock}
     */
    protected AtomicReference<Map<Integer, SortedMap<Long, TSBlock>>> dirtyBlocksRef = new AtomicReference<>();
    @Getter
    private static DirtyBlockManager INSTANCE = new DirtyBlockManager();
    private int maxMemoryDirtyBlocks = 64; // default
    private int currentDirtyBlockNumber = 0;
    private ThreadPoolExecutor ioExecutor;
    private TSBlockPersister blockPersister;
    private LinkedList<AtomicReference<Map<Integer, SortedMap<Long, TSBlock>>>> allFlyingDirtyBlocks = new LinkedList<>();

    @Override
    public void init() {
        dirtyBlocksRef.set(new ConcurrentHashMap<>());
        ioExecutor = ManagedThreadPool.getInstance().ioExecutor();
        blockPersister = TSBlockPersister.getINSTANCE();
        blockPersister.init();
        maxMemoryDirtyBlocks = TSDBConfig.getConfigInstance().getMaxMemoryDirtyBlocks();
        log.debug("init max memory dirty blocks {}", maxMemoryDirtyBlocks);
    }

    private DirtyBlockManager() {

    }

    private void checkDirtyBlockShouldFlush() {
        if (currentDirtyBlockNumber < maxMemoryDirtyBlocks) {
            return;
        }
        doClearDirtyBlock();
    }

    public TSBlock getDirtyBlockForWrite(int metricId, long timestamp) {
        long basetime = TimeUtils.getBlockBaseTime(timestamp);
        SortedMap<Long, TSBlock> metricDirtyBlockMap = dirtyBlocksRef.get().get(metricId);
        if (metricDirtyBlockMap == null) {
            dirtyBlocksRef.get().putIfAbsent(metricId, new TreeMap<>());
            metricDirtyBlockMap = dirtyBlocksRef.get().get(metricId);
        }
        TSBlock tsBlock = metricDirtyBlockMap.get(basetime);
        if (tsBlock != null && tsBlock.inBlock(timestamp)) {
            return tsBlock;
        } else {
            tsBlock = newTSBlock(metricId, timestamp);
            currentDirtyBlockNumber++;
            checkDirtyBlockShouldFlush();
            TSBlock mayConflictBlock = metricDirtyBlockMap.put(basetime, tsBlock);
            if (mayConflictBlock != null) {
                log.error("OH NO, Thread management something wrong happened. Dirty Block creation conflicted!");
                tsBlock = mayConflictBlock;
            }
            return tsBlock;
        }
    }

    public List<TSBlock> searchDirtyBlockForRead(int metricId, long start, long end) {
        SortedMap<Long, TSBlock> resultMap = new TreeMap<>();
        if (dirtyBlocksRef.get().containsKey(metricId)) {
            long startBase = TimeUtils.getBlockBaseTime(start);
            long endBase = TimeUtils.getBlockBaseTime(end);
            SortedMap<Long, TSBlock> dirtyBlocks = dirtyBlocksRef.get().get(metricId);
            if (dirtyBlocks != null) {
                dirtyBlocks = dirtyBlocks.subMap(startBase, endBase + 1);
                resultMap.putAll(dirtyBlocks);
            }
        }
        if (!allFlyingDirtyBlocks.isEmpty()) {
            for (AtomicReference<Map<Integer, SortedMap<Long, TSBlock>>> flyingBlocksRef : allFlyingDirtyBlocks) {
                Map<Integer, SortedMap<Long, TSBlock>> flyingBlocks = flyingBlocksRef.get();
                if (flyingBlocks != null) {
                    SortedMap<Long, TSBlock> candidateBlocks = flyingBlocks.get(metricId);
                    if (candidateBlocks != null & candidateBlocks.isEmpty() == false) {
                        for (Long t : candidateBlocks.keySet()) {
                            TSBlock checkingBlock = candidateBlocks.get(t);
                            if (checkingBlock.haveDataInRange(start, end)) {
                                resultMap.compute(t, (ot, old) -> old == null ? checkingBlock :
                                        TSBlockUtils.mergeBlocks(old, checkingBlock));
                            }
                        }
                    }
                }
            }
        }
        return Lists.newArrayList(resultMap.values());
    }

    public void doClearDirtyBlock() {
        synchronized (dirtyBlocksRef) {
            Map<Integer, SortedMap<Long, TSBlock>> dirtyBlock = dirtyBlocksRef.get();
            if (dirtyBlock.size() > 0) {
                log.debug("clear dirty block:{}", currentDirtyBlockNumber);
                dirtyBlock = dirtyBlocksRef.getAndSet(new ConcurrentHashMap<>());
                currentDirtyBlockNumber = 0;
                AtomicReference<Map<Integer, SortedMap<Long, TSBlock>>> flyingBlocks = new AtomicReference<>(dirtyBlock);
                allFlyingDirtyBlocks.add(flyingBlocks);
                ioExecutor.submit(new ClearDirtyBlockTask(flyingBlocks, blockPersister));
            }
        }
    }

    @Override
    public void close() {
        blockPersister.close();
    }
}
