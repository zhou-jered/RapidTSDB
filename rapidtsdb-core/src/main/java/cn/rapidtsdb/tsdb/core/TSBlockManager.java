package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.TSDBTaskCallback;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.persistent.TSBlockPersister;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.store.StoreHandler;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import cn.rapidtsdb.tsdb.tasks.ClearDirtyBlockTask;
import cn.rapidtsdb.tsdb.utils.TSBlockUtils;
import cn.rapidtsdb.tsdb.utils.TimeUtils;
import lombok.extern.log4j.Log4j2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import static cn.rapidtsdb.tsdb.core.TSBlockFactory.newTSBlock;

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

            if (currentBlock.isNextAjacentBlock(forwardBlock)) {
                forwardBlock = forwardRoundBlockRef.get().putIfAbsent(metricId, forwardBlock);
                return forwardBlock;
            } else {
                log.error("Refused to Write too much ahead of time data, metricId:{}, write time:{}, currentTime:{}", metricId, timestamp, currentBlock.getBaseTime());
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


    @Override
    public void triggerRoundCheck(TSDBTaskCallback completedCallback) {
        preRoundBlockRef.set(currentBlockCacheRef.get());
        blockPersister.persistTSBlockAsync(currentBlockCacheRef.get(), completedCallback);
        currentBlockCacheRef.set(forwardRoundBlockRef.get());
        forwardRoundBlockRef.set(newTSMap());
        blockPersister.persistTSBlockAsync(preRoundBlockRef.get(), completedCallback);
        Set<TSBlock> dirtyBlock = dirtyBlocksRef.get();
        if (dirtyBlock.size() > 0) {
            dirtyBlocksRef.set(new HashSet<>());
            ioExecutor.submit(new ClearDirtyBlockTask(dirtyBlock, blockPersister));
        }
    }

    public List<TSBlock> getBlockWithTimeRange(int metricId, long start, long end) {
        List<TSBlock> tsBlocks = blockPersister.getTSBlocks(metricId, start, end);
        TSBlock preRoundBlock = preRoundBlockRef.get().get(metricId);
        TSBlock thisRoundBlock = currentBlockCacheRef.get().get(metricId);
        TSBlock forwordRoundBlock = forwardRoundBlockRef.get().get(metricId);

        if (preRoundBlock != null && preRoundBlock.isDirty()) {
            tryMergeInListBlock(preRoundBlock, tsBlocks, start, end);
        }
        if (thisRoundBlock != null) {
            tryMergeInListBlock(thisRoundBlock, tsBlocks, start, end);
        }
        if (forwordRoundBlock != null) {
            tryMergeInListBlock(forwordRoundBlock, tsBlocks, start, end);
        }
        return tsBlocks;
    }

    private void tryMergeInListBlock(TSBlock newerBlock, List<TSBlock> oldersBlocks, long startSec, long endSec) {
        long basetime = newerBlock.getBaseTime();
        if (basetime >= startSec && basetime <= endSec) {
            boolean inList = false;
            for (int i = 0; i < oldersBlocks.size(); i++) {
                TSBlock block = oldersBlocks.get(i);
                if (basetime == block.getBaseTime()) {
                    inList = true;
                    TSBlock mergedBlock = TSBlockUtils.orderedMassiveMerge(block, newerBlock);
                    oldersBlocks.set(i, mergedBlock);
                }
            }
            if (!inList) {
                boolean inserted = false;
                for (int i = oldersBlocks.size() - 1; i > 0; i--) {
                    if (basetime < oldersBlocks.get(i).getBaseTime() &&
                            basetime > oldersBlocks.get(i - 1).getBaseTime()) {
                        oldersBlocks.add(i, newerBlock);
                        inserted = true;
                        break;
                    }
                }
                if (!inserted) {
                    oldersBlocks.add(newerBlock);
                }
            }
        }
    }

    public Iterator<TSBlock> getBlockStreamByTimeRange(int metricId, long start, long end) {
        return null;
    }

    private void flushMemoryBlock() {
        blockPersister.persistTSBlockSync(currentBlockCacheRef.get());
    }


    @Override
    public void tryRecoveryMemoryData(List<Integer> metricsIdList) {
        log.info("Start");
        long currentSeconds = TimeUtils.currentSeconds();
        long basetime = TimeUtils.getBlockBaseTimeSeconds(currentSeconds);
        for (Integer mid : metricsIdList) {
            TSBlock tsBlock = blockPersister.getTSBlock(mid, basetime);
            if (tsBlock != null) {
                tsBlock.afterRecovery();
                currentBlockCacheRef.get().put(mid, tsBlock);
            }
        }
        log.info("Success");
    }

    /**
     * this most important data struct of the time series data
     *
     * @return
     */
    private Map<Integer, TSBlock> newTSMap() {
        return new ConcurrentHashMap<Integer, TSBlock>();
    }


}
