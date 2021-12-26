package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.TSDBTaskCallback;
import cn.rapidtsdb.tsdb.common.TimeUtils;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.persistent.TSBlockPersister;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.utils.CollectionUtils;
import cn.rapidtsdb.tsdb.utils.TSBlockUtils;
import lombok.extern.log4j.Log4j2;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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


    private AtomicReference<Map<Integer, TSBlock>> currentBlockCacheRef = new AtomicReference<>();
    private AtomicReference<Map<Integer, TSBlock>> forwardRoundBlockRef = new AtomicReference<>();
    private TSBlockPersister blockPersister;

    private DirtyBlockManager dirtyBlockManager = DirtyBlockManager.getINSTANCE();


    TSBlockManager(TSDBConfig tsdbConfig) {
        this.tsdbConfig = tsdbConfig;
        blockPersister = TSBlockPersister.getINSTANCE();
    }

    @Override
    public void close() {
        flushMemoryBlock();
        dirtyBlockManager.doClearDirtyBlock();
        dirtyBlockManager.close();
        blockPersister.close();
    }

    @Override
    public void init() {
        blockPersister.init();
        dirtyBlockManager.init();
        currentBlockCacheRef.set(newTSMap());
        forwardRoundBlockRef.set(newTSMap());
    }


    public TSBlock getTargetWriteBlock(int metricId, long timestamp) {

        Map<Integer, TSBlock> currentBlockCache = currentBlockCacheRef.get();
        TSBlock currentBlock = currentBlockCache.get(metricId);
        if (currentBlock == null) {
            log.debug("Create new TSBlock of timestamp:{}", timestamp);
            currentBlock = newTSBlock(metricId, timestamp);
            TSBlock possiblePersistedBlock = blockPersister.getTSBlock(metricId, currentBlock.getBaseTime());
            if (possiblePersistedBlock != null) {
                currentBlock = TSBlockUtils.mergeBlocks(currentBlock, possiblePersistedBlock);
            }
            TSBlock existedBlocks = currentBlockCache.putIfAbsent(metricId, currentBlock);
            if (existedBlocks != null) {
                currentBlock = existedBlocks;
            }
        }
        if (currentBlock.inBlock(timestamp)) {
            return currentBlock;
        }

        TSBlock forwardBlock = forwardRoundBlockRef.get().get(metricId);
        if (forwardBlock != null) {
            if (forwardBlock.inBlock(timestamp)) {
                return forwardBlock;
            }
        } else if (currentBlock.inNextBlock(timestamp)) {
            forwardBlock = newTSBlock(metricId, timestamp);
            forwardRoundBlockRef.get().put(metricId, forwardBlock);
            return forwardBlock;
        }

        TSBlock dirtyBlock = dirtyBlockManager.getDirtyBlockForWrite(metricId, timestamp);
        return dirtyBlock;
    }


    @Override
    public void triggerRoundCheck(TSDBTaskCallback completedCallback) {
        log.debug("Block Round check");
        Map<Integer, TSBlock> current = currentBlockCacheRef.get();
        currentBlockCacheRef.set(forwardRoundBlockRef.get());
        forwardRoundBlockRef.set(newTSMap());
        blockPersister.persistTSBlockAsync(current, completedCallback);
        dirtyBlockManager.doClearDirtyBlock();
    }


    public List<TSBlock> getBlockWithTimeRange(int metricId, long start, long end) {
        List<TSBlock> tsBlocks = blockPersister.getTSBlocks(metricId, start, end);
        TSBlock thisRoundBlock = currentBlockCacheRef.get().get(metricId);
        TSBlock forwordRoundBlock = forwardRoundBlockRef.get().get(metricId);
        if (thisRoundBlock != null) {
            tryMergeInListBlock(thisRoundBlock, tsBlocks, start, end);
        }
        if (forwordRoundBlock != null) {
            tryMergeInListBlock(forwordRoundBlock, tsBlocks, start, end);
        }
        List<TSBlock> dirtyBlocks = dirtyBlockManager.searchDirtyBlockForRead(metricId, start, end);
        if (CollectionUtils.isNotEmpty(dirtyBlocks)) {
            for (TSBlock block : dirtyBlocks) {
                tryMergeInListBlock(block, tsBlocks, start, end);
            }
        }
        return tsBlocks;
    }

    private void tryMergeInListBlock(TSBlock newerBlock, List<TSBlock> oldersBlocks, long start, long end) {
        long basetime = newerBlock.getBaseTime();

        if (newerBlock.haveDataInRange(start, end)) {
            boolean inList = false;
            for (int i = 0; i < oldersBlocks.size(); i++) {
                TSBlock block = oldersBlocks.get(i);
                if (basetime == block.getBaseTime()) {
                    inList = true;
                    TSBlock mergedBlock = TSBlockUtils.orderedMassiveMerge(block, newerBlock);
                    oldersBlocks.set(i, mergedBlock);
                    break;
                }
            }
            if (!inList) {
                boolean inserted = false;
                for (int i = oldersBlocks.size() - 1; i > 0; i--) {
                    if (basetime < oldersBlocks.get(i).getBaseTime() && basetime > oldersBlocks.get(i - 1).getBaseTime()) {
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
        log.info("Start Recovery Data");
        long currentSeconds = TimeUtils.currentSeconds();
        long basetime = TimeUtils.getBlockBaseTime(currentSeconds);
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
