package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.io.TSBlockSerializer;
import cn.rapidtsdb.tsdb.core.persistent.TSBlockPersister;
import cn.rapidtsdb.tsdb.core.persistent.file.FileLocation;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.store.StoreHandler;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import cn.rapidtsdb.tsdb.utils.TimeUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

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

    private TSBlockTime tsTime = new TSBlockTime();

    private static final int BLOCK_SIZE_SECONDS = 2 * 60;

    private AtomicReference<Map<Integer, TSBlock>> currentBlockCacheRef = new AtomicReference<>();

    //waiting the later data
    private AtomicReference<Map<Integer, TSBlock>> preBlock = new AtomicReference<>();

    private AtomicReference<Map<Integer, TSBlock>> forwardBlock = new AtomicReference<>();

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
        currentBlockCacheRef.set(newTSMap());


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

            currentBlock = nextTSBlock(metricId, timestamp);
            currentBlockCache.put(metricId, currentBlock);
            return currentBlock;
        }

        TSBlock lastBlock = preBlock.get().get(metricId);
        if (lastBlock != null && lastBlock.inBlock(timestamp)) {
            return lastBlock;
        }
        return null;
    }


    public TSBlock nextTSBlock(int metricId, long timestamp) {
        TSBlock tsBlock = new TSBlock(tsTime.getCurrentBlockTimeSeconds() + BLOCK_SIZE_SECONDS, BLOCK_SIZE_SECONDS, TimeUtils.TimeUnitAdaptorFactory.DEFAULT);
        if (tsBlock.inBlock(timestamp)) {
            return tsBlock;
        } else {
            return null;
        }
    }


    @Override
    public void triggerPersist(Runnable completedCallback) {
        Map<Integer, TSBlock> newRoundTSMap = newTSMap();
        Map<Integer, TSBlock> old = currentBlockCacheRef.get();
        currentBlockCacheRef.compareAndSet(old, newRoundTSMap);
        ioExecutor.submit(() -> {
            blockPersister.persistTSBlock(old);
            completedCallback.run();
        });
    }

    public List<TSBlock> getBlockWithTimeRange(int metricId, long start, long end) {
        List<TSBlock> tsBlocks = blockPersister.getTSBlocks(metricId, start, end);
        return tsBlocks;
    }

    public Iterator<TSBlock> getBlockStreamByTimeRange(int metricId, long start, long end) {
        return null;
    }

    private void flushMemoryBlock() {
        blockPersister.persistTSBlock(currentBlockCacheRef.get());
    }

    public Map<Integer, TSBlock> newTSMap() {
        return new ConcurrentHashMap<Integer, TSBlock>();
    }


}
