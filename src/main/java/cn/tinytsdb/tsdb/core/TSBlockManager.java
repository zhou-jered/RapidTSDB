package cn.tinytsdb.tsdb.core;

import cn.tinytsdb.tsdb.config.MetricConfig;
import cn.tinytsdb.tsdb.config.TSDBConfig;
import cn.tinytsdb.tsdb.core.persistent.Persistently;
import cn.tinytsdb.tsdb.lifecycle.Closer;
import cn.tinytsdb.tsdb.lifecycle.Initializer;
import cn.tinytsdb.tsdb.store.StoreHandler;
import cn.tinytsdb.tsdb.store.StoreHandlerFactory;
import cn.tinytsdb.tsdb.utils.BinaryUtils;
import cn.tinytsdb.tsdb.utils.TimeUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import sun.reflect.generics.reflectiveObjects.LazyReflectiveObjectGenerator;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.SocketHandler;

/**
 * TSBlock Logical Manager
 * response for @TSBlock store, search, compress
 */
@Log4j2
public class TSBlockManager implements Persistently, Initializer, Closer {

    private final static String METRIC_LOCATION_SEPARATOR_FILE = "mp.idx";

    TSDBConfig tsdbConfig;

    StoreHandler storeHandler;

    private static final TimeUtils.TimeUnitAdaptor TIME_UNIT_ADAPTOR_SECONDS = TimeUtils.ADAPTER_SECONDS;
    private static final int BLOCK_SIZE_SECONDS = 2 * 60;

    private Map<Integer, TSBlock> currentBlockCache = new ConcurrentHashMap<>(10240);
    private Map<Integer, int[]> metricLocationSeparator = new ConcurrentHashMap<>(10240);

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
    public void persistTSBlock(TSBlock tsBlock) {
        TSBlockMeta blockMeta = createTSBlockMeta(tsBlock);

    }

    public List<TSBlock> getBlockWithTimeRange(int metricId, long start, long end) {
        return null;
    }

    public Iterator<TSBlock> getBlockStreamByTimeRange(int metricId, long start, long end) {
        return null;
    }

    private TSBlockMeta createTSBlockMeta(TSBlock tsBlock) {
        tsBlock.frzeeWrite();
        TSBlockMeta blockMeta = new TSBlockMeta();
        blockMeta.setBaseTime(TIME_UNIT_ADAPTOR_SECONDS.adapt(tsBlock.getBaseTime()));
        blockMeta.setDpsSize(tsBlock.getDataPoints().size());
        blockMeta.setTimeBitsLen(tsBlock.getTime().getTotalBitsLength());
        blockMeta.setValuesBitsLen(tsBlock.getValues().getTotalBitsLength());

        try {
            MessageDigest messageDigest = MessageDigest.getInstance("md5");
            messageDigest.update(tsBlock.getTime().getData(), 0, tsBlock.getTime().getBytesOffset());
            messageDigest.update(tsBlock.getValues().getData(), 0, tsBlock.getValues().getBytesOffset());
            byte[] bs = messageDigest.digest();
            String md5 = BinaryUtils.hexBytes(bs);
            blockMeta.setMd5Checksum(md5);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return blockMeta;

    }


}
