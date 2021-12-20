package cn.rapidtsdb.tsdb.store;

import cn.rapidtsdb.tsdb.core.io.IOLock;
import cn.rapidtsdb.tsdb.core.persistent.file.FileLocation;
import cn.rapidtsdb.tsdb.plugins.BlockStoreHandlerPlugin;
import cn.rapidtsdb.tsdb.plugins.FileStoreHandlerPlugin;
import cn.rapidtsdb.tsdb.plugins.PluginManager;
import cn.rapidtsdb.tsdb.plugins.polo.BlockPojo;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

@Log4j2
public class DefaultBlockStoreHandler implements BlockStoreHandlerPlugin {

    FileStoreHandlerPlugin fileStoreHandler;

    @Override
    public boolean blockExists(int metricid, long basetime) {
        FileLocation fileLocation = FilenameStrategy.getFileLocation(metricid, basetime);
        final String fullFilename = fileLocation.getPathWithFilename();
        return fileStoreHandler.fileExisted(fullFilename);
    }

    @Override
    public void storeBlock(int metricId, long baseTime, byte[] data) {
        FileLocation fileLocation = FilenameStrategy.getFileLocation(metricId, baseTime);
        final String fullFilename = fileLocation.getPathWithFilename();
        try {
            log.debug("{} start write:{}", metricId, fileLocation);
            Lock metricLock = IOLock.getMetricLock(metricId);
            try (OutputStream outputStream = fileStoreHandler.openFileOutputStream(fullFilename);) {
                if (metricLock.tryLock(3, TimeUnit.SECONDS)) {
                    outputStream.write(data);
                    log.debug("{} write :{}, completed", metricId, fileLocation);
                } else {
                    log.warn("metric IOLock failed: {}", metricId);
                }
            } catch (InterruptedException e) {
            } finally {
                metricLock.unlock();
            }

        } catch (IOException e) {
            e.printStackTrace();
            log.error("Write File {} Exception", fileLocation.getPathWithFilename(), e);
        }
    }

    @Override
    public byte[] getBlockData(int metricId, long baseTime) {
        FileLocation fileLocation = FilenameStrategy.getFileLocation(metricId, baseTime);
        String fn = fileLocation.getPathWithFilename();
        if (!fileStoreHandler.fileExisted(fn)) {
            return null;
        }
        try (InputStream inputStream = fileStoreHandler.openFileInputStream(fn);) {
            return IOUtils.toByteArray(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    @Override
    public BlockPojo[] multiGetBlock(int metricId, Iterator<Long> timeScanner) {
        BlockPojo[] result = new BlockPojo[0];
        List<BlockPojo> dynamicResult = new LinkedList<>();
        while (timeScanner.hasNext()) {
            long t = timeScanner.next();
            byte[] blockBytes = getBlockData(metricId, t);
            if (blockBytes != null) {
                BlockPojo bp = new BlockPojo(metricId, t, blockBytes);
                dynamicResult.add(bp);
            }
        }
        result = dynamicResult.toArray(result);
        return result;
    }

    @Override
    public BlockPojo[] multiGetBlock(int[] metricIds, long basetime) {
        List<BlockPojo> dynamicArray = new LinkedList<>();
        for (int mid : metricIds) {
            byte[] blockData = getBlockData(mid, basetime);
            if (blockData != null) {
                BlockPojo bp = new BlockPojo(mid, basetime, blockData);
                dynamicArray.add(bp);
            }
        }
        BlockPojo[] result = dynamicArray.toArray(new BlockPojo[0]);
        return result;
    }

    @Override
    public Map<Integer, BlockPojo[]> multiGetBlock(int[] metricIds, Iterator<Long> timeScanner) {
        Map<Integer, BlockPojo[]> result = new HashMap<>();
        for (int mid : metricIds) {
            result.put(mid, multiGetBlock(mid, timeScanner));
        }
        return result;
    }

    @Override
    public Stream<BlockPojo> scanBlocks(int metricId, Iterator<Long> timeScanner) {
        throw new UnsupportedOperationException("todo, call zjy implement this function");
    }

    @Override
    public Stream<Map<Integer, BlockPojo>> crossScanBlocks(int[] metricId, Iterator<Long> timeScanner) {
        throw new UnsupportedOperationException("todo, call zjy implement this function");
    }

    @Override
    public String getInterestedPrefix() {
        return null;
    }

    @Override
    public void config(Map<String, String> subConfig) {

    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public void prepare() {
        fileStoreHandler = PluginManager.getPlugin(FileStoreHandlerPlugin.class);
    }


    static class FilenameStrategy {

        public static FileLocation getFileLocation(int metric, long baseTimeSeconds) {
            return new FileLocation(getDirectory(metric, baseTimeSeconds), getTodayBlockFilename(metric, baseTimeSeconds));
        }

        public static String getDirectory(int metric, long baseTimeSecconds) {
            return String.valueOf(metric);
        }

        public static String getTodayBlockFilename(int metricId, long baseTimeSeconds) {
            return "T" + metricId + ":" + baseTimeSeconds + ".data";
        }

    }
}
