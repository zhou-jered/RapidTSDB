package cn.rapidtsdb.tsdb.core.persistent;

import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.store.StoreHandler;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import com.google.common.primitives.Longs;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class AppendOnlyLogManager implements Initializer, Closer {

    private AtomicLong logIdx = new AtomicLong(0);
    private final String AOL_FILE = "aol.data";
    private final String LOG_START_IDX_FILE = "aol.start.idx"; // the logical log offset of the file start
    private final String LOG_END_IDX_FILE = "aol.end.idx"; // the logical log length
    private volatile boolean initialized = false;
    private StoreHandler storeHandler;
    private final int MAX_WRITE_LENGTH = 1024 * 1024 * 1024 / AOLog.SERIES_BYTES_LENGTH;

    private RandomAccessFile seekableAolFile = null;
    private FileChannel writeChannel;
    private int writeLength = 0;

    private static AppendOnlyLogManager instance = null;

    public static AppendOnlyLogManager getInstance() {
        if (instance == null) {
            instance = new AppendOnlyLogManager();
        }
        return instance;
    }

    @Override
    public void init() {
        if (initialized) {
            return;
        }
        initialized = true;
        storeHandler = StoreHandlerFactory.getStoreHandler();
        try {
            File aolFile = storeHandler.getFile(AOL_FILE);
            seekableAolFile = new RandomAccessFile(aolFile, "rw");
            writeChannel = seekableAolFile.getChannel();
            long idxVal = getLogFileIdx(LOG_END_IDX_FILE);
            logIdx.set(idxVal);
            long startIdx = getLogFileIdx(LOG_START_IDX_FILE);
            writeLength = (int) (idxVal - startIdx + 1);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("GET AOLOG OUTPUT EX", e);
            throw new RuntimeException(e);
        }
    }


    public void appendLog(AOLog alog) {

        try {
            currentLogOs.write(alog.getSeri());
            if (logIdx.incrementAndGet() % 2730 == 0) {
                currentLogOs.flush(); // flush disk file
            }
        } catch (IOException e) {
            try {
                currentLogOs = storeHandler.openFileAppendStream(AOL_FILE);
                currentLogOs.write(alog.getSeri());
            } catch (IOException ioException) {
                ioException.printStackTrace();
                log.error("AOL EX", ioException);
            }
        }
    }

    public long getLogIndex() {
        return logIdx.get();
    }

    public void appendLog(int idx, long timestamp, double val) {
        appendLog(new AOLog(idx, timestamp, val));
    }

    public AOLog[] recoverLog(long offset) {
        try {
            if (storeHandler.fileExisted(AOL_FILE)) {
                InputStream inputStream = storeHandler.openFileInputStream(AOL_FILE);
                List<AOLog> aoLogList = new ArrayList<>(inputStream.available() / AOLog.SERIES_BYTES_LENGTH);
                byte[] series = new byte[AOLog.SERIES_BYTES_LENGTH];
                while (inputStream.available() > AOLog.SERIES_BYTES_LENGTH) {
                    inputStream.read(series);
                    AOLog aoLog = AOLog.fromSeries(series);
                    aoLogList.add(aoLog);
                }
                log.info("Recover AOL Length : {}", aoLogList.size());
                AOLog[] logArray = new AOLog[aoLogList.size()];
                aoLogList.toArray(logArray);
                return logArray;
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("READ AOL EX", e);

            throw new RuntimeException(e);
        }
        return null;
    }

    private void reinit() {
        initialized = false;
        this.close();
        this.init();
    }

    private void rollingFile() {

    }

    private long getLogFileIdx(String idxFilename) {
        if (!storeHandler.fileExisted(idxFilename)) {
            return 0;
        }
        try {
            InputStream fis = storeHandler.openFileInputStream(idxFilename);
            DataInputStream dis = new DataInputStream(fis);
            long fileStartIdx = dis.readLong();
            dis.close();
            fis.close();
            return fileStartIdx;
        } catch (IOException e) {
            e.printStackTrace();
            log.error("", e);
            throw new RuntimeException(e);
        }
    }


    private boolean persistLogIdx() {
        try (OutputStream outputStream = storeHandler.openFileOutputStream(LOG_END_IDX_FILE)) {
            IOUtils.write(Longs.toByteArray(logIdx.get()), outputStream);
            DataOutputStream dos = new DataOutputStream(outputStream);
            dos.writeLong(logIdx.get());
        } catch (IOException e) {
            e.printStackTrace();
            log.error("AOLOG INDEX OUTPUT EX", e);
            return false;
        }
        return true;
    }

    @Override
    public void close() {
        try {
            writeChannel.close();
            persistLogIdx();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @SneakyThrows
    public static void main(String[] args) {

        RandomAccessFile randomAccessFile = new RandomAccessFile("op.log", "rw");
        FileChannel fileChannel = randomAccessFile.getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        int c = 0;
        int writeLen = 0;
        while (true) {
            byte[] bs = ("log-" + c++ + "\n").getBytes();
            byteBuffer.put(bs);
            byteBuffer.flip();
            fileChannel.write(byteBuffer);
            byteBuffer.clear();
            writeLen += bs.length;
            Thread.sleep((long) (Math.random() * 50));
            if (writeLen > 3000) {
                randomAccessFile.seek(0);
                writeLen = 0;
                System.err.println("Reset Zero: c:" + fileChannel.size() + "   f:" + randomAccessFile.length());
            }
            System.out.println("write :" + c);
        }
    }

}
