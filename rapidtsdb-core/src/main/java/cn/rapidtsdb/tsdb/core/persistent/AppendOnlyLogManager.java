package cn.rapidtsdb.tsdb.core.persistent;

import cn.rapidtsdb.tsdb.TsdbRunnableTask;
import cn.rapidtsdb.tsdb.app.AppInfo;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.metrics.DBMetrics;
import cn.rapidtsdb.tsdb.plugins.StoreHandlerPlugin;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import cn.rapidtsdb.tsdb.utils.TimeUtils;
import lombok.extern.log4j.Log4j2;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class AppendOnlyLogManager implements Initializer, Closer {

    private AtomicLong logIdx = new AtomicLong(0);
    private final String AOL_FILE = "aol.data";
    private final String LOG_START_IDX_FILE = "aol.start.idx"; // the logical log offset of the file start
    private final String LOG_END_IDX_FILE = "aol.end.idx"; // the logical log length
    private volatile boolean initialized = false;
    private StoreHandlerPlugin storeHandler;
    private int MAX_WRITE_LENGTH = 1024 * 1024 * 1024 / AOLog.SERIES_BYTES_LENGTH;

    private RandomAccessFile seekableAolFile = null;
    private FileChannel writeChannel;
    private int writeLength = 0;
    private WriteLogTask writeLogTask;
    private Thread writeThread;
    private BlockingQueue<AOLog> bufferQ = new LinkedBlockingQueue<>();

    private static AppendOnlyLogManager instance = new AppendOnlyLogManager();

    public static AppendOnlyLogManager getInstance() {
        return instance;
    }

    @Override
    public void init() {
        if (initialized) {
            return;
        }
        initialized = true;
        storeHandler = StoreHandlerFactory.getStoreHandler();
        writeLogTask = new WriteLogTask(bufferQ);
        writeThread = ManagedThreadPool.getInstance().newThread(writeLogTask);
        writeThread.start();

    }

    public void appendLog(AOLog alog) {
        bufferQ.offer(alog);
    }

    public long getLogIndex() {
        return logIdx.get();
    }

    public void appendLog(int metricId, long timestamp, double val) {
        appendLog(new AOLog(metricId, timestamp, val));
    }

    public AOLog[] recoverLog(long offset) {
        final long checkpoint = offset;
        long startIdx = getLogFileIdx(LOG_START_IDX_FILE);
        long endIdx = getLogFileIdx(LOG_END_IDX_FILE);
        

        int logForwardWriteLength = (int) (endIdx - startIdx); // the logical startIdx and endIdx will never distance too long to match int type
        log.debug("Recovered startIdx:{}, endIdx:{}", startIdx, endIdx);
        assert startIdx <= endIdx;
        if (offset > endIdx) {
            log.error("unsatisfied checkpoint offset:{}, maxLogIdx:{}, Sorry you had lost some data.", offset, endIdx);
            return null;
        }

        AOLog[] preRollingLogs = null;
        AOLog[] forwardLogs = null;
        if (checkpoint < startIdx) {
            int preReadLength = (int) (startIdx - checkpoint); // should be safe to cast to int
            long fileByteSize = storeHandler.getFileSize(AOL_FILE);
            if (fileByteSize == MAX_WRITE_LENGTH * AOLog.SERIES_BYTES_LENGTH) {
                if (MAX_WRITE_LENGTH - preReadLength < logForwardWriteLength) {
                    log.error("Sorry for rolling range overwrite. Something wrong happened");
                    return null;
                } else {
                    preRollingLogs = readAoLogFileContent((MAX_WRITE_LENGTH - preReadLength) * AOLog.SERIES_BYTES_LENGTH, preReadLength);
                }
            } else {
                log.error("Sorry you can not read rolling back file content");
            }
            forwardLogs = readAoLogFileContent(0, logForwardWriteLength);
        } else {
            forwardLogs = readAoLogFileContent((int) ((checkpoint - startIdx) * AOLog.SERIES_BYTES_LENGTH), (int) (endIdx - checkpoint));
        }
        if (preRollingLogs == null) {
            return forwardLogs;
        } else if (forwardLogs != null) {
            AOLog[] retLogs = new AOLog[preRollingLogs.length + forwardLogs.length];
            System.arraycopy(preRollingLogs, 0, retLogs, 0, preRollingLogs.length);
            System.arraycopy(forwardLogs, 0, retLogs, preRollingLogs.length, forwardLogs.length);
            return retLogs;
        }
        return null;
    }

    private AOLog[] readAoLogFileContent(int fileOffsetByte, final int readLogSize) {
        if (readLogSize == 0) {
            return null;
        }
        if (!storeHandler.fileExisted(AOL_FILE)) {
            log.error("AOLOG file not exists. data lost");
            return null;
        }
        long fileSize = storeHandler.getFileSize(AOL_FILE);
        long needReadByteSize = fileOffsetByte + readLogSize * AOLog.SERIES_BYTES_LENGTH;
        if (needReadByteSize > fileSize) {
            log.error("Sorry, needReadByteSize:{}  is more than total AOLOG file size:{}.", needReadByteSize, fileSize);
            return null;
        }
        File aolFile = storeHandler.getFile(AOL_FILE);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(aolFile, "r"); FileChannel fileChannel = randomAccessFile.getChannel();) {
            randomAccessFile.seek(fileOffsetByte);
            ByteBuffer byteBuffer = ByteBuffer.allocate(AOLog.SERIES_BYTES_LENGTH);
            AOLog[] aoLogs = new AOLog[readLogSize];
            int i = 0;
            byte[] logSeries = new byte[AOLog.SERIES_BYTES_LENGTH];
            while (true) {
                int readByte = fileChannel.read(byteBuffer);
                if (readByte < AOLog.SERIES_BYTES_LENGTH) {
                    log.error("AOLOG ENTRY ERROR");
                    AOLog[] breakReturnLogs = new AOLog[i];
                    System.arraycopy(aoLogs, 0, breakReturnLogs, 0, i);
                    aoLogs = breakReturnLogs;
                    break;
                }
                byteBuffer.flip();
                byteBuffer.get(logSeries);
                AOLog aoLog = AOLog.fromSeries(logSeries);
                aoLogs[i] = aoLog;
                i++;
            }
            return aoLogs;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            log.error(e);
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e);
        }
        return null;
    }

    private void reinit() {
        initialized = false;
        this.close();
        this.init();
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


    private boolean persistLogIdx(String filename, long val) {
        try (OutputStream outputStream = storeHandler.openFileOutputStream(filename)) {
            DataOutputStream dos = new DataOutputStream(outputStream);
            dos.writeLong(val);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("AOLOG INDEX OUTPUT EX", e);
            return false;
        }
        return true;
    }

    @Override
    public void close() {
        close(3);
    }

    public void close(int waitSeconds) {
        try {
            try {
                writeLogTask.waitQWriteComplete(waitSeconds);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            writeChannel.close();
            persistLogIdx(LOG_END_IDX_FILE, logIdx.get());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class WriteLogTask extends TsdbRunnableTask {

        private BlockingQueue<AOLog> logQueue = null;

        public WriteLogTask(BlockingQueue<AOLog> logQueue) {
            this.logQueue = logQueue;
        }

        @Override
        public void run() {
            File aolFile = storeHandler.getFile(AOL_FILE);
            try {
                seekableAolFile = new RandomAccessFile(aolFile, "rw");
                long logicalEndIdx = getLogFileIdx(LOG_END_IDX_FILE);
                logIdx.set(logicalEndIdx);
                long startIdx = getLogFileIdx(LOG_START_IDX_FILE);
                writeLength = (int) (logicalEndIdx - startIdx);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                log.error(e);
            }
            writeChannel = seekableAolFile.getChannel();
            ByteBuffer byteBuffer = ByteBuffer.allocate(AOLog.SERIES_BYTES_LENGTH + 1);
            while (true) {
                AOLog aoLog = null;
                try {
                    aoLog = logQueue.poll(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    if (AppInfo.getApplicationState() != AppInfo.ApplicationState.SHUTDOWN) {
                        log.warn(e);
                        DBMetrics.getInstance().event("Interrupted");
                    }
                }
                reportAOLogQueueMetrics();
                if (aoLog != null) {
                    byteBuffer.clear();
                    byteBuffer.put(aoLog.series());
                    byteBuffer.flip();
                    try {
                        writeChannel.write(byteBuffer);
                        log.debug("write aolog");
                        logIdx.incrementAndGet();
                        writeLength++;
                        if (writeLength % MAX_WRITE_LENGTH == 0) {
                            rollingAolFIle();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        log.error(e);
                        DBMetrics.getInstance().event("IOException");
                    }
                }
            }
        }

        public void waitQWriteComplete(int timeoutSeconds) throws InterruptedException {
            int totalWaitMs = 0;
            while (logQueue.size() > 0 && totalWaitMs < timeoutSeconds * 1000) {
                Thread.sleep(200);
                totalWaitMs += 200;
            }
        }

        private void rollingAolFIle() {
            try {
                seekableAolFile.seek(0);
                final long fileStartIdx = logIdx.get();
                log.debug("Rolling  fileStartIdx:{}, writeLen:{}", fileStartIdx, writeLength);
                persistLogIdx(LOG_START_IDX_FILE, fileStartIdx);
                writeLength = 0;

            } catch (IOException e) {
                e.printStackTrace();
                log.error(e);
            }
        }

        private long lastReportTime = TimeUtils.currentSeconds();

        private void reportAOLogQueueMetrics() {
            if (TimeUtils.currentSeconds() - lastReportTime >= 1) {
                DBMetrics.getInstance().reportMetrics("AOLOG_BUFFER_QUEUE", logQueue.size());
                lastReportTime = TimeUtils.currentSeconds();
            }
        }

        @Override
        public int getRetryLimit() {
            return 3;
        }

        @Override
        public String getTaskName() {
            return "WriteLogTask";
        }
    }


    public void setMAX_WRITE_LENGTH(int MAX_WRITE_LENGTH) {
        if (AppInfo.getApplicationState() == AppInfo.ApplicationState.TESTING) {
            this.MAX_WRITE_LENGTH = MAX_WRITE_LENGTH;
        } else {
            throw new RuntimeException("Can not set MAX WRITE LENGTH");
        }
    }
}
