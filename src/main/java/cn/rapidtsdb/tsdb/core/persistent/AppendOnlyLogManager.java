package cn.rapidtsdb.tsdb.core.persistent;

import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.store.StoreHandler;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import com.google.common.primitives.Longs;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class AppendOnlyLogManager implements Initializer, Persistently , Closer {

    private AtomicLong logIdx = new AtomicLong(0);
    private final String AOL_FILE = "aol.data";
    private final String LOG_IDX_FILE = "aol.idx";
    private volatile boolean initialized = false;
    private StoreHandler storeHandler;
    private OutputStream currentLogOs;

    private static AppendOnlyLogManager instance = null;

    public static AppendOnlyLogManager getInstance() {
        if (instance == null) {
            instance = new AppendOnlyLogManager();
        }
        return instance;
    }

    @Override
    public void init() {
        if(initialized) {
            return;
        }
        initialized = true;
        storeHandler = StoreHandlerFactory.getStoreHandler();
        try {
            currentLogOs = storeHandler.openFileAppendStream(AOL_FILE);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("GET AOLOF OUTPUT EX",e);
            throw new RuntimeException(e);
        }
        recoverLog();
    }

    public void appendLog(AOLog alog) {
        try {
            currentLogOs.write(alog.getSeri());
            if(logIdx.incrementAndGet()%2730 == 0) {
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

    public void appendLog(long idx, long timestamp, double val) {
        appendLog(new AOLog(idx, timestamp, val));
    }

    public AOLog[] recoverLog() {
        try {
            if(storeHandler.fileExisted(AOL_FILE)) {
                InputStream inputStream = storeHandler.openFileInputStream(AOL_FILE);
                List<AOLog> aoLogList = new ArrayList<>(inputStream.available()/ AOLog.SERIES_BYTES_LENGTH);
                byte[] series = new byte[AOLog.SERIES_BYTES_LENGTH];
                while(inputStream.available()> AOLog.SERIES_BYTES_LENGTH) {
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


    public boolean resetLogBuf() {
        if(persistLogIdx()) {
            try {
                currentLogOs = storeHandler.openFileOutputStream(AOL_FILE);
            } catch (IOException e) {
                e.printStackTrace();
                log.error("GET AOLOG OUTPUT EX", e);
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    private boolean persistLogIdx() {
        try (OutputStream outputStream = storeHandler.openFileOutputStream(LOG_IDX_FILE)){
            IOUtils.write(Longs.toByteArray(logIdx.get()), outputStream);
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
            currentLogOs.flush();
            currentLogOs.close();
            persistLogIdx();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
