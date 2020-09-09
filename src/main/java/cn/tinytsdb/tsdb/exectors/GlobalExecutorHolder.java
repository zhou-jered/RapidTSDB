package cn.tinytsdb.tsdb.exectors;

import cn.tinytsdb.tsdb.config.TSDBConfig;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class GlobalExecutorHolder {

    private ThreadPoolExecutor threadPoolExecutor;
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private GlobalExecutorHolder() {
    }

    private GlobalExecutorHolder(TSDBConfig tsdbConfig) {
        threadPoolExecutor = new ThreadPoolExecutor(tsdbConfig.getExecutorIoCore(), tsdbConfig.getExecutorIoMax(), 1, TimeUnit.HOURS, new LinkedBlockingDeque<>());
    }

    public ThreadPoolExecutor ioExecutor() {
        return threadPoolExecutor;
    }

    public ScheduledExecutorService scheduledExecutor() {
        return scheduledExecutorService;
    }

    /**
     * return an integer which between 0 to 100, to indicate the system load level,
     * Some low priority task may depend this method result
     *
     * @return
     */
    public int getSystemLoadLevel() {
        int approximate = (int) (100 - threadPoolExecutor.getTaskCount());
        return approximate < 0 ? -1: approximate;
    }


    private static GlobalExecutorHolder INSTANCE = null;

    static {
        INSTANCE = new GlobalExecutorHolder(TSDBConfig.getConfigInstance());
    }

    public static GlobalExecutorHolder getInstance() {
        return INSTANCE;
    }


}
