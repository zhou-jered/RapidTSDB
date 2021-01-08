package cn.rapidtsdb.tsdb.exectors;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.*;

/**
 *
 */
@Log4j2
public class GlobalExecutorHolder {

    private ThreadPoolExecutor threadPoolExecutor;
    private ThreadPoolExecutor failedTaskExecutor;
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("RapidTSDB-Scheduler-%d").build());

    private GlobalExecutorHolder() {
    }

    private GlobalExecutorHolder(TSDBConfig tsdbConfig) {
        threadPoolExecutor = new ThreadPoolExecutor(tsdbConfig.getExecutorIoCore(), tsdbConfig.getExecutorIoMax(), 1, TimeUnit.HOURS, new LinkedBlockingDeque<>(),
                new ThreadFactoryBuilder().setNameFormat("RapidTSDB-IO-%d").build());
    }

    public ThreadPoolExecutor ioExecutor() {
        return threadPoolExecutor;
    }

    public ScheduledExecutorService scheduledExecutor() {
        return scheduledExecutorService;
    }

    public void submitFailedTask(Runnable failedTask) {
        if (failedTaskExecutor == null) {
            TSDBConfig config = TSDBConfig.getConfigInstance();
            synchronized (this) {
                if (failedTaskExecutor == null) {
                    int qSize = config.getFailedTaskQueueSize();
                    if (qSize < 0) {
                        qSize = Integer.MAX_VALUE;
                    }
                    failedTaskExecutor = new ThreadPoolExecutor(config.getFailedTaskExecutorIoCore(), config.getFailedTaskExecutorIoMax(), 1, TimeUnit.HOURS, new LinkedBlockingQueue<>(qSize),
                            new ThreadFactoryBuilder().setNameFormat("RapidTSDB-FailedTask-%d").build());
                    failedTaskExecutor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
                        @Override
                        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                            log.error("Failed Task Queue Full, Task Rejected, {}", r);
                        }
                    });
                }
            }
            failedTaskExecutor.submit(failedTask);
        }
    }

    /**
     * return an integer which between 0 to 100, to indicate the system load level,
     * Some low priority task may depend this method result
     *
     * @return
     */
    public int getSystemLoadLevel() {
        int approximate = (int) (100 - threadPoolExecutor.getTaskCount());
        return approximate < 0 ? -1 : approximate;
    }


    private static GlobalExecutorHolder INSTANCE = null;

    static {
        INSTANCE = new GlobalExecutorHolder(TSDBConfig.getConfigInstance());
    }

    public static GlobalExecutorHolder getInstance() {
        return INSTANCE;
    }


}
