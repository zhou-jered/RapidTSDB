package cn.rapidtsdb.tsdb.executors;

import cn.rapidtsdb.tsdb.TsdbRunnableTask;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
@Log4j2
public class ManagedThreadPool implements Closer {

    private ThreadPoolExecutor ioExecutor;
    private ThreadPoolExecutor failedTaskExecutor;
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("RapidTSDB-Scheduler-%d").build());
    private List<Thread> managedThreads = new ArrayList<>();
    private AtomicInteger threadIdx = new AtomicInteger(1);

    private ManagedThreadPool() {
    }

    private ManagedThreadPool(TSDBConfig tsdbConfig) {
        ioExecutor = new ThreadPoolExecutor(tsdbConfig.getExecutorIoCore(), tsdbConfig.getExecutorIoMax(), 1, TimeUnit.HOURS, new LinkedBlockingDeque<>(),
                new ThreadFactoryBuilder().setNameFormat("RapidTSDB-IO-%d").build());
    }

    public ThreadPoolExecutor ioExecutor() {
        return ioExecutor;
    }

    public ScheduledExecutorService scheduledExecutor() {
        return scheduledExecutorService;
    }

    public Thread newThread(TsdbRunnableTask runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("Rapid-Thread-Managed-" + threadIdx.incrementAndGet());
        managedThreads.add(thread);
        return thread;
    }

    public void submitFailedTask(TsdbRunnableTask failedTask) {
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
        int approximate = (int) (100 - ioExecutor.getTaskCount());
        return approximate < 0 ? -1 : approximate;
    }


    public void waitShutdownComplete() {

    }

    private static ManagedThreadPool INSTANCE = null;

    static {
        INSTANCE = new ManagedThreadPool(TSDBConfig.getConfigInstance());
    }

    public static ManagedThreadPool getInstance() {
        return INSTANCE;
    }

    @Override
    public void close() {
        ioExecutor.shutdown();
        if (failedTaskExecutor != null) {
            failedTaskExecutor.shutdown();
        }
        scheduledExecutorService.shutdown();
        for (Thread thread : managedThreads) {
            thread.interrupt();
        }
        log.info("ManagedThreadPool Closed");
    }
}
