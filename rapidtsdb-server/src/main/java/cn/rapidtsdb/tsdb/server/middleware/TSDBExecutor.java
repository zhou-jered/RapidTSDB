package cn.rapidtsdb.tsdb.server.middleware;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.meta.MetricTransformer;
import cn.rapidtsdb.tsdb.meta.exception.IllegalCharsException;
import cn.rapidtsdb.tsdb.object.BizMetric;
import cn.rapidtsdb.tsdb.object.TSDataPoint;
import cn.rapidtsdb.tsdb.object.TSQuery;
import cn.rapidtsdb.tsdb.object.TSQueryResult;
import lombok.extern.log4j.Log4j2;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public class TSDBExecutor {

    private static AtomicInteger state = new AtomicInteger(0);
    private static final TSDBExecutor EXECUTOR = new TSDBExecutor();
    private static final int NEW = 0;
    private static final int RUNNING = 1;
    private static final int SHUTDOWN = 2;
    private TSDB db;
    private MetricTransformer metricTransformer;

    private WriteQueue writeQueue;
    private QueueCoordinator queueCoordinator;

    private ThreadPoolExecutor threadPoolExecutor;

    public static TSDBExecutor getEXECUTOR() {
        return EXECUTOR;
    }

    public static void start(TSDB db, TSDBConfig config) {
        if (state.compareAndSet(NEW, RUNNING)) {
            int concurrent = config.getDbServerThreads();
            EXECUTOR.db = db;
            EXECUTOR.writeQueue = new WriteQueue(concurrent);
            EXECUTOR.metricTransformer = new MetricTransformer();
            EXECUTOR.queueCoordinator = new QueueCoordinator(concurrent);
            EXECUTOR.threadPoolExecutor = new ThreadPoolExecutor(concurrent, concurrent,
                    1, TimeUnit.HOURS, new LinkedBlockingQueue<>(), new ExecutorThreadFactory(),
                    new ThreadPoolExecutor.AbortPolicy());
            for (int i = 0; i < concurrent; i++) {
                int qidx = i;
                ExecutorRunnable executorRunnable = new ExecutorRunnable(db, EXECUTOR.writeQueue, qidx);
                EXECUTOR.threadPoolExecutor.submit(executorRunnable);
            }
        }
    }

    public boolean write(BizMetric metric, TSDataPoint dp) {
        WriteCommand writeCommand = new WriteCommand(metric, dp); // todo gc review
        return writeQueue.write(queueCoordinator, writeCommand);
    }

    public boolean write(BizMetric metric, Map<Long, Double> dps) {
        WriteCommand writeCommand = new WriteCommand(metric, dps); // todo gc review
        return writeQueue.write(queueCoordinator, writeCommand);
    }

    public TSQueryResult read(TSQuery query) {
        return db.queryTimeSeriesData(query);
    }

    private TSDBExecutor() {

    }

    public void shutdown() {
        threadPoolExecutor.shutdown();
        state.set(SHUTDOWN);
    }

    public boolean isShutdown() {
        return state.get() == SHUTDOWN;
    }

    static class ExecutorRunnable implements Runnable {

        private TSDB tsdb;
        private WriteQueue Q;
        private int qidx;

        public ExecutorRunnable(TSDB tsdb, WriteQueue q, int qidx) {
            this.tsdb = tsdb;
            Q = q;
            this.qidx = qidx;
        }

        @Override
        public void run() {
            while (true) {
                WriteCommand cmd = null;
                try {
                    cmd = Q.pollCommand(qidx);
                } catch (InterruptedException e) {
                    if (TSDBExecutor.getEXECUTOR().isShutdown()) {
                        return;
                    }
                }
                MetricTransformer metricTransformer = new MetricTransformer();
                try {
                    String internalMetric = metricTransformer.toInternalMetric(cmd.getMetric());
                    Iterator<TSDataPoint> dpIter = cmd.iter();
                    while (dpIter.hasNext()) {
                        TSDataPoint dp = dpIter.next();
                        tsdb.writeMetric(internalMetric, dp.getValue(), dp.getTimestamp());
                    }
                } catch (IllegalCharsException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    final static class ExecutorThreadFactory implements ThreadFactory {
        int threadIdx = 1;

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("TSDB-Executor-" + threadIdx++);
            return thread;
        }
    }

}
