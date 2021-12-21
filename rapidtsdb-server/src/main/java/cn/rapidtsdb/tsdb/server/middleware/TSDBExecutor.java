package cn.rapidtsdb.tsdb.server.middleware;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.core.pojo.TSEngineQuery;
import cn.rapidtsdb.tsdb.core.pojo.TSEngineQueryResult;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.meta.MetricTransformer;
import cn.rapidtsdb.tsdb.meta.exception.IllegalCharsException;
import cn.rapidtsdb.tsdb.object.BizMetric;
import cn.rapidtsdb.tsdb.object.QueryStats;
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
public class TSDBExecutor implements Initializer, Closer {

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

    @Override
    public void close() {
        metricTransformer.close();
    }

    @Override
    public void init() {
        if (state.compareAndSet(NEW, RUNNING)) {
            metricTransformer = new MetricTransformer();
            metricTransformer.init();
        }
    }

    public static TSDBExecutor getEXECUTOR() {
        return EXECUTOR;
    }

    public void startExecute(TSDB db, TSDBConfig config) {
        EXECUTOR.db = db;
        int concurrent = config.getDbServerThreads();
        writeQueue = new WriteQueue(concurrent);
        queueCoordinator = new QueueCoordinator(concurrent);
        threadPoolExecutor = new ThreadPoolExecutor(concurrent, concurrent,
                1, TimeUnit.HOURS, new LinkedBlockingQueue<>(), new ExecutorThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        for (int i = 0; i < concurrent; i++) {
            int qidx = i;
            ExecutorRunnable executorRunnable = new ExecutorRunnable(db, EXECUTOR.writeQueue, qidx);
            EXECUTOR.threadPoolExecutor.submit(executorRunnable);

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
        BizMetric bizMetric = new BizMetric(query.getMetric(), query.getTags());
        try {
            //todo
//            String internalPrefix = query.getMetric()+ me
//            metricsKeyManager.scanMetrics()
            String internalMetric = metricTransformer.toInternalMetric(bizMetric);
            TSEngineQuery engineQuery = new TSEngineQuery(internalMetric, query.getStartTime(),
                    query.getEndTime(), query.getDownSampler());
            long start = System.nanoTime();
            TSEngineQueryResult engineQueryResult = db.queryTimeSeriesData(engineQuery);
            Map<Long, Double> dps = engineQueryResult.getDps();
            long cost = System.nanoTime() - start;
            QueryStats queryStats = QueryStats.builder()
                    .costMs(cost / 1000)
                    .dpsNumber(dps.size())
                    .scannedDpsNumber(engineQueryResult.getScannerPointNumber())
                    .build();
            TSQueryResult queryResult = TSQueryResult.builder()
                    .info(queryStats)
                    .dps(dps).build();
            return queryResult;
        } catch (IllegalCharsException e) {
            throw new RuntimeException(e);
        }
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
