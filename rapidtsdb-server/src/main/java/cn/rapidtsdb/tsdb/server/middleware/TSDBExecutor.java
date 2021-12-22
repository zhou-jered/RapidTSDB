package cn.rapidtsdb.tsdb.server.middleware;

import cn.rapidtsdb.tsdb.calculate.Aggregator;
import cn.rapidtsdb.tsdb.calculate.CalculatorFactory;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.core.persistent.MetricsKeyManager;
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
import cn.rapidtsdb.tsdb.utils.CollectionUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private MetricsKeyManager metricsKeyManager;
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
            metricsKeyManager = MetricsKeyManager.getInstance();
            metricsKeyManager.init();
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
        if (StringUtils.isEmpty(query.getAggregator())) {
            throw new RuntimeException("Aggregator can not be null");
        }
        Aggregator aggregator = CalculatorFactory.getAggregator(query.getAggregator());
        if (aggregator == null) {
            throw new RuntimeException("Can not get aggregator of :" + query.getAggregator());
        }
        Map<String, String> tags = query.getTags();
        if (CollectionUtils.isEmpty(tags)) {
            return queryWithoutTag(query);
        } else {
            return queryWithTags(query);
        }

    }

    private TSQueryResult queryWithoutTag(TSQuery query) {
        long start = System.nanoTime();
        TSEngineQueryResult engineQueryResult = queryInternal(query.getMetric(),
                query.getStartTime(), query.getEndTime(), query.getDownSampler());
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
    }

    private TSQueryResult queryWithTags(TSQuery taggedQuery) {
        Map<String, String> tags = taggedQuery.getTags();
        List<String> filterdInternalMetricParts = metricTransformer.concatTags(tags);
        String scannedMetricPrefix = metricTransformer.getMetricTagScanPrefix(taggedQuery.getMetric());
        List<String> internalMetrics = metricsKeyManager.scanMetrics(scannedMetricPrefix, filterdInternalMetricParts);
        Aggregator aggregator = CalculatorFactory.getAggregator(taggedQuery.getAggregator());
        QueryStats queryStats = new QueryStats();
        int totalScannedDpsNumber = 0;
        long startMs = System.currentTimeMillis();
        Map<Long, Double> resultDps = new HashMap<>();
        Set<String> aggregatortedTags = new HashSet<>();
        for (String im : internalMetrics) {
            BizMetric bizMetric = metricTransformer.toBizMetric(im);
            Map<String, String> singleMTags = bizMetric.getTags();
            for (String st : singleMTags.keySet()) {
                if (!tags.containsKey(st)) {
                    aggregatortedTags.add(st);
                }
            }
            TSEngineQueryResult engineQueryResult = queryInternal(im, taggedQuery.getStartTime(), taggedQuery.getEndTime(), taggedQuery.getDownSampler());
            resultDps = aggregator.aggregator(resultDps, engineQueryResult.getDps());
            totalScannedDpsNumber += engineQueryResult.getScannerPointNumber();
        }
        long costMs = System.currentTimeMillis() - startMs;
        queryStats.setCostMs(costMs);
        queryStats.setScannedDpsNumber(totalScannedDpsNumber);
        TSQueryResult tsQueryResult = TSQueryResult
                .builder().dps(resultDps)
                .info(queryStats)
                .metric(taggedQuery.getMetric())
                .tags(taggedQuery.getTags())
                .aggregatedTags(aggregatortedTags.toArray(new String[0]))
                .build();
        return tsQueryResult;
    }

    private TSEngineQueryResult queryInternal(String metric, long start, long end, String downSampler) {
        TSEngineQuery engineQuery = new TSEngineQuery(metric, start,
                end, downSampler);
        TSEngineQueryResult engineQueryResult = db.queryTimeSeriesData(engineQuery);
        return engineQueryResult;
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
