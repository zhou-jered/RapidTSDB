package cn.rapidtsdb.tsdb.server.middleware;

import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.core.TSDataPoint;
import cn.rapidtsdb.tsdb.meta.BizMetric;
import cn.rapidtsdb.tsdb.meta.MetricTransformer;
import cn.rapidtsdb.tsdb.meta.exception.IllegalCharsException;

import java.util.concurrent.atomic.AtomicInteger;

public class TSDBExecutor {

    private static AtomicInteger state = new AtomicInteger(0);
    private static final TSDBExecutor EXECUTOR = new TSDBExecutor();
    private static final int NEW = 0;
    private static final int RUNNING = 1;
    private static final int SHUTDOWN = 2;
    private TSDB db;

    private WriteQueue writeQueue;

    public static TSDBExecutor getEXECUTOR() {
        return EXECUTOR;
    }

    public static void start(TSDB db) {
        if (state.compareAndSet(NEW, RUNNING)) {
            WriteQueue writeQueue = new WriteQueue(5);
        }
    }

    public boolean write(BizMetric metric, TSDataPoint dps) {
        return true;
    }

    public boolean write(BizMetric metric, TSDataPoint[] dps) {
        return true;
    }

    private TSDBExecutor() {

    }

    public void shutdown() {

    }

    static class ExecutorRunnable implements Runnable {

        private TSDB tsdb;
        private WriteQueue Q;
        private QueueBinder queueBinder = new QueueBinder();

        @Override
        public void run() {
            WriteCommand cmd = Q.pollCommand(queueBinder);
            MetricTransformer metricTransformer = new MetricTransformer();
            try {
                String internalMetric = metricTransformer.toInternalMetric(cmd.getMetric());
                TSDataPoint[] dps = cmd.getDps();
                for (TSDataPoint dp : dps) {
                    tsdb.writeMetric(internalMetric, dp.getValue(), dp.getTimestamp());
                }
            } catch (IllegalCharsException e) {
                e.printStackTrace();
            }
        }
    }
}
