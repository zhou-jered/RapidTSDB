package cn.rapidtsdb.tsdb;

import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class TSDataOperationQueue {
    private static int taskLoadBalanceFactor = 3;
    private static final Map<Integer, BlockingQueue<TSDBDataOperationTask>> taskExecutorBinder = new ConcurrentHashMap<>();
    private static final TSDataOperationQueue Q = new TSDataOperationQueue();


    private TSDataOperationQueue() {
//        taskLoadBalanceFactor = TSDBConfig.getConfigInstance().getTaskLoadBalancer()
        for (int i = 0; i < taskLoadBalanceFactor; i++) {
            taskExecutorBinder.put(i, new LinkedBlockingQueue<>());
        }
    }

    public static final TSDataOperationQueue getQ() {
        return Q;
    }

    public void start() {
        for (int i = 0; i < taskLoadBalanceFactor; i++) {
            Thread thread = ManagedThreadPool.getInstance().newThread(new TSDataExecRunnable(taskExecutorBinder.get(i)));
            thread.start();
        }
    }

    public void submitTask(TSDBDataOperationTask task) {
        int metricID = task.getMetricId();
        final int binderKey = metricID % taskLoadBalanceFactor;
        taskExecutorBinder.get(binderKey).offer(task);
    }

    private static class TSDataExecRunnable extends TsdbRunnableTask {
        @Override
        public int getRetryLimit() {
            return 0;
        }

        @Override
        public String getTaskName() {
            return "TSDataExecRunnable";
        }

        private final BlockingQueue<TSDBDataOperationTask> taskQ;

        public TSDataExecRunnable(BlockingQueue<TSDBDataOperationTask> taskQ) {

            this.taskQ = taskQ;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    TSDBDataOperationTask task = taskQ.take();
                    task.run();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
