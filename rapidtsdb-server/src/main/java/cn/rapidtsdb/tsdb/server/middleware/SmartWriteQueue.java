package cn.rapidtsdb.tsdb.server.middleware;

import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import lombok.extern.log4j.Log4j2;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 1. 缓冲客户端的命令
 * 2. 可持久化
 * 3. like kafka
 * <p>
 * 对外暴露的接口有 添加操作（push），获取操作（pull），
 * 并发的概念有
 */
@Log4j2
class SmartWriteQueue implements Initializer {

    private BlockingQueue[] QS;
    private final int concurrentLevel;

    private Date statusTime;
    private double throughout;
    private AtomicInteger inflow = new AtomicInteger(0);
    private AtomicInteger outflow = new AtomicInteger(0);

    private SimpleDateFormat reportDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public SmartWriteQueue(int concurrentLevel) {
        concurrentLevel = Math.max(1, concurrentLevel);
        this.concurrentLevel = concurrentLevel;
        QS = new BlockingQueue[concurrentLevel];
        for (int i = 0; i < concurrentLevel; i++) {
            QS[i] = new LinkedBlockingQueue();
        }
    }

    @Override
    public void init() {
        ManagedThreadPool.getInstance().scheduledExecutor()
                .scheduleAtFixedRate(new QueueStatutReseter(), 1, 1, TimeUnit.SECONDS);
    }

    public boolean write(QueueCoordinator queueCoordinator, WriteCommand writeCommand) {
        final int i = queueCoordinator.getQueueIndex(writeCommand.getMetric().getMetric());
        if (QS[i].size() > 1024) {
            log.error("Report Internal Q Buffer Back Pressured {}", QS[i].size());
        }
        return QS[i].add(writeCommand);
    }

    public WriteCommand pollCommand(int qidx) throws InterruptedException {
        BlockingQueue<WriteCommand> queue = QS[qidx];
        return queue.take();
    }


    public Map<String, String> queueMetrics() {
        Map<String, String> metrics = new HashMap<>();
        double currentThroughput = throughout;
        metrics.put("time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(statusTime));
        metrics.put("throughput", String.valueOf(throughout));
        return metrics;
    }


    class QueueStatutReseter implements Runnable {
        @Override
        public void run() {

        }
    }

}
