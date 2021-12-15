package cn.rapidtsdb.tsdb.server.middleware;

import lombok.extern.log4j.Log4j2;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 1. 缓冲客户端的命令
 * 2. 可持久化
 * 3. like kafka
 * <p>
 * 对外暴露的接口有 添加操作（push），获取操作（pull），
 * 并发的概念有
 */
@Log4j2
class WriteQueue {

    private BlockingQueue[] QS;
    private final int concurrentLevel;

    public WriteQueue(int concurrentLevel) {
        concurrentLevel = Math.max(1, concurrentLevel);
        this.concurrentLevel = concurrentLevel;
        QS = new BlockingQueue[concurrentLevel];
        for (int i = 0; i < concurrentLevel; i++) {
            QS[i] = new LinkedBlockingQueue();
        }
    }

    public boolean write(QueueCoordinator queueCoordinator, WriteCommand writeCommand) {
        final int i = queueCoordinator.getQueueIndex(writeCommand.getMetric().getMetric());
        return QS[i].add(writeCommand);
    }

    public WriteCommand pollCommand(int qidx) throws InterruptedException {
        BlockingQueue<WriteCommand> queue = QS[qidx];
        return queue.take();
    }
}
