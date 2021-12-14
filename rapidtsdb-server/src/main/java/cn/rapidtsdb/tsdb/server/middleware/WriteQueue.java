package cn.rapidtsdb.tsdb.server.middleware;

import lombok.extern.log4j.Log4j2;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 1. 缓冲客户端的命令
 * 2. 可持久化
 * 3. like kafka
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

    public void write() {

    }

    public WriteCommand pollCommand(QueueBinder binder) {
        int qi = binder.getBinderQueueIndex(concurrentLevel);
        BlockingQueue<WriteCommand> queue = QS[qi];
        return queue.poll();
    }
}
