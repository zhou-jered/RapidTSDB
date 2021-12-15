package cn.rapidtsdb.tsdb.server.middleware;

/**
 * 负责 metric 到 internal Queue的映射管理工作
 * internal Queue 对外暴露的 notion 有 index
 */
public class QueueCoordinator {

    private int qNumber;


    public QueueCoordinator(int qNumber) {
        this.qNumber = qNumber;
    }

    public int getQueueIndex(String metric) {
        int n = Math.abs(metric.hashCode()) % qNumber;
        return n;
    }
}
