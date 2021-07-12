package cn.rapidtsdb.tsdb.client;

import java.util.List;

public interface TSDBClient {
    void writeMetric(String metric, long timestamp, double value);

    void writeMetric(String metric, Datapoint dp);

    void writeMetrics(String metric, double value);

    void writeMetrics(String metric, List<Datapoint> dps);
}
