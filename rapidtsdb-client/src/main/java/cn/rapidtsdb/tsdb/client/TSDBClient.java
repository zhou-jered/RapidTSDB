package cn.rapidtsdb.tsdb.client;

import java.util.List;

public interface TSDBClient {
    void writeMetric(String metric, long timestamp, double value);

    void writeMetric(String metric, Datapoint dp);

    void writeMetric(String metric, double value);

    void writeMetrics(String metric, List<Datapoint> dps);

    List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp);

    List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, String downsampler);

    List<Datapoint> readMetricsWithAggregation(long startTimestamp, long endTimestamp, String downsampler, String... metrics);
}

