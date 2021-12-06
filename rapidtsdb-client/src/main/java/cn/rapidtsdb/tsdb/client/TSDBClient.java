package cn.rapidtsdb.tsdb.client;

import cn.rapidtsdb.tsdb.client.event.TSDBUserEventListener;

import java.util.List;
import java.util.Map;

public interface TSDBClient {
    
    WriteMetricResult writeMetric(String metric, long timestamp, double value);

    WriteMetricResult writeMetric(String metric, Datapoint dp);

    WriteMetricResult writeMetric(String metric, double value);

    WriteMetricResult writeMetrics(String metric, List<Datapoint> dps);

    WriteMetricResult writeMetric(String metric, long timestamp, double value, Map<String, String> tags);

    WriteMetricResult writeMetric(String metric, Datapoint dp, Map<String, String> tags);

    WriteMetricResult writeMetric(String metric, double value, Map<String, String> tags);

    WriteMetricResult writeMetrics(String metric, List<Datapoint> dps, Map<String, String> tags);

    List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, String aggregator);

    List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, String downsampler, String aggregator);

    List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, Map<String, String> tags, String aggregator);

    List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, Map<String, String> tags, String downsampler, String aggregator);

    void close();

    void addEventListener(TSDBUserEventListener listener);

    List<TSDBUserEventListener> getEventListener();

}

