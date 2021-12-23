package cn.rapidtsdb.tsdb.client;

import cn.rapidtsdb.tsdb.client.event.TSDBUserEventListener;
import cn.rapidtsdb.tsdb.object.TSDataPoint;
import cn.rapidtsdb.tsdb.object.TSQueryResult;

import java.util.List;
import java.util.Map;

public interface TSDBClient {

    WriteMetricResult writeMetric(String metric, long timestamp, double value);

    WriteMetricResult writeMetric(String metric, TSDataPoint dp);

    WriteMetricResult writeMetric(String metric, double value);

    WriteMetricResult writeMetrics(String metric, Map<Long, Double> dps);

    WriteMetricResult writeMetric(String metric, long timestamp, double value, Map<String, String> tags);

    WriteMetricResult writeMetric(String metric, TSDataPoint dp, Map<String, String> tags);

    WriteMetricResult writeMetric(String metric, double value, Map<String, String> tags);

    WriteMetricResult writeMetrics(String metric, Map<Long, Double> dps, Map<String, String> tags);

    TSQueryResult readMetrics(String metric, long startTimestamp, long endTimestamp, String aggregator);

    TSQueryResult readMetrics(String metric, long startTimestamp, long endTimestamp, String aggregator, String downsampler);

    TSQueryResult readMetrics(String metric, long startTimestamp, long endTimestamp, Map<String, String> tags, String aggregator);

    TSQueryResult readMetrics(String metric, long startTimestamp, long endTimestamp, Map<String, String> tags, String aggregator, String downsampler);

    void close();

    void addEventListener(TSDBUserEventListener listener);

    List<TSDBUserEventListener> getEventListener();

}

