package cn.rapidtsdb.tsdb.client;

import java.util.List;
import java.util.Map;

public interface TSDBClient {


    void start(boolean keepAlive, long keepAliveTimeMills);

    void writeMetric(String metric, long timestamp, double value);

    void writeMetric(String metric, Datapoint dp);

    void writeMetric(String metric, double value);

    void writeMetrics(String metric, List<Datapoint> dps);

    void writeMetric(String metric, long timestamp, double value, Map<String, String> tags);

    void writeMetric(String metric, Datapoint dp, Map<String, String> tags);

    void writeMetric(String metric, double value, Map<String, String> tags);

    void writeMetrics(String metric, List<Datapoint> dps, Map<String, String> tags);

    List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, String aggregator);

    List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, String downsampler, String aggregator);

    List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, Map<String, String> tags, String aggregator);

    List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, Map<String, String> tags, String downsampler, String aggregator);


}

