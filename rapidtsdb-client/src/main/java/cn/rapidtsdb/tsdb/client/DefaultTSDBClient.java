package cn.rapidtsdb.tsdb.client;

import java.util.List;
import java.util.Map;

public class DefaultTSDBClient implements TSDBClient {

    TSDBClientConfig config;

    public DefaultTSDBClient(TSDBClientConfig config) {
        this.config = config;
    }

    @Override
    public void start(boolean keepAlive, long keepAliveTimeMills) {
        
    }

    @Override
    public void writeMetric(String metric, long timestamp, double value) {

    }

    @Override
    public void writeMetric(String metric, Datapoint dp) {

    }

    @Override
    public void writeMetric(String metric, double value) {

    }

    @Override
    public void writeMetrics(String metric, List<Datapoint> dps) {

    }

    @Override
    public void writeMetric(String metric, long timestamp, double value, Map<String, String> tags) {

    }

    @Override
    public void writeMetric(String metric, Datapoint dp, Map<String, String> tags) {

    }

    @Override
    public void writeMetric(String metric, double value, Map<String, String> tags) {

    }

    @Override
    public void writeMetrics(String metric, List<Datapoint> dps, Map<String, String> tags) {

    }

    @Override
    public List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, String aggregator) {
        return null;
    }

    @Override
    public List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, String downsampler, String aggregator) {
        return null;
    }

    @Override
    public List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, Map<String, String> tags, String aggregator) {
        return null;
    }

    @Override
    public List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, Map<String, String> tags, String downsampler, String aggregator) {
        return null;
    }
}
