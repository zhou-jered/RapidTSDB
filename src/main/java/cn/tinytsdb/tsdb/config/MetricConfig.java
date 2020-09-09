package cn.tinytsdb.tsdb.config;

import cn.tinytsdb.tsdb.utils.TimeUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@AllArgsConstructor
public class MetricConfig {
    /**
     * @see TimeUtils.TimeUnitAdaptorFactory
     * record data in seconds currency or millseconds currency
     */
    final String timeScale = "s";
    final String timeUnit = timeScale;
    final int blockLengthSeconds = 120 * 60;

    public static MetricConfig DEFAULT_CONFIG =  new MetricConfig();
    private static Map<String, MetricConfig> metricConfigMap = new ConcurrentHashMap<>(1024);
    public static MetricConfig getMetricConfig(int metricId) {
        return metricConfigMap.getOrDefault(metricId, DEFAULT_CONFIG);
    }
}
