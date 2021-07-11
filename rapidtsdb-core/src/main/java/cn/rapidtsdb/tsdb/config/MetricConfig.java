package cn.rapidtsdb.tsdb.config;

import cn.rapidtsdb.tsdb.utils.TimeUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MetricConfig {
    /**
     * @see TimeUtils.TimeUnitAdaptorFactory
     * record data in seconds currency or millseconds currency
     */
    private String timeScale = "ms";
    private String timeUnit = timeScale;
    private String storeScheme = "file";
    private final int blockLengthSeconds = 120 * 60; // maybe support variable block length in the future.

    public static MetricConfig DEFAULT_CONFIG = new MetricConfig();
    private static Map<String, MetricConfig> metricConfigMap = new ConcurrentHashMap<>(1024);

    public static MetricConfig getMetricConfig(int metricId) {
        return metricConfigMap.getOrDefault(metricId, DEFAULT_CONFIG);
    }
}
