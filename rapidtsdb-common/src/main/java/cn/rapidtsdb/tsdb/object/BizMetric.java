package cn.rapidtsdb.tsdb.object;

import cn.rapidtsdb.tsdb.common.LRUCache;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class BizMetric {
    private String metric;
    private Map<String, String> tags;

    public static BizMetric of(String metric, Map<String, String> tags) {
        return new BizMetric(metric, tags);
    }

    public static BizMetric cache(String metric) {
        BizMetric bizMetric = metricCache.get(metric);
        if (bizMetric == null) {
            bizMetric = new BizMetric(metric, null);
            metricCache.put(metric, bizMetric);
        }
        return bizMetric;
    }

    static LRUCache<String, BizMetric> metricCache = new LRUCache<>();

}
