package cn.rapidtsdb.tsdb.object;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TSQuery {
    private String metric;
    private Map<String, String> tags;
    private long startTime;
    private long endTime;
    private String downSampler;
    private String aggregator;

    public TSQuery(String metric, long startTime, long endTime) {
        this.metric = metric;
        this.startTime = startTime;
        this.endTime = endTime;
    }


}
