package cn.rapidtsdb.tsdb.object;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class TSQueryResult {
    private String metric;
    private Map<String, String> tags;
    private String[] aggregatedTags;
    private Map<Long, Double> dps;
    private QueryStats info;
}
