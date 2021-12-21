package cn.rapidtsdb.tsdb.core.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TSEngineQuery {
    private String metric;
    private long startTimestamp;
    private long endTimestamp;
    private String downSampler;
}
