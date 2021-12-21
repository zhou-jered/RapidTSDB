package cn.rapidtsdb.tsdb.core.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.SortedMap;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TSEngineQueryResult {
    private int scannerPointNumber;
    private long scanCostNanos;
    private SortedMap<Long, Double> dps;
}
