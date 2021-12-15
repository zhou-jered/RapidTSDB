package cn.rapidtsdb.tsdb.client;

import cn.rapidtsdb.tsdb.object.TSDataPoint;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReadMetricsResult {
    private TSDataPoint[] dps; // should be sorted
}
