package cn.rapidtsdb.tsdb.client;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReadMetricsResult {
    private Datapoint[] dps; // should be sorted
}
