package cn.rapidtsdb.tsdb.object;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QueryStats {
    private long costMs;
    private int scannedDpsNumber;
    private int dpsNumber;
}
