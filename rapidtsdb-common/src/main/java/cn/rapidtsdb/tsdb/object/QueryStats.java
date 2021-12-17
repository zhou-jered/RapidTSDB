package cn.rapidtsdb.tsdb.object;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueryStats {
    private long costMs;
    private int scannedDpsNumber;
    private int dpsNumber;
}
