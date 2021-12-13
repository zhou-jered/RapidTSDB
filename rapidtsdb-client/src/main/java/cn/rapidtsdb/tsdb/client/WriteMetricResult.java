package cn.rapidtsdb.tsdb.client;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WriteMetricResult {
    private boolean success;
    private int errCode;
    private String errMsg;

    public WriteMetricResult(boolean success) {
        this.success = success;
    }

    public static final WriteMetricResult OK = new WriteMetricResult(true);
}
