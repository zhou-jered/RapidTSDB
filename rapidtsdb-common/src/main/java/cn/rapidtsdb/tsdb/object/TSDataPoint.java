package cn.rapidtsdb.tsdb.object;

import lombok.Data;

@Data
public class TSDataPoint {
    private long timestamp;
    private double value;

    public TSDataPoint(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }
}
