package cn.tinytsdb.tsdb.core.persistent;

import com.google.common.primitives.Longs;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AOLog {
    private long metricsIdx;
    private long timestamp;
    private double val;
    private byte[] seri;

    public static final int SERIES_BYTES_LENGTH = 24;

    public AOLog(long idx, long timestamp, double val) {
        this.metricsIdx = idx;
        this.timestamp = timestamp;
        this.val = val;
    }

    public byte[] series() {
        if (seri == null) {
            seri = new byte[32];
            byte[] temp = Longs.toByteArray(metricsIdx);
            System.arraycopy(temp,0, seri, 0, 8);
            temp = Longs.toByteArray(timestamp);
            System.arraycopy(temp, 0, seri, 8, 8);
            temp = Longs.toByteArray(Double.doubleToLongBits(val));
            System.arraycopy(temp, 0, seri, 16, 8);
        }
        return seri;
    }

    /**
     * internal call, should keep series not null
     * and a length 24
     * @param series
     * @return
     */
    static AOLog fromSeries(byte[] series) {
        AOLog alog = new AOLog();
        alog.metricsIdx = Longs.fromByteArray(series);
        alog.timestamp = Longs.fromBytes(series[8],series[9],series[10],series[11],
                series[12],series[13],series[14],series[15]);
        alog.val = Double.longBitsToDouble(Longs.fromBytes(
                series[16],series[17],series[18],series[19],
                series[20],series[21],series[22],series[23]));
        return alog;
    }


}
