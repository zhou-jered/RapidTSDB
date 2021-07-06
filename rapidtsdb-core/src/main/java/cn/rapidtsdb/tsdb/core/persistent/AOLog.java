package cn.rapidtsdb.tsdb.core.persistent;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class AOLog {
    @Getter
    private int metricsIdx;
    @Getter
    private long timestamp;
    @Getter
    private double val;
    private byte[] seri;

    public static final int SERIES_BYTES_LENGTH = 20;

    public AOLog(int idx, long timestamp, double val) {
        this.metricsIdx = idx;
        this.timestamp = timestamp;
        this.val = val;
    }

    public byte[] series() {
        if (seri == null) {
            seri = new byte[SERIES_BYTES_LENGTH];
            byte[] temp = Ints.toByteArray(metricsIdx);
            System.arraycopy(temp, 0, seri, 0, 4);
            temp = Longs.toByteArray(timestamp);
            System.arraycopy(temp, 0, seri, 4, 8);
            temp = Longs.toByteArray(Double.doubleToLongBits(val));
            System.arraycopy(temp, 0, seri, 12, 8);
        }
        return seri;
    }

    /**
     * internal call, should keep series not null
     * and a length 24
     *
     * @param series
     * @return
     */
    static AOLog fromSeries(byte[] series) {
        AOLog alog = new AOLog();
        alog.metricsIdx = Ints.fromByteArray(series);
        alog.timestamp = Longs.fromBytes(series[4], series[5], series[6], series[7],
                series[8], series[9], series[10], series[11]);
        alog.val = Double.longBitsToDouble(Longs.fromBytes(
                series[12], series[13], series[14], series[15],
                series[16], series[17], series[18], series[19]));

        return alog;
    }


}
