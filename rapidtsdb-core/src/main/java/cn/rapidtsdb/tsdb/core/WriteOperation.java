package cn.rapidtsdb.tsdb.core;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WriteOperation {
    private int metricId;
    private long timestamp;
    private double val;

    public byte[] series() {
        byte[] sd = new byte[4 + 8 + 8];
        System.arraycopy(Ints.toByteArray(metricId), 0, sd, 0, 4);
        System.arraycopy(Longs.toByteArray(timestamp), 0, sd, 4, 8);
        System.arraycopy(Longs.toByteArray(Double.doubleToLongBits(val)), 0, sd, 12, 8);
        return sd;
    }

    public static WriteOperation fromBytes(byte[] bs) {
        if (bs.length < 4 + 8 + 8) {
            throw new RuntimeException("Data corrupted");
        }
        WriteOperation wo = new WriteOperation();
        wo.metricId = Ints.fromByteArray(bs);
        wo.timestamp = Longs.fromBytes(bs[4], bs[5], bs[6], bs[7], bs[8], bs[9], bs[10],
                bs[11]);
        wo.val = Double.longBitsToDouble(Longs.fromBytes(bs[12], bs[13], bs[14], bs[15],
                bs[16], bs[17], bs[18], bs[19]));
        return wo;
    }
}
