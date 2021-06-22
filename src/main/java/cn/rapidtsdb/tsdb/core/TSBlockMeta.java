package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TSBlockMeta implements Serializable {

    private int metricId;
    private long baseTime; //seconds
    private int dpsSize;
    private int timeBitsLen;
    private int valuesBitsLen;
    private byte[] md5Checksum;

    private final static int SERIES_LEN = 4 + 8 + 4 + 4 + 4 + 16;

    public String getSimpleInfo() {
        return "baseTime:" + baseTime + ", size:" + dpsSize + ", md5:" + BinaryUtils.hexBytes(md5Checksum);
    }

    public byte[] series() {
        byte[] seriesData = new byte[SERIES_LEN];

        ByteAppender reverseByteAppender = (pos, len, val, result) -> {
            long value = val;
            for (int i = pos - 1; i >= (pos - len); --i) {
                result[i] = (byte) ((int) (value & 255L));
                value >>= 8;
            }
        };

        int idx = 4;
        reverseByteAppender.append(idx, 4, metricId, seriesData);

        idx += 8;
        reverseByteAppender.append(idx, 8, baseTime, seriesData);

        idx += 4;
        reverseByteAppender.append(idx, 4, dpsSize, seriesData);

        idx += 4;
        reverseByteAppender.append(idx, 4, timeBitsLen, seriesData);

        idx += 4;
        reverseByteAppender.append(idx, 4, valuesBitsLen, seriesData);

        System.arraycopy(md5Checksum, 0, seriesData, idx, 16);
        return seriesData;
    }


    @FunctionalInterface
    interface ByteAppender {
        void append(int idx, int len, long val, byte[] result);
    }

    public static TSBlockMeta fromSeries(byte[] series) {
        return fromSeries(series, 0);
    }

    public static TSBlockMeta fromSeries(byte[] series, int position) {
        if (series == null || series.length - position < SERIES_LEN) {
            throw new RuntimeException("Error BlockMeta Series Len:" + (series.length - position));
        }
        TSBlockMeta blockMeta = new TSBlockMeta();
        blockMeta.metricId = Ints.fromBytes(series[position], series[position + 1], series[position + 2], series[position + 3]);
        position += 4;

        blockMeta.baseTime = Longs.fromBytes(series[position], series[position + 1], series[position + 2], series[position + 3], series[position + 4], series[position + 5], series[position + 6], series[position + 7]);
        position += 8;

        blockMeta.dpsSize = Ints.fromBytes(series[position], series[position + 1], series[position + 2], series[position + 3]);
        position += 4;

        blockMeta.timeBitsLen = Ints.fromBytes(series[position], series[position + 1], series[position + 2], series[position + 3]);
        position += 4;

        blockMeta.valuesBitsLen = Ints.fromBytes(series[position], series[position + 1], series[position + 2], series[position + 3]);
        position += 4;

        blockMeta.md5Checksum = new byte[16];
        System.arraycopy(series, position, blockMeta.md5Checksum, 0, 16);
        return blockMeta;
    }

    public static void main(String[] args) {

        byte[] md5 = null;
        try {
            MessageDigest digest = MessageDigest.getInstance("md5");
            md5 = digest.digest("asf".getBytes());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        TSBlockMeta blockMeta = new TSBlockMeta();
        blockMeta.setBaseTime(45345);
        blockMeta.setDpsSize(345);
        blockMeta.setTimeBitsLen(498719845);
        blockMeta.setValuesBitsLen(39283);
        blockMeta.setMd5Checksum(md5);

        byte[] series = blockMeta.series();

        System.out.println(series.length);
        System.out.println(series.length == SERIES_LEN);
        TSBlockMeta recoverMeta = TSBlockMeta.fromSeries(series);
        blockMeta.equals(recoverMeta);
        System.out.println("Equals:" + blockMeta.equals(recoverMeta));

    }

}
