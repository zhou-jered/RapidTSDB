package cn.rapidtsdb.tsdb.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

public class DoubleXor {
    public static DoubleXorResult doubleXor(Double pre, Double current) {
        long preBits = Double.doubleToLongBits(pre);
        long currentBits = Double.doubleToLongBits(current);
        long rawResult = preBits ^ currentBits;
        DoubleXorResult result = fromRawResult(rawResult);
        return result;
    }

    public static DoubleXorResult fromRawResult(long rawResult) {
        DoubleXorResult result = new DoubleXorResult();
        if (rawResult != 0) {
            result.setRawResult(rawResult);
            byte left0 = (byte) (63 - bitsRightZeroTable.get(Long.highestOneBit(rawResult)));
            byte right0 = bitsRightZeroTable.get(Long.lowestOneBit(rawResult));
            result.setLeft0(left0);
            result.setRight0(right0);
        } else {
            result.setLeft0((byte) 32);
            result.setRight0((byte) 32);
            result.setZero(true);
        }
        return result;
    }


    private static Map<Long, Byte> bitsRightZeroTable = new HashMap<>(128);

    static {
        bitsRightZeroTable.put(0L, (byte) 0);
        long n = 1;
        for (byte i = 0; i < 64; i++) {
            bitsRightZeroTable.put(n << i, i);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DoubleXorResult {
        private long rawResult;
        private byte left0;
        private byte right0;
        boolean zero = false;

        public long getMeaningLongBits() {
            return rawResult << left0;
        }

        public byte getMeaningBitsLength() {
            return (byte) (64 - left0 - right0);
        }

        public boolean inSubRange(DoubleXorResult other) {
            return other.left0 >= this.left0 && other.right0 >= this.right0;
        }
    }
}
