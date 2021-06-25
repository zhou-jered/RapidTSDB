package cn.rapidtsdb.tsdb.core;

import com.google.common.primitives.Longs;
import lombok.Getter;

import static cn.rapidtsdb.tsdb.core.ByteMask.RIGHT_MASK;

public class TSBytes {
    @Getter
    private byte[] data;

    @Getter
    private int bytesOffset = 0;
    @Getter
    private int bitsOffset;

    public static final int MAXUMUN_REMAIN_BYTES = 128;

    TSBytes() {
        this(60 * 60);
    }

    TSBytes(int initDataSize) {
        initDataSize = Math.max(initDataSize, 60 * 60);
        this.data = new byte[initDataSize];
    }

    TSBytes(byte[] data) {
        this.data = data;
    }

    /**
     * @param newData         the actual data bits is in the right hand of the newData bits. for example,
     *                        if the written data is 0b11, the newData form should be 0b0000011
     * @param newDataBitsSize
     */
    void appendBitesDataInternal(byte newData, int newDataBitsSize) {
        int writeIdx = bytesOffset;
        int remainBits = 8 - bitsOffset;
        int afterWriteRemainBits = remainBits - newDataBitsSize;
        if (afterWriteRemainBits >= 0) {
            data[writeIdx] |= (newData << (afterWriteRemainBits));
        } else {
            data[writeIdx] |= (newData >>> (-afterWriteRemainBits));
            data[writeIdx + 1] |= (newData << (remainBits + (8 - newDataBitsSize)));
        }
        incBitsOffset(newDataBitsSize);
    }

    void appendShortBites(short newData, int newDataBitsSize) {
        byte b1 = (byte) (newData >>> 8);
        byte b2 = (byte) newData;
        appendBitesDataInternal(b1, newDataBitsSize - 8);
        appendBitesDataInternal(b2, 8);
    }

    void appendIntBites(int newData) {
        for (int i = 3; i >= 0; i--) {
            byte b = (byte) (newData >>> (i * 8));
            appendBitesDataInternal(b, 8);
        }
    }

    void appendLongBits(long val) {
//        byte overflowData = (byte) (((byte) val) & RIGHT_MASK[bitsOffset]);
//        long existedData = data[bytesOffset];
//        existedData <<= (64 - bitsOffset);
//
//        int writeIdx = bytesOffset;
//        long writtenData = (existedData | val);
//        for (int i = 0; i < 8; i++) {
//            data[writeIdx + (7 - i)] = (byte) (writtenData & 0xff);
//            writtenData >>= 8;
//        }
//        incBytesOffset(8);
//        int overflowBitsSize = bitsOffset;
//        bitsOffset = 0;
//        appendBitesDataInternal(overflowData, overflowBitsSize);
        appendBytes(Longs.toByteArray(val), 64);
    }

    void appendBytes(byte[] bytes, int bitsSize) {

        if (bytes.length > MAXUMUN_REMAIN_BYTES && (bytes.length + bytesOffset + MAXUMUN_REMAIN_BYTES >= data.length)) {
            expandDataSize(bytes.length + bytesOffset + MAXUMUN_REMAIN_BYTES);
        }

        if (bitsOffset == 0) {
            boolean fullBits = (bitsSize % 8 == 0);
            System.arraycopy(bytes, 0, data, bytesOffset, bitsSize / 8 + (fullBits ? 0 : 1));
            incBytesOffset(bitsSize / 8);
            if (!fullBits) {
                incBitsOffset(bitsSize % 8);
            }
        } else {
            int remainBits = 8 - bitsOffset;
            for (int i = 0; i < bitsSize / 8; i++) {
                byte b = bytes[i];
                data[bytesOffset] |= (b >> bitsOffset) & RIGHT_MASK[remainBits];
                data[bytesOffset + 1] |= b << (8 - bitsOffset);
                bytesOffset++;
            }
            if (bitsSize % 8 > 0) {
                appendBitesDataInternal((byte) ((byte) (bytes[bitsSize / 8] >> (8 - bitsSize % 8)) & RIGHT_MASK[bitsSize % 8]), bitsSize % 8);
            }
        }
    }


    void incBytesOffset(int byteInc) {
        this.bytesOffset += byteInc;
        if (data.length - bytesOffset < MAXUMUN_REMAIN_BYTES) {
            expandDataSize();
        }
    }

    void incBitsOffset(int incBits) {
        this.bitsOffset += incBits;
        this.incBytesOffset(bitsOffset / 8);
        this.bitsOffset %= 8;
    }

    void setByteData(byte[] bytes, int byteOffset, int bitsLength) {
        System.arraycopy(bytes, byteOffset, bytes, 0, bitsLength / 8 + 1);
        this.bytesOffset = bitsLength / 8;
        this.bitsOffset = bitsLength % 8;
    }

    private void expandDataSize() {
        int toSize = (int) (bytesOffset * 1.5);
        expandDataSize(toSize);
    }

    private void expandDataSize(int toSize) {
        byte[] newBytes = new byte[toSize];
        System.arraycopy(data, 0, newBytes, 0, bytesOffset + 1);
        data = newBytes;
    }

    public int getTotalBitsLength() {
        return bytesOffset * 8 + bitsOffset;
    }

    public double getMemoryUsedKB() {
        double kb = data.length;
        return kb / 1024;
    }

    public double getMemoryActualUsed() {
        double kb = bytesOffset;
        return kb / 1024;
    }

}
