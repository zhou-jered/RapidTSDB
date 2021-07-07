package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.utils.BinaryUtils;
import org.junit.Assert;
import org.junit.Test;

public class TSBytesTest {

    @Test
    public void testAutoExpand() {
        final int initSize = 60 * 60;
        TSBytes tsBytes = new TSBytes(initSize);
        Assert.assertEquals(tsBytes.getData().length, initSize);
        tsBytes.incBytesOffset(initSize - 10);
        Assert.assertTrue(tsBytes.getData().length > initSize);
        int i = 1;
        while (i < initSize * 2) {
            i += 128;
            tsBytes.incBytesOffset(128);
        }
        Assert.assertTrue(tsBytes.getData().length > initSize * 2);
    }

    @Test
    public void testAppendByte() {
        TSBytes tsBytes = new TSBytes();
        String s = bytes2String(tsBytes.getData(), 32);
        Assert.assertEquals("00000000 00000000 00000000 00000000", s.trim());

        tsBytes.appendBitesDataInternal((byte) -1, 8);
        s = bytes2String(tsBytes.getData(), 32);
        Assert.assertEquals("11111111 00000000 00000000 00000000", s.trim());

        tsBytes.appendBitesDataInternal((byte) -1, 4);
        s = bytes2String(tsBytes.getData(), 32);
        Assert.assertEquals("11111111 11110000 00000000 00000000", s.trim());

        tsBytes.appendBitesDataInternal((byte) 15, 3);
        s = bytes2String(tsBytes.getData(), 32);
        Assert.assertEquals("11111111 11111110 00000000 00000000", s.trim());
        System.out.println(s);

        tsBytes.appendBitesDataInternal((byte) -1, 8);
        tsBytes.appendBitesDataInternal((byte) -1, 8);
        tsBytes.appendBitesDataInternal((byte) -1, 8);
        s = bytes2String(tsBytes.getData(), 32);
        Assert.assertEquals("11111111 11111111 11111111 11111111", s.trim());
    }


    @Test
    public void testAppendInt() {


    }

    @Test
    public void testAppendLong() {

    }


    @Test
    public void testAppendBytesArray() {
    }

    @Test
    public void testTimeEncode() {
        TSBlock tsBlock = TSBlockFactory.newTSBlock(12, 0);
        TSBytes timeBytes = tsBlock.getTime();
        tsBlock.appendDataPoint(12, 12.3);
        System.out.println(bytes2String(timeBytes.getData(), timeBytes.getTotalBitsLength()));
        tsBlock.appendDataPoint(102, 33.3);

        System.out.println(bytes2String(timeBytes.getData(), timeBytes.getTotalBitsLength()));
        System.out.println("byteoffset:" + timeBytes.getBytesOffset() + " bitoffset:" + timeBytes.getBitsOffset() + " totbits:" + timeBytes.getTotalBitsLength());
        tsBlock.appendDataPoint(105, 88.1);

        System.out.println(bytes2String(timeBytes.getData(), timeBytes.getTotalBitsLength()));
        tsBlock.appendDataPoint(106, 88);
        tsBlock.appendDataPoint(107, 89.1);

        System.out.println(bytes2String(timeBytes.getData(), timeBytes.getTotalBitsLength()));
        System.out.println(tsBlock.getDataPoints());
    }

    private static String bytes2String(byte[] bytes, int bitsSize) {
        String s = "";
        int wBits = 0;
        for (int i = 0; i < bytes.length; i++) {
            String t = BinaryUtils.byteBinary(bytes[i]);
            wBits += 8;
            if (wBits > bitsSize) {
                t = t.substring(0, t.length() + (bitsSize - wBits));
                s += t + " ";
                break;
            }
            s += t + " ";
        }
        return s;
    }

    public static String seeByte(byte b) {
        String bs = Integer.toBinaryString(b);
        while (bs.length() < 8) {
            bs = "0" + bs;
        }
        if (bs.length() > 8) {
            return bs.substring(bs.length() - 8);
        }
        return bs;
    }

}
