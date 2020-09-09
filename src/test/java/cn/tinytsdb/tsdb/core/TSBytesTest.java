package cn.tinytsdb.tsdb.core;

import cn.tinytsdb.tsdb.utils.BinaryUtils;
import org.junit.Assert;
import org.junit.Test;

public class TSBytesTest {

    @Test
    public void testAutoExpand() {
        final int initSize= 60 * 60;
        TSBytes tsBytes = new TSBytes(initSize);
        Assert.assertEquals(tsBytes.getData().length, initSize);
        tsBytes.incBytesOffset(initSize-10);
        Assert.assertTrue(tsBytes.getData().length > initSize);
        int i = 1;
        while(i<initSize*2) {
            i+=128;
            tsBytes.incBytesOffset(128);
        }
        Assert.assertTrue(tsBytes.getData().length > initSize*2);
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

    private String bytes2String(byte[] bytes, int bitsSize) {
        String s = "";
        int wBits = 0;
        for(int i=0;i<bytes.length;i++) {
            String t = BinaryUtils.byteBinary(bytes[i]);
            wBits+=8;
            if(wBits>bitsSize) {
                t = t.substring(0, t.length()+ (bitsSize-wBits));
                s+=t+" ";
                break;
            }
            s+=t+" ";
        }
        return s;
    }
}
