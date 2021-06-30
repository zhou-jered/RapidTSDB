package cn.rapidtsdb.tsdb.core;

/**
 * byte bits & mask
 */
public class ByteMask {


    static long B_MASK_FULL =  0xff;
    static long B_MASK_NONE =  0x0;
    static long B_MASK_LEFT_1 =  0x80;
    static long B_MASK_LEFT_2 =  0xc0;
    static long B_MASK_LEFT_3 =  0xe0;
    static long B_MASK_LEFT_4 =  0xf0;
    static long B_MASK_LEFT_5 =  0xf8;
    static long B_MASK_LEFT_6 =  0xfc;
    static long B_MASK_LEFT_7 =  0xfe;
    static long B_MASK_RIGHT_1 =  0x01;
    static long B_MASK_RIGHT_2 =  0x03;
    static long B_MASK_RIGHT_3 =  0x07;
    static long B_MASK_RIGHT_4 =  0x0f;
    static long B_MASK_RIGHT_5 =  0x1f;
    static long B_MASK_RIGHT_6 =  0x3f;
    static long B_MASK_RIGHT_7 =  0x7f;
    static long[] LEFT_MASK = new long[]{B_MASK_NONE, B_MASK_LEFT_1, B_MASK_LEFT_2, B_MASK_LEFT_3, B_MASK_LEFT_4, B_MASK_LEFT_5, B_MASK_LEFT_6, B_MASK_LEFT_7, B_MASK_FULL};
    static long[] RIGHT_MASK = new long[]{B_MASK_NONE, B_MASK_RIGHT_1, B_MASK_RIGHT_2, B_MASK_RIGHT_3, B_MASK_RIGHT_4, B_MASK_RIGHT_5, B_MASK_RIGHT_6, B_MASK_RIGHT_7, B_MASK_FULL};

}
