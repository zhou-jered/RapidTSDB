package cn.rapidtsdb.tsdb.lab;

import cn.rapidtsdb.tsdb.utils.BinaryUtils;
import lombok.extern.log4j.Log4j2;

@Log4j2(topic = "t")
public class CodecLab {

    static void runBitsMap() {

        System.out.println(1 << 7);
        System.out.println(1 << 8);
        System.out.println(byteBinary((byte) (1 << 7)));
        System.out.println(byteBinary((byte) (1 << 8)));
    }

    static void t() {double pre = 3.112;
        long preLong = Double.doubleToLongBits(pre);
        double ths = 2.0;
        long thisLong = Double.doubleToLongBits(ths);
        System.out.println("pre bits:" + BinaryUtils.longBinary(preLong));
        System.out.println("xor bits:" + BinaryUtils.longBinary(preLong ^ thisLong));
        long a = 0b1001111110011111100111111001111;
        System.out.println(a);

        long xorBits = a << (64 - 12 - 31);
        System.out.println("recover xor bits: " + BinaryUtils.longBinary(xorBits));
        System.out.println("recover result: " + Double.longBitsToDouble(xorBits^preLong));
        System.out.println("test recover: " + Double.longBitsToDouble(preLong ^ thisLong ^ preLong));

    }

    static void t1() {
        long l = 52398457283945l;

        System.out.println(BinaryUtils.longBinary(l));
        System.out.println(BinaryUtils.longBinary(l ^ 0));
        System.out.println(BinaryUtils.longBinary(l ^ -1));
    }

    public static void main(String[] args) {
        t1();
    }

    static String longBinary(long v) {
        String s = "";
        for (int i = 7; i >= 0; i--) {
            byte byt = (byte) (v >> (i * 8));
            s += byteBinary(byt) + " ";
        }
        return s;
    }

    static String byteBinary(byte b) {
        char[] chars = new char[8];
        for (int i = 7; i >= 0; i--) {
            char c = (((b & (1 << i)) == (1 << i)) ? '1' : '0');
            chars[7 - i] = c;
        }
        return new String(chars);
    }


}
