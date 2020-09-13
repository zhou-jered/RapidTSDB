package cn.tinytsdb.tsdb.utils;

public class BinaryUtils {

    public static String byteBinary(byte b) {
        char[] chars = new char[8];
        for (int i = 7; i >= 0; i--) {
            char c = (((b & (1 << i)) == (1 << i)) ? '1' : '0');
            chars[7 - i] = c;
        }
        return new String(chars);
    }

    public static String bytesBinary(byte[] bytes, int bitsLength) {
        String s = "";
        for (int i = 0; i < bitsLength / 8; i++) {
            s += byteBinary(bytes[i]) + " ";
        }
        String remainBits = byteBinary(bytes[bitsLength/8]).substring(0, bitsLength%8);
        return s+remainBits;
    }

    public static String longBinary(long v) {
        String s = "";
        for (int i = 7; i >= 0; i--) {
            byte byt = (byte) (v >> (i * 8));
            s += byteBinary(byt) + " ";
        }
        return s;
    }

    public static String intBinary(int v) {
        String s = "";
        for (int i = 3; i >= 0; i--) {
            byte byt = (byte) (v >> (i * 8));
            s += byteBinary(byt) + " ";
        }
        return s;
    }


    public static String hexBytes(byte[] bytes) {
        if(bytes==null) {
            return "";
        }
        String hex = "";
        for (byte b : bytes) {
            String bHex = Long.toHexString(b);
            if(bHex.length()==1) {
                bHex="0"+bHex;
            } else if (bHex.length()>2) {
                bHex = bHex.substring(bHex.length()-2);
            }
            hex += bHex;
        }
        return hex;
    }


    public static void main(String[] args) {
        byte b = -4;
        byte r = 0b0110000;
        System.out.println("b bytes: " + byteBinary(b));
        System.out.println(r+" : bytes: " +byteBinary(r));
        int bitsOffset = 5;
        r |= b>>bitsOffset;
        System.out.println("After : " + byteBinary(r));
        byte mask = 0x7;
        System.out.println(byteBinary(b));
        System.out.println(byteBinary((byte) 0x7));
        byte res = (byte) (b&mask);
        System.out.println(byteBinary(res));
    }

}
