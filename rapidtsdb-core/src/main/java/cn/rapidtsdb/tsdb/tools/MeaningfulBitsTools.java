package cn.rapidtsdb.tsdb.tools;

import cn.rapidtsdb.tsdb.core.TSBytes;
import cn.rapidtsdb.tsdb.utils.BinaryUtils;

public class MeaningfulBitsTools {

    public static void valuesBits(TSBytes values) {
        byte[] bytes = values.getData();
        int byteOffset = values.getBytesOffset();
        int bitOffset= values.getBitsOffset();

        String s = "";
        for(int i =0;i<8;i++) {
            s += BinaryUtils.byteBinary(bytes[i])+" ";
        }
        s+="[first val] ";
        int bitReadPos = 64;
        while(bitReadPos < values.getTotalBitsLength()) {
            if(probeBit(bytes, bitReadPos) == 0) {
                s+="0[0] ";
                bitReadPos+=1;
            } else if(probeBit(bytes, bitReadPos+1) == 0) {
                s+= "[10] ";
                bitReadPos+=2;
            } else {
                s+= "[11] ";
                bitReadPos+=2;
            }

        }

    }

    private static int probeBit(byte[] bytes, int bitPos) {
        int readBytesIdx = bitPos / 8;
        return bytes[readBytesIdx] & (1 << (7 - bitPos % 8));
    }

}
