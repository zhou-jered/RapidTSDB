package cn.rapidtsdb.tsdb.core;

import lombok.Getter;

/**
 * Immutable TSBlock
 */
@Getter
public class TSBlockSnapshot {
    private TSBlock tsBlock;
    private int dataVersion;
    private int clearedVersion;
    private int dpsSize;
    private int timeBytesLength;
    private int timeBitsLength;
    private int valuesBitsLength;
    private int valuesBytesLength;

    TSBlockSnapshot(TSBlock tsBlock) {
        this.tsBlock = tsBlock;
        this.dataVersion = tsBlock.getBlockVersion();
        this.clearedVersion = tsBlock.getClearedVersion();
        this.dpsSize = tsBlock.getDataPoints().size();
        this.timeBytesLength = tsBlock.getTime().getBytesOffset();
        this.valuesBytesLength = tsBlock.getValues().getBytesOffset();
        this.timeBitsLength = tsBlock.getTime().getTotalBitsLength();
        this.valuesBitsLength = tsBlock.getValues().getTotalBitsLength();
    }
}
