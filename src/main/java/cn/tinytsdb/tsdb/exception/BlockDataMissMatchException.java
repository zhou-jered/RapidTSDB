package cn.tinytsdb.tsdb.exception;

import lombok.Data;

@Data
public class BlockDataMissMatchException extends RuntimeException{
    private long dataTimestamp;
    private long baseTime;
    private int blockLengthSeconds;

    public BlockDataMissMatchException(long dataTimestamp, long baseTime, int blockLengthSeconds) {
        super(dataTimestamp+" not in this block, baseTime: "+ baseTime +" blockLength:"+blockLengthSeconds);
        this.dataTimestamp = dataTimestamp;
        this.baseTime = baseTime;
        this.blockLengthSeconds = blockLengthSeconds;
    }

}
