package cn.tinytsdb.tsdb.core;

import cn.tinytsdb.tsdb.core.persistent.file.FileLocation;
import cn.tinytsdb.tsdb.utils.TimeUtils;
import lombok.Getter;
import lombok.Setter;

public class HistoryTSBlock extends TSBlock{

    @Getter @Setter
    private FileLocation fileLocation;
    @Getter @Setter
    private TSBlockMeta originBlockMeta;
    @Getter @Setter
    private long fileBytesOffset;
    @Getter
    private long lastUpdateTimestamp;

    public HistoryTSBlock(long baseTime, int blockLengthSeconds, TimeUtils.TimeUnitAdaptor timeUnitAdapter) {
        super(baseTime, blockLengthSeconds, timeUnitAdapter);
    }


    @Override
    public void appendDataPoint(long timestamp, double val) {

        lastUpdateTimestamp = TimeUtils.currentTimestamp();
        super.appendDataPoint(timestamp, val);
    }



}
