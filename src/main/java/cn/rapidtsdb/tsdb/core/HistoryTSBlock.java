package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.utils.TimeUtils;
import cn.rapidtsdb.tsdb.core.persistent.file.FileLocation;
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
