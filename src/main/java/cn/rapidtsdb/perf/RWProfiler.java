package cn.rapidtsdb.perf;

import cn.rapidtsdb.tsdb.core.AbstractTSBlockManager;
import cn.rapidtsdb.tsdb.core.TSBlock;
import cn.rapidtsdb.tsdb.core.TSBlockManagerProvider;
import cn.rapidtsdb.tsdb.utils.TimeUtils;

import java.util.concurrent.TimeUnit;

public class RWProfiler {

    public static void main(String[] args) {
        long baseTime = TimeUtils.truncateDayMills(System.currentTimeMillis()) / 1000;
        TSBlock tsBlock = new TSBlock(baseTime, (int) TimeUnit.HOURS.toSeconds(2)
                , TimeUtils.TimeUnitAdaptorFactory.getTimeAdaptor("s"));
        tsBlock.appendDataPoint(baseTime + 1, 1233);
        AbstractTSBlockManager blockManager = TSBlockManagerProvider.getBlockManagerInstance();

    }

}
