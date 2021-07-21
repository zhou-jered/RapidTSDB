package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.common.TimeUtils;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

public class TSBlockTime implements Initializer {

    @Override
    public void init() {
        long currentSeconds = TimeUtils.currentSeconds();
        currentBlockTimeSeconds = currentSeconds - currentSeconds % TimeUnit.HOURS.toSeconds(2);
    }

    @Setter
    @Getter
    private long currentBlockTimeSeconds;


}
