package cn.rapidtsdb.tsdb.tasks;

import cn.rapidtsdb.tsdb.core.TSDB;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class TwoHoursTriggerTask implements Runnable {

    TSDB tsdb;

    public TwoHoursTriggerTask(TSDB tsdb) {
        this.tsdb = tsdb;
    }

    @Override
    public void run() {
        tsdb.triggerBlockPersist();
    }
}
