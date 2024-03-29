package cn.rapidtsdb.tsdb.tasks;

import cn.rapidtsdb.tsdb.TSDBRetryableTask;
import cn.rapidtsdb.tsdb.core.TSDB;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class TwoHoursTriggerTask extends TSDBRetryableTask implements Runnable {

    TSDB tsdb;

    public TwoHoursTriggerTask(TSDB tsdb) {
        this.tsdb = tsdb;
    }

    @Override
    public void run() {
        log.debug("TwoHoursTriggerTask run");
        tsdb.triggerBlockPersist();
    }

    @Override
    public int getRetryLimit() {
        return 0;
    }

    @Override
    public String getTaskName() {
        return "TwoHoursTriggerTask";
    }
}
