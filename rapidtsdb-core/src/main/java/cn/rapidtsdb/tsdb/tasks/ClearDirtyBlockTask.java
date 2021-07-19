package cn.rapidtsdb.tsdb.tasks;

import cn.rapidtsdb.tsdb.TSDBRunnableTask;
import cn.rapidtsdb.tsdb.core.TSBlock;
import cn.rapidtsdb.tsdb.core.persistent.TSBlockPersister;
import lombok.extern.log4j.Log4j2;

import java.util.Map;

@Log4j2
public class ClearDirtyBlockTask extends TSDBRunnableTask implements Runnable {


    private Map<Integer, TSBlock> dirtyBlocks;
    private TSBlockPersister blockPersister;

    public ClearDirtyBlockTask(Map<Integer, TSBlock> dirtyBlocks, TSBlockPersister blockPersister) {
        this.dirtyBlocks = dirtyBlocks;
        this.blockPersister = blockPersister;
    }

    @Override
    public void run() {
        log.debug("Clear Dirty Block Tasking Running, dirty Block Size:{}", dirtyBlocks.size());

    }

    @Override
    public int getRetryLimit() {
        return 10;
    }

    @Override
    public String getTaskName() {
        return "RapidTSDB-DirtyBlock-Clear-Task";
    }
}
