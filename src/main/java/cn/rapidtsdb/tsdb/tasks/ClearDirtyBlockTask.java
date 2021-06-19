package cn.rapidtsdb.tsdb.tasks;

import cn.rapidtsdb.tsdb.core.TSBlock;
import cn.rapidtsdb.tsdb.core.persistent.TSBlockPersister;

import java.util.Set;

public class ClearDirtyBlockTask implements Runnable {


    private Set<TSBlock> dirtyBlocks;
    private TSBlockPersister blockPersister;

    public ClearDirtyBlockTask(Set<TSBlock> dirtyBlocks, TSBlockPersister blockPersister) {
        this.dirtyBlocks = dirtyBlocks;
        this.blockPersister = blockPersister;
    }

    @Override
    public void run() {

    }
}
