package cn.rapidtsdb.tsdb.tasks;

import cn.rapidtsdb.tsdb.TSDBRetryableTask;
import cn.rapidtsdb.tsdb.core.TSBlock;
import cn.rapidtsdb.tsdb.core.persistent.TSBlockPersister;
import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicReference;

@Log4j2
public class ClearDirtyBlockTask extends TSDBRetryableTask implements Runnable {


    private AtomicReference<Map<Integer, SortedMap<Long, TSBlock>>> flyingTSBlocks;
    private TSBlockPersister blockPersister;

    public ClearDirtyBlockTask(AtomicReference<Map<Integer, SortedMap<Long, TSBlock>>> flyingTSBlocks, TSBlockPersister blockPersister) {
        this.flyingTSBlocks = flyingTSBlocks;
        this.blockPersister = blockPersister;
    }

    @Override
    public void run() {
        Map<Integer, SortedMap<Long, TSBlock>> metricBlocks = flyingTSBlocks.get();
        /**
         * Dirty blocks are persist batch by batch, so the state of blocks may in the flying or in
         * the storage, but its ok, this inconsistently will only cause data duplicated, the query logical
         * will handle it and remove the duplicated data points.
         * The situation mentioned above happened in a very low possibility.
         */
        for (int mid : metricBlocks.keySet()) {
            Map<Long, TSBlock> currentBatchBlocks = metricBlocks.get(mid);
            log.debug("Clear Dirty Block Tasking Running, dirty Block Size:{}", currentBatchBlocks.size());
            blockPersister.persistBlocksSync(mid, metricBlocks.get(mid).values());
        }
        // after persist, clear the reference to tell Manager find the Block data in the storage
        flyingTSBlocks.set(null);

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
