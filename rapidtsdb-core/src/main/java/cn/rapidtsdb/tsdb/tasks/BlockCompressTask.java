package cn.rapidtsdb.tsdb.tasks;

import cn.rapidtsdb.tsdb.BlockCompressStrategy;
import cn.rapidtsdb.tsdb.TSDBRunnableTask;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import lombok.extern.log4j.Log4j2;

/**
 * Scanning the persisted storage and
 * Using the CompressStrategy to compress the older block data
 */
@Log4j2
public class BlockCompressTask extends TSDBRunnableTask {

    BlockCompressStrategy compressStrategy;

    public BlockCompressTask() {
        compressStrategy = TSDBConfig.getConfigInstance().getBlockCompressStrategy();
    }

    @Override
    public int getRetryLimit() {
        return 10;
    }

    @Override
    public String getTaskName() {
        return "BlockCompressTask";
    }

    @Override
    public void run() {
        log.info("{} running", getTaskName());
    }
}
