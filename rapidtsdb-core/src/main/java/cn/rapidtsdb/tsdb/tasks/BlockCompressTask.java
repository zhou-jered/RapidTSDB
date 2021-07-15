package cn.rapidtsdb.tsdb.tasks;

import cn.rapidtsdb.tsdb.BlockCompressStrategy;
import cn.rapidtsdb.tsdb.TsdbRunnableTask;
import cn.rapidtsdb.tsdb.config.TSDBConfig;

/**
 * Scanning the persisted storage and
 * Using the CompressStrategy to compress the older block data
 */
public class BlockCompressTask extends TsdbRunnableTask {

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

    }
}
