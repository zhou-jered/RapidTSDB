package cn.rapidtsdb.tsdb.rpc;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.lifecycle.Runner;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class RpcManager implements Initializer, Runner, Closer {

    private TSDB tsdb;
    private TSDBConfig tsdbConfig;

    public RpcManager(TSDB tsdb) {
        this.tsdb = tsdb;
        tsdbConfig = TSDBConfig.getConfigInstance();
    }

    @Override
    public void close() {
        log.info("Closing RpcManager");
        log.info("RpcManager close completed");
    }

    @Override
    public void init() {

    }

    @Override
    public void run() {

    }
}
