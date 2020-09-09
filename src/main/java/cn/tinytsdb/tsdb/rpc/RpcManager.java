package cn.tinytsdb.tsdb.rpc;

import cn.tinytsdb.tsdb.config.TSDBConfig;
import cn.tinytsdb.tsdb.core.TSDB;
import cn.tinytsdb.tsdb.lifecycle.Closer;
import cn.tinytsdb.tsdb.lifecycle.Initializer;
import cn.tinytsdb.tsdb.lifecycle.Runner;


public class RpcManager implements Initializer, Runner, Closer {


    private TSDB tsdb;
    private TSDBConfig tsdbConfig;

    @Override
    public void close() {

    }

    @Override
    public void init() {
        tsdbConfig = TSDBConfig.getConfigInstance();

    }

    @Override
    public void run() {

    }
}
