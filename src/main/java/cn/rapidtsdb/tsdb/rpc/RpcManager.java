package cn.rapidtsdb.tsdb.rpc;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.lifecycle.Runner;


public class RpcManager implements Initializer, Runner, Closer {


    private TSDB tsdb;
    private TSDBConfig tsdbConfig;

    @Override
    public void close() {

    }

    @Override
    public void init() {

    }

    @Override
    public void run() {

    }
}
