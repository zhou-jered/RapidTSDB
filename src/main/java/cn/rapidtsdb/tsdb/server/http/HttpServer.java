package cn.rapidtsdb.tsdb.server.http;

import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.lifecycle.Runner;


public class HttpServer implements Initializer, Runner, Closer {

    private TSDB db;

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
