package cn.rapidtsdb.tsdb.server;

import cn.rapidtsdb.tsdb.app.TsdbRunnableTask;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.lifecycle.Runner;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class TSDBServer implements Initializer, Runner, Closer {

    private TSDB db;

    NioEventLoopGroup workerGroup = null;
    NioEventLoopGroup bossGroup = null;
    ServerBootstrap serverBootstrap;
    private String ip = "0.0.0.0";
    private int port;
    ChannelFuture serverChannelFuture = null;

    public TSDBServer(TSDB db) {
        this.db = db;
    }

    @Override
    public void close() {
        if (serverChannelFuture != null) {
            try {
                serverChannelFuture.channel().closeFuture().sync();
            } catch (InterruptedException e) {
            } finally {
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }
        }
    }

    @Override
    public void init() {
        TSDBConfig config = TSDBConfig.getConfigInstance();
        String bindIp = config.getRpcGrpcIp();
        if (bindIp != null && bindIp.length() > 0) {
            ip = bindIp;
        }
        port = config.getRpcGrpcPort();
        serverBootstrap = new ServerBootstrap();
        //todo configurable
        bossGroup = new NioEventLoopGroup(5);
        workerGroup = new NioEventLoopGroup(10);
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new TSDBChannelInitializer())
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
    }

    @Override
    public void run() {
        Thread serverThread = ManagedThreadPool.getInstance().newThread(new TsdbRunnableTask() {
            @Override
            public int getRetryLimit() {
                return 0;
            }

            @Override
            public String getTaskName() {
                return "TSDB-Rpc-Server";
            }

            @Override
            public void run() {
                run0();
            }
        });
        serverThread.start();
    }

    private void run0() {
        try {
            serverChannelFuture = serverBootstrap.bind(port);
            serverChannelFuture.sync();
            log.info("Server listening: {}", port);
            serverChannelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {

        }
    }
}
