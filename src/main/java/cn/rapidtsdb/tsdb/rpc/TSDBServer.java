package cn.rapidtsdb.tsdb.rpc;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.lifecycle.Runner;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class TSDBServer implements Initializer, Runner, Closer {


    ServerBootstrap serverBootstrap;
    private String ip = "0.0.0.0";
    private int port;

    @Override
    public void close() {

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
        EventLoopGroup boss = new NioEventLoopGroup(5);
        EventLoopGroup worker = new NioEventLoopGroup(10);
        serverBootstrap.group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {

                    }
                }).option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

    }

    @Override
    public void run() {

    }
}
