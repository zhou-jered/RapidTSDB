package cn.rapidtsdb.tsdb.server;

import cn.rapidtsdb.tsdb.TSDataOperationQueue;
import cn.rapidtsdb.tsdb.app.AppInfo;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.lifecycle.Runner;
import cn.rapidtsdb.tsdb.server.config.ServerConfig;
import cn.rapidtsdb.tsdb.server.config.ServerProtocol;
import cn.rapidtsdb.tsdb.server.handler.admin.AdminChannelInitializer;
import cn.rapidtsdb.tsdb.server.handler.console.ConsoleChannelInitializer;
import cn.rapidtsdb.tsdb.server.handler.http.HttpChannelInitializer;
import cn.rapidtsdb.tsdb.server.handler.rpc.BinChannelInitializer;
import cn.rapidtsdb.tsdb.server.handler.rpc.ServerSessionRegistry;
import cn.rapidtsdb.tsdb.server.utils.ServerUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ProtocolServerInstance implements Initializer, Runner, Closer {
    private ServerConfig serverConfig;
    NioEventLoopGroup workerGroup = null;
    NioEventLoopGroup bossGroup = null;
    ServerBootstrap serverBootstrap;
    private String ip = "0.0.0.0";
    private int port;
    ChannelFuture serverChannelFuture = null;

    public ProtocolServerInstance(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    private ServerProtocol getServerProtocol() {
        return serverConfig.getProtocol();
    }

    @Override
    public void close() {
        log.info("{} shutdown", serverConfig.getProtocol());
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
        String bindIp = serverConfig.getIp();
        if (bindIp != null && bindIp.length() > 0) {
            ip = bindIp;
        }
        port = serverConfig.getPort();
        serverBootstrap = new ServerBootstrap();
        //todo configurable
        bossGroup = new NioEventLoopGroup(5);
        workerGroup = new NioEventLoopGroup(10);
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(getServerInitializer(serverConfig.getProtocol()));
        ServerUtils.configServerTcp(serverConfig.getProtocol().name(),
                serverBootstrap, serverConfig.getTcp());
        initProtocolComponent();
    }

    private void initProtocolComponent() {
        switch (serverConfig.getProtocol()) {
            case bin:
                ServerSessionRegistry.init(serverConfig);
                break;
            default:
                ;
        }
    }

    @Override
    public void run() {
        TSDataOperationQueue queue = TSDataOperationQueue.getQ();
        queue.start();
        serverChannelFuture = serverBootstrap.bind(port);
        serverChannelFuture.addListener((ChannelFutureListener) future -> {
                    if (future.isDone() && future.isSuccess()) {
                        AppInfo.setApplicationState(AppInfo.ApplicationState.RUNNING);
                        log.info("{} Server listening: {}", serverConfig.getProtocol(), port);
                    } else if (future.isCancelled()) {
                        log.error("Server Launch Cancelled");
                    } else if (!future.isSuccess()) {
                        log.error("Server Failed,{}", future.cause());
                    }
                }
        );
    }

    private ChannelInitializer<NioSocketChannel> getServerInitializer(ServerProtocol protocol) {
        switch (protocol) {
            case admin:
                return new AdminChannelInitializer();
            case console:
                return new ConsoleChannelInitializer();
            case bin:
                return new BinChannelInitializer();
            case http:
            case httpopentsdb:
                return new HttpChannelInitializer();
            default:
                throw new RuntimeException("Should not be here, PROTOCOL ERROR");
        }
    }
}
