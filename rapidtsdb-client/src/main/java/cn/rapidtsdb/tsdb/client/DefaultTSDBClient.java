package cn.rapidtsdb.tsdb.client;

import cn.rapidtsdb.tsdb.client.event.TSDBUserEventListener;
import cn.rapidtsdb.tsdb.client.handler.ClientChannelInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

@Log4j2
class DefaultTSDBClient implements TSDBClient {

    TSDBClientConfig config;
    TSDBClientSession clientSession;

    private final static boolean DEFAULT_KEEP_ALIVE = true;

    public DefaultTSDBClient(TSDBClientConfig config) {
        this.config = config;
    }


    public void connect() {
        connect(DEFAULT_KEEP_ALIVE);
    }


    public void connect(boolean keepAlive) {
        EventLoopGroup worker = new NioEventLoopGroup(config.getClientThreads());
        Bootstrap bootstrap = new Bootstrap().group(worker);
        bootstrap.channel(NioSocketChannel.class)
                .handler(new ClientChannelInitializer(clientSession))
                .option(ChannelOption.SO_KEEPALIVE, keepAlive)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .option(ChannelOption.TCP_NODELAY, true);
        ChannelFuture channelFuture = bootstrap.connect(fromBootstarp(config.getServerBootstrap()))
                .addListener(ch -> {
                    if (ch.isSuccess()) {
                        log.info("connect {} success", config.getServerBootstrap());
                    } else {
                        log.error("Connect {} failed:{}", config.getServerBootstrap(), ch.cause().getMessage());
                    }
                });
        channelFuture.syncUninterruptibly();
        if (channelFuture.isSuccess()) {
            Channel channel = channelFuture.channel();
            clientSession = new TSDBClientSession(channel);
        } else {
            Throwable cause = channelFuture.cause();
            if (cause != null) {
                throw new RuntimeException(cause);
            } else {
                throw new RuntimeException("Connect " + config.getServerBootstrap() + " Failed");
            }
        }


        log.info("TSDBClient START");
    }


    public void auth() {

    }

    @Override
    public void writeMetric(String metric, long timestamp, double value) {

    }

    @Override
    public void writeMetric(String metric, Datapoint dp) {

    }

    @Override
    public void writeMetric(String metric, double value) {

    }

    @Override
    public void writeMetrics(String metric, List<Datapoint> dps) {

    }

    @Override
    public void writeMetric(String metric, long timestamp, double value, Map<String, String> tags) {

    }

    @Override
    public void writeMetric(String metric, Datapoint dp, Map<String, String> tags) {

    }

    @Override
    public void writeMetric(String metric, double value, Map<String, String> tags) {

    }

    @Override
    public void writeMetrics(String metric, List<Datapoint> dps, Map<String, String> tags) {

    }

    @Override
    public List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, String aggregator) {
        return null;
    }

    @Override
    public List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, String downsampler, String aggregator) {
        return null;
    }

    @Override
    public List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, Map<String, String> tags, String aggregator) {
        return null;
    }

    @Override
    public List<Datapoint> readMetrics(String metric, long startTimestamp, long endTimestamp, Map<String, String> tags, String downsampler, String aggregator) {
        return null;
    }

    @Override
    public void addEventListener(TSDBUserEventListener listener) {

    }

    @Override
    public List<TSDBUserEventListener> getEventListener() {
        return null;
    }

    private InetSocketAddress fromBootstarp(String bootsrap) {
        String host = "127.0.0.1";
        int port = 9099;
        if (StringUtils.isNotEmpty(bootsrap)) {
            String[] parts = bootsrap.trim().split(":");
            host = parts[0];
            if (parts.length > 1) {
                port = Integer.parseInt(parts[1]);
            }
        }
        InetSocketAddress inetSocketAddress = new InetSocketAddress(host, port);
        return inetSocketAddress;
    }


}
