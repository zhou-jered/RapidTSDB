package cn.rapidtsdb.tsdb.client;

import cn.rapidtsdb.tsdb.client.handler.ClientChannelInitializer;
import io.netty.bootstrap.Bootstrap;
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
public class DefaultTSDBClient implements TSDBClient {

    TSDBClientConfig config;

    public DefaultTSDBClient(TSDBClientConfig config) {
        this.config = config;
    }

    @Override
    public void start(boolean keepAlive, long keepAliveTimeMills) {
        EventLoopGroup worker = new NioEventLoopGroup(config.getClientThreads());
        Bootstrap bootstrap = new Bootstrap().group(worker);
        bootstrap.channel(NioSocketChannel.class)
                .handler(new ClientChannelInitializer(config))
                .option(ChannelOption.SO_KEEPALIVE, keepAlive)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .option(ChannelOption.TCP_NODELAY, true);
        bootstrap.connect(fromBootstarp(config.getServerBootstrap()))
                .addListener(ch -> {
                    if (ch.isSuccess()) {
                        log.info("connect {} success", config.getServerBootstrap());
                    } else {
                        log.error("Connect {} failed:{}", config.getServerBootstrap(), ch.cause().getMessage());
                    }
                });
        log.info("TSDBClient START");
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
