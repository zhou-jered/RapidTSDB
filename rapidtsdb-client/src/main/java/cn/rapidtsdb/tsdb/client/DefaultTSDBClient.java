package cn.rapidtsdb.tsdb.client;

import cn.rapidtsdb.tsdb.client.event.TSDBUserEventListener;
import cn.rapidtsdb.tsdb.client.handler.ClientChannelInitializer;
import cn.rapidtsdb.tsdb.client.handler.v1.ClientSession;
import cn.rapidtsdb.tsdb.client.handler.v1.ClientSessionRegistry;
import cn.rapidtsdb.tsdb.model.proto.ConnectionAuth;
import cn.rapidtsdb.tsdb.model.proto.TSDataMessage;
import io.netty.bootstrap.Bootstrap;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
class DefaultTSDBClient implements TSDBClient {

    private TSDBClientConfig config;
    private ClientSession clientSession;
    private AtomicInteger reqIdIndex = new AtomicInteger(0);

    private final static boolean DEFAULT_KEEP_ALIVE = true;

    public DefaultTSDBClient(TSDBClientConfig config) {
        this.config = config;
    }


    public void connect() {
        connect(DEFAULT_KEEP_ALIVE);
    }


    public void connect(boolean keepAlive) {
        EventLoopGroup worker = new NioEventLoopGroup(config.getClientThreads());
        worker.scheduleAtFixedRate(() -> checkReqIdIndex(), TimeUnit.MINUTES.toMillis(10), TimeUnit.MINUTES.toMillis(10), TimeUnit.MILLISECONDS);
        ClientSessionRegistry.getRegistry().scheduleRegistry(worker);
        Bootstrap bootstrap = new Bootstrap().group(worker);
        bootstrap.channel(NioSocketChannel.class)
                .handler(new ClientChannelInitializer())
                .option(ChannelOption.SO_KEEPALIVE, keepAlive)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .option(ChannelOption.SO_RCVBUF, 4096)
                .option(ChannelOption.SO_SNDBUF, 4096)
                .option(ChannelOption.TCP_NODELAY, true);
        ChannelFuture channelFuture = bootstrap.connect(fromBootstarp(config.getServerBootstrap())).addListener(ch -> {
            if (ch.isSuccess()) {
                log.info("connect {} success", config.getServerBootstrap());
            } else {
                log.error("Connect {} failed:{}", config.getServerBootstrap(), ch.cause().getMessage());
            }
        });
        log.info("channgel regist");
        clientSession = ClientSessionRegistry.getRegistry().regist(channelFuture.channel(), config);

        channelFuture.syncUninterruptibly();
        if (!channelFuture.isSuccess()) {
            ClientSessionRegistry.getRegistry().deregist(channelFuture.channel());
            Throwable cause = channelFuture.cause();
            if (cause != null) {
                throw new RuntimeException(cause);
            } else {
                throw new RuntimeException("Connect " + config.getServerBootstrap() + " Failed");
            }
        }
        log.info("TSDBClient START");
    }


    public void auth(String authType, Map<String, String> authParams) {
        ConnectionAuth.ProtoAuthMessage.Builder authMessageBuilder =
                ConnectionAuth.ProtoAuthMessage.newBuilder()
                        .setAuthType(authType)
                        .setReqId(reqIdIndex.incrementAndGet());
        if (authParams != null && authParams.size() > 0) {
            authParams.forEach((apk, apv) -> {
                ConnectionAuth.ProtoAuthParams pap = ConnectionAuth.ProtoAuthParams.newBuilder()
                        .setKey(apk)
                        .setValue(apv)
                        .build();
                authMessageBuilder.addAuthParams(pap);
            });
        }
        clientSession.auth(authMessageBuilder.build());
    }

    @Override
    public WriteMetricResult writeMetric(String metric, long timestamp, double value) {
        return null;
    }

    @Override
    public WriteMetricResult writeMetric(String metric, Datapoint dp) {
        return null;
    }

    @Override
    public WriteMetricResult writeMetric(String metric, double value) {
        TSDataMessage.ProtoSimpleDatapoint sdp = TSDataMessage.ProtoSimpleDatapoint.newBuilder()
                .setMetric(metric)
                .setTimestamp(TSTimer.getCachedTimer().getCurrentMills())
                .setVal(value)
                .setReqId(99)
                .build();
        return clientSession.write(sdp);
    }

    @Override
    public WriteMetricResult writeMetrics(String metric, List<Datapoint> dps) {
        return null;
    }

    @Override
    public WriteMetricResult writeMetric(String metric, long timestamp, double value, Map<String, String> tags) {
        return null;
    }

    @Override
    public WriteMetricResult writeMetric(String metric, Datapoint dp, Map<String, String> tags) {
        return null;
    }

    @Override
    public WriteMetricResult writeMetric(String metric, double value, Map<String, String> tags) {
        return null;
    }

    @Override
    public WriteMetricResult writeMetrics(String metric, List<Datapoint> dps, Map<String, String> tags) {
        return null;
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
    public void close() {
        clientSession.close();
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

    public void checkReqIdIndex() {
        if (reqIdIndex.get() > 1e5) {
            while (true) {
                int old = reqIdIndex.get();
                boolean setSucc = reqIdIndex.compareAndSet(old, 0);
                if (setSucc) {
                    break;
                }
            }
        }
    }

}
