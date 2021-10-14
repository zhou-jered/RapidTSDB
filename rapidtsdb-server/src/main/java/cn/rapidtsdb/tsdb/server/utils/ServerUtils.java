package cn.rapidtsdb.tsdb.server.utils;

import cn.rapidtsdb.tsdb.server.config.ServerTcpConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ServerUtils {
    public static void configServerTcp(String server, ServerBootstrap serverBootstrap, ServerTcpConfig tcpConfig) {
        if (tcpConfig == null) {
            return;
        }
        if (tcpConfig.getBackLog() != null) {
            int backLog = tcpConfig.getBackLog();
            log.debug("config server {}, backlog: {}", server, backLog);
            serverBootstrap.option(ChannelOption.SO_BACKLOG, backLog);
        }
        if (tcpConfig.getLinger() != null) {
            int linger = tcpConfig.getLinger();
            log.debug("config server {}, linger: {}", server, linger);
            serverBootstrap.childOption(ChannelOption.SO_LINGER, tcpConfig.getLinger());
        }
        
        if (tcpConfig.getRcvBuf() != null) {
            int rcvBuf = tcpConfig.getRcvBuf();
            log.debug("config server {}, rcvBuf: {}", server, rcvBuf);
            serverBootstrap.childOption(ChannelOption.SO_RCVBUF, rcvBuf);
        }
        if (tcpConfig.getSndBuf() != null) {
            int sndBuf = tcpConfig.getSndBuf();
            log.debug("config server {}, backlog: {}", server, sndBuf);
            serverBootstrap.childOption(ChannelOption.SO_SNDBUF, sndBuf);
        }

        if (tcpConfig.getNoDelay() != null) {
            boolean noDelay = tcpConfig.getNoDelay();
            log.debug("config server {}, nodelay: {}", server, noDelay);
            serverBootstrap.childOption(ChannelOption.TCP_NODELAY, noDelay);
        }
        if (tcpConfig.getKeepAlive() != null) {
            boolean keepAlive = tcpConfig.getKeepAlive();
            log.debug("config server {}, keepAlive: {}", server, keepAlive);
            serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, keepAlive);
        }
        if (tcpConfig.getReuseaddr() != null) {
            boolean reuseaddr = tcpConfig.getReuseaddr();
            log.debug("config server {}, reuseaddr: {}", server, reuseaddr);
            serverBootstrap.childOption(ChannelOption.SO_REUSEADDR, reuseaddr);
        }
    }
}
