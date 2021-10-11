package cn.rapidtsdb.tsdb.server.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ServerTcpConfig {

    private Integer backLog;
    private Integer linger;
    private Integer timeout;
    private Integer rcvBuf;
    private Integer sndBuf;
    
    private Boolean noDelay;
    private Boolean keepAlive;
    private Boolean reuseaddr;
    private Boolean broadcast;


    public static final String NODELAY = "nodelay";
    public static final String BACKLOG = "backlog";
    public static final String KEEPALIVE = "keepalive";
    public static final String LINGER = "linger";
    public static final String TIMEOUT = "timeout";
    public static final String REUSEADDR = "reuseaddr";
    public static final String BROADCAST = "broadcast";
    public static final String RCVBUF = "rcvbuf";
    public static final String SNDBUF = "sndbuf";

}
