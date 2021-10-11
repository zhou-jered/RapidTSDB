package cn.rapidtsdb.tsdb.server.config;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class ServerConfig {

    private ServerProtocol protocol;
    private boolean enable = false;
    private int port;
    private String ip; //bind ip
    private int ioCore = 1;
    private int ioMax = 1;
    private ServerTcpConfig tcp;

}
