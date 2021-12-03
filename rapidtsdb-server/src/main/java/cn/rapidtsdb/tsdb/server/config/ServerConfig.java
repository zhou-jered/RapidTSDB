package cn.rapidtsdb.tsdb.server.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

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
    private int maxClientNumber = 1024;
    private ServerTcpConfig tcp;

}
