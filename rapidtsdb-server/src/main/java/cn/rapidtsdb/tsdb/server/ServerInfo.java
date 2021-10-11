package cn.rapidtsdb.tsdb.server;

import cn.rapidtsdb.tsdb.server.config.ServerConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ServerInfo {
    private String SERVER_VERSION = "0.0.1";
    private List<String> supportedProtocol = new ArrayList<>();
    private List<ServerConfig> configs;
}
