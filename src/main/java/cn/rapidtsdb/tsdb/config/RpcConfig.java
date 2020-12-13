package cn.rapidtsdb.tsdb.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RpcConfig {

    private EndPointConfig grpc;
    private EndPointConfig http;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EndPointConfig {
        private boolean enable = true;
        private String ip;
        private int port;
    }
}
