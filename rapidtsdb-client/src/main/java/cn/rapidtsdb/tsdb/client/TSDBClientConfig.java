package cn.rapidtsdb.tsdb.client;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;

public class TSDBClientConfig {

    @Getter
    private String serverBootstrap;
    @Getter
    private String protocolVersion;

    private void validateConfig() {
        if (StringUtils.isEmpty(serverBootstrap)) {
            throw new RuntimeException("Config Error, No ServerBootstrap config");
        }
    }


    public static class TSDBClientConfigBuilder {
        private TSDBClientConfig config;

        public TSDBClientConfigBuilder() {
            this.config = new TSDBClientConfig();
        }

        public TSDBClientConfigBuilder serverBootstrap(String serverBootstrap) {
            config.serverBootstrap = serverBootstrap;
            return this;
        }

        public TSDBClientConfigBuilder protocolVersion(String protocolVersion) {
            config.protocolVersion = protocolVersion;
            return this;
        }

        public TSDBClientConfig build() {
            config.validateConfig();
            return config;
        }
    }

    public static TSDBClientConfigBuilder newBuilder() {
        return new TSDBClientConfigBuilder();
    }
}
