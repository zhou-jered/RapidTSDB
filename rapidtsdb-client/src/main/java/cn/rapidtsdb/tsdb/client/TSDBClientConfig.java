package cn.rapidtsdb.tsdb.client;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;

import java.util.Properties;

import static cn.rapidtsdb.tsdb.client.TSDBClientConfigConstants.*;

@Log4j2
public class TSDBClientConfig {

    @Getter
    private String serverBootstrap;
    @Getter
    private String protocolVersion = "1";
    @Getter
    private String authType = AUTH_TYPE_NONE;
    @Getter
    private byte[] authCredentials = null;
    @Getter
    private String writeMode = WRITE_MODE_BATCH;
    @Getter
    private int clientThreads = 2;

    private void validateConfig() {
        if (StringUtils.isEmpty(serverBootstrap)) {
            throw new RuntimeException("Config Error, No ServerBootstrap config");
        }
        if (!protocolVersion.equals("1")) {
            log.error("Unsupported Protocol version:{}", protocolVersion);
            throw new RuntimeException("protocol version unsupported");
        }
        if (!authType.equals(AUTH_TYPE_NONE) && !authType.equals(AUTH_TYPE_TOKNE)) {
            log.error("Unknown auth type:{}", authType);
            throw new RuntimeException("Unknown authType");
        }
        if (!writeMode.equals(WRITE_MODE_PUSH) && !writeMode.equals(WRITE_MODE_BATCH)) {
            log.error("Unknown write mode:{}", writeMode);
            throw new RuntimeException("Unknown writeMode");
        }
        if (clientThreads < 1 || clientThreads > 128) {
            log.error("unsupported client thread number config:{}", clientThreads);
            throw new RuntimeException("Unsupported client therads config, support client thread number range [1-128]");
        }
    }


    public static class TSDBClientConfigBuilder {
        private TSDBClientConfig config;

        public static TSDBClientConfigBuilder newBuilder() {
            return new TSDBClientConfigBuilder();
        }

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

        public TSDBClientConfigBuilder authType(String authType) {
            config.authType = authType;
            return this;
        }

        public TSDBClientConfigBuilder authCredentials(byte[] credentials) {
            config.authCredentials = credentials;
            return this;
        }

        public TSDBClientConfigBuilder clientThreads(int clientThread) {
            config.clientThreads = clientThread;
            return this;
        }

        public TSDBClientConfigBuilder properties(Properties properties) {
            if (properties != null) {
                if (properties.containsKey("server.bootstrap")) {
                    config.serverBootstrap = properties.getProperty("server.bootstrap");
                }
                if (properties.containsKey("protocol.version")) {
                    config.protocolVersion = properties.getProperty("protocol.version");
                }
                if (properties.containsKey("auth.type")) {
                    config.authType = properties.getProperty("auth.type");
                }
                if (properties.containsKey("auth.credentials")) {
                    config.authCredentials = properties.getProperty("auth.credentials").getBytes();
                }
                if (properties.containsKey("client.threads")) {
                    config.clientThreads = Integer.parseInt(properties.getProperty("client.threads"));
                }
            } else {
                log.error("config client using NULL properties, nothing config changed");
            }
            return null;
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
