package cn.rapidtsdb.tsdb.client;

import cn.rapidtsdb.tsdb.model.proto.ConnectionAuth;
import lombok.extern.log4j.Log4j2;

import java.util.Map;

@Log4j2
public class TSDBClientFactory {
    public static TSDBClient getTSDBClient(TSDBClientConfig clientConfig) {
        DefaultTSDBClient defaultTSDBClient = new DefaultTSDBClient(clientConfig);
        defaultTSDBClient.connect();

        ConnectionAuth.ProtoAuthMessage.Builder authMessageBuilder = ConnectionAuth.ProtoAuthMessage.newBuilder()
                .setAuthType(clientConfig.getAuthType());
        Map<String, String> authParams = clientConfig.getAuthParams();
        if (authParams != null && authParams.size() > 0) {
            authParams.forEach((apk, apv) -> {
                ConnectionAuth.ProtoAuthMessage pam = null;
                ConnectionAuth.ProtoAuthParams pap = ConnectionAuth.ProtoAuthParams.newBuilder()
                        .setKey(apk)
                        .setValue(apv)
                        .build();
                authMessageBuilder.addAuthParams(pap);
            });
            log.debug("send auth");
            defaultTSDBClient.auth(authMessageBuilder.build());
        }
        return defaultTSDBClient;
    }
}
