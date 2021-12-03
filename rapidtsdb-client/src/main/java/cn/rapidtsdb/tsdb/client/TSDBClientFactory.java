package cn.rapidtsdb.tsdb.client;

import cn.rapidtsdb.tsdb.model.proto.ConnectionAuth;

import java.util.Map;

public class TSDBClientFactory {
    public static TSDBClient getTSDBClient(TSDBClientConfig clientConfig) {
        DefaultTSDBClient defaultTSDBClient = new DefaultTSDBClient(clientConfig);
        defaultTSDBClient.connect();

        ConnectionAuth.ProtoAuthMessage.Builder authMessageBuilder = ConnectionAuth.ProtoAuthMessage.newBuilder()
                .setAuthType(clientConfig.getAuthType());
        Map<String, String> authParams = clientConfig.getAuthParams();

        if (authParams != null && authParams.size() > 0) {
            authParams.forEach((apk, apv) -> {
                ConnectionAuth.ProtoAuthParams pap = ConnectionAuth.ProtoAuthParams.newBuilder()
                        .setKey(apk)
                        .setValue(apv)
                        .build();
                authMessageBuilder.addAuthParams(pap);
            });
            defaultTSDBClient.auth(authMessageBuilder.build());
        }
        return defaultTSDBClient;
    }
}
