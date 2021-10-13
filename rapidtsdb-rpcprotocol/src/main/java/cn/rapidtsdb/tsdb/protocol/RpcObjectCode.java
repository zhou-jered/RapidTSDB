package cn.rapidtsdb.tsdb.protocol;

import cn.rapidtsdb.tsdb.model.proto.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RpcObjectCode {
    private static Map<String, Integer> protoObjectCodeMap = new ConcurrentHashMap<>(128);

    static {
        protoObjectCodeMap.put(ConnectionInit.ProtoConnectionConfig.class.getCanonicalName(), 1);
        protoObjectCodeMap.put(ConnectionAuth.ProtoAuthMessage.class.getCanonicalName(), 2);

        protoObjectCodeMap.put(ConnectionAuth.ProtoAuthMessage.class.getCanonicalName(), 3);
        protoObjectCodeMap.put(ConnectionAuth.ProtoAuthParams.class.getCanonicalName(), 4);
        protoObjectCodeMap.put(ConnectionAuth.ProtoAuthResp.class.getCanonicalName(), 5);


        protoObjectCodeMap.put(TSDataMessage.ProtoDatapoint.class.getCanonicalName(), 6);
        protoObjectCodeMap.put(TSDataMessage.ProtoDatapoints.class.getCanonicalName(), 7);
        protoObjectCodeMap.put(TSDataMessage.ProtoTSTag.class.getCanonicalName(), 8);

        protoObjectCodeMap.put(TSDBResponse.ProtoCommonResponse.class.getCanonicalName(), 9);
        protoObjectCodeMap.put(TSDBResponse.ProtoDataResponse.class.getCanonicalName(), 10);

        protoObjectCodeMap.put(TSQueryMessage.ProtoTSQuery.class.getCanonicalName(), 11);


    }

    public static final int getObjectCode(String classCanonicalName) {

        Integer code = protoObjectCodeMap.get(classCanonicalName);
        if (code != null) {
            return code;
        }
        throw new RuntimeException("UnDefined ProtoObject Code");
    }

}
