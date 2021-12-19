package cn.rapidtsdb.tsdb.protocol;

import com.google.protobuf.Parser;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RpcObjectCode {

    private static Map<String, Integer> protoObjectCodeMap = new ConcurrentHashMap<>(128);
    private static Map<Integer, Parser> protoParserMap = new ConcurrentHashMap<>();

    static {
        protoObjectCodeMap.put("cn.rapidtsdb.tsdb.model.proto.ConnectionAuth$ProtoAuthParams", 1);
        protoObjectCodeMap.put("cn.rapidtsdb.tsdb.model.proto.ConnectionAuth$ProtoAuthMessage", 2);
        protoObjectCodeMap.put("cn.rapidtsdb.tsdb.model.proto.ConnectionAuth$ProtoAuthResp", 3);
//        protoObjectCodeMap.put("cn.rapidtsdb.tsdb.model.proto.ConnectionInit$ProtoConnectionConfig", 4); // removed
//        protoObjectCodeMap.put("cn.rapidtsdb.tsdb.model.proto.TSDataMessage$ProtoTSTag", 5); // removed
        protoObjectCodeMap.put("cn.rapidtsdb.tsdb.model.proto.TSDataMessage$ProtoDatapoint", 6);
        protoObjectCodeMap.put("cn.rapidtsdb.tsdb.model.proto.TSDataMessage$ProtoSimpleDatapoint", 7);
        protoObjectCodeMap.put("cn.rapidtsdb.tsdb.model.proto.TSDataMessage$ProtoDatapoints", 8);
        protoObjectCodeMap.put("cn.rapidtsdb.tsdb.model.proto.TSDBResponse$ProtoDataResponse", 9);
        protoObjectCodeMap.put("cn.rapidtsdb.tsdb.model.proto.TSDBResponse$ProtoCommonResponse", 10);
        protoObjectCodeMap.put("cn.rapidtsdb.tsdb.model.proto.TSQueryMessage$ProtoTSQuery", 11);

        for (String clsName : protoObjectCodeMap.keySet()) {
            Integer code = protoObjectCodeMap.get(clsName);
            try {
                Class clz = Class.forName(clsName);
                Method method = clz.getMethod("parser");
                Parser parser = (Parser) method.invoke(null);
                protoParserMap.put(code, parser);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    public static final Parser getObjectProtoParser(int objectCode) {
        Parser parser = protoParserMap.get(objectCode);
        return parser;
    }


    public static final short getObjectCode(Class protoClass) {
        return getObjectCode(protoClass.getName());
    }

    public static final short getObjectCode(String classCanonicalName) {

        Integer code = protoObjectCodeMap.get(classCanonicalName);
        if (code != null) {
            return code.shortValue();
        }
        throw new RuntimeException("UnDefined ProtoObject Code");
    }

}
