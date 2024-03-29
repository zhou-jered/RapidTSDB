package cn.rapidtsdb.tsdb.protocol.gen;

import cn.rapidtsdb.tsdb.model.proto.ConnectionAuth;
import cn.rapidtsdb.tsdb.model.proto.TSDBResponse;
import cn.rapidtsdb.tsdb.model.proto.TSDataMessage;
import cn.rapidtsdb.tsdb.model.proto.TSQueryMessage;

public class ObjectCodeGen {
    private static Class[] protoClasses = new Class[]{
            ConnectionAuth.ProtoAuthParams.class,
            ConnectionAuth.ProtoAuthMessage.class,
            ConnectionAuth.ProtoAuthResp.class,
            TSDataMessage.ProtoDatapoint.class,
            TSDataMessage.ProtoSimpleDatapoint.class,
            TSDataMessage.ProtoDatapoints.class,
            TSDBResponse.ProtoDataResponse.class,
            TSDBResponse.ProtoCommonResponse.class,
            TSQueryMessage.ProtoTSQuery.class,
            TSDBResponse.ProtoDataQueryStat.class
    };

    public static void main(String[] args) {


        String mapEntryTemplate = "protoObjectCodeMap.put(\"%s\", %s);";
        for (int i = 0; i < protoClasses.length; i++) {
            String classNameKey = protoClasses[i].getName();
            String mapDef = String.format(mapEntryTemplate, classNameKey, i + 1);
            System.out.println(mapDef);
        }
    }


}
