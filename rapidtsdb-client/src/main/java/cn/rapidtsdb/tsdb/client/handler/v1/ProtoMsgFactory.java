package cn.rapidtsdb.tsdb.client.handler.v1;

import cn.rapidtsdb.tsdb.protocol.RpcObjectCode;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ProtoMsgFactory {

    public static GeneratedMessageV3 getProtoObject(int code, byte[] protoBytes) throws InvalidProtocolBufferException {
        Parser parser = RpcObjectCode.getObjectProtoParser(code);
        if (parser == null) {
            log.error("Can not get Object Parser of code:{}", code);
            return null;
        }
        return (GeneratedMessageV3) parser.parseFrom(protoBytes);
    }

}
