package cn.rapidtsdb.tsdb.lab;


import cn.rapidtsdb.tsdb.model.TSQueryModel.TSQuery;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.log4j.Log4j2;

import java.util.Base64;

@Log4j2
public class ProtoBufLab {

    public static void main(String[] args) throws InvalidProtocolBufferException {

        TSQuery tsQuery = TSQuery.newBuilder()
                .setMetrics("Com.zzz")
                .setStartTime(System.currentTimeMillis() - 7200)
                .setEndTime(System.currentTimeMillis())
                .build();
        byte[] bytes = tsQuery.toByteArray();
        System.out.println("encode size: " + bytes.length);

        System.out.println(tsQuery.getAllFields());

        System.out.println(Base64.getEncoder().encodeToString(bytes));
        System.out.println(TSQuery.parseFrom(bytes));

    }
}
