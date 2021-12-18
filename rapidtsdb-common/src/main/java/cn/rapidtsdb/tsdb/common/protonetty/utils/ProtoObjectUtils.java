package cn.rapidtsdb.tsdb.common.protonetty.utils;


import cn.rapidtsdb.tsdb.model.proto.TSQueryMessage.ProtoTSQuery;
import cn.rapidtsdb.tsdb.object.TSDataPoint;
import cn.rapidtsdb.tsdb.object.TSQuery;

import static cn.rapidtsdb.tsdb.model.proto.TSDataMessage.ProtoSimpleDatapoint;

public class ProtoObjectUtils {


    public static TSDataPoint getDp(ProtoSimpleDatapoint pdp) {
        return new TSDataPoint(pdp.getTimestamp(), pdp.getVal());
    }


    public static TSQuery getTSQuery(ProtoTSQuery protoTSQuery) {
        TSQuery tsQuery = TSQuery.builder()
                .metric(protoTSQuery.getMetrics())
                .startTime(protoTSQuery.getStartTime())
                .endTime(protoTSQuery.getEndTime())
                .tags(protoTSQuery.getTagsMap())
                .downSampler(protoTSQuery.getDownSampler())
                .aggregator(protoTSQuery.getAggregator())
                .build();
        return tsQuery;
    }

}
