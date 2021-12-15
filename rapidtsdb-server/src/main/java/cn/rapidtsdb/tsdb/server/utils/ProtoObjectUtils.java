package cn.rapidtsdb.tsdb.server.utils;

import cn.rapidtsdb.tsdb.meta.BizMetric;
import cn.rapidtsdb.tsdb.model.proto.TSQueryMessage.ProtoTSQuery;
import cn.rapidtsdb.tsdb.object.TSDataPoint;
import cn.rapidtsdb.tsdb.object.TSQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.rapidtsdb.tsdb.model.proto.TSDataMessage.ProtoDatapoint;
import static cn.rapidtsdb.tsdb.model.proto.TSDataMessage.ProtoSimpleDatapoint;
import static cn.rapidtsdb.tsdb.model.proto.TSDataMessage.ProtoTSTag;

public class ProtoObjectUtils {

    public static BizMetric getBizMetric(String metric, List<ProtoTSTag> protoTags) {
        BizMetric bizMetric;
        if (protoTags.size() > 0) {
            Map<String, String> mpTags = new HashMap<>();
            protoTags.forEach(ptag -> {
                mpTags.put(ptag.getKey(), ptag.getValue());
            });
            bizMetric = BizMetric.of(metric, mpTags);
        } else {
            bizMetric = BizMetric.cache(metric);
        }
        return bizMetric;
    }

    public static TSDataPoint getDp(ProtoSimpleDatapoint pdp) {
        return new TSDataPoint(pdp.getTimestamp(), pdp.getVal());
    }

    public static TSDataPoint[] getDps(List<ProtoDatapoint> dpList) {
        int i = 0;
        TSDataPoint[] dps = new TSDataPoint[dpList.size()];
        for (ProtoDatapoint dp : dpList) {
            dps[i++] = new TSDataPoint(dp.getTimestamp(), dp.getVal());
        }
        return dps;
    }

    public static Map<String, String> getTags(List<ProtoTSTag> protoTags) {
        if (protoTags != null && protoTags.size() > 0) {
            Map<String, String> mpTags = new HashMap<>();
            protoTags.forEach(ptag -> {
                mpTags.put(ptag.getKey(), ptag.getValue());
            });
            return mpTags;
        }
        return null;
    }

    public static TSQuery getTSQuery(ProtoTSQuery protoTSQuery) {
        TSQuery tsQuery = TSQuery.builder()
                .metric(protoTSQuery.getMetrics())
                .startTime(protoTSQuery.getStartTime())
                .endTime(protoTSQuery.getEndTime())
                .tags(getTags(protoTSQuery.getTagsList()))
                .downSampler(protoTSQuery.getDownSampler())
                .aggregator(protoTSQuery.getAggregator())
                .build();
        return tsQuery;
    }

}
