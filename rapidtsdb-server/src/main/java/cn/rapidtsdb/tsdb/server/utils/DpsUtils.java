package cn.rapidtsdb.tsdb.server.utils;

import cn.rapidtsdb.tsdb.core.TSDataPoint;
import cn.rapidtsdb.tsdb.meta.BizMetric;
import cn.rapidtsdb.tsdb.model.proto.TSDataMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DpsUtils {

    public static BizMetric getBizMetric(String metric, List<TSDataMessage.ProtoTSTag> protoTags) {
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

    public static TSDataPoint getDp(TSDataMessage.ProtoSimpleDatapoint pdp) {
        return new TSDataPoint(pdp.getTimestamp(), pdp.getVal());
    }

    public static TSDataPoint[] getDps(List<TSDataMessage.ProtoDatapoint> dpList) {
        int i = 0;
        TSDataPoint[] dps = new TSDataPoint[dpList.size()];
        for (TSDataMessage.ProtoDatapoint dp : dpList) {
            dps[i++] = new TSDataPoint(dp.getTimestamp(), dp.getVal());
        }
        return dps;
    }

}
