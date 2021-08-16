package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.meta.exception.IllegalCharsException;
import cn.rapidtsdb.tsdb.utils.CollectionUtils;
import org.apache.commons.lang.StringUtils;

public class MetricTransformer {

    private MetricsTagUidManager uidManager;

    public BizMetric toBizMetric(String internalMetric) {
        return null;
    }

    char METRICS_TAG_SPLIITER = MetaReservedSpecialMetricChars.tagSeparatorChar;
    char TAG_KV_SPLLITER = MetaReservedSpecialMetricChars.tagKVSeparatorChar;


    public String toInternalMetric(BizMetric bizMetric) throws IllegalCharsException {
        if (bizMetric == null) {
            return null;
        }
        String bizname = bizMetric.getMetric();
        if (StringUtils.isEmpty(bizname)) {
            return null;
        }
        if (MetaReservedSpecialMetricChars.hasSpecialChar(bizname.toCharArray())) {
            throw new IllegalCharsException("Metric Name Can not contain "
                    + METRICS_TAG_SPLIITER + " or " + TAG_KV_SPLLITER
                    , '-');
        }
        if (CollectionUtils.isEmpty(bizMetric.getTags())) {
            return bizname;
        }
        return null;
    }

}
