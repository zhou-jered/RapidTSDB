package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.meta.exception.IllegalCharsException;
import cn.rapidtsdb.tsdb.object.BizMetric;
import cn.rapidtsdb.tsdb.utils.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.Map;

public class MetricTransformer implements Initializer, Closer {

    private MetricsTagUidManager uidManager;

    public BizMetric toBizMetric(String internalMetric) {
        return null;
    }

    char METRICS_TAG_SPLIITER = MetaReservedSpecialMetricChars.tagSeparatorChar;
    char TAG_KV_SPLLITER = MetaReservedSpecialMetricChars.tagKVSeparatorChar;

    @Override
    public void close() {
        if (uidManager != null) {
            uidManager.close();
        }
    }


    @Override
    public void init() {
        uidManager = MetricsTagUidManager.getInstance();
        uidManager.init();
    }

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
        Map<String, String> tags = bizMetric.getTags();
        String[] sortedKeys = tags.keySet().toArray(new String[0]);
        Arrays.sort(sortedKeys);

        return null;
    }


}
