package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.meta.exception.IllegalCharsException;
import cn.rapidtsdb.tsdb.object.BizMetric;
import cn.rapidtsdb.tsdb.utils.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetricTransformer implements Initializer, Closer {

    private MetricsTagUidManager uidManager;

    public BizMetric toBizMetric(String internalMetric) {
        String[] parts = internalMetric.split(String.valueOf(TAG_SPILTER));
        BizMetric bizMetric = new BizMetric();
        bizMetric.setMetric(parts[0]);
        if (parts.length > 1) {
            bizMetric.setTags(new HashMap<>());
        }
        for (int i = 1; i < parts.length; i++) {
            String[] idxKv = parts[i].split("\\" + TAG_KV_SPLLITER);
            bizMetric.getTags().put(uidManager.getTagByIndex(Integer.parseInt(idxKv[0])),
                    uidManager.getTagByIndex(Integer.parseInt(idxKv[1])));
        }
        return bizMetric;
    }

    char TAG_SPILTER = MetaReservedSpecialMetricChars.tagSeparatorChar;
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
                    + TAG_SPILTER + " or " + TAG_KV_SPLLITER
                    , '-');
        }
        if (CollectionUtils.isEmpty(bizMetric.getTags())) {
            return bizname;
        }
        Map<String, String> tags = bizMetric.getTags();
        String[] sortedKeys = tags.keySet().toArray(new String[0]);
        Arrays.sort(sortedKeys);
        StringBuffer internalMetric = new StringBuffer();
        internalMetric.append(bizname);
        for (String k : sortedKeys) {
            int kIdx = uidManager.getTagIndex(k);
            int vIdx = uidManager.getTagIndex(tags.get(k));
            internalMetric.append(TAG_SPILTER);
            internalMetric.append(kIdx);
            internalMetric.append(TAG_KV_SPLLITER);
            internalMetric.append(vIdx);
        }
        return internalMetric.toString();
    }

    public List<String> concatTags(Map<String, String> tags) {
        List<String> tagIdxList = new ArrayList<>(tags.size());
        for (String k : tags.keySet()) {
            String s = "" + uidManager.getTagIndex(k) +
                    TAG_KV_SPLLITER + uidManager.getTagIndex(tags.get(k));
            tagIdxList.add(s);
        }
        return tagIdxList;
    }

    public String getMetricTagScanPrefix(String bizMetric) {
        return bizMetric + TAG_SPILTER;
    }

}
