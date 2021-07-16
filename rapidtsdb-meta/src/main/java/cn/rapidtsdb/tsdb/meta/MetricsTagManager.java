package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.core.persistent.MetricsKeyManager;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.plugins.StoreHandlerPlugin;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import com.esotericsoftware.kryo.kryo5.Kryo;

import java.util.List;
import java.util.Map;

public class MetricsTagManager implements Initializer, Closer {

    public static MetricsTagManager INSTANCE = new MetricsTagManager();
    private StoreHandlerPlugin storeHandler = StoreHandlerFactory.getStoreHandler();
    private static final String tagFilename = "mt.data";
    private Kryo kryo = new Kryo();
    private MetricsKeyManager metricsKeyManager;

    @Override
    public void close() {
        persistData();
    }


    @Override
    public void init() {
        recoveryData();
    }


    public String getInternalMetricName(String metric, Map<String, String> tags) {
        return null;
    }

    public BizMetric getBizMetricFromInternalMetric(String internalMetric) {
        return null;
    }


    public List<TagKV> getMetricAllTagKV(String parentMetric) {
        return null;
    }

    public List<TagKV> getMetricTagValues(String parentMetric, String tagK) {
        return null;
    }

    public List<TagKV> getMetricTagsValues(String parentMetric, List<String> tagKeys) {
        return null;
    }

    private int getTagKeyIndex(String key) {
        return -1;
    }

    public String getTagKeyByIndex(int keyIdx) {
        return null;
    }

    private int getTagValueIndex(String tagValue) {
        return -1;
    }

    private String getTagValueByIndex(int valueIdx) {
        return null;
    }

    private String getMetricSuffixFromTags(Map<String, String> tags) {
        return null;
    }

    private Map<String, String> getTagsFromMetricSuffix(String metricSuffix) {
        return null;
    }

    private void persistData() {

    }

    private void recoveryData() {

    }
}
