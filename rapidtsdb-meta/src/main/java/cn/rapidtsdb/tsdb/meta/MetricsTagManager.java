package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.plugins.StoreHandlerPlugin;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import cn.rapidtsdb.tsdb.utils.CollectionUtils;
import com.esotericsoftware.kryo.kryo5.Kryo;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

public class MetricsTagManager implements Initializer, Closer {


    private final int UID_FILE_RANGE_STEP = 10000;
    private Map<Integer, Lock> uidRangLockMap = new ConcurrentHashMap<>();
    private StoreHandlerPlugin storeHandler = null;
    private Kryo kryo = new Kryo();
    private LRUCache<Integer, String> id2TagCache = new LRUCache<>();
    private LRUCache<Integer, String> id2KeyCache = new LRUCache<>();

    @Override
    public void close() {
        persistData();
    }


    @Override
    public void init() {
        storeHandler = StoreHandlerFactory.getStoreHandler();
        recoveryData();
    }


    public String getInternalMetricName(String metric, Map<String, String> tags) {
        String suffix = null;
        if (CollectionUtils.isNotEmpty(tags)) {
            SortedSet<String> keySet = new TreeSet(tags.keySet());
            for (String k : keySet) {
                int kUid = getTagKeyIndex(k);
                int vUid = getTagValueIndex(tags.get(k));

            }
        }
        return metric + suffix;
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


    private String getUidRangeFilename(int uid) {
        int floored = getFlooredUid(uid);
        return "meta-idx-" + floored;
    }

    private int getFlooredUid(int uid) {
        int floored = uid - uid % UID_FILE_RANGE_STEP;
        return floored;
    }

    static class Node {
        char val;
        List<Node> children;
        Node parent;

        public Node() {
        }

        public Node(Node parent, char val) {
            this.parent = parent;
            this.val = val;
        }


    }


}
