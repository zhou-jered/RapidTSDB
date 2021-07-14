package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.plugins.StoreHandlerPlugin;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import com.esotericsoftware.kryo.kryo5.Kryo;

import java.util.List;

public class MetricsTagManager implements Initializer, Closer {

    public static MetricsTagManager INSTANCE = new MetricsTagManager();
    private StoreHandlerPlugin storeHandler = StoreHandlerFactory.getStoreHandler();
    private static final String tagFilename = "mt.data";
    private Kryo kryo = new Kryo();

    @Override
    public void close() {

    }

    @Override
    public void init() {

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

}
