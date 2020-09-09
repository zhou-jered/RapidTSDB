package cn.tinytsdb.tsdb.core.persistent;

import cn.tinytsdb.tsdb.config.TSDBConfig;
import cn.tinytsdb.tsdb.lifecycle.Initializer;
import cn.tinytsdb.tsdb.store.StoreHandler;
import cn.tinytsdb.tsdb.utils.TimeUtils;
import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


@Log4j2
public class MetricsKeyManager implements Initializer, Persistently {

    private TSDBConfig tsdbConfig;

    StoreHandler storeHandler;
    /**
     * take care of this map's memory usage
     */
    private AtomicInteger metricKeyIdx = new AtomicInteger(0);
    private String metricsKeyFile = "mk.data";
    private String metricsKeyIdxFile = "mk.idx";
    private volatile boolean initialized = false;
    private String METRICS_LEGAL_CHARS = "plokmijnuhbygvtfcrdxeszwaqPLOKMIJNUHBYGVTFCRDXESZWAQ0987654321@#$-_.+=";
    private TrieNode trieNodeRoot = new TrieNode('0');
    private Kryo kryo = new Kryo();
    private long lastPersistenceTime;

    private Map<String, Integer> idxCache = new ConcurrentHashMap<>(1024 * 10);

    private static MetricsKeyManager instance = new MetricsKeyManager();

    private MetricsKeyManager() {
        tsdbConfig = TSDBConfig.getConfigInstance();
    }

    public static MetricsKeyManager getInstance() {
        return instance;
    }

    @Override
    public void init() {
        if (initialized) {
            return;
        }
        initialized = true;
        kryo.register(TrieNode.class);
        kryo.register(ArrayList.class);
        if (tsdbConfig.getAdvancedConfig().getMetricsIdxCacheSize() != null && tsdbConfig.getAdvancedConfig().getMetricsIdxCacheSize() > 0) {
            idxCache = new ConcurrentHashMap<>(tsdbConfig.getAdvancedConfig().getMetricsIdxCacheSize());
        }
        recoverFromFile();
    }

    public int getMetricsIndex(String metrics) {
        if (StringUtils.isEmpty(metrics)) {
            throw new RuntimeException("Empty metrics");
        }
        Integer idx = idxCache.get(metrics);
        if(idx!=null) {
            return idx;
        }
        char[] chars = metrics.toCharArray();
        for (char c : chars) {
            if (METRICS_LEGAL_CHARS.indexOf(c) < 0) {
                throw new RuntimeException("Illegal Metrics char: " + c + " in metrics:" + metrics);
            }
        }
        idx =  getMetricsIndexInternal(chars);
        idxCache.put(metrics, idx);
        return idx;
    }

    private int getMetricsIndexInternal(char[] metricsChars) {
        TrieNode currentNode = trieNodeRoot;
        for (int i = 0; i < metricsChars.length; i++) {
            char currentChar = metricsChars[i];
            currentNode = currentNode.getChildNode(currentChar);
        }
        if (currentNode.getVal() == 0) {
            currentNode.setValue(metricKeyIdx.incrementAndGet());
            persistenceMetrics();
        }
        currentNode.setValue(metricKeyIdx.addAndGet(1));
        return currentNode.getVal();
    }

    private void persistenceMetrics() {
        if (TimeUtils.currentMills() - lastPersistenceTime > 60 * 1000) {
            doPersist();
            lastPersistenceTime = TimeUtils.currentMills();
        }
    }

    private void doPersist() {
        try {
            Output output = new Output(storeHandler.getFileOutputStream(metricsKeyFile));
            kryo.writeObject(output, trieNodeRoot);
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void recoverFromFile() {
        try {
            InputStream inputStream = storeHandler.getFileInputStream(metricsKeyIdxFile);
            if (inputStream != null) {
                String idxString = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                if (StringUtils.isNotBlank(idxString)) {
                    Integer idx = Integer.parseInt(idxString);
                    metricKeyIdx.set(idx);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            InputStream inputStream = storeHandler.getFileInputStream(metricsKeyFile);
            if (inputStream != null) {
                Input input = new Input(inputStream);
                trieNodeRoot = kryo.readObject(input, TrieNode.class);
                log.info("recover Metrics");
            } else {
                log.debug("Metrics Key Persistence file not Existed.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (metricKeyIdx.get() < 1 || !tsdbConfig.getAdvancedConfig().isReadMkIdx()) {
            metricKeyIdx.set(trieNodeRoot.getMaxIndexValue());
        }
    }


    @Data
    @NoArgsConstructor
    public static class TrieNode implements Serializable {

        private char c;
        private List<TrieNode> childNode;
        private int val;

        public TrieNode(char c) {
            this.c = c;
            childNode = new ArrayList<>();
        }

        public void setValue(int value) {
            this.val = value;
        }

        public synchronized TrieNode getChildNode(char c) {
            for (TrieNode node : childNode) {
                if (node.c == c) {
                    return node;
                }
            }
            TrieNode node = new TrieNode(c);
            childNode.add(node);
            return node;
        }

        public int getMaxIndexValue() {
            if (childNode.size() == 0) {
                return this.val;
            } else {
                int maxVal = Integer.MIN_VALUE;
                for (TrieNode node : childNode) {
                    maxVal = Math.max(maxVal, node.getMaxIndexValue());
                }
                return maxVal;
            }
        }
    }


}
