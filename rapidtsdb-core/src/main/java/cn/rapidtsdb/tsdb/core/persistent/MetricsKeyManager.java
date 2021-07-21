package cn.rapidtsdb.tsdb.core.persistent;

import cn.rapidtsdb.tsdb.TSDBRunnableTask;
import cn.rapidtsdb.tsdb.common.LRUCache;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.plugins.StoreHandlerPlugin;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


@Log4j2
public class MetricsKeyManager implements Initializer, Closer {

    private final int STATUS_UNINIT = 1;
    private final int STATUS_INITIALIZING = 2;
    private final int STATUS_RUNNING = 3;
    private AtomicInteger status = new AtomicInteger(STATUS_UNINIT);


    private TSDBConfig tsdbConfig;

    StoreHandlerPlugin storeHandler;
    /**
     * take care of this map's memory usage
     */
    private AtomicInteger metricKeyIdx = new AtomicInteger(1);
    private String metricsKeyFile = "mk.data";
    private String metricsKeyIdxFile = "mk.idx";
    private String metricsKeyListFile = "mk.list";
    private String METRICS_LEGAL_CHARS = "plokmijnuhbygvtfcrdxeszwaqPLOKMIJNUHBYGVTFCRDXESZWAQ0987654321@#$-_.+=:;,^/";
    private TrieNode trieNodeRoot = new TrieNode('0');
    private boolean[] legalCharMap = new boolean[256];
    private transient Kryo kryo = new Kryo();
    private Lock metricsWriteLock = new ReentrantLock(false);

    private LRUCache<String, Integer> idxCache = new LRUCache(1024 * 10);

    private static MetricsKeyManager instance = new MetricsKeyManager();

    private MetricsKeyManager() {

    }

    public static MetricsKeyManager getInstance() {
        return instance;
    }

    @Override
    public void init() {
        if (!status.compareAndSet(STATUS_UNINIT, STATUS_INITIALIZING)) {
            return;
        }
        for (int i = 0; i < legalCharMap.length; i++) {
            legalCharMap[i] = true;
        }
        for (int i = 0; i < METRICS_LEGAL_CHARS.length(); i++) {
            legalCharMap[METRICS_LEGAL_CHARS.charAt(i)] = true;
        }
        tsdbConfig = TSDBConfig.getConfigInstance();
        storeHandler = StoreHandlerFactory.getStoreHandler();
        kryo.register(TrieNode.class);
        kryo.register(ArrayList.class);
        if (tsdbConfig.getAdvancedConfig().getMetricsIdxCacheSize() != null && tsdbConfig.getAdvancedConfig().getMetricsIdxCacheSize() > 0) {
            idxCache = new LRUCache(tsdbConfig.getAdvancedConfig().getMetricsIdxCacheSize());
        }
        recoverFromFile();
    }

    public Set<String> getAllMetrics() {
        if (!storeHandler.fileExisted(metricsKeyListFile)) {
            return new HashSet<>();
        }
        try {
            Set<String> allMetrics = new HashSet<>();
            InputStream inputStream = storeHandler.openFileInputStream(metricsKeyListFile);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = reader.readLine()) != null) {
                allMetrics.add(line);
            }
            return allMetrics;
        } catch (IOException e) {
            e.printStackTrace();
            log.error("");
            throw new RuntimeException(e);
        }
    }

    public int getMetricsIndex(String metric) {
        if (StringUtils.isEmpty(metric)) {
            throw new RuntimeException("Empty metrics");
        }
        if (status.get() != STATUS_RUNNING) {
            throw new RuntimeException("MetricsKeyManager not prepared");
        }
        Integer idx = idxCache.get(metric);
        if (idx != null) {
            return idx;
        }
        char[] chars = metric.toCharArray();
        for (char c : chars) {
            if (((int) c) > 256 || !legalCharMap[c]) {
                throw new RuntimeException("Illegal Metrics char: " + c + " code:" + ((int) c) + " in metrics:" + metric);
            }
        }
        idx = getMetricsIndexInternal(chars);
        idxCache.put(metric, idx);
        return idx;
    }

    private int getMetricsIndexInternal(char[] metricsChars) {
        TrieNode currentNode = trieNodeRoot;
        for (int i = 0; i < metricsChars.length; i++) {
            char currentChar = metricsChars[i];
            currentNode = currentNode.getChildNode(currentChar);
        }
        if (isNewInsertedNode(currentNode)) {
            currentNode.setValue(metricKeyIdx.incrementAndGet());
            persistenceMetrics(metricsChars);
        }
        return currentNode.getVal();
    }

    private boolean isNewInsertedNode(TrieNode node) {
        return node.getVal() == 0;
    }

    private void persistenceMetrics(char[] newMetricChars) {
        Runnable delegate = () -> {
            doPersist();
            try (OutputStream outputStream = storeHandler.openFileAppendStream(metricsKeyListFile);) {
                OutputStreamWriter writer = new OutputStreamWriter(outputStream);
                writer.write(newMetricChars);
                writer.write("\n");
                writer.close();
            } catch (IOException e) {
                log.error("write metrics list file exception", e);
            }
        };
        PersistNewMetricsTask task = new PersistNewMetricsTask(metricsWriteLock, delegate);
        ManagedThreadPool.getInstance().ioExecutor().submit(task);
    }

    private void doPersist() {
        try {
            Output output = new Output(storeHandler.openFileOutputStream(metricsKeyFile));
            kryo.writeObject(output, trieNodeRoot);
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            OutputStream outputStream = storeHandler.openFileOutputStream(metricsKeyIdxFile);
            DataOutputStream dos = new DataOutputStream(outputStream);
            dos.writeInt(metricKeyIdx.get());
            dos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() {
        doPersist();
    }

    private void recoverFromFile() {
        if (storeHandler.fileExisted(metricsKeyIdxFile)) {
            try (InputStream inputStream = storeHandler.openFileInputStream(metricsKeyIdxFile);) {

                if (inputStream != null) {
                    DataInputStream dis = new DataInputStream(inputStream);
                    Integer idx = dis.readInt();
                    metricKeyIdx.set(idx);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (storeHandler.fileExisted(metricsKeyFile)) {
            try (InputStream inputStream = storeHandler.openFileInputStream(metricsKeyFile);) {

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
        }

        if (metricKeyIdx.get() < 1 || !tsdbConfig.getAdvancedConfig().isReadMkIdx()) {
            metricKeyIdx.set(trieNodeRoot.getMaxIndexValue());
        }
        status.set(STATUS_RUNNING);
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

    private static class PersistNewMetricsTask extends TSDBRunnableTask {

        private Lock metricWriteLock;
        private Runnable delegate;

        public PersistNewMetricsTask(Lock metricWriteLock, Runnable delegate) {
            this.metricWriteLock = metricWriteLock;
            this.delegate = delegate;
        }

        @Override
        public int getRetryLimit() {
            return 0;
        }

        @Override
        public String getTaskName() {
            return "PersistNewMetricsTask";
        }

        @Override
        public void run() {
            try {
                metricWriteLock.lockInterruptibly();
                delegate.run();
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    log.info("persist metrics info Interrupted, trying do again");
                    //give the second chance to run it
                    delegate.run();
                } else {
                    log.error("", e);
                }
            } finally {
                metricWriteLock.unlock();
            }
        }
    }


}
