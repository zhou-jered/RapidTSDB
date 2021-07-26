package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.TSDBDataOperationTask;
import cn.rapidtsdb.tsdb.TSDBRunnableTask;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.plugins.StoreHandlerPlugin;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Output;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

@Log4j2
public class MetricsTagUidManager implements Initializer, Closer {


    private final int STATUS_UNINIT = 1;
    private final int STATUS_INITIALIZING = 2;
    private final int STATUS_RUNNING = 3;
    private AtomicInteger status = new AtomicInteger(STATUS_UNINIT);


    private final int UID_FILE_RANGE_STEP = 10000;
    private Map<Integer, Lock> uidRangLockMap = new ConcurrentHashMap<>();
    private StoreHandlerPlugin storeHandler = null;
    private Kryo kryo = new Kryo();
    private LRUCache<Integer, String> id2TagCache = new LRUCache<>();
    private LRUCache<Integer, String> id2KeyCache = new LRUCache<>();

    private transient Node root = new Node(null, '0');
    private transient AtomicInteger nodeIdx = new AtomicInteger(0);

    private static final String TRIE_FILE = "meta.tag.tree";
    private static final String TAG_IDX_FILE = "meta.tag.idx"; //store the max index
    private static final String TAG_REVERSE_IDX_FILE = "meta.tag.range-";
    private Node[] trieTerminatedNodeArray = new Node[10000];

    @Override
    public void close() {
        persistDataSync();
    }


    @Override
    public void init() {
        if (!status.compareAndSet(STATUS_UNINIT, STATUS_INITIALIZING)) {
            return;
        }
        kryo.register(Node.class);
        kryo.register(List.class);
        kryo.register(ArrayList.class);
        storeHandler = StoreHandlerFactory.getStoreHandler();
        recoveryData();
        status.set(STATUS_RUNNING);
    }


    private int getTagIndex(String key) {
        if (StringUtils.isEmpty(key)) {
            return -1;
        }
        Node tailNode = getTailNodeByKey(key);
        return tailNode.getVal();
    }

    private Node getTailNodeByKey(String key) {
        Node current = root;
        char[] chars = key.toCharArray();
        for (char c : chars) {
            current = current.getOrCreateChildNode(c);
        }
        if (!isTerminatedNode(current)) {
            current.setVal(nodeIdx.incrementAndGet());
            rememberTerminatedNodeIdx(current);
        }
        return current;
    }

    public String getTagByIndex(int keyIdx) {
        if (keyIdx > nodeIdx.get()) {
            return null;
        }
        Node node = trieTerminatedNodeArray[keyIdx];
        if (node == null) {
            loadNodeReverseIndex(keyIdx);
        }
        node = trieTerminatedNodeArray[keyIdx];
        if (node == null) {
            return null;
        }
        return node.getNodeKeyString();
    }

    private void rememberTerminatedNodeIdx(Node node) {
        int arrayLength = trieTerminatedNodeArray.length;
        if (node.getVal() >= arrayLength) {
            expandTerminatedNodeArray(arrayLength);
        }
        String keyNameRangeFile = getUidRangeFilename(node.getVal());
        trieTerminatedNodeArray[node.getVal()] = node;
    }


    private void recoveryData() {

    }


    private void persistDataSync() {

        log.debug("Persist Metric Uid Info");
        try (OutputStream treeOp = storeHandler.openFileOutputStream(TRIE_FILE);) {
            kryo.writeObject(new Output(treeOp), root);
        } catch (IOException e) {
            e.printStackTrace();
            log.fatal("Exception during persist Tree:", e);
        }

        try (OutputStream idxOp = storeHandler.openFileOutputStream(TAG_IDX_FILE)) {
            DataOutputStream dos = new DataOutputStream(idxOp);
            dos.writeInt(nodeIdx.get());
        } catch (IOException e) {
            e.printStackTrace();
            log.fatal("Exception During persit uid index");
        }

    }

    private void persistDataAsync() {

    }

    private void expandTerminatedNodeArray(int originalSize) {
        synchronized (trieTerminatedNodeArray) {
            if (trieTerminatedNodeArray.length == originalSize) {
                Node[] newArray = new Node[trieTerminatedNodeArray.length + 10000];
                System.arraycopy(trieTerminatedNodeArray, 0, newArray, 0, trieTerminatedNodeArray.length);
                trieTerminatedNodeArray = newArray;
            }
        }
    }

    private boolean isTerminatedNode(Node node) {
        return node.val == 0;
    }

    private void loadNodeReverseIndex(int keyIdx) {
        String filename = getUidRangeFilename(keyIdx);
        if (storeHandler.fileExisted(filename)) {
            try (BufferedReader reader =
                         new BufferedReader(new InputStreamReader(
                                 storeHandler.openFileInputStream(filename)));) {
                String tmpKey;
                while ((tmpKey = reader.readLine()) != null) {
                    Node tailNode = getTailNodeByKey(tmpKey);
                    trieTerminatedNodeArray[tailNode.getVal()] = tailNode;
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("READ {} exception", filename, e);
            }

        }
    }


    private String getUidRangeFilename(int uid) {
        int floored = getFlooredUid(uid);
        return "meta-idx-" + floored;
    }

    private int getFlooredUid(int uid) {
        int floored = uid - uid % UID_FILE_RANGE_STEP;
        return floored;
    }

    @Data
    static class Node {
        char c;
        int val;
        List<Node> children;
        Node parent;

        public Node() {
        }

        public Node(Node parent, char c) {
            this.parent = parent;
            this.c = c;
        }

        public synchronized Node getOrCreateChildNode(char c) {
            for (Node node : children) {
                if (node.c == c) {
                    return node;
                }
            }
            Node node = new Node(this, c);
            if (children == null) {
                children = new ArrayList<>();
            }
            children.add(node);
            return node;
        }

        public String getNodeKeyString() {
            StringBuffer sb = new StringBuffer();
            Node curNode = this;
            while (curNode.parent != null) {
                sb.append(curNode.getC());
                curNode = curNode.parent;
            }
            return sb.reverse().toString();
        }
    }

    private class AppendFullKeyNameTask extends TSDBRunnableTask {

        private String keyName;
        private int keyIdx;

        public AppendFullKeyNameTask(String keyName, int keyIdx) {
            this.keyName = keyName;
            this.keyIdx = keyIdx;
        }

        @Override
        public int getRetryLimit() {
            return 10;
        }

        @Override
        public String getTaskName() {
            return "AppendFullKeyNameTask";
        }

        @Override
        public void run() {
            String keyNameRangeFile = getUidRangeFilename(keyIdx);
            try (OutputStream appnedOp = storeHandler.openFileAppendStream(keyNameRangeFile);) {

            } catch (Exception e) {

            }
        }
    }


}
