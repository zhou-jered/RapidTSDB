package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.plugins.StoreHandlerPlugin;
import cn.rapidtsdb.tsdb.store.StoreHandlerFactory;
import com.esotericsoftware.kryo.kryo5.Kryo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

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

    private transient Node root;
    private transient AtomicInteger nodeIdx;

    private static final String TRIE_FILE = "meta.tag.tree";
    private static final String TAG_IDX_FILE = "meta.tag.idx"; //store the max index
    private static final String TAG_REVERSE_IDX_FILE = "meta.tag.range-";

    @Override
    public void close() {
        persistData();
    }


    @Override
    public void init() {
        kryo.register(Node.class);
        kryo.register(List.class);
        kryo.register(ArrayList.class);
        storeHandler = StoreHandlerFactory.getStoreHandler();
        recoveryData();
    }


    private int getTagIndex(String key) {
        return -1;
    }

    public String getTagByIndex(int keyIdx) {
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

        public synchronized Node getChildNode(char c) {
            for (Node node : children) {
                if (node.val == c) {
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


    }


}
