package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.TSDBRetryableTask;
import cn.rapidtsdb.tsdb.common.LRUCache;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.plugins.FileStoreHandlerPlugin;
import cn.rapidtsdb.tsdb.plugins.PluginManager;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Log4j2
public class MetricsTagUidManager implements Initializer, Closer {

    private static MetricsTagUidManager instance = new MetricsTagUidManager();

    private final int STATUS_UNINIT = 1;
    private final int STATUS_INITIALIZING = 2;
    private final int STATUS_RUNNING = 3;
    private final int STATUS_CLOSED = 4;
    private AtomicInteger status = new AtomicInteger(STATUS_UNINIT);


    private final int UID_FILE_RANGE_STEP = 10000;
    private Map<Integer, Lock> uidRangLockMap = new ConcurrentHashMap<>();
    private FileStoreHandlerPlugin storeHandler = null;
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
        status.set(STATUS_CLOSED);
        persistDataSync();
    }


    @Override
    public void init() {
        if (!status.compareAndSet(STATUS_UNINIT, STATUS_INITIALIZING)) {
            return;
        }
        storeHandler = PluginManager.getPlugin(FileStoreHandlerPlugin.class);
        recoveryData();
        synchronized (status) {
            status.set(STATUS_RUNNING);
            status.notifyAll();
        }
    }

    public void forceInit() {
        status.set(STATUS_UNINIT);
        init();
    }

    public int nodeNumber() {
        return root.size();
    }


    public int getTagIndex(String key) {
        if (StringUtils.isEmpty(key)) {
            return -1;
        }
        if (status.get() == STATUS_RUNNING) {
            Node tailNode = getTailNodeByKey(key);
            return tailNode.getVal();
        } else {
            waitingRunning();
            return getTagIndex(key);
        }
    }


    public String getTagByIndex(int keyIdx) {
        if (status.get() == STATUS_RUNNING) {
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
        } else {
            waitingRunning();
            return getTagByIndex(keyIdx);
        }
    }

    private void waitingRunning() {
        if (status.get() != STATUS_RUNNING) {
            synchronized (status) {
                if (status.get() == STATUS_RUNNING) {
                    return;
                }
                try {
                    status.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private Node getTailNodeByKey(String key) {
        Node current = root;
        char[] chars = key.toCharArray();
        for (char c : chars) {
            current = current.getOrCreateChildNode(c);
        }
        if (!isTerminatedNote(current)) {
            // make is terminated node
            current.setVal(nodeIdx.incrementAndGet());
            rememberTerminatedNodeIdx(key, current);
        }
        return current;
    }

    private void rememberTerminatedNodeIdx(String nodeString, Node node) {
        int arrayLength = trieTerminatedNodeArray.length;
        if (node.getVal() >= arrayLength) {
            expandTerminatedNodeArray(arrayLength, node.getVal());
        }
        AppendFullKeyNameTask appendFullKeyNameTask = new AppendFullKeyNameTask(nodeString, node.getVal());
        ManagedThreadPool.getInstance()
                .ioExecutor().submit(appendFullKeyNameTask);
        trieTerminatedNodeArray[node.getVal()] = node;
    }


    private void recoveryData() {
        log.debug("Recovery Metric Uid Data");
        if (storeHandler.fileExisted(TRIE_FILE)) {
            try (InputStream inputStream = storeHandler.openFileInputStream(TRIE_FILE)) {
                BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream, 1024 * 128);
                DataInputStream dis = new DataInputStream(bufferedInputStream);
                root = new Node();
                root.readFromSteam(dis);
                dis.close();
            } catch (Exception e) {
                log.error("Can Not open file " + TRIE_FILE, e);
            }
        }
        if (storeHandler.fileExisted(TAG_IDX_FILE)) {
            try (InputStream inputStream = storeHandler.openFileInputStream(TAG_IDX_FILE)) {
                DataInputStream dis = new DataInputStream(inputStream);
                nodeIdx.set(dis.readInt());
            } catch (Exception e) {
                log.error("Read " + TAG_IDX_FILE + " Error", e);
            }
        } else {
            int idx = getMaxNodeIdx(root);
            nodeIdx.set(idx + 1);
        }
    }


    private void persistDataSync() {

        log.debug("Persist Metric Uid Info");
        try (OutputStream treeOp = storeHandler.openFileOutputStream(TRIE_FILE)) {
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(treeOp, 1024 * 128);
            DataOutputStream dos = new DataOutputStream(bufferedOutputStream);
            root.writeToStream(dos);
            dos.close();
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


    private Future persistDataAsync() {
        return ManagedThreadPool.getInstance().ioExecutor()
                .submit(() -> {
                    persistDataSync();
                });
    }

    private void expandTerminatedNodeArray(int originalSize, int expectSize) {
        synchronized (trieTerminatedNodeArray) {
            int newSize = expectSize - expectSize % UID_FILE_RANGE_STEP + UID_FILE_RANGE_STEP;
            Node[] newArray = new Node[newSize];
            System.arraycopy(trieTerminatedNodeArray, 0, newArray, 0, trieTerminatedNodeArray.length);
            trieTerminatedNodeArray = newArray;
        }
    }

    private boolean isTerminatedNote(Node node) {
        return node.val != 0;
    }


    private void loadNodeReverseIndex(int keyIdx) {
        String filename = getUidRangeFilename(keyIdx);
        if (storeHandler.fileExisted(filename)) {
            Lock rangeLock = getKeyIdxRangeLock(keyIdx);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(storeHandler.openFileInputStream(filename)));) {
                String tmpKey;
                while ((tmpKey = reader.readLine()) != null) {
                    Node tailNode = getTailNodeByKey(tmpKey);
                    trieTerminatedNodeArray[tailNode.getVal()] = tailNode;
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("READ {} exception", filename, e);
            } finally {
                rangeLock.unlock();
            }
        } else {
            throw new RuntimeException("No NodeReverseIndex file:" + filename + " FIND");
        }
    }

    private Lock getKeyIdxRangeLock(int idx) {
        int flooredIdx = getFlooredUid(idx);
        Lock lock = uidRangLockMap.get(flooredIdx);
        if (lock == null) {
            uidRangLockMap.putIfAbsent(flooredIdx, new ReentrantLock());
            lock = uidRangLockMap.get(flooredIdx);
        }
        return lock;
    }

    private String getUidRangeFilename(int idx) {
        int floored = getFlooredUid(idx);
        return TAG_REVERSE_IDX_FILE + floored;
    }

    private int getFlooredUid(int uid) {
        int floored = uid - uid % UID_FILE_RANGE_STEP;
        return floored;
    }


    private int getMaxNodeIdx(Node root) {
        Queue<Node> Q = new LinkedList<>();
        Q.offer(root);
        int maxIdx = 0;
        while (Q.isEmpty() == false) {
            Node current = Q.poll();
            if (isTerminatedNote(current)) {
                maxIdx = Math.max(maxIdx, current.getVal());
            }
            if (current.getChildren() != null) {
                Q.addAll(current.getChildren());
            }
        }
        return maxIdx;
    }

    @Data
    static class Node {
        char c;
        int val;
        List<Node> children;
        transient Node parent;

        public Node() {
        }

        public Node(Node parent, char c) {
            this.parent = parent;
            this.c = c;
        }

        public synchronized Node getOrCreateChildNode(char c) {
            if (children == null) {
                children = new ArrayList<>();
            }
            for (Node node : children) {
                if (node.c == c) {
                    return node;
                }
            }
            Node node = new Node(this, c);
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

        public int size() {
            int childSize = 0;
            if (children != null && !children.isEmpty()) {
                for (Node child : children) {
                    childSize += child.size();
                }
            }
            return childSize + 1;
        }

        final static byte hasChild = -1;
        final static byte noChild = 0;

        public void writeToStream(DataOutputStream outputStream) throws IOException {
            outputStream.writeChar(c);
            outputStream.writeInt(val);
            if (children != null && children.size() > 0) {
                int sz = children.size();
                outputStream.writeByte(hasChild);
                outputStream.writeInt(sz);
                for (Node child : children) {
                    child.writeToStream(outputStream);
                }
            } else {
                outputStream.writeByte(noChild);
            }
        }

        public void readFromSteam(DataInputStream dataInputStream) throws IOException {
            this.c = dataInputStream.readChar();
            this.val = dataInputStream.readInt();
            byte childIndicator = dataInputStream.readByte();
            if (childIndicator == hasChild) {
                int sz = dataInputStream.readInt();
                this.children = new ArrayList<>(sz);
                for (int i = 0; i < sz; i++) {
                    Node child = new Node();
                    child.readFromSteam(dataInputStream);
                    this.children.add(child);
                }
            }
        }

    }

    private static final ReentrantLock AppendFullKeyNameTask_LOCK = new ReentrantLock();

    private class AppendFullKeyNameTask extends TSDBRetryableTask {

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
            AppendFullKeyNameTask_LOCK.lock();
            String keyNameRangeFile = getUidRangeFilename(keyIdx);
            try (OutputStream appnedOp = storeHandler.openFileAppendStream(keyNameRangeFile)) {
                appnedOp.write(keyName.getBytes());
                appnedOp.write("\n".getBytes());
            } catch (Exception e) {

            } finally {
                AppendFullKeyNameTask_LOCK.unlock();
            }
        }
    }

    public static MetricsTagUidManager getInstance() {
        return instance;
    }

    private MetricsTagUidManager() {
    }
}
