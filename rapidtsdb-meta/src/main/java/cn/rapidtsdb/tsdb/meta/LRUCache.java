package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.common.TimeUtils;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LRUCache<K, V> {

    public static final int DEFAULT_MAX_SIZE = 10240;
    private final int maxSize;
    private Map<K, WrappedValue<V>> cache = new ConcurrentHashMap<>();
    private HeapNode[] heap;
    private int heapIdx = 0;
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public LRUCache() {
        this(DEFAULT_MAX_SIZE);
    }

    public LRUCache(int maxSize) {
        this.maxSize = maxSize;
        heap = new HeapNode[maxSize + 1];
    }

    public void put(K k, V v) {
        try {
            rwLock.writeLock().lockInterruptibly();
            if (cache.containsKey(k)) {
                WrappedValue wc = cache.get(k);
                wc.value = v;
                touchNode(cache.get(k).nodeIdx);
            } else {
                WrappedValue wv = insertHeap(v);
                cache.put(k, wv);
            }
        } catch (InterruptedException e) {

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public V get(K k) {
        try {
            rwLock.readLock().lockInterruptibly();
            WrappedValue<V> wv = cache.get(k);
            if (wv != null) {
                touchNode(wv.nodeIdx);
            }
            return wv.value;
        } catch (InterruptedException e) {

        } finally {
            rwLock.readLock().unlock();
        }
        return null;
    }

    public void invalid(K k) {
        try {
            rwLock.writeLock().lockInterruptibly();
            WrappedValue wv = cache.get(k);
            if (wv != null) {
                cache.remove(k);
                removeHeapNode(wv.nodeIdx);
            }
        } catch (InterruptedException e) {
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public int cacheSize() {
        return cache.size();
    }

    public void invalidAll() {
        try {
            rwLock.writeLock().lockInterruptibly();
            cache.clear();
            heapIdx = 0;
        } catch (InterruptedException e) {
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void touchNode(int nodeIdx) {
        try {
            rwLock.writeLock().lock(); // reenter
            heap[nodeIdx].accessTime = TimeUtils.currentMills();
            while (nodeIdx < heapIdx) {
                int leftIdx = nodeIdx * 2;
                int rightIdx = nodeIdx * 2 + 1;
                int targetIdx = leftIdx;
                if (leftIdx >= heapIdx) {
                    break;
                }
                if (rightIdx < heapIdx && heap[rightIdx].accessTime < heap[leftIdx].accessTime) {
                    targetIdx = rightIdx;
                }
                HeapNode tmp = heap[nodeIdx];
                heap[nodeIdx] = heap[targetIdx];
                heap[targetIdx] = tmp;
                nodeIdx = targetIdx;
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private synchronized WrappedValue<V> insertHeap(V val) {
        long accessTime = TimeUtils.currentMills();
        HeapNode newNode = new HeapNode(val, accessTime);
        try {
            rwLock.writeLock().lock();
            if (heapIdx >= maxSize) {
                HeapNode tobeRemoved = heap[0];
                cache.remove(tobeRemoved.key);
                int replacedIdx = 0;
                while (replacedIdx < heapIdx) {
                    int leftIdx = replacedIdx * 2;
                    int rightIdx = replacedIdx * 2 + 1;
                    if (leftIdx >= heapIdx) {
                        break;
                    } else if (rightIdx >= heapIdx) {
                        replacedIdx = leftIdx;
                    } else {
                        replacedIdx = heap[leftIdx].accessTime <= heap[rightIdx].accessTime ? leftIdx : rightIdx;
                    }
                    heap[replacedIdx / 2] = heap[replacedIdx];
                }
            }
            int nodeIdx = heapIdx;
            heap[nodeIdx] = newNode;
            heapIdx++;
            return new WrappedValue<>(val, nodeIdx);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void removeHeapNode(int nodeIdx) {
        assert nodeIdx < heapIdx && nodeIdx >= 0;
        try {
            rwLock.writeLock().lockInterruptibly();
            heap[nodeIdx] = heap[heapIdx - 1];
            int currentNodeIdx = nodeIdx;
            if (currentNodeIdx * 2 < heapIdx) {
                while (currentNodeIdx < heapIdx) {
                    int leftIdx = currentNodeIdx * 2;
                    int rightIdx = currentNodeIdx * 2 + 1;
                    if (leftIdx >= heapIdx) {
                        break;
                    } else if (rightIdx >= heapIdx) {
                        //only left child
                        currentNodeIdx = leftIdx;
                        if (heap[currentNodeIdx].accessTime > heap[leftIdx].accessTime) {
                            swapHeapNode(currentNodeIdx, leftIdx);
                        }
                    } else {
                        //both child
                        int candidateIdx = heap[leftIdx].accessTime <= heap[rightIdx].accessTime ? leftIdx : rightIdx;
                        if (heap[currentNodeIdx].accessTime > heap[candidateIdx].accessTime) {
                            swapHeapNode(currentNodeIdx, candidateIdx);
                            currentNodeIdx = candidateIdx;
                        }
                    }
                }
            }
            heapIdx--;

        } catch (InterruptedException e) {

        } finally {
            rwLock.writeLock().unlock();
        }

    }

    private void swapHeapNode(int aIdx, int bIdx) {
        HeapNode tempNode = heap[aIdx];
        heap[aIdx] = heap[bIdx];
        heap[bIdx] = tempNode;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    static class HeapNode {
        Object key;
        long accessTime;
    }

    static class WrappedValue<T> {
        T value;
        int nodeIdx;

        public WrappedValue(T value, int nodeIdx) {
            this.value = value;
            this.nodeIdx = nodeIdx;
        }
    }
}
