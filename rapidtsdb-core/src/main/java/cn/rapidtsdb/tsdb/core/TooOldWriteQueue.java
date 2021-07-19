package cn.rapidtsdb.tsdb.core;

import java.util.LinkedList;
import java.util.NoSuchElementException;

/**
 * Sometime Write Operation can not get
 * a block can written in, so we need a write buffer,
 * in s short future, flush these write operation in block.
 * <p>
 * These are the situation that data can not write in block immediately
 * 1. Write older data more than one data round(2 hours before)
 * 2. Load balancer reassign data shard. when shard reassign, write command will change node
 * immediately, but the new node will spend a little time to get ready for writing. so we need
 * a buffer to pending the write operation.
 *
 * <p>
 * To prevent memory out of usage, configuration item pending.max.size
 * is used to limit the memory usage, when pending operation more than max memory usage,
 * oldest data will be discarded.
 * <p>
 * The default value if pending.max.size is 100m, can buffer about 5 millions
 * write operation, if the default value can not hold the pending write, something wrong must
 * be happened, check the system is better than larger the config buffer size.
 *
 * <p>
 * The Data in Pended Write Operation stay unreadable
 * </p>
 *
 * <p>
 * Non Thread Safe
 * </p>
 */
public class TooOldWriteQueue {

    //default 100M
    private int maxPendingSize = 5 * 1024 * 1024;
    private LinkedList<WriteOperation> pendingOperaitons = new LinkedList<>();


    public TooOldWriteQueue() {
    }

    public TooOldWriteQueue(int maxPendingSize) {
        this.maxPendingSize = maxPendingSize;
    }

    public void write(int mid, long timestamp, double val) {
        WriteOperation operation = new WriteOperation(mid, timestamp, val);
        pendingOperaitons.add(operation);
    }

    public WriteOperation pop() {
        try {
            return pendingOperaitons.pop();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    public int size() {
        return pendingOperaitons.size();
    }

    public boolean isNotEmpty() {
        return size() > 0;
    }

}
