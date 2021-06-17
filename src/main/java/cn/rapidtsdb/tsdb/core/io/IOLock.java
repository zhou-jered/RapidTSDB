package cn.rapidtsdb.tsdb.core.io;

import lombok.extern.log4j.Log4j2;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Log4j2
public class IOLock {

    private IOLock() {
    }

    private static ConcurrentHashMap<Integer, Lock> mLocks = new ConcurrentHashMap<>();

    public static Lock getMetricLock(int metric) {
        if (!mLocks.contains(metric)) {
            mLocks.putIfAbsent(metric, new ReentrantLock());
        }
        return mLocks.get(metric);
    }
}