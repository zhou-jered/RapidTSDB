package cn.rapidtsdb.tsdb.tools;

import cn.rapidtsdb.tsdb.common.TimeUtils;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class BlockBaseTimeScanner implements Iterator<Long> {

    private long start;
    private long end;

    private long current;

    public BlockBaseTimeScanner(long start, long end) {
        this.start = start;
        this.end = end;
        current = TimeUtils.getBlockBaseTime(start);
    }

    public int size() {
        return (int) ((end - start) / TimeUnit.HOURS.toMillis(2) + 1);
    }

    @Override
    public boolean hasNext() {
        return current < end;
    }

    @Override
    public Long next() {
        long ret = current;
        current += TimeUnit.HOURS.toMillis(2);
        return ret;
    }
}
