package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.exception.BlockDataMissMatchException;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static cn.rapidtsdb.tsdb.core.ByteMask.RIGHT_MASK;

@Log4j2
public class TSBlock {

    /**
     * base time always the seconds unit,
     * this values won't must writen to the series data
     */
    @Getter
    private long baseTime; // in millseconds unit
    @Getter
    private int blockLengthMills;

    @Getter
    private TSBytes time;
    @Getter
    private TSBytes values;
    private transient Long preTime;
    private transient int preTimeDelta;
    protected AtomicInteger blockVersion = new AtomicInteger(0);
    @Getter
    protected volatile int clearedVersion = -1;

    private Double preWrittenValue;
    private DoubleXor.DoubleXorResult preWrittenValueXorResult;


    private Lock writeLock = new ReentrantLock();
    /**
     * data point int flat mode
     */

    private static final byte VALUE_CONTROL_BITS_IN = 2; // 0b10
    private static final byte VALUE_CONTROL_BITS_NEW = 3; // 0b11

    public static final int DEFAULT_TIME_BYTES_LENGTH = Math.max(2 * 7200, 800);
    public static final int DEFAULT_VALUE_BYTES_LENGTH = 21600;
    private WeakReference<TreeMap<Long, Double>> cachedDataPoints = new WeakReference<>(null);


    public TSBlock(long baseTime, int blockLengthMills) {
        this.baseTime = baseTime;
        this.blockLengthMills = blockLengthMills;
        initState();
    }

    private void initState() {
        time = new TSBytes(DEFAULT_TIME_BYTES_LENGTH);
        values = new TSBytes(DEFAULT_VALUE_BYTES_LENGTH);
        preTime = null;
        preTimeDelta = 0;
        preWrittenValue = null;
        preWrittenValueXorResult = null;
    }

    public boolean inBlock(long timestamp) {
        long diffMills = timestamp - baseTime;
        return diffMills >= 0 && diffMills < blockLengthMills;
    }

    public boolean afterBlock(long timestamp) {
        long diffSeconds = timestamp - baseTime;
        return diffSeconds >= blockLengthMills;
    }

    public boolean isNextAjacentBlock(TSBlock tsBlock) {
        return tsBlock.getBaseTime() - baseTime == TSBlockFactory.BLOCK_SIZE_MILLSECONDS;
    }


    /**
     * allow 2 hours time range  data points dis ordered
     *
     * @param timestamp
     * @param val
     */
    public void appendDataPoint(long timestamp, double val) {
        if (!inBlock(timestamp)) {
            throw new BlockDataMissMatchException(timestamp, baseTime, blockLengthMills);
        }
        writeLock.lock();
        appendTime(timestamp);
        appendValue(val);
        incrDataVersion();
        cachedDataPoints.clear();
        writeLock.unlock();
    }

    public TSBlockSnapshot snapshot() {
        writeLock.lock();
        TSBlockSnapshot snapshot = new TSBlockSnapshot(this);
        writeLock.unlock();
        return snapshot;
    }

    public void recoveryTimeBytes(byte[] bs, int byteOffset, int timeBitsLength) {
        writeLock.lock();
        if (time.getBytesOffset() != 0) {
            throw new RuntimeException("Can not recovery Time Bytes, because Block time had been written.");
        }
        time.setByteData(bs, byteOffset, timeBitsLength);
        List<Long> timePoints = decodeTimestamp();
        if (timePoints.size() > 0) {
            preTime = timePoints.get(timePoints.size() - 1);
        }
        if (timePoints.size() > 1) {
            preTimeDelta = (int) (preTime - timePoints.get(timePoints.size() - 2));
        }
        writeLock.unlock();
    }

    public void recovertValueBytes(byte[] bs, int byteOffset, int valueBitsLength) {
        writeLock.lock();
        if (values.getBytesOffset() != 0) {
            throw new RuntimeException("Can not recovery Value Bytes, because the Value Block had been written.");
        }
        values.setByteData(bs, byteOffset, valueBitsLength);
        List<Double> vals = decodeValues();
        if (vals.size() > 0) {
            preWrittenValue = vals.get(vals.size() - 1);
        }
        if (vals.size() > 1) {
            preWrittenValueXorResult = DoubleXor.doubleXor(vals.get(vals.size() - 2), vals.get(vals.size() - 1));
        }
        writeLock.unlock();
    }

    public double getMemoryUsedKB() {
        double kb = time.getMemoryUsedKB() + values.getMemoryUsedKB();
        return kb;
    }

    public double getMemoryActualUsed() {
        double kb = time.getMemoryActualUsed() + values.getMemoryActualUsed();
        return kb;
    }

    private void appendTime(long timestamp) {
        long t = timestamp;
        if (preTime != null) {
            int thisDelta = (int) (t - preTime);
            int dod = thisDelta - preTimeDelta;
            if (dod == 0) {
                time.incBitsOffset(1);
            } else if (dod >= -63 && dod <= 64) {
                //write 0b10
                time.appendBitesDataInternal((byte) 2, 2);
                time.appendBitesDataInternal((byte) dod, 7);
            } else if (dod >= -255 && dod <= 256) {
                // write 0b110
                time.appendBitesDataInternal((byte) 6, 3);
                time.appendShortBites((short) dod, 9);
            } else if (dod >= 2047 && dod <= 2048) {
                // write 0b1110  14 = 2+4+8
                time.appendBitesDataInternal((byte) 14, 4);
                short shortDod = (short) dod;
                time.appendShortBites(shortDod, 12);
            } else {
                // write 0b1111
                time.appendBitesDataInternal((byte) 15, 4);
                time.appendIntBites(dod);
            }
            preTimeDelta = thisDelta;
        } else {
            time.appendLongBits(t);
        }
        preTime = t;
    }


    private void appendValue(double value) {
        if (preWrittenValue != null) {
            DoubleXor.DoubleXorResult thisXor = DoubleXor.doubleXor(preWrittenValue, value);
            if (thisXor.isZero()) {
                values.incBitsOffset(1);
            } else {

                if (preWrittenValueXorResult != null && preWrittenValueXorResult.inSubRange(thisXor)) {
                    byte header = VALUE_CONTROL_BITS_IN; // 0b10
                    values.appendBitesDataInternal(header, 2);
                    byte[] writtenBits = Longs.toByteArray(thisXor.getRawResult() << preWrittenValueXorResult.getLeft0());
                    values.appendBytes(writtenBits, preWrittenValueXorResult.getMeaningBitsLength());
                } else {
                    long longBits = thisXor.getMeaningLongBits();
                    byte header = VALUE_CONTROL_BITS_NEW; // 0b11
                    //append leading zero
                    byte leadingZeroNumber = thisXor.getLeft0();
                    byte meaningfulBitsLength = thisXor.getMeaningBitsLength();
                    values.appendBitesDataInternal(header, 2);
                    values.appendBitesDataInternal(leadingZeroNumber, 5);
                    values.appendBitesDataInternal(meaningfulBitsLength, 6);
                    values.appendBytes(Longs.toByteArray(longBits), thisXor.getMeaningBitsLength());
                }
                preWrittenValueXorResult = thisXor;
            }
        } else {
            long lv = Double.doubleToLongBits(value);
            values.appendLongBits(lv);
        }
        preWrittenValue = value;
    }

    public TreeMap<Long, Double> getDataPoints() {
        TreeMap<Long, Double> dps = cachedDataPoints.get();
        if (dps != null) {
            return dps;
        }
        List<Long> decodedTimestamp = decodeTimestamp();
        List<Double> decodedValues = decodeValues();
        if (decodedTimestamp.size() != decodedValues.size()) {
            String dumpFilename = Dumper.getInstance().dumpLog2Tmp(time.getData(), values.getData(), decodedTimestamp, decodedValues);
            log.error("[dump file: {}]. Fatal, Not aligned time and values, " + decodedTimestamp.size() + ":" + decodedValues.size(), dumpFilename);
            log.error("decode time:{}", decodedTimestamp);
            log.error("decode values:{}", decodedValues);
            throw new RuntimeException("Fatal, Not aligned time and values, " + decodedTimestamp.size() + ":" + decodedValues.size());
        }
        dps = new TreeMap<>();
        for (int i = 0; i < decodedTimestamp.size(); i++) {
            dps.put(decodedTimestamp.get(i), decodedValues.get(i));
        }
//        handleDuplicateDatapoint((ArrayList<TSDataPoint>) dps);
        cachedDataPoints = new WeakReference<>(dps);
        return dps;
    }

    public void rewriteBytesData(Map<Long, Double> dps) {
        try {
            writeLock.lock();
            initState();
            if (dps != null) {
                dps.forEach((k, v) -> {
                    appendDataPoint(k, v);
                });
            }
        } finally {
            writeLock.unlock();
        }
    }


    private List<Long> decodeTimestamp() {
        List<Long> timestamps = Lists.newArrayListWithCapacity(120 * 60);
        int readBitsIdx = 0;

        int bitsLimit = time.getBytesOffset() * 8 + time.getBitsOffset();
        byte[] data = time.getData();
        if (data.length < 8) {
            return Lists.newArrayList();
        }
        long first = Longs.fromByteArray(data);
        timestamps.add(first);
        long preVal = first;
        long preDelta = 0;
        readBitsIdx = 64;
        while (readBitsIdx < bitsLimit) {
            long v = preVal;
            long dod = 0;
            if (probeBit(data, readBitsIdx) == 0) {
                readBitsIdx++;
            } else if (probeBit(data, readBitsIdx + 1) == 0) {
                // header 10
                long t = readBits2Long(data, readBitsIdx + 2, 7);
                dod = recoverSign(t, 7);
                readBitsIdx += 2 + 7; // header + bits
            } else if (probeBit(data, readBitsIdx + 2) == 0) {
                // header 110
                long t = readBits2Long(data, readBitsIdx + 3, 9);
                dod = recoverSign(t, 9);
                readBitsIdx += 3 + 9;
            } else if (probeBit(data, readBitsIdx + 3) == 0) {
                // header 1110
                long t = readBits2Long(data, readBitsIdx + 4, 12);
                dod = recoverSign(t, 12);
                readBitsIdx += 4 + 12;
            } else {
                long t = readBits2Long(data, readBitsIdx + 4, 32);
                dod = recoverSign(t, 32);
                readBitsIdx += 4 + 32;
            }
            v = preVal + preDelta + dod;
            timestamps.add(v);
            preDelta = v - preVal;
            preVal = v;
        }
        return timestamps;
    }

    private List<Double> decodeValues() {
        List<Double> decodedValues = Lists.newArrayListWithCapacity(120 * 60);
        int bitsLimit = values.getBytesOffset() * 8 + values.getBitsOffset();
        byte[] data = values.getData();
        if (data.length < 8) {
            return Lists.newArrayList();
        }

        long first = Longs.fromByteArray(data);
        long preVal = first;
        DoubleXor.DoubleXorResult preXorResult = null;
        decodedValues.add(Double.longBitsToDouble(first));
        int readBitsIdx = 64;
        while (readBitsIdx < bitsLimit) {
            long xor = 0;
            if (probeBit(data, readBitsIdx) == 0) {
                readBitsIdx++;
            } else if (probeBit(data, readBitsIdx + 1) == 0) {
                xor = readBits2Long(data, readBitsIdx + 2, preXorResult.getMeaningBitsLength());
                xor <<= preXorResult.getRight0();
                readBitsIdx += 2 + preXorResult.getMeaningBitsLength();
            } else {
                long leadingZero = readBits2Long(data, readBitsIdx + 2, 5);
                long meaningfulBitsLength = readBits2Long(data, readBitsIdx + 2 + 5, 6);
                long meaningfulBits = readBits2Long(data, readBitsIdx + 2 + 5 + 6, (int) meaningfulBitsLength);
                xor = meaningfulBits << (64 - meaningfulBitsLength - leadingZero);
                readBitsIdx += 2 + 5 + 6 + meaningfulBitsLength;
//                    long xor = bitsVal ^ preVal;
//                    preXorResult = DoubleXor.fromRawResult(xor);
//                    preVal = bitsVal;
//                    readBitsIdx += 2 + 5 + 6 + meaningfulBitsLength; // header + leadingZero + meaninfulBitsLength + meaningfulBits
//                    v = Double.longBitsToDouble(bitsVal);
            }
            long bitsVal = xor ^ preVal;
            double v = Double.longBitsToDouble(bitsVal);
            preVal = bitsVal;
            if (xor > 0) {
                preXorResult = DoubleXor.fromRawResult(xor);
            }
            decodedValues.add(v);
        }
        return decodedValues;
    }

    /**
     * caller should maintainer the bitPos available
     *
     * @param bytes
     * @param bitPos
     * @return
     */
    private int probeBit(byte[] bytes, int bitPos) {
        int readBytesIdx = bitPos / 8;
        return bytes[readBytesIdx] & (1 << (7 - bitPos % 8));
    }

    private long readBits2Long(byte[] bytes, int bitOffset, int bitLen) {
        long v = 0;
        int readingBitOffset = bitOffset;
        int unreadBitLen = bitLen;

        while (unreadBitLen > 0) {
            int byteIdx = readingBitOffset / 8;
            int bitIdx = readingBitOffset % 8;
            int thisReadBitsLen = Math.min(8 - bitIdx, unreadBitLen);
            v <<= thisReadBitsLen;
            v |= (bytes[byteIdx] >> (8 - bitIdx - thisReadBitsLen)) & RIGHT_MASK[thisReadBitsLen];
            unreadBitLen -= thisReadBitsLen;
            readingBitOffset += thisReadBitsLen;
        }
        return v;
    }

    private long recoverSign(long v, int bitLen) {
        //restore sign
        v <<= (64 - bitLen);
        v >>= (64 - bitLen);
        return v;
    }

    public void incrDataVersion() {
        blockVersion.incrementAndGet();
    }

    public void markVersionClear(int version) {
        this.clearedVersion = version;
    }


    public boolean isDirty() {
        return blockVersion.get() == clearedVersion;
    }

    public int getBlockVersion() {
        return blockVersion.get();
    }

    public void afterRecovery() {
        time.preExpandSpace(DEFAULT_TIME_BYTES_LENGTH);
        values.preExpandSpace(DEFAULT_VALUE_BYTES_LENGTH);
    }
}
