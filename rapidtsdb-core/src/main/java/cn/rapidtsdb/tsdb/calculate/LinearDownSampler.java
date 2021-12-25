package cn.rapidtsdb.tsdb.calculate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class LinearDownSampler implements DownSampler {

    private final int timeRangeMills;
    private LinearFunction downSamplerFunction;

    public LinearDownSampler(int timeRangeMills, LinearFunction downSamplerFunction) {
        this.timeRangeMills = timeRangeMills;
        this.downSamplerFunction = downSamplerFunction;
    }

    @Override
    public SortedMap<Long, Double> downSample(SortedMap<Long, Double> orderedDps) {
        SortedMap<Long, Double> result = new TreeMap<>();
        List<Double> valueBuffer = new ArrayList<>();
        if (orderedDps != null && orderedDps.size() > 0) {
            BufferScope bufferScope = new BufferScope(timeRangeMills);
            Iterator<Long> keyIter = orderedDps.keySet().iterator();
            bufferScope.init(orderedDps.firstKey());
            while (keyIter.hasNext()) {
                long tp = keyIter.next();
                double val = orderedDps.get(tp);
                if (bufferScope.inScope(tp)) {
                    valueBuffer.add(val);
                } else {
                    if (valueBuffer.size() > 0) {
                        double aggreVal = downSamplerFunction.apply(valueBuffer);
                        valueBuffer.clear();
                        long scopeTp = bufferScope.start;
                        result.put(scopeTp, aggreVal);
                    }
                    bufferScope.init(tp);
                    valueBuffer.add(val);
                }
            }

            if (valueBuffer.size() > 0) {
                double aggreVal = downSamplerFunction.apply(valueBuffer);
                valueBuffer.clear();
                result.put(bufferScope.start, aggreVal);
            }
        }
        return result;
    }

    private static class BufferScope {
        private int scopeSize;
        private long start;
        private long end;

        public BufferScope(int scopeSize) {
            this.scopeSize = scopeSize;
        }

        public void init(long initVal) {
            start = initVal - initVal % scopeSize;
            start = (start > 0 ? start : 0);
            end = start + scopeSize;
        }

        public void stepForward() {
            start = end;
            end = start + scopeSize;
        }

        public boolean inScope(long val) {
            return val >= start && val < end;
        }

        public boolean inNextScope(long val) {
            return val >= (end) && val < (end + scopeSize);
        }
    }
}
