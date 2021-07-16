package cn.rapidtsdb.tsdb.calculate;

import cn.rapidtsdb.tsdb.core.TSDataPoint;

import java.util.ArrayList;
import java.util.List;

public class LinearDownSampler implements DownSampler {

    private final int timeRangeMills;
    private LinearFunction downSamplerFunction;

    public LinearDownSampler(int timeRangeMills, LinearFunction downSamplerFunction) {
        this.timeRangeMills = timeRangeMills;
        this.downSamplerFunction = downSamplerFunction;
    }

    @Override
    public List<TSDataPoint> downSample(List<TSDataPoint> orderedDps) {
        List<TSDataPoint> result = new ArrayList<>();
        List<Double> valueBuffer = new ArrayList<>();
        if (orderedDps != null && orderedDps.size() > 0) {

            BufferScope bufferScope = new BufferScope(timeRangeMills);
            bufferScope.init(orderedDps.get(0).getTimestamp());
            for (TSDataPoint dp : orderedDps) {
                if (bufferScope.inScope(dp.getTimestamp())) {
                    valueBuffer.add(dp.getValue());
                } else if (bufferScope.inNextScope(dp.getTimestamp())) {
                    if (valueBuffer.size() > 0) {
                        double aggreVal = downSamplerFunction.apply(valueBuffer);
                        valueBuffer.clear();
                        result.add(new TSDataPoint(bufferScope.start, aggreVal));
                    }
                    bufferScope.stepForward();
                    valueBuffer.add(dp.getValue());
                } else {
                    bufferScope.init(dp.getTimestamp());
                    valueBuffer.add(dp.getValue());
                }

            }
            if (valueBuffer.size() > 0) {
                double aggreVal = downSamplerFunction.apply(valueBuffer);
                valueBuffer.clear();
                result.add(new TSDataPoint(bufferScope.start, aggreVal));
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
