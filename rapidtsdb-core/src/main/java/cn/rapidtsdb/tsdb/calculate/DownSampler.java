package cn.rapidtsdb.tsdb.calculate;

import cn.rapidtsdb.tsdb.object.TSDataPoint;

import java.util.List;
import java.util.SortedMap;

@FunctionalInterface
public interface DownSampler {
    SortedMap<Long, Double>  downSample(SortedMap<Long, Double> dps);
}
