package cn.rapidtsdb.tsdb.calculate;

import cn.rapidtsdb.tsdb.core.TSDataPoint;

import java.util.List;

@FunctionalInterface
public interface DownSampler {
    List<TSDataPoint> downSample(List<TSDataPoint> dps);
}
