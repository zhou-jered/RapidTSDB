package cn.rapidtsdb.tsdb.calculate;

import cn.rapidtsdb.tsdb.core.TSDataPoint;

import java.util.List;

public interface Downsampler {
    List<TSDataPoint> downsample(List<TSDataPoint> dps);
}
