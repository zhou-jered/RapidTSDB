package cn.tinytsdb.tsdb.calculate;

import cn.tinytsdb.tsdb.core.TSDataPoint;

import java.util.List;

public interface Downsampler {
    List<TSDataPoint> downsample(List<TSDataPoint> dps);
}
