package cn.rapidtsdb.tsdb.core.persistent;

import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

public interface IMetricsKeyManager extends Initializer, Closer {
    
    Set<String> getAllMetrics();

    int getMetricsIndex(String metric, boolean createWhenNotExist);

    List<String> scanMetrics(String metricsPrefix);

    List<String> scanMetrics(String metricsPrefix, @Nullable List<String> mustIncluded);
}
