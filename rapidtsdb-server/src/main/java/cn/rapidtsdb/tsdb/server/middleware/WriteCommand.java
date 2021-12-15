package cn.rapidtsdb.tsdb.server.middleware;

import cn.rapidtsdb.tsdb.object.TSDataPoint;
import cn.rapidtsdb.tsdb.meta.BizMetric;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WriteCommand {
    private BizMetric metric;
    private TSDataPoint[] dps;
}
