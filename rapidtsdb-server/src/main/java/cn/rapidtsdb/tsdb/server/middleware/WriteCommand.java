package cn.rapidtsdb.tsdb.server.middleware;

import cn.rapidtsdb.tsdb.object.BizMetric;
import cn.rapidtsdb.tsdb.object.TSDataPoint;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Iterator;
import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
public class WriteCommand {

    @Getter
    private BizMetric metric;
    private Map<Long, Double> dps;
    private TSDataPoint dp;

    public WriteCommand(BizMetric metric, Map<Long, Double> dps) {
        this.metric = metric;
        this.dps = dps;
    }

    public WriteCommand(BizMetric metric, TSDataPoint dp) {
        this.metric = metric;
        this.dp = dp;
    }

    public Iterator<TSDataPoint> iter() {
        if (dps == null) {
            return new SinlgeDpIter(dp);
        } else {
            return new MapDpsIterator(dps);
        }
    }


    static class MapDpsIterator implements Iterator<TSDataPoint> {

        private Map<Long, Double> dps;
        private Iterator<Long> tpIter;

        public MapDpsIterator(Map<Long, Double> dps) {
            this.dps = dps;
            tpIter = dps.keySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return tpIter.hasNext();
        }

        @Override
        public TSDataPoint next() {
            Long nextTp = tpIter.next();
            return new TSDataPoint(nextTp, dps.get(nextTp));
        }
    }

    static class SinlgeDpIter implements Iterator<TSDataPoint> {
        private TSDataPoint dp;

        public SinlgeDpIter(TSDataPoint dp) {
            this.dp = dp;
        }

        @Override
        public boolean hasNext() {
            return dp != null;
        }

        @Override
        public TSDataPoint next() {
            TSDataPoint e = dp;
            dp = null;
            return e;
        }
    }

}
