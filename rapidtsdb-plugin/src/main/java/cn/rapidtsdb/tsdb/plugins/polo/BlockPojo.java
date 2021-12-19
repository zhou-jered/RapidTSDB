package cn.rapidtsdb.tsdb.plugins.polo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BlockPojo implements Comparable<BlockPojo> {
    private int metricId;
    private long basetime;
    private byte[] data;

    @Override
    public int compareTo(BlockPojo o) {
        return Long.compare(basetime, o.basetime);
    }
}
