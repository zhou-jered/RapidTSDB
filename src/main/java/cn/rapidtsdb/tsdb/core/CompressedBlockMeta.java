package cn.rapidtsdb.tsdb.core;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Daily, Monthly, Yearly Block Metadata
 */
@Data
@NoArgsConstructor
public class CompressedBlockMeta {
    private long baseTime;

}
