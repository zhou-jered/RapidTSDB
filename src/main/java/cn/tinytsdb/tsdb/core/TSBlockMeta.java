package cn.tinytsdb.tsdb.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TSBlockMeta implements Serializable {

    private long baseTime;
    private int dpsSize;
    private int timeBitsLen;
    private int valuesBitsLen;
    private String md5Checksum;

}
