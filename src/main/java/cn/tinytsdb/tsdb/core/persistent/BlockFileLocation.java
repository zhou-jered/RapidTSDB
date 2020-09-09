package cn.tinytsdb.tsdb.core.persistent;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class BlockFileLocation {
    private String dir;
    private String filename;


}
