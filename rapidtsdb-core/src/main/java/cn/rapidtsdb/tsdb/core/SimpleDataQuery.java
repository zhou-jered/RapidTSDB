package cn.rapidtsdb.tsdb.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SimpleDataQuery {
    private String metric;
    private long startTime;
    private long endTime;
}
