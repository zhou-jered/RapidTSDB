package cn.rapidtsdb.tsdb.obj.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Datapoint {
    private long timestamp;
    private double val;
}
