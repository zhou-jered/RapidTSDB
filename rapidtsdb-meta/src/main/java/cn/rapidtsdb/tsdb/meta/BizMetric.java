package cn.rapidtsdb.tsdb.meta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class BizMetric {
    private String metric;
    private Map<String, String> tags;
}
