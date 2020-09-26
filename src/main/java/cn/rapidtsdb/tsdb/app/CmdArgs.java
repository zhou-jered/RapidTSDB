package cn.rapidtsdb.tsdb.app;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class CmdArgs {
    private String shortName;
    private String fullName;
    private Class type;
}
