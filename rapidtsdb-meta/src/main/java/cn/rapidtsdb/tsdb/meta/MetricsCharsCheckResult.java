package cn.rapidtsdb.tsdb.meta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MetricsCharsCheckResult {
    private boolean pass;
    private char illegalChar;
    private int pos;
    private String failedField;

    public boolean checkPass() {
        return pass;
    }

    public String errMsg() {
        return "Illegal char:" + illegalChar + " of:" + failedField + ", at the position:" + pos;
    }

    public static final MetricsCharsCheckResult failed(char illegalChar, int fp, String field) {
        return new MetricsCharsCheckResult(false, illegalChar, fp, field);
    }

    public static final MetricsCharsCheckResult OK = new MetricsCharsCheckResult(true, '0', 0, null);
}
