package cn.rapidtsdb.tsdb.server.handler.rpc.v1;

import cn.rapidtsdb.tsdb.meta.MetricsChars;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

public class Validator {

    /**
     * todo check tags
     *
     * @param metric
     * @param tags
     * @return
     */
    public static ValidateResult validMetric(String metric, Map<String, String> tags) {
        ValidateResult vr = new ValidateResult();
        if (StringUtils.isBlank(metric)) {
            vr.failedField = "metric";
            vr.failedMsg = "metric empty";
        } else {
            Character illgealC = MetricsChars.checkChars(metric);
            if (illgealC != null) {
                vr.failedField = "metric";
                vr.failedMsg = "illegal char:" + illgealC;
            }
        }
        return vr;
    }


    @Data
    public static class ValidateResult {
        boolean valid = true;
        String failedField;
        String failedMsg;

        public String errMsg() {
            return "Field:" + failedField + " illegal for " + failedMsg;
        }
    }
}
