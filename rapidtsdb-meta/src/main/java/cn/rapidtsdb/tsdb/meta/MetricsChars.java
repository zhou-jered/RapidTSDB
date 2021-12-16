package cn.rapidtsdb.tsdb.meta;

import cn.rapidtsdb.tsdb.object.BizMetric;

import java.util.Map;

public class MetricsChars {
    public static final String METRICS_LEGAL_CHARS = "plokmijnuhbygvtfcrdxeszwaqPLOKMIJNUHBYGVTFCRDXESZWAQ0987654321[]{}!@#$-_.+=:;,";
    private static boolean[] legalCharMap = new boolean[256];
    private static int maxMetricLength;
    private static int maxTagKeyLength;
    private static int maxTagValueLength;

    //todo call it
    public static void init(int maxMetricLength, int maxTagKeyLength, int maxTagValueLength) {
        MetricsChars.maxMetricLength = maxMetricLength;
        MetricsChars.maxTagKeyLength = maxTagKeyLength;
        MetricsChars.maxTagValueLength = maxTagValueLength;
    }

    static {
        for (int i = 0; i < legalCharMap.length; i++) {
            legalCharMap[i] = false;
        }
        for (int i = 0; i < METRICS_LEGAL_CHARS.length(); i++) {
            legalCharMap[METRICS_LEGAL_CHARS.charAt(i)] = true;
        }
    }

    public static MetricsCharsCheckResult checkMetricChars(BizMetric metric) {
        String m = metric.getMetric();
        char[] chars = m.toCharArray();
        int fp = failedPos(chars);
        if (fp >= 0) {
            MetricsCharsCheckResult r = MetricsCharsCheckResult.failed(chars[fp], fp, "metric");
            return r;
        }
        MetricsCharsCheckResult charsCheckResult = null;
        Map<String, String> tags = metric.getTags();
        if (tags != null && tags.size() > 0) {
            for (String k : tags.keySet()) {
                int fpk = failedPos(k.toCharArray());
                if (fpk >= 0) {
                    charsCheckResult = MetricsCharsCheckResult.failed(k.charAt(fpk), fpk, "Tag Key:" + k);
                }
                int fpv = failedPos(tags.get(k).toCharArray());
                if (fpv >= 0) {
                    charsCheckResult = MetricsCharsCheckResult.failed(tags.get(k).charAt(fpv), fpv, "Tag Value:" + tags.get(k));
                }
            }
        }
        return charsCheckResult;
    }

    private static int failedPos(char[] chars) {
        for (int i = 0; i < chars.length; i++) {
            if ((int) chars[i] > legalCharMap.length) {
                return i;
            }
            if (!legalCharMap[chars[i]]) {
                return i;
            }
        }
        return -1;
    }
}
