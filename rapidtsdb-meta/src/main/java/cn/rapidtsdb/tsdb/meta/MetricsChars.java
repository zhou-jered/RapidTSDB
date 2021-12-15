package cn.rapidtsdb.tsdb.meta;

public class MetricsChars {
    public static final String METRICS_LEGAL_CHARS = "plokmijnuhbygvtfcrdxeszwaqPLOKMIJNUHBYGVTFCRDXESZWAQ0987654321@#$-_.+=:;,^/";
    private static boolean[] legalCharMap = new boolean[256];

    static {
        for (int i = 0; i < legalCharMap.length; i++) {
            legalCharMap[i] = false;
        }
        for (int i = 0; i < METRICS_LEGAL_CHARS.length(); i++) {
            legalCharMap[METRICS_LEGAL_CHARS.charAt(i)] = true;
        }
    }

    public static Character checkChars(String metric) {
        char[] chars = metric.toCharArray();
        for (char c : chars) {
            if (((int) c) > 256 || !legalCharMap[c]) {
                return c;
            }
        }
        return null;
    }
}
