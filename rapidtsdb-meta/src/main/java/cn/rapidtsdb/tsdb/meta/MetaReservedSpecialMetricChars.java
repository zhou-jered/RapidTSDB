package cn.rapidtsdb.tsdb.meta;

class MetaReservedSpecialMetricChars {
    public static final char tagKVSeparatorChar = '^';
    public static final char tagSeparatorChar = '/';

    public static boolean hasSpecialChar(char[] chars) {
        if (chars != null) {
            for (char c : chars) {
                if (c == tagKVSeparatorChar || c == tagSeparatorChar) {
                    return true;
                }
            }
        }
        return false;
    }
}
