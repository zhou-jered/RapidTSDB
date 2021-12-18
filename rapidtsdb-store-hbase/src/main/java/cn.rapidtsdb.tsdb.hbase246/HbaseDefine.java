package cn.rapidtsdb.tsdb.hbase246;

public class HbaseDefine {
    public static final String DATA_TABLE = "rapid-data";

    public static final String CF_DATA = "d";
    public static final String CF_META = "t";
    public static final String CF_TAGK = "tk";
    public static final String CF_TAGV = "tv";
    public static final String CF_METRIC = "m";

    public static final String[] COLUMN_FAMILIES = new String[]{
            CF_DATA, CF_META, CF_TAGK, CF_TAGV, CF_METRIC
    };

}
