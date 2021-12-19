package cn.rapidtsdb.tsdb.hbase246;

import java.nio.charset.StandardCharsets;

public class HbaseDefine {
    public static final String DATA_TABLE = "rapid-data";

    public static final String CF_DATA = "d";
    public static final String CF_META = "t";
    public static final String CF_TAGK = "tk";
    public static final String CF_TAGV = "tv";
    public static final String CF_METRIC = "m";

    public static final byte[] CF_DATA_BYTES = "d".getBytes(StandardCharsets.UTF_8);
    public static final byte[] CF_META_BYTES = "t".getBytes(StandardCharsets.UTF_8);
    public static final byte[] CF_TAGK_BYTES = "tk".getBytes(StandardCharsets.UTF_8);
    public static final byte[] CF_TAGV_BYTES = "tv".getBytes(StandardCharsets.UTF_8);
    public static final byte[] CF_METRIC_BYTES = "m".getBytes(StandardCharsets.UTF_8);


    public static final String[] COLUMN_FAMILIES = new String[]{
            CF_DATA, CF_META, CF_TAGK, CF_TAGV, CF_METRIC
    };

}
