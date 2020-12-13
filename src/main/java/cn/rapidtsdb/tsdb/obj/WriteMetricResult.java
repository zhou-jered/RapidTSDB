package cn.rapidtsdb.tsdb.obj;

public class WriteMetricResult {
    private int code;
    private String msg;

    public WriteMetricResult(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }


    public static final int SUCCESS_CODE = 0;
    /**
     * two indicate client issue
     * three indicate server issue
     */
    public static final int CODE_TIME_EXPIRED = 10;
    public static final int CODE_METRIC_EMPTY = 11;
    public static final int CODE_SPACE_FULL = 122;
    public static final int CODE_PART_FAILED = 125;
    public static final int CODE_INTERNAL_ERROR = 999;

    public static final WriteMetricResult SUCCESS = new WriteMetricResult(0, "ok");
    public static final WriteMetricResult FAILED_METRIC_EMPTY = new WriteMetricResult(CODE_METRIC_EMPTY, "metric empty");
    public static final WriteMetricResult FAILED_TIME_EXPIRED = new WriteMetricResult(CODE_TIME_EXPIRED, "metric empty");


}
