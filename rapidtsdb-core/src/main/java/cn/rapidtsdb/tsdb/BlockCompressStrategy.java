package cn.rapidtsdb.tsdb;

public interface BlockCompressStrategy {
    /**
     * @return the number of days that after block time will compressed into a daily block
     * return -1 indicate that no compression
     */
    int compressDailyBlockAfterDays();

    /**
     * must great than @compressDailyBlockAfterDays
     * return a negative number indicate that no compression
     *
     * @return the number of days
     */
    int compressMonthlyBlockAfterDays();

    /**
     * must great than @compressDailyBlockAfterDays
     * return a negative number indicate that no compression
     *
     * @return
     */
    int compressYearlyBlockAfterDays();
}
