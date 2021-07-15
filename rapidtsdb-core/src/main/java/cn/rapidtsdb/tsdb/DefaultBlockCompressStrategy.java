package cn.rapidtsdb.tsdb;

public class DefaultBlockCompressStrategy implements BlockCompressStrategy {
    @Override
    public int compressDailyBlockAfterDays() {
        return 7;
    }

    @Override
    public int compressMonthlyBlockAfterDays() {
        return 30;
    }

    @Override
    public int compressYearlyBlockAfterDays() {
        return 120;
    }
}
