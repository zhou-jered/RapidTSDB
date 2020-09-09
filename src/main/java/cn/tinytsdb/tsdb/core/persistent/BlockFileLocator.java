package cn.tinytsdb.tsdb.core.persistent;

import cn.tinytsdb.tsdb.utils.TimeUtils;
import lombok.Data;

import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

/**
 * provider block file name,
 * Persist filename rules:
 * metric_id_type(1)_blockBaseTime
 * <p>
 * Persist File Directory rules:
 * /{$year}/${month}/${day}/${filename}
 */
public class BlockFileLocator {

    private static TimeUtils.TimeUnitAdaptor SECONDS_ADAPTER = TimeUtils.ADAPTER_SECONDS;

    public BlockFileLocation getBlockFileLocation(int metricId, long blockBaseTimestamp) {
        long timestampSeconds = SECONDS_ADAPTER.adapt(blockBaseTimestamp);
        FastDatetime fdt = FastDatetime.fromSecondsTimestamp(timestampSeconds);
        String dir = fdt.getDateDir();
        String filename = String.format("%d_%d_%d", metricId, 1, blockBaseTimestamp);
        return new BlockFileLocation(dir, filename);
    }


    private static class FastDatetime {
        int fullyear;
        int month;
        int day;

        public String getDateDir() {
            return String.format("%4d/%02d/%02d/", fullyear, month, day);
        }

        static FastDatetime fromSecondsTimestamp(long timestampSeconds) {
            FastDatetime fdt = new FastDatetime();
            LocalDate localDate = LocalDate.ofEpochDay(timestampSeconds / TimeUnit.DAYS.toSeconds(1));
            fdt.fullyear = localDate.getYear();
            fdt.month = localDate.getMonthValue();
            fdt.day = localDate.getDayOfMonth();
            return fdt;
        }
    }

}
