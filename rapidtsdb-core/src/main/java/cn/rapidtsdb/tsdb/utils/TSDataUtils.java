package cn.rapidtsdb.tsdb.utils;

import cn.rapidtsdb.tsdb.object.TSDataPoint;

import java.util.List;

public class TSDataUtils {
    public static int binarySearchByTimestamp(List<TSDataPoint> dps, long searchTime, boolean includeEquals) {
        if (dps == null || dps.size() == 0) {
            return 0;
        }
        int left = 0; // left inclusive
        int right = dps.size(); // right exclusive
        while (left < right) {
            if (left + 1 == right) {
                long leftTime = dps.get(left).getTimestamp();
                if (searchTime > leftTime) {
                    return right;
                } else if (searchTime < leftTime) {
                    return left;
                } else {
                    return includeEquals ? left : right;
                }
            }
            int mid = (left + right) / 2;
            long midTimestamp = dps.get(mid).getTimestamp();
            if (searchTime > midTimestamp) {
                left = mid;
            } else if (searchTime < midTimestamp) {
                right = mid;
            } else {
                if (includeEquals) {
                    return mid;
                } else {
                    return mid + 1;
                }
            }
        }
        return 0;
    }


}
