package cn.tinytsdb.tsdb.core;

import cn.tinytsdb.tsdb.utils.BinaryUtils;
import cn.tinytsdb.tsdb.utils.TimeUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;

/**
 * Define the Manager Rules of TSBlocks.
 */
public abstract class AbstractTSBlockManager {

    protected static final TimeUtils.TimeUnitAdaptor TIME_UNIT_ADAPTOR_SECONDS = TimeUtils.ADAPTER_SECONDS;

    public abstract TSBlock getCurrentWriteBlock(int metricId, long timestamp);

    public abstract TSBlock searchHistoryBlock(int metricId, long timestamp);

    public abstract TSBlock newTSBlock(int metricId, long timestamp);

    public abstract void persistTSBlockSync(int metricId, TSBlock tsBlock);

    public abstract void persistTSBlockAsync(int metricId, TSBlock tsBlock);

    public abstract List<TSBlock> getBlockWithTimeRange(int metricId, long start, long end);

    public abstract Iterator<TSBlock> getBlockStreamByTimeRange(int metricId, long start, long end);

    protected static TSBlockMeta createTSBlockMeta(TSBlock tsBlock) {
        tsBlock.frzeeWrite();
        TSBlockMeta blockMeta = new TSBlockMeta();
        blockMeta.setBaseTime(TIME_UNIT_ADAPTOR_SECONDS.adapt(tsBlock.getBaseTime()));
        blockMeta.setDpsSize(tsBlock.getDataPoints().size());
        blockMeta.setTimeBitsLen(tsBlock.getTime().getTotalBitsLength());
        blockMeta.setValuesBitsLen(tsBlock.getValues().getTotalBitsLength());

        try {
            MessageDigest messageDigest = MessageDigest.getInstance("md5");
            messageDigest.update(tsBlock.getTime().getData(), 0, tsBlock.getTime().getBytesOffset());
            messageDigest.update(tsBlock.getValues().getData(), 0, tsBlock.getValues().getBytesOffset());
            byte[] bs = messageDigest.digest();
            String md5 = BinaryUtils.hexBytes(bs);
            blockMeta.setMd5Checksum(bs);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return blockMeta;

    }
}
