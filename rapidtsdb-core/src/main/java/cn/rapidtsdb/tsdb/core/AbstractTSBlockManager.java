package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.TSDBTaskCallback;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;

/**
 * Define the Manager Rules of TSBlocks.
 */
public abstract class AbstractTSBlockManager implements Initializer, Closer {
    protected TSDBConfig tsdbConfig;

    public abstract TSBlock getTargetWriteBlock(int metricId, long timestamp);

    /**
     * Every Two Hours Trigger once
     */
    public abstract void triggerRoundCheck(TSDBTaskCallback completedCallback);

    public abstract List<TSBlock> getBlockWithTimeRange(int metricId, long start, long end);

    public abstract Iterator<TSBlock> getBlockStreamByTimeRange(int metricId, long start, long end);



    public static TSBlockMeta createTSBlockMeta(TSBlockSnapshot snapshot, int metricId) {

        TSBlockMeta blockMeta = new TSBlockMeta();
        blockMeta.setMetricId(metricId);
        blockMeta.setBaseTime(snapshot.getTsBlock().getBaseTime());
        blockMeta.setDpsSize(snapshot.getDpsSize());
        blockMeta.setTimeBitsLen(snapshot.getTimeBitsLength());
        blockMeta.setValuesBitsLen(snapshot.getValuesBitsLength());
        try {
            byte lastDataByte;
            MessageDigest messageDigest = MessageDigest.getInstance("md5");

            int bytesLength = snapshot.getTimeBytesLength();
            int bits = snapshot.getTimeBitsLength();
            if (bits % 8 != 0 && bytesLength > 1) {
                messageDigest.update(snapshot.getTsBlock().getTime().getData(), 0, bytesLength - 1);
                lastDataByte = snapshot.getTsBlock().getTime().getData()[bytesLength - 1];
                lastDataByte &= ByteMask.LEFT_MASK[bits % 8];
                messageDigest.update(lastDataByte);
            } else {
                messageDigest.update(snapshot.getTsBlock().getTime().getData(), 0, snapshot.getTimeBytesLength());
            }


            bytesLength = snapshot.getValuesBytesLength();
            bits = snapshot.getValuesBitsLength();
            if (bits % 8 != 0 && bytesLength > 1) {
                messageDigest.update(snapshot.getTsBlock().getValues().getData(), 0, bytesLength - 1);
                lastDataByte = snapshot.getTsBlock().getValues().getData()[bytesLength - 1];
                lastDataByte &= ByteMask.LEFT_MASK[bits % 8];
                messageDigest.update(lastDataByte);
            } else {
                messageDigest.update(snapshot.getTsBlock().getValues().getData(), 0, snapshot.getValuesBytesLength());
            }

            byte[] md5 = messageDigest.digest();
            blockMeta.setMd5Checksum(md5);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return blockMeta;

    }


    public abstract void tryRecoveryMemoryData(List<Integer> metricsIdList);
}
