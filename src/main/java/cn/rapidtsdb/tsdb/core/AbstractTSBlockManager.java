package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.core.persistent.TSDBCheckPointManager;
import cn.rapidtsdb.tsdb.utils.TimeUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Define the Manager Rules of TSBlocks.
 */
public abstract class AbstractTSBlockManager {

    //todo leave or stay here
    protected TSDBCheckPointManager checkPointManager;

    protected static final TimeUtils.TimeUnitAdaptor TIME_UNIT_ADAPTOR_SECONDS = TimeUtils.ADAPTER_SECONDS;

    protected AtomicReference<Set<TSBlock>> dirtyBlocksRef = new AtomicReference<>();

    public abstract TSBlock getCurrentWriteBlock(int metricId, long timestamp);
    
    /**
     * Every Two Hours Trigger once
     */
    public abstract void triggerRoundCheck(Runnable completedCallback);

    public abstract List<TSBlock> getBlockWithTimeRange(int metricId, long start, long end);

    public abstract Iterator<TSBlock> getBlockStreamByTimeRange(int metricId, long start, long end);


    public void markDirtyBlock(TSBlock block) {
        dirtyBlocksRef.get().add(block);
    }

    public static TSBlockMeta createTSBlockMeta(TSBlockSnapshot snapshot, int metricId) {

        TSBlockMeta blockMeta = new TSBlockMeta();
        blockMeta.setMetricId(metricId);
        blockMeta.setBaseTime(TIME_UNIT_ADAPTOR_SECONDS.adapt(snapshot.getTsBlock().getBaseTime()));
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


}
