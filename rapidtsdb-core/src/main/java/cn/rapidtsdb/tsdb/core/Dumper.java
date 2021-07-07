package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.utils.TimeUtils;
import lombok.extern.log4j.Log4j2;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Log4j2
public class Dumper {

    private static final Dumper INSTANCE = new Dumper();
    public static final int NO_DATA_IDENTIFIER = 0xff123;
    public static final int HAVE_DATA_INDENTIFIER = 0xeeff33;
    public static final int DUMPER_IDENTIFIER = 0x88bbca;


    public static Dumper getInstance() {
        return INSTANCE;
    }

    private Dumper() {
    }

    public String dumpLog2Tmp(byte[] timeData, byte[] valuesData, List<Long> timestamps, List<Double> decodedValues) {
        String tempDir = System.getProperty("java.io.tmpdir");
        tempDir = (tempDir == null ? "" : tempDir);
        LocalDate localDate = LocalDate.now();
        long tp = TimeUtils.currentMills();
        String filename = tempDir + localDate.format(DateTimeFormatter.BASIC_ISO_DATE) + "_" + tp + ".dump";
        log.info("Dumping data to file : {}", filename);


        try (FileOutputStream fos = new FileOutputStream(filename)) {
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fos);
            DataOutputStream dos = new DataOutputStream(bufferedOutputStream);
            dos.writeInt(DUMPER_IDENTIFIER);
            if (timeData != null) {
                dos.writeInt(HAVE_DATA_INDENTIFIER);
                dos.writeInt(timeData.length);
                dos.write(timeData);
            } else {
                dos.writeInt(NO_DATA_IDENTIFIER);
            }

            if (valuesData != null) {
                dos.writeInt(HAVE_DATA_INDENTIFIER);
                dos.writeInt(valuesData.length);
                dos.write(valuesData);
            } else {
                dos.writeInt(NO_DATA_IDENTIFIER);
            }

            if (timestamps != null && timestamps.size() > 0) {
                dos.writeInt(HAVE_DATA_INDENTIFIER);
                dos.writeInt(timestamps.size());
                for (long t : timestamps) {
                    dos.writeLong(t);
                }
            } else {
                dos.writeInt(NO_DATA_IDENTIFIER);
            }

            if (decodedValues != null && decodedValues.size() > 0) {
                dos.writeInt(HAVE_DATA_INDENTIFIER);
                dos.writeInt(decodedValues.size());
                for (double v : decodedValues) {
                    dos.writeDouble(v);
                }
            } else {
                dos.writeInt(NO_DATA_IDENTIFIER);
            }
            dos.flush();

        } catch (Exception e) {
            log.error("Write Dump file Exception {}", e.getMessage());
            e.printStackTrace();
            return null;
        }
        log.error("Dumper file to :{}", filename);
        return filename;
    }


}
