package cn.rapidtsdb.tsdb.utils;

import cn.rapidtsdb.tsdb.core.Dumper;

import java.io.*;

/**
 * dumper format:
 * Dumper File Identifier | Parts
 * Parts: timeBytes | valuesBytes | decodedTimestamp | decodedValues
 * PartFormat:
 * have_or_no_data_identifier -4 bytes | length or size - 4 bytes | data
 */
public class DumpFileAnalyzer {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("No File Specified");
            return;
        }
        String filename = args[0];
        System.out.println("Analying " + filename);
        File file = new File(filename);
        if (file.exists() == false) {
            System.out.println("File [" + filename + "] not Found");
        }

        try {
            FileInputStream fis = new FileInputStream(file);
            DataInputStream dis = new DataInputStream(fis);

            int tryIdentifier = dis.readInt();
            if (tryIdentifier != Dumper.DUMPER_IDENTIFIER) {
                throw new RuntimeException("This File is not a RapidTSDB Dumped File");
            }

            //time part
            seeBytes("Time", dis);
            ;
            //value part
            seeBytes("Value", dis);

            // decoded timestamp
            seeLong("DecodedTime", dis);

            //decoded values
            seeDouble("DecodedValue", dis);


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void seeBytes(String byteName, DataInputStream dis) throws IOException {

        if (haveThisPartData(dis)) {
            int byteLength = dis.readInt();
            System.out.println(byteName + " Bytes Size:" + byteLength);
            for (int i = 0; i < byteLength; i++) {
                byte b = dis.readByte();
                System.out.print(b + "[" + Integer.toHexString(b) + "] ");
                if (i > 0 && i % 200 == 0) {
                    System.out.println();
                }
            }

        } else {
            System.out.println("No " + byteName + " Bytes Found.");
        }
        System.out.println();
    }

    private static void seeLong(String longName, DataInputStream dis) throws IOException {

        if (haveThisPartData(dis)) {
            int dataLength = dis.readInt();
            System.out.println(longName + " Size:" + dataLength);
            for (int i = 0; i < dataLength; i++) {
                long b = dis.readLong();
                System.out.print(b + " ");
                if (i > 0 && i % 200 == 0) {
                    System.out.println();
                }
            }

        } else {
            System.out.println("No " + longName + " Bytes Found.");
        }
        System.out.println();
    }


    private static void seeDouble(String doublename, DataInputStream dis) throws IOException {

        if (haveThisPartData(dis)) {
            int dataLength = dis.readInt();
            System.out.println(doublename + " Size:" + dataLength);
            for (int i = 0; i < dataLength; i++) {
                double b = dis.readDouble();
                System.out.print(b + " ");
                if (i > 0 && i % 200 == 0) {
                    System.out.println();
                }
            }

        } else {
            System.out.println("No " + doublename + " Bytes Found.");
        }
        System.out.println();
    }

    private static boolean haveThisPartData(DataInputStream dis) throws IOException {
        int partIdentifier = dis.readInt();
        return partIdentifier == Dumper.HAVE_DATA_INDENTIFIER;
    }

}
