package cn.rapidtsdb.tsdb.protocol;

public class OperationCommandCode {
    public static final int NO_COMMAND = 0x0;
    public static final int COMMAND_AUTH = 0x1;


    //write
    public static final int COMMAND_WRITE_SIGNLE = 0x20;
    public static final int COMMAND_WRITE_BATCH = 0x21;

    //read
    public static final int COMMAND_READ_0 = 0x30;


    //admin
    public static final int COMMAND_DEASSIGN_SHARD = 0x50;
    public static final int COMMAND_ASSIGN_SHARD = 0x51;


}
