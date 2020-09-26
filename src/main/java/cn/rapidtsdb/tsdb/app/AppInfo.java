package cn.rapidtsdb.tsdb.app;

import com.google.common.collect.Lists;

import java.util.List;

public class AppInfo {

    public static final int MAGIC_NUMBER = 0xbabcaffe;

    public static final String TEMPFILE = System.getProperty("java.io.tmpdir");

    private static List<CmdArgs> allArgs = Lists.newArrayList();

    public static String getVersion() {
        return null;
    }

    public static String getLaunchTime() {
        return null;
    }

    public static int getPid() {
        return 1;
    }

    public static List<CmdArgs> getAllArgs() {
        return allArgs;
    }


    static  {

    }

    private static void initCmdArgs() {
        allArgs.add(new CmdArgs("c","conf", String.class)); // configuration file
        allArgs.add(new CmdArgs("d","dataDir",String.class)); // date dir
        allArgs.add(new CmdArgs("e","cacheDir",String.class)); //cache dir
    }

}
