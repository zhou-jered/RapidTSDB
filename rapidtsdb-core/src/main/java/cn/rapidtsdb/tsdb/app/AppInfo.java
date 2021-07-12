package cn.rapidtsdb.tsdb.app;

import com.google.common.collect.Lists;

import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class AppInfo {

    public static final int MAGIC_NUMBER = 0xbabcaffe;
    private static final String version = "0.1";
    private static final long launchTime = System.currentTimeMillis();
    private static String pid;

    public static final String TEMPFILE = System.getProperty("java.io.tmpdir");

    private static List<CmdArgs> allArgs = Lists.newArrayList();

    public static String getVersion() {
        return version;
    }

    public static String getLaunchTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(launchTime));
    }

    public String getPid() {

        return pid;
    }

    public static List<CmdArgs> getAllArgs() {
        return allArgs;
    }


    static {
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        pid = jvmName.split("@")[0];
    }

    public static ApplicationState getApplicationState() {
        return applicationState;
    }

    public static void setApplicationState(ApplicationState state) {
        applicationState = state;
    }

    private static void initCmdArgs() {
        allArgs.add(new CmdArgs("c", "conf", String.class)); // configuration file
        allArgs.add(new CmdArgs("d", "dataDir", String.class)); // date dir
        allArgs.add(new CmdArgs("e", "cacheDir", String.class)); //cache dir
    }

    private static ApplicationState applicationState = ApplicationState.INIT;

    public enum ApplicationState {
        INIT,
        RUNNING,
        TESTING,
        SHUTDOWN
    }

}
