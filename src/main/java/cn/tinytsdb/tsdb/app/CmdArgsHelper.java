package cn.tinytsdb.tsdb.app;

public class CmdArgsHelper {

    public static CmdArgs getArgsByShortName(String shortName) {
        return AppInfo.getAllArgs().stream().filter(v->v.getShortName().equals(shortName))
                .findFirst().orElseGet(null);
    }

    public static CmdArgs getArgsByFullName(String shortName) {
        return AppInfo.getAllArgs().stream().filter(v->v.getFullName().equals(shortName))
                .findFirst().orElseGet(null);
    }

}
