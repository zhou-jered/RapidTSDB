package cn.rapidtsdb.tsdb.utils;

import cn.rapidtsdb.tsdb.app.CmdArgs;
import cn.rapidtsdb.tsdb.app.CmdArgsHelper;

import java.util.HashMap;
import java.util.Map;

//@Slf4j
public class CliParser {

    public static Map<String, String> parseCmdArgs(String[] args) {
        Map<String, String> configs = new HashMap<>();
        String argK="";
        String argV="";
        ArgsStage stage = ArgsStage.k;
        for(String arg : args) {
            if(arg.startsWith("-")) {
                if(stage==ArgsStage.v) {
                    throw new RuntimeException("Error Config Item " + arg);
                }
            } else if(stage == ArgsStage.k) {
                throw new RuntimeException("Error Config Item " + arg);
            }

            if(arg.startsWith("--")) {
                argK = arg.substring(2);
                stage = ArgsStage.v;
            } else if(arg.startsWith("-")) {
                String argShortName = arg.substring(1);
                CmdArgs cmdArgs= CmdArgsHelper.getArgsByShortName(argShortName);
                if(cmdArgs==null) {
                    throw new RuntimeException("Unknown short config item : " + arg);
                }
                argK = cmdArgs.getFullName();
                stage = ArgsStage.v;
            } else {
                argV = arg;
                stage = ArgsStage.k;
                configs.put(argK, argV);
            }
        }
        return configs;
    }

    private static enum  ArgsStage {
        k,v;
    }
}
