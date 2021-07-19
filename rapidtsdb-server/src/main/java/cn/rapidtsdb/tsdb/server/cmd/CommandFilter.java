package cn.rapidtsdb.tsdb.server.cmd;

public interface CommandFilter {
    boolean filterCommand(CommandContext context, AbstractCommand cmd);
}
