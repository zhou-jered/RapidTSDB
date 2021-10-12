package cn.rapidtsdb.tsdb.server.cmd;

public interface AbstractCommand {
    CommandType getCommandType();

    int getCommandCode();
}
