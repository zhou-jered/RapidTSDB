package cn.rapidtsdb.tsdb.server.cmd;

import cn.rapidtsdb.tsdb.core.TSDB;

public interface AbstractCommand {
    CommandType getCommandType();

    int getCommandCode();

    void execute(TSDB db);
}
