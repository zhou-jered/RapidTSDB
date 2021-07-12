package cn.rapidtsdb.tsdb;

import cn.rapidtsdb.tsdb.core.TSDB;

public class TSDBBridge {
    private static TSDB database;

    public synchronized static final void regist(TSDB db) {
        if (database == null) {
            database = db;
        } else if (database != db) {
            throw new RuntimeException("More than One Database Instance");
        }
    }

    public static final TSDB getDatabase() {
        return database;
    }
}
