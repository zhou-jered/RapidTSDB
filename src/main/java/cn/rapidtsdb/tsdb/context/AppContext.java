package cn.rapidtsdb.tsdb.context;

import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import com.google.common.collect.Maps;

import java.util.Map;

public class AppContext implements Initializer, Closer {

    public static final String DEFAULT_ENV = "__default";

    private static AppContext defaultContext = new AppContext();

    private static Map<String, AppContext> envContext = Maps.newHashMap();

    public static AppContext getDefaultContext() {
        return defaultContext;
    }

    public synchronized static AppContext getContext(String env) {
        if(envContext.containsKey(env)) {
            return envContext.get(env);
        } else {
            AppContext ctx  = new AppContext();
            envContext.putIfAbsent(env, ctx);
            return ctx;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void init() {

    }

    private AppContext() {
    }

}
