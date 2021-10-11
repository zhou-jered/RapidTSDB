package cn.rapidtsdb.tsdb.client.event;

import java.util.Map;

public interface TSDBUserEventListener {
    void onEvent(TSDBUserEvent event, Map<String, String> data);
    
}
