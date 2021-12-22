package cn.rapidtsdb.tsdb.server;

import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.lifecycle.Runner;
import cn.rapidtsdb.tsdb.server.config.ServerConfig;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;

@Log4j2
public class TSDBServer implements Initializer, Runner, Closer {

    private ServerInfo serverInfo;
    private List<ProtocolServerInstance> instances;

    public TSDBServer(ServerInfo serverInfo) {
        this.serverInfo = serverInfo;
    }

    @Override
    public void close() {
        for (ProtocolServerInstance instance : instances) {
            instance.close();
        }
    }

    @Override
    public void init() {
        if (instances == null) {
            instances = new ArrayList<>();
        }
        for (ServerConfig sc : serverInfo.getConfigs()) {
            if (sc.isEnable()) {
                log.info("Launching {} Server at port:{}", sc.getProtocol(), sc.getPort());
                ProtocolServerInstance psi = new ProtocolServerInstance(sc);
                instances.add(psi);
                psi.init();
            } else {
                log.debug("Skip disabled protocol server:{}", sc.getProtocol());
            }
        }
    }

    @Override
    public void run() {
        for (ProtocolServerInstance psi : instances) {
            psi.run();
        }
    }


}
