package cn.rapidtsdb.tsdb.server.cmd;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CommandContext {
    private String sourceClient; // sdk, http or console

    /**
     * internal format PROTOCOL/VERSION
     * Example:
     * HTTP/1.1 HTTP/2.0  BIN/1.0 CONSOLE/1.0
     */
    private String protocol; // openTSDB, Binary, HTTP, console,

}
