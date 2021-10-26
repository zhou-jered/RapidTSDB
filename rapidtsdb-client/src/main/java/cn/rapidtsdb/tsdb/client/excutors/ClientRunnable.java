package cn.rapidtsdb.tsdb.client.excutors;


import lombok.Data;

@Data
public class ClientRunnable implements Runnable {

    private String taskName;
    private boolean allowFailed = false;
    private int maxRetryTimes = -1;

    public ClientRunnable(String taskName) {
        this.taskName = taskName;
    }

    public ClientRunnable(String taskName, boolean allowFailed) {
        this.taskName = taskName;
        this.allowFailed = allowFailed;
    }

    public ClientRunnable(String taskName, boolean allowFailed, int maxRetryTimes) {
        this.taskName = taskName;
        this.allowFailed = allowFailed;
        this.maxRetryTimes = maxRetryTimes;
    }

    
    public ClientRunnable() {
    }

    @Override
    public void run() {

    }
}
