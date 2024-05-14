package task;

public class ClientTask {
    public String clientAddr;
    public String sql;

    public ClientTask(String clientAddr, String sql) {
        this.clientAddr = clientAddr;
        this.sql = sql;
    }
}
