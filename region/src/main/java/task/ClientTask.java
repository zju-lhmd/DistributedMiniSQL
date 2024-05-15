package task;

import utils.Constants;

public class ClientTask {
    public String clientAddr;
    public String sql;
    public Constants.ClientType type;

    public ClientTask(String clientAddr, String sql, Constants.ClientType clientType) {
        this.clientAddr = clientAddr;
        this.sql = sql;
        this.type = clientType;
    }

    @Override
    public String toString() {
        return "ClientTask{" +
                "clientAddr='" + clientAddr + '\'' +
                ", sql='" + sql + '\'' +
                ", clientType=" + type +
                '}';
    }
}
