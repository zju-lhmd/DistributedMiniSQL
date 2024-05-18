package task;

import utils.Constants;

public class ClientTask {
    public String clientAddr;
    public String table;
    public String sql;
    public Constants.ClientType type;

    public ClientTask(String clientAddr, String table, String sql, Constants.ClientType clientType) {
        this.clientAddr = clientAddr;
        this.table = table;
        this.sql = sql;
        this.type = clientType;
    }

    @Override
    public String toString() {
        return "ClientTask{" +
                "clientAddr='" + clientAddr + '\'' +
                ", table='" + table + '\'' +
                ", sql='" + sql + '\'' +
                ", type=" + type +
                '}';
    }
}
