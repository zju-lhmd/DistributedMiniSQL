package task;

import utils.Constants;

import java.util.ArrayList;

public class MasterCreateTask extends MasterTask {
    public String sql;
    public ArrayList<String> regionAddr;
    public MasterCreateTask(String table, String sql, ArrayList<String> regionAddr, int aid) {
        this.sql = sql;
        this.regionAddr = new ArrayList<>(regionAddr);
        this.table = table;
        this.aid = aid;
        this.type = Constants.MasterType.CREATE;
    }

    @Override
    public String toString() {
        return "MasterCreateTask{" +
                "sql='" + sql + '\'' +
                ", regionAddr=" + regionAddr +
                ", aid=" + aid +
                ", table='" + table + '\'' +
                ", masterType=" + type +
                '}';
    }
}
