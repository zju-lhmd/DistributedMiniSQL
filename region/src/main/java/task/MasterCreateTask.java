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
        this.masterType = Constants.MasterType.CREATE;
    }
}
