package task;

import utils.Constants;

import java.util.ArrayList;

public class MasterRecoverTask extends MasterTask {
    public ArrayList<String> regionAddr;

    public MasterRecoverTask(String table, ArrayList<String> regionAddr) {
        this.table = table;
        this.regionAddr = new ArrayList<>(regionAddr);
        this.type = Constants.MasterType.RECOVER;
    }

    @Override
    public String toString() {
        return "MasterRecoverTask{" +
                "regionAddr=" + regionAddr +
                ", table='" + table + '\'' +
                ", type=" + type +
                '}';
    }
}
