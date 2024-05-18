package task;

import utils.Constants;

import java.util.ArrayList;

public class MasterRecoverTask extends MasterTask {
    public String regionAddr;

    public MasterRecoverTask(String table, String regionAddr) {
        this.table = table;
        this.regionAddr = regionAddr;
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
