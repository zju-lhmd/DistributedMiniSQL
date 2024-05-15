package task;

import utils.Constants;

import java.util.ArrayList;

public class MasterRecoverTask extends MasterTask {
    public ArrayList<String> regionAddr;

    public MasterRecoverTask(String table, ArrayList<String> regionAddr, int aid) {
        this.table = table;
        this.regionAddr = new ArrayList<>(regionAddr);
        this.aid = aid;
        this.type = Constants.MasterType.RECOVER;
    }

    @Override
    public String toString() {
        return "MasterRecoverTask{" +
                "regionAddr=" + regionAddr +
                ", aid=" + aid +
                ", table='" + table + '\'' +
                ", masterType=" + type +
                '}';
    }
}
