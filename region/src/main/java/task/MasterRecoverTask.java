package task;

import utils.Constants;

import java.util.ArrayList;

public class MasterRecoverTask extends MasterTask {
    public ArrayList<String> regionAddr;

    public MasterRecoverTask(String table, ArrayList<String> regionAddr, int aid) {
        this.table = table;
        this.regionAddr = new ArrayList<>(regionAddr);
        this.aid = aid;
        this.masterType = Constants.MasterType.RECOVER;
    }
}
