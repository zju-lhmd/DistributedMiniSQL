package task;

import utils.Constants;

import java.util.ArrayList;

public class MasterUpgradeTask extends MasterTask {
    public ArrayList<String> slaveAddr;

    public MasterUpgradeTask(String table, ArrayList<String> slaveAddr, int aid) {
        this.table = table;
        this.slaveAddr = new ArrayList<>(slaveAddr);
        this.aid = aid;
        this.masterType = Constants.MasterType.UPGRADE;
    }
}
