package task;

import utils.Constants;

import java.util.ArrayList;

public class MasterUpgradeTask extends MasterTask {
    public ArrayList<String> slaveAddr;

    public MasterUpgradeTask(String table, ArrayList<String> slaveAddr) {
        this.table = table;
        this.slaveAddr = new ArrayList<>(slaveAddr);
        this.type = Constants.MasterType.UPGRADE;
    }

    @Override
    public String toString() {
        return "MasterUpgradeTask{" +
                "slaveAddr=" + slaveAddr +
                ", table='" + table + '\'' +
                ", type=" + type +
                '}';
    }
}
