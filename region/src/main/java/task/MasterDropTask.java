package task;

import utils.Constants;

public class MasterDropTask extends MasterTask {
    public MasterDropTask(String table, int aid) {
        this.table = table;
        this.aid = aid;
        this.masterType = Constants.MasterType.DROP;
    }
}
