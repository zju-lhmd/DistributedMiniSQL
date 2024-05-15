package task;

import utils.Constants;

public class MasterDropTask extends MasterTask {
    public MasterDropTask(String table, int aid) {
        this.table = table;
        this.aid = aid;
        this.type = Constants.MasterType.DROP;
    }

    @Override
    public String toString() {
        return "MasterDropTask{" +
                "aid=" + aid +
                ", table='" + table + '\'' +
                ", masterType=" + type +
                '}';
    }
}
