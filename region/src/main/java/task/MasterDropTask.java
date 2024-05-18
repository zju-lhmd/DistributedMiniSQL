package task;

import utils.Constants;

public class MasterDropTask extends MasterTask {
    public MasterDropTask(String table) {
        this.table = table;
        this.type = Constants.MasterType.DROP;
    }

    @Override
    public String toString() {
        return "MasterDropTask{" +
                "table='" + table + '\'' +
                ", type=" + type +
                '}';
    }
}
