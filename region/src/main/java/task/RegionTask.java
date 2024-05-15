package task;

import utils.Constants;

public class RegionTask {
    public String exec;
    public Constants.RegionType type;
    public RegionTask(String exec, Constants.RegionType type) {
        this.exec = exec;
        this.type = type;
    }
}
