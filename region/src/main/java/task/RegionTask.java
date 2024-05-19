package task;

import utils.Constants;

public class RegionTask extends Task {
    public String exec;
    public Constants.RegionType type;
    public java.nio.ByteBuffer buff;
    public RegionTask(String exec, java.nio.ByteBuffer buff, Constants.RegionType type) {
        this.exec = exec;
        this.buff = buff;
        this.type = type;
    }

    @Override
    public String toString() {
        return "RegionTask{" +
                "exec='" + exec + '\'' +
                ", type=" + type +
                ", buff=" + buff +
                '}';
    }
}
