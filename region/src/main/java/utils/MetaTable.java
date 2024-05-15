package utils;

import java.util.List;

public class MetaTable {
    public boolean isMaster;
    public List<String> slaveAddress;

    public MetaTable(boolean isMaster, List<String> slaveAddress) {
        this.isMaster = isMaster;
        this.slaveAddress = slaveAddress;
    }
}