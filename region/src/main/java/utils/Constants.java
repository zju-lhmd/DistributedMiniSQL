package utils;

public class Constants {
    /**
     * CREATE: create table
     * DROP: drop table
     * RECOVER: recover data from somewhere
     * UPGRADE: become master from slave
     * */
    public enum MasterType {
        CREATE, DROP, RECOVER, UPGRADE;
    }
    public enum ClientType {
        READ, WRITE;
    }
    public enum RegionType {
        SYNC, COPY;
    }
    public enum ConnectType {
        CLIENT, MASTER, REGION;
    }

}
