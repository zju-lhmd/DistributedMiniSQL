package zookeeper;

// Manage zookeeper connection
import database.DBConnection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

public class ZooKeeperManager {
    private CuratorFramework client;
    private String regionPath = "/regions";
    public boolean judgeConnection() {
        return client.getZookeeperClient().isConnected();
    }
    public boolean judgeNodeExist(String regionName) {
        try {
            return client.checkExists().forPath(regionPath + "/" + regionName) != null;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public ZooKeeperManager(String zkHost) {
        client = CuratorFrameworkFactory.newClient(zkHost, new ExponentialBackoffRetry(1000, 3));
        client.start();
    }

    public void init(String region_name) {
        System.out.println("region_name:" + region_name);
        String tables = DBConnection.showTables();
        if (judgeNodeExist("master")) {
            System.out.println("master exists");
            if (judgeNodeExist(region_name)) {
                deleteNode(region_name);
                System.out.println("delete node" + region_name);
                try {
                    if (tables != null) {
                        for (String table : tables.split(" ")) {
                            DBConnection.update("drop table " + table + ";");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println("master not exists");
        }
        createNode(region_name);
        System.out.println("Table names:" + tables);
        setNodeData(region_name, tables.getBytes());
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void close() {
        client.close();
    }

    public void createNode(String regionName, byte[] data) {
        try {
            client.create().creatingParentsIfNeeded().forPath(regionPath + "/" + regionName, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createNode(String regionName) {
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(regionPath + "/" + regionName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteNode(String regionName) {
        try {
            client.delete().forPath(regionPath + "/" + regionName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public byte[] getNodeData(String regionName) {
        try {
            return client.getData().forPath(regionPath + "/" + regionName);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void setNodeData(String regionName, byte[] data) {
        try {
            client.setData().forPath(regionPath + "/" + regionName, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}