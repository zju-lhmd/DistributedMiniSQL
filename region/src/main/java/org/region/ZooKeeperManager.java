package org.region;

// Manage zookeeper connection
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZooKeeperManager {
    private CuratorFramework client;
    private String regionPath = "/region";
    public boolean judgeConnection() {
        return client.getZookeeperClient().isConnected();
    }
    public boolean judgeNodeExist(String path) {
        try {
            return client.checkExists().forPath(path) != null;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public ZooKeeperManager(String zkHost) {
        client = CuratorFrameworkFactory.newClient(zkHost, new ExponentialBackoffRetry(1000, 3));
        client.start();

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