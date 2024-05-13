package org.region;

import org.region.ZooKeeperManager;

public class Main {
    public static void main(String[] args) {
        ZooKeeperManager zooKeeperManager = new ZooKeeperManager("10.214.241.121:2181");
        System.out.println("ZooKeeper client created: " + zooKeeperManager.getClient());
        while (true) {
            try {
                Thread.sleep(1000);
                System.out.println("ZooKeeper connection status: " + zooKeeperManager.judgeConnection());
//                System.out.println("ZooKeeper node /test exists: " + zooKeeperManager.judgeNodeExist("/test"));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}