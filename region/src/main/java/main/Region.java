package main;

import api.r2r;
import database.DBConnection;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
//import org.apache.thrift.transport.TT
import task.*;
import utils.Constants;
import utils.JdbcUtil;
import utils.MetaTable;
import zookeeper.ZooKeeperManager;
import utils.TableDump;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.net.InetAddress;

public class Region
{
    public static void regionCall(String address, String exec, Constants.RegionType type) throws TException {
        TTransport transport = null;
        transport = new TSocket(address.split(":")[0], Integer.parseInt(address.split(":")[1]), 30000);
        TProtocol protocol = new TBinaryProtocol(transport);
        TMultiplexedProtocol server_protocol = new TMultiplexedProtocol(protocol, "R");
        r2r.Client client = new r2r.Client(server_protocol);
        transport.open();
        switch (type) {
            case SYNC:
                client.sync(exec);
                break;
            case COPY:
                client.copy(exec);
                break;
        }
        transport.close();
    }
    public static void main( String[] args ) throws UnknownHostException {
        String ip = InetAddress.getLocalHost().getHostAddress();
        int port = 8080;
        int capacity = 200;
        ArrayBlockingQueue<ClientTask> clientQueue = new ArrayBlockingQueue<>(capacity);
        ArrayBlockingQueue<MasterTask> masterQueue = new ArrayBlockingQueue<>(capacity);
        ArrayBlockingQueue<RegionTask> regionQueue = new ArrayBlockingQueue<>(capacity);

        ZooKeeperManager zooKeeperManager = new ZooKeeperManager("10.214.241.121:2181");
        String region_path = "/regions/" + ip + ":" + port;
        String region_name = ip + ":" + port;
        if (zooKeeperManager.judgeNodeExist("/master")) {
            System.out.println("master exists");
            if (zooKeeperManager.judgeNodeExist(region_path)) {
                zooKeeperManager.deleteNode(region_name);
            }
        } else {
            System.out.println("master not exists");
        }
        zooKeeperManager.createNode(region_name);
        RegionServer server = new RegionServer(clientQueue, masterQueue, regionQueue);

        new Thread(() -> {
            while (true) {
                ClientTask task = null;
                try {
                    task = clientQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                switch (task.type) {
                    case READ:
                        System.out.println("[CLIENT READ] " + task);
                        try {
                            DBConnection.query(task.sql);
                        } catch (SQLException e) {
                            System.out.println(e.getMessage());
//                            throw new RuntimeException(e);
                        }
                        break;
                    case WRITE:
                        System.out.println("[CLIENT WRITE] " + task);
                        try {
                            DBConnection.update(task.sql);
                        } catch (SQLException e) {
                            System.out.println(e.getMessage());
//                            throw new RuntimeException(e);
                        }
                        break;
                }
            }
        }).start();

        new Thread(() -> {
            HashMap<String, MetaTable> tableHashMap = new HashMap<>();

            while (true) {
                MasterTask task = null;
                try {
                    task = masterQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                switch (task.type) {
                    case CREATE:
                        System.out.println("[Master CREATE] " + task);
                        MasterCreateTask masterTask = (MasterCreateTask) task;
                        try {
                            DBConnection.update(masterTask.sql);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        tableHashMap.put(task.table, new MetaTable(true, masterTask.regionAddr));
                        for (String address: masterTask.regionAddr) {
                            try {
                                regionCall(address, masterTask.sql, Constants.RegionType.SYNC);
                            } catch (TException e) {
                                System.out.println("[Master Error] table create error!");
                                throw new RuntimeException(e);
                            }
                        }
                        break;
                    case DROP:
                        System.out.println("[Master DROP] " + task);
                        MasterDropTask dropTask = (MasterDropTask) task;
                        String sql = "drop table " + dropTask.table;
                        try {
                            DBConnection.update(sql);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        for (String address : tableHashMap.get(dropTask.table).slaveAddress) {
                            try {
                                regionCall(address, sql, Constants.RegionType.SYNC);
                            } catch (TException e) {
                                System.out.println("[Master Error] table drop error!");
                                throw new RuntimeException(e);
                            }
                        }
                        break;
                    case RECOVER:
                        System.out.println("[Master RECOVER] " + task);
                        MasterRecoverTask recoverTask = (MasterRecoverTask) task;
                        String address = recoverTask.regionAddr.get(0);
                        try {
                            regionCall(address, recoverTask.table, Constants.RegionType.COPY);
                        } catch (TException e) {
                            System.out.println("[Master Error] table recover error!");
                            throw new RuntimeException(e);
                        }
                        break;
                    case UPGRADE:
                        System.out.println("[Master UPGRADE] " + task);
                        MasterUpgradeTask upgradeTask = (MasterUpgradeTask) task;
                        tableHashMap.replace(upgradeTask.table, new MetaTable(true, upgradeTask.slaveAddr));
                        break;
                }
            }
        }).start();

        new Thread(() -> {
            while (true) {
                RegionTask task = null;
                try {
                    task = regionQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                switch (task.type) {
                    case SYNC:
                        try {
                            DBConnection.update(task.exec);
                        } catch (SQLException e) {
                            System.out.println(e.getMessage());
                            throw new RuntimeException(e);
                        }
                        System.out.println("[SYNC] " + task);
                        break;
                    case COPY:
                        System.out.println("[COPY] " + task);
                        try {
                            TableDump.dbBackUpMysql(JdbcUtil.getUser(), JdbcUtil.getPassword(), JdbcUtil.getUrl(), "./sql/", task.exec);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                        break;
                }
            }
        }).start();

        System.out.println("region server listening on " + port);
        server.start(port);
    }
}
