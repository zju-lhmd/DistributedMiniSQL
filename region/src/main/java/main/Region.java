package main;

import api.Hits;
import api.r2c;
import api.r2r;
import database.DBConnection;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import task.*;
import utils.Constants;
import utils.JdbcUtil;
import utils.MetaTable;
import zookeeper.ZooKeeperManager;
import utils.TableDump;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.net.InetAddress;

public class Region
{
    public static void regionCall(String address, String exec, Constants.RegionType type) throws TException {
        TTransport transport = new TSocket(address.split(":")[0], Integer.parseInt(address.split(":")[1]), 30000);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        TMultiplexedProtocol server_protocol = new TMultiplexedProtocol(protocol, "R");
        r2r.Client client = new r2r.Client(server_protocol);
        transport.open();
        switch (type) {
            case SYNC:
                client.sync(exec);
                break;
            case COPY:
                TableDump.dbBackUpMysql(JdbcUtil.getUser(), JdbcUtil.getPassword(), JdbcUtil.getUrl(), "./sql/", exec);
                client.copy(exec, ByteBuffer.wrap(TableDump.fileToBinary("./sql/" + exec + ".sql")));
                break;
        }
        transport.close();
    }
    public static void clientCall(String address, Hits hits, Constants.ClientType type) throws TException {
        TTransport transport = new TSocket(address.split(":")[0], Integer.parseInt(address.split(":")[1]), 30000);
        TProtocol protocol = new TBinaryProtocol(transport);
        TMultiplexedProtocol server_protocol = new TMultiplexedProtocol(protocol, "R");
        r2c.Client client = new r2c.Client(server_protocol);
        transport.open();
        switch (type) {
            case READ:
                client.readResp(0, hits);
                break;
            case WRITE:
                client.writeResp(0);
                break;
        }
        transport.close();
    }
    public static void main( String[] args ) throws UnknownHostException {
        String ip = InetAddress.getLocalHost().getHostAddress();
        int port = 8080;
        int capacity = 200;
        HashMap<String, MetaTable> tableHashMap = new HashMap<>();
        ArrayBlockingQueue<ClientTask> clientQueue = new ArrayBlockingQueue<>(capacity);
        ArrayBlockingQueue<MasterTask> masterQueue = new ArrayBlockingQueue<>(capacity);
        ArrayBlockingQueue<RegionTask> regionQueue = new ArrayBlockingQueue<>(capacity);

        ZooKeeperManager zooKeeperManager = new ZooKeeperManager("10.214.241.121:2181");
        String region_name = ip + ":" + port;
        zooKeeperManager.init(region_name);

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
                            Hits hits = DBConnection.query(task.sql);
                            clientCall(task.clientAddr, hits, Constants.ClientType.READ);
                        } catch (SQLException | TException e) {
                            e.printStackTrace();
//                            throw new RuntimeException(e);
                        }
                        break;
                    case WRITE:
                        System.out.println("[CLIENT WRITE] " + task);
                        try {
                            DBConnection.update(task.sql);
                            clientCall(task.clientAddr, null, Constants.ClientType.WRITE);
                            for (String address: tableHashMap.get(task.table).slaveAddress) {
                                regionCall(address, task.table + "@@@" + task.sql, Constants.RegionType.SYNC);
                            }
                        } catch (SQLException | TException e) {
                            e.printStackTrace();
//                            throw new RuntimeException(e);
                        }
                        break;
                }
            }
        }).start();

        new Thread(() -> {
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
                        MasterCreateTask createTask = (MasterCreateTask) task;
                        try {
                            DBConnection.update(createTask.sql);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        tableHashMap.put(createTask.table, new MetaTable(true, createTask.regionAddr));
                        for (String address: createTask.regionAddr) {
                            try {
                                regionCall(address, createTask.table + "@@@" + createTask.sql, Constants.RegionType.SYNC);
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
                                regionCall(address, dropTask.table + "@@@" +sql, Constants.RegionType.SYNC);
                            } catch (TException e) {
                                System.out.println("[Master Error] table drop error!");
                                throw new RuntimeException(e);
                            }
                        }
                        break;
                    case RECOVER:
                        System.out.println("[Master RECOVER] " + task);
                        MasterRecoverTask recoverTask = (MasterRecoverTask) task;
                        String address = recoverTask.regionAddr;
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
                        System.out.println("[SYNC] " + task);
                        String table = task.exec.split("@@@")[0];
                        String sql = task.exec.split("@@@")[1];
                        if (sql.toLowerCase().contains("create")) {
                            tableHashMap.put(table, new MetaTable(false, null));
                        }
                        if (sql.toLowerCase().contains("drop")) {
                            tableHashMap.remove(table);
                        }
                        try {
                            DBConnection.update(sql);
                        } catch (SQLException e) {
                            System.out.println(e.getMessage());
                            throw new RuntimeException(e);
                        }
                        break;
                    case COPY:
                        System.out.println("[COPY] " + task);
                        tableHashMap.put(task.exec, new MetaTable(false, null));
                        TableDump.binaryToFile("./sql/" + task.exec + ".sql", task.buff.array());
                        TableDump.dbRestoreMysql(JdbcUtil.getUser(), JdbcUtil.getPassword(), JdbcUtil.getUrl(), "./sql/", task.exec);
                        break;
                }
            }
        }).start();

        System.out.println("region server listening on " + port);
        server.start(port);
    }
}
