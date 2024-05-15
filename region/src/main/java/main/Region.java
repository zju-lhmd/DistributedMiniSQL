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
import task.ClientTask;
import task.MasterCreateTask;
import task.MasterTask;
import task.RegionTask;
import utils.MetaTable;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;

public class Region
{
    public static void main( String[] args )
    {
        int port = 8080;
        int capacity = 200;
        ArrayBlockingQueue<ClientTask> clientQueue = new ArrayBlockingQueue<>(capacity);
        ArrayBlockingQueue<MasterTask> masterQueue = new ArrayBlockingQueue<>(capacity);
        ArrayBlockingQueue<RegionTask> regionQueue = new ArrayBlockingQueue<>(capacity);

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

//        new Thread(() -> {
//            HashMap<String, MetaTable> tableHashMap = new HashMap<>();
//
//            while (true) {
//                MasterTask task = null;
//                try {
//                    task = masterQueue.take();
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//                switch (task.type) {
//                    case CREATE:
//                        System.out.println("[Master CREATE] " + task);
//                        tableHashMap.put(task.table, new MetaTable(true, ((MasterCreateTask) task).regionAddr));
//                        for (String address: ((MasterCreateTask) task).regionAddr) {
//                            TTransport transport = null;
//                            try {
//                                transport = new TSocket(address.split(":")[0], Integer.parseInt(address.split(":")[1]), 30000);
//                            } catch (TTransportException e) {
//                                System.out.println("[Master Error] address " + address + " can not connect!");
//                                throw new RuntimeException(e);
//                            }
//                            TProtocol protocol = new TBinaryProtocol(transport);
//                            TMultiplexedProtocol server_protocol = new TMultiplexedProtocol(protocol, "R");
//                            r2r.Client client = new r2r.Client(server_protocol);
//                            try {
//                                transport.open();
//                            } catch (TTransportException e) {
//                                System.out.println("[Master Error] transport open error!");
//                                throw new RuntimeException(e);
//                            }
//                            try {
//                                client.sync(((MasterCreateTask) task).sql);
//                            } catch (TException e) {
//                                System.out.println(e.getMessage());
//                                throw new RuntimeException(e);
//                            }
//                            transport.close();
//                        }
//
//                        break;
//                    case DROP:
//                        System.out.println("[Master DROP] " + task);
//                        break;
//                    case RECOVER:
//                        System.out.println("[Master RECOVER] " + task);
//                        break;
//                    case UPGRADE:
//                        System.out.println("[Master UPGRADE] " + task);
//                        break;
//                }
//            }
//        }).start();

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
                        break;
                }
            }
        }).start();

        System.out.println("region server listening on " + port);
        server.start(port);
    }
}
