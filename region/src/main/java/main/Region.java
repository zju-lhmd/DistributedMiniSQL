package main;

import database.DBConnection;
import task.ClientTask;
import task.MasterTask;
import task.RegionTask;

import java.sql.SQLException;
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
                        System.out.println("[READ] " + task);
                        try {
                            DBConnection.query(task.sql);
                        } catch (SQLException e) {
                            System.out.println(e.getMessage());
//                            throw new RuntimeException(e);
                        }
                        break;
                    case WRITE:
                        System.out.println("[WRITE] " + task);
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
//
//        new Thread(() -> {
//            while (true) {
//                MasterTask task = null;
//                try {
//                    task = masterQueue.take();
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//                switch (task.type) {
//                    case CREATE:
//                        System.out.println("[CREATE] " + task);
//                        break;
//                    case DROP:
//                        System.out.println("[DROP] " + task);
//                        break;
//                    case RECOVER:
//                        System.out.println("[RECOVER] " + task);
//                        break;
//                    case UPGRADE:
//                        System.out.println("[UPGRADE] " + task);
//                        break;
//                }
//            }
//        }).start();

//        new Thread(() -> {
//            while (true) {
//                RegionTask task = null;
//                try {
//                    task = regionQueue.take();
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//                switch (task.type) {
//                    case SYNC:
//
//                        System.out.println("[SYNC] " + task);
//                        break;
//                    case COPY:
//                        System.out.println("[COPY] " + task);
//                        break;
//                }
//            }
//        }).start();

        System.out.println("region server listening on " + port);
        server.start(port);
    }
}
