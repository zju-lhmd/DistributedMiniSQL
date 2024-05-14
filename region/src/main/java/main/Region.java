package main;

import task.ClientTask;
import task.MasterTask;
import task.RegionTask;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * Hello world!
 *
 */

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

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        System.out.println("2");
                        ClientTask task = clientQueue.take();
                        System.out.println(task.clientAddr + " " + task.sql);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }).start();

        System.out.println("region server listening on " + port);
        server.start(port);

        System.out.println("region ready to handle task");

    }
}
