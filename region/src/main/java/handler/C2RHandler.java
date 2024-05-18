package handler;
import api.c2r;
import org.apache.thrift.TException;
import task.ClientTask;
import utils.Constants;

import java.util.concurrent.BlockingQueue;

public class C2RHandler implements c2r.Iface {
    BlockingQueue<ClientTask> queue;
    public C2RHandler(BlockingQueue<ClientTask> clientQueue) {
        queue = clientQueue;
    }
    @Override
    public void read(String clientAddr, String sql) throws TException {
        try {
            queue.put(new ClientTask(clientAddr, null, sql, Constants.ClientType.READ));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(String clientAddr, String table, String sql) throws TException {
        try {
            queue.put(new ClientTask(clientAddr, table, sql, Constants.ClientType.WRITE));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
