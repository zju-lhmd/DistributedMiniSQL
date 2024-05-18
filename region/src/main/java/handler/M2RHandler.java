package handler;

import api.m2r;
import org.apache.thrift.TException;
import task.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class M2RHandler implements m2r.Iface {
    BlockingQueue<MasterTask> queue;
    public M2RHandler(BlockingQueue<MasterTask> masterQueue) {
        this.queue = masterQueue;
    }
    @Override
    public void create(String table, String sql, List<String> regionAddr) throws TException {
        try {
            queue.put(new MasterCreateTask(table, sql, (ArrayList<String>) regionAddr));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void drop(String table) throws TException {
        try {
            queue.put(new MasterDropTask(table));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void recover(String table, String regionAddr) throws TException {
        try {
            queue.put(new MasterRecoverTask(table, regionAddr));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void upgrade(String table, List<String> slaveAddr) throws TException {
        try {
            queue.put(new MasterUpgradeTask(table, (ArrayList<String>) slaveAddr));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
