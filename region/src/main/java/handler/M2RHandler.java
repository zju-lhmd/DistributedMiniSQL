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
    public void create(String table, String sql, List<String> regionAddr, int aid) throws TException {
        try {
            queue.put(new MasterCreateTask(table, sql, (ArrayList<String>) regionAddr, aid));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void drop(String table, int aid) throws TException {
        try {
            queue.put(new MasterDropTask(table, aid));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void recover(String table, List<String> regionAddr, int aid) throws TException {
        try {
            queue.put(new MasterRecoverTask(table, (ArrayList<String>) regionAddr, aid));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void upgrade(String table, List<String> slaveAddr, int aid) throws TException {
        try {
            queue.put(new MasterUpgradeTask(table, (ArrayList<String>) slaveAddr, aid));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
