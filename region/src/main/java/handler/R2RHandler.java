package handler;

import api.r2r;
import org.apache.thrift.TException;
import task.RegionTask;
import utils.Constants;

import java.util.concurrent.BlockingQueue;

public class R2RHandler implements r2r.Iface {
    public BlockingQueue<RegionTask> queue;
    public R2RHandler(BlockingQueue<RegionTask> regionQueue) {
        this.queue = regionQueue;
    }
    @Override
    public void sync(String sql) throws TException {
        try {
            queue.put(new RegionTask(sql, Constants.RegionType.SYNC));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void copy(String table) throws TException {
        try {
            queue.put(new RegionTask(table, Constants.RegionType.COPY));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void syncResp(int state) throws TException {
        if (state == 0) {
            System.out.println("Synchronize Success!");
        }
    }

    @Override
    public void copyResp(int state, String dump) throws TException {
        if (state == 0) {
            System.out.println("Copy Success!");
        }
    }
}
