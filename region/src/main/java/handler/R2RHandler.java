package handler;

import api.r2r;
import org.apache.thrift.TException;
import task.RegionTask;
import utils.Constants;

import java.util.concurrent.BlockingQueue;

public class R2RHandler implements r2r.Iface, Handler {
    public BlockingQueue<RegionTask> queue;
    public R2RHandler(BlockingQueue<RegionTask> regionQueue) {
        this.queue = regionQueue;
    }
    @Override
    public void sync(String sql) throws TException {
        try {
            queue.put(new RegionTask(sql, null, Constants.RegionType.SYNC));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void copy(String name, java.nio.ByteBuffer buff) throws TException {
        try {
            queue.put(new RegionTask(name, buff, Constants.RegionType.COPY));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
