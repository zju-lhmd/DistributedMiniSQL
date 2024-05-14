package main;

import api.c2r;
import api.m2r;
import api.r2r;
import handler.C2RHandler;
import handler.M2RHandler;
import handler.R2RHandler;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import task.ClientTask;
import task.MasterTask;
import task.RegionTask;

import java.util.concurrent.BlockingQueue;

public class RegionServer {
    TMultiplexedProcessor processor;
    public RegionServer(BlockingQueue<ClientTask> q1, BlockingQueue<MasterTask> q2, BlockingQueue<RegionTask> q3) {
        C2RHandler clientHandler = new C2RHandler(q1);
        TProcessor clientProcessor = new c2r.Processor<>(clientHandler);

        M2RHandler masterHandler = new M2RHandler(q2);
        TProcessor masterProcessor = new m2r.Processor<>(masterHandler);

        R2RHandler regionHandler = new R2RHandler(q3);
        TProcessor regionProcessor = new r2r.Processor<>(regionHandler);

        processor = new TMultiplexedProcessor();
        processor.registerProcessor("client", clientProcessor);
        processor.registerProcessor("master", masterProcessor);
        processor.registerProcessor("region", regionProcessor);
    }

    public void start(int port) {
        try {
            TServerSocket serverTransport = new TServerSocket(port);
            //多线程服务模型
            TThreadPoolServer.Args tArgs = new TThreadPoolServer.Args(serverTransport);
            tArgs.processor(processor);
            //客户端协议要一致
            tArgs.protocolFactory(new TBinaryProtocol.Factory());
            // 线程池服务模型，使用标准的阻塞式IO，预先创建一组线程处理请求。
            TServer server = new TThreadPoolServer(tArgs);
            server.serve();
        } catch (TTransportException e) {
            System.out.println("server start error!");
            throw new RuntimeException(e);
        }
    }
}
