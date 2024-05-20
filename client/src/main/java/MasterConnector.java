import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import java.util.concurrent.*;
import java.util.*;

public class MasterConnector implements m2c.Iface {

    private TTransport transport;
    private c2m.Client client;
    private CompletableFuture<List<String>> future;
    private TServer server;

    public MasterConnector(String masterHost, int masterPort, String clientAddr, int clientPort) throws Exception {
        transport = new TSocket(masterHost, masterPort);
        transport.open();
        TMultiplexedProtocol multiplexProtocolClient = new TMultiplexedProtocol(new TBinaryProtocol(transport), "C");
        client = new c2m.Client(multiplexProtocolClient);

        TServerTransport serverTransport = new TServerSocket(clientPort);
        m2c.Processor<MasterConnector> processor = new m2c.Processor<>(this);
        TMultiplexedProcessor multiplexedProcessor = new TMultiplexedProcessor();
        multiplexedProcessor.registerProcessor("M", processor);
        server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(multiplexedProcessor));
        CompletableFuture.runAsync(() -> server.serve());
    }

    public CompletableFuture<List<String>> query(String clientAddr, String table) throws Exception {
        future = new CompletableFuture<>();
        client.query(clientAddr, table);
        return future;
    }

    public CompletableFuture<List<String>> create(String clientAddr, String table, String sql) throws Exception {
        future = new CompletableFuture<>();
        client.create(clientAddr, table, sql);
        return future;
    }

    public CompletableFuture<List<String>> drop(String clientAddr, String table) throws Exception {
        future = new CompletableFuture<>();
        client.drop(clientAddr, table);
        return future;
    }

    @Override
    public void queryResp(int state, List<String> regionAddr) {
        if (future != null) {
            if(state==0)
                future.complete(regionAddr);
            else
                future.complete(null);
        }
    }

    @Override
    public void createResp(int state, List<String> regionAddr) {
        if (future != null) {
            if(state==0)
                future.complete(regionAddr);
            else
                future.complete(null);
        }
    }

    @Override
    public void dropResp(int state) {
        if (future != null) {
            if(state==0)
                future.complete(new ArrayList<>());
            else
                future.complete(null);
        }
    }

    public void closeConnection() {
        transport.close();
        server.stop();
    }
}