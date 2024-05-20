import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.*;
import java.util.concurrent.*;
import java.util.*;

public class RegionConnector implements r2c.Iface {

    private TTransport transport;
    private c2r.Client client;
    private CompletableFuture<Hits> future;
    private TServer server;

    public RegionConnector(String regionHost, int regionPort, String clientAddr, int clientPort) throws Exception {
        int timeout = 5000;
        transport = new TSocket(regionHost, regionPort);
        ((TSocket) this.transport).setTimeout(timeout);
        transport.open();
        TMultiplexedProtocol multiplexProtocolClient = new TMultiplexedProtocol(new TBinaryProtocol(transport), "C");
        client = new c2r.Client(multiplexProtocolClient);

        TServerTransport serverTransport = new TServerSocket(clientPort);
        r2c.Processor<RegionConnector> processor = new r2c.Processor<>(this);
        TMultiplexedProcessor multiplexedProcessor = new TMultiplexedProcessor();
        multiplexedProcessor.registerProcessor("R", processor);
        server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(multiplexedProcessor));
        CompletableFuture.runAsync(() -> server.serve());
    }

    public CompletableFuture<Hits> read(String clientAddr, String sql) throws Exception {
        future = new CompletableFuture<>();
        client.read(clientAddr, sql);
        return future;
    }

    public CompletableFuture<Hits> write(String clientAddr, String sql, String tableName) throws Exception {
        future = new CompletableFuture<>();
        client.write(clientAddr, tableName, sql);
        return future;
    }

    @Override
    public void readResp(int state, Hits hits) {
        if (future != null) {
            if(state == 0)
                future.complete(hits);
            else if(state == 2)
                future.complete(new Hits("", new ArrayList<>()));
            else {
                hits.records=null;
                future.complete(hits);
            }
        }
    }

    @Override
    public void writeResp(int state, String msg) {
        if (future != null) {
            if (state == 0)
                future.complete(new Hits("", new ArrayList<>()));
            else if (state == 2) {
                future.complete(null);
            } else {
                future.complete(new Hits(msg, null));
            }
        }
    }

    public void closeConnection() {
        transport.close();
        server.stop();
    }
}