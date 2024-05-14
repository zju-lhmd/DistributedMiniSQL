package main;

import api.c2r;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class client {
    public static final String SERVER_IP = "127.0.0.1";
    public static final int SERVER_PORT = 8080;
    public static final int TIMEOUT = 30000;

    public static void main( String[] args ) throws TException {
        // 设置传输通道
        TTransport transport = new TSocket(SERVER_IP, SERVER_PORT, TIMEOUT);
        // 协议要和服务端一致
        //使用二进制协议
        TProtocol protocol = new TBinaryProtocol(transport);
        TMultiplexedProtocol server_protocol = new TMultiplexedProtocol(protocol, "client");
        //创建Client
        c2r.Client client = new c2r.Client(server_protocol);
        transport.open();
        client.read("127.0.0.1", "select * from students;");
        //关闭资源
        transport.close();
    }
}
