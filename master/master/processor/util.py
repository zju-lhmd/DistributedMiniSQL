from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


def parse_addr(addr):
    host, port = addr.split(':')
    return host, int(port)


def remote_call(addr, cls, meth, *args, **kwargs):
    host, port = parse_addr(addr)
    # make socket
    transport = TSocket.TSocket(host, port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = cls(protocol)

    # connect
    transport.open()

    method = getattr(client, meth)
    method(*args, **kwargs)

    # disconnect
    transport.close()
