import threading

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.TMultiplexedProcessor import TMultiplexedProcessor

from .api.c2m import c2m
from .handler import C2MHandler


class MasterServer:
    def __init__(self, port, queue):
        processors = {
            'C': c2m.Processor(C2MHandler(queue)),
        }
        self._thread = ServerThread(port, processors)

    def start(self):
        self._thread.start()


class ServerThread(threading.Thread):
    def __init__(self, port, processors):
        super().__init__(daemon=True)
        self._port = port
        self._processors = processors

    def run(self):
        # register services
        processor_map = TMultiplexedProcessor()

        for name, processor in self._processors.items():
            processor_map.registerProcessor(name, processor)

        transport = TSocket.TServerSocket(port=self._port)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        server = TServer.TThreadPoolServer(processor_map, transport, tfactory, pfactory)
        server.daemon = True

        server.serve()
