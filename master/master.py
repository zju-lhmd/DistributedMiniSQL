import signal

from config import ZK_ADDR, ZK_MASTER
from zkclient import ZkClient
from regioncluster import RegionCluster


class Master:
    def __init__(self):
        self._zk = ZkClient(ZK_ADDR)
        self._cluster = RegionCluster(self._zk)

        signal.signal(signal.SIGINT, self.stop)

    def start(self):
        self._zk.start()
        # create Znode '/master'
        # TODO: write snapshot to /master
        ok = self._zk.create(ZK_MASTER, ephemeral=True)
        if ok is False:
            self.stop()     # exit when master already exists

        # read regions
        self._cluster.init()

        print('Master READY')
        # Thrift-based server
        while True:
            pass

    def stop(self, signum=None, frame=None):
        self._zk.stop()

        print('Master EXIT')
        exit(0)
