import signal

from config import *
from zkclient import ZkClient
from rscluster import RsCluster


class Master:
    def __init__(self):
        self._zk = ZkClient(ZK_ADDR)
        self._cluster = RsCluster(self._zk)

        signal.signal(signal.SIGINT, self.stop)

    def start(self):
        self._zk.start()
        # create Znode '/master'
        # TODO: write snapshot
        ok = self._zk.create('/master', ephemeral=True)
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
