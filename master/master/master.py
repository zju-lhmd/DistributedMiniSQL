import sys
import signal

from .master_state import MasterState
from .zkclient import ZkClient
from .task_queue import TaskQueue
from .master_server import MasterServer
from .region_cluster import RegionCluster
from .watcher import RegionsWatcher


class Master:
    def __init__(self, config):
        self.state = MasterState.Idle
        self.config = config

        self._zk = ZkClient(config['ZK_ADDR'])
        self._queue = TaskQueue()
        self._waits = {}    # TODO: wait for region response
        self._cluster = RegionCluster(self._zk, config['DDB_COPY_NUM'])
        self._server = MasterServer(config['MASTER_SERVER_PORT'], self._queue, self._cluster)

        signal.signal(signal.SIGINT, self.stop)

    def start(self):
        print('ZkClient START')
        self._zk.start()

        # attempt to register master
        ok, path = self._zk.create(self.config['ZK_MASTER'], ephemeral=True)
        if ok is False:
            self.stop()
        self.state = MasterState.Active

        # init metadata
        self._cluster.init()
        self._zk.add_children_watch('/regions', RegionsWatcher(self._queue))

        # start server
        self._server.start()

        print('Master READY')

        # do tasks
        self._serve()

    def _serve(self):
        while True:
            task = self._queue.get()
            # TODO: do task

    def stop(self, signum=None, frame=None):
        print('ZkClient STOP')
        self._zk.stop()

        print('Master EXIT')
        sys.exit(0)
