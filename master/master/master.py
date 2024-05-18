import sys
import socket
import signal

from .zkclient import ZkClient
from .task_queue import TaskQueue
from .region_cluster import RegionCluster
from .master_server import MasterServer
from .watcher import RegionsWatcher
from .processor import ProcessorDict
from .processor.util import remote_call
from .api.m2r import m2r


class Master:
    def __init__(self):
        self.zk_addr = '127.0.0.1:2181'
        self.copy_num = 2
        self.server_port = 3932

        self._zk = ZkClient(self.zk_addr)
        self._queue = TaskQueue()
        self._cluster = RegionCluster(self.copy_num)
        self._processors = ProcessorDict(self._cluster)
        self._server = MasterServer(self.server_port, self._queue)

        signal.signal(signal.SIGINT, self.stop)

    def start(self):
        print('[Zkclient] Connect to ' + self.zk_addr)
        self._zk.start()

        # attempt to register master
        server_addr = [
            (s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close())
            for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]
        ][0][1] + ':' + str(self.server_port)
        ok, path = self._zk.create('/master', data=server_addr.encode('utf-8'), ephemeral=True)
        if ok is False:
            self.stop()
        print('[Master] Register "/master" on ZooKeeper')

        # initialize metadata
        self._init_cluster()
        self._zk.add_children_watch('/regions', RegionsWatcher(self._queue, self._cluster.regions))
        print('[Master] Metadata initialize')

        # start server
        print('[Server] Listen on ' + server_addr)
        self._server.start()

        print('[Master] Ready')
        # do tasks
        self._serve()
        
    def _init_cluster(self):
        # initialize regions
        regs = self._zk.get_children('/regions')
        for reg in regs:
            region = self._cluster.add_region(reg)
            data, stat = self._zk.get(f'/regions/{reg}')
            if data is not None:
                tbls = data.decode('utf-8').split(' ')
                region.tables = tbls

            # initialize tables
            for tbl in region.tables:
                if tbl not in self._cluster.tables:
                    self._cluster.add_table(tbl)
                self._cluster.table(tbl).slaves.append(reg)

            # upgrade a slave on each table to satisfy Master-Slave
            for tbl in self._cluster.tables:
                table = self._cluster.table(tbl)

                master = self._cluster.find_min_load_among(table.slaves)
                table.upgrade(master)
                region.masters += 1

                remote_call(master, m2r.Client, 'upgrade', tbl, table.slaves)

        self._cluster.print()

    def _serve(self):
        while True:
            task = self._queue.get()
            self._processors.process(task)

    def stop(self, signum=None, frame=None):
        self._zk.stop()
        print('[Zkclient] Disconnect')

        print('[Master] Exit')
        sys.exit(0)
