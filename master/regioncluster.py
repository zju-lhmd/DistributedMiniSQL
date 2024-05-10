from threading import Semaphore

from config import *
from zkclient import ZkClient
from entity import Region, Table
from watcher import RegionWatcher


class ClusterLock:
    def __init__(self):
        self._sem = Semaphore(1)

    def acquire(self):
        self._sem.acquire()

    def release(self):
        self._sem.release()


class RegionCluster(ClusterLock):
    def __init__(self, zk):
        assert isinstance(zk, ZkClient)

        super(RegionCluster, self).__init__()
        self._zk = zk
        self._regions = {}
        self._tables = {}

    def init(self):
        # read all regions registered on zookeeper
        regs = self._zk.get_children(ZK_RS_DIR)
        for reg in regs:
            data, stat = self._zk.get(f'{ZK_RS_DIR}/{reg}')
            tbls = None
            if data is not None:
                tbls = data.decode('utf-8').split(ZK_SEPERATOR)
            self._regions[reg] = Region(reg, tbls)

            # register tables for searching
            for tbl in self._regions[reg].tables:
                if tbl not in self._tables:
                    self._tables[tbl] = Table(tbl)
                self._tables[tbl].slaves.append(reg)

        # mark tables that have too few slaves
        needs = []

        # upgrade one slave on each table to satisfy Master-Slave structure
        for tbl in self._tables.values():
            # mark
            if len(tbl.slaves) < DDB_COPY_NUM:
                needs.append(tbl.name)

            # select a slave to be upgraded to master
            candidate = tbl.slaves[0]
            min_load = self._regions[candidate].load()
            for slave in tbl.slaves:
                load = self._regions[slave].load()
                if load < min_load:
                    min_load = load
                    candidate = slave

            # TODO: call thrift

            nil = tbl.upgrade(candidate)
            self._regions[candidate].increase_masters(1)

        # recover
        # TODO: recover tables that have too few slaves

        print(self._regions)
        print(self._tables)
        print(needs)

        # add watchers
        self._zk.add_children_watch(ZK_RS_DIR, RegionWatcher(self))

    def search_table(self, tbl):
        if tbl in self._tables:
            return self._tables[tbl]
        return None
