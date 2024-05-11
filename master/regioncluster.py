import heapq
from threading import Semaphore

from config import ZK_REGION_DIR, ZK_SEPARATOR, DDB_COPY_NUM
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
    ZK_REGION_DIR = ZK_REGION_DIR
    ZK_SEPARATOR = ZK_SEPARATOR
    DDB_COPY_NUM = DDB_COPY_NUM

    def __init__(self, zk):
        assert isinstance(zk, ZkClient)

        super(RegionCluster, self).__init__()
        self._zk = zk
        self._regions = {}
        self._tables = {}

    def init(self):
        # read all regions registered on zookeeper
        regs = self._zk.get_children(RegionCluster.ZK_REGION_DIR)
        for reg in regs:
            data, stat = self._zk.get(f'{RegionCluster.ZK_REGION_DIR}/{reg}')
            tbls = None
            if data is not None:
                tbls = data.decode('utf-8').split(RegionCluster.ZK_SEPARATOR)
            self._regions[reg] = Region(reg, tbls)

            # register tables for searching
            for tbl in self._regions[reg].tables:
                if tbl not in self._tables:
                    self._tables[tbl] = Table(tbl)
                self._tables[tbl].slaves.append(reg)

        # recover tables that have too few slaves
        needs = [table for table in self._tables.values() if len(table.slaves) < RegionCluster.DDB_COPY_NUM]
        for table in needs:
            reg = self.find_min_load_without(table.slaves)

            # TODO: call thrift to recover

            table.slaves.append(reg)
            self._regions[reg].tables.append(table.name)

        # upgrade one slave on each table to satisfy Master-Slave structure
        for table in self._tables.values():
            # select a slave to be upgraded to master
            slave = min(table.slaves, key=lambda x: self._regions[x].load())

            # TODO: call thrift to upgrade

            table.upgrade(slave)
            self._regions[slave].increase_masters(1)

        self.print()

        # add watchers
        self._zk.add_children_watch(RegionCluster.ZK_REGION_DIR, RegionWatcher(self))

    def zk(self):
        return self._zk

    def region_list(self):
        return self._regions.keys()

    def find_min_load(self):
        return self.find_min_load_among(self.region_list())

    def find_min_load_without(self, regs):
        left = list(set(self.region_list()) - set(regs))
        return self.find_min_load_among(left)

    def find_min_load_among(self, regs):
        return min(regs, key=lambda x: self._regions[x].load())

    def find_n_min_load(self):
        heap = [(self._regions[reg].load(), reg) for reg in self.region_list()]
        heapq.heapify(heap)
        return [heapq.heappop(heap)[1] for _ in range(RegionCluster.DDB_COPY_NUM)]

    def get_region(self, reg):
        return self._regions.get(reg)

    def add_region(self, reg):
        self._regions[reg] = Region(reg)

    def remove_region(self, reg):
        self._regions.pop(reg)

    def get_table(self, tbl):
        return self._tables.get(tbl)

    def add_table(self, tbl):
        pass

    def remove_table(self, tbl):
        pass

    def print(self):
        print(self._regions)
        print(self._tables)
