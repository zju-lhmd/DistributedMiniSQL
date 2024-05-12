import heapq

from .entity import Region, Table
from .assigns import BaseAssign


class RegionCluster:
    def __init__(self, zk, copy_num):
        self.zk = zk
        self.copy_num = copy_num

        self._assign_id = 1
        self._regions = {}
        self._tables = {}
        self._assigns = {}

    @property
    def regions(self):
        return self._regions.keys()

    @property
    def tables(self):
        return self._tables.keys()

    def region(self, reg):
        return self._regions.get(reg)

    def add_region(self, reg):
        region = Region(reg)
        self._regions[reg] = region
        return region

    def remove_region(self, reg):
        self._regions.pop(reg)

    def table(self, tbl):
        return self._tables.get(tbl)

    def add_table(self, tbl):
        table = Table(tbl)
        self._tables[tbl] = table
        return table

    def remove_table(self, tbl):
        self._tables.pop(tbl)

    def make_assign(self, assign):
        assert isinstance(assign, BaseAssign)

        gen_id = self._assign_id
        self._assign_id += 1
        self._assigns[gen_id] = assign
        return gen_id

    def do_assign(self, assign_id):
        self._assigns[assign_id].apply()
        self._assigns.pop(assign_id)

    def find_min_load_among(self, regs):
        return min(regs, key=lambda x: self._regions[x].load)

    def find_min_load_without(self, regs):
        left = list(set(self.regions) - set(regs))
        return self.find_min_load_among(left)

    def find_n_min_load(self, n):
        heap = [(self._regions[reg].load, reg) for reg in self.regions]
        heapq.heapify(heap)
        return [heapq.heappop(heap)[1] for _ in range(n)]

    def init(self):
        # initialize regions
        regs = self.zk.get_children('/regions')
        for reg in regs:
            self.add_region(reg)
            data, stat = self.zk.get(f'/regions/{reg}')
            if data is not None:
                tbls = data.decode('utf-8').split(' ')
                self.region(reg).tables = tbls

            # initialize tables
            for tbl in self.region(reg).tables:
                if tbl not in self._tables:
                    self.add_table(tbl)
                self.table(tbl).slaves.append(reg)

        # recover tables that have too few slaves
        for tbl in self.tables:
            table = self.table(tbl)
            loss = self.copy_num - len(table.slaves)
            if loss <= 0:
                continue
            # TODO: loss may be larger than 1, need revise
            reg = self.find_min_load_without(table.slaves)

            # TODO: call thrift to recover tables

            table.slaves.append(reg)
            self.region(reg).tables.append(table.name)

        # upgrade a slave on each table to satisfy Master-Slave
        for tbl in self.tables:
            table = self.table(tbl)
            # select a slave to be upgraded to master
            slave = self.find_min_load_among(table.slaves)

            # TODO: call thrift to upgrade slave

            table.upgrade(slave)
            self._regions[slave].masters += 1

        self.print()

    def print(self):
        print(self._regions)
        print(self._tables)
