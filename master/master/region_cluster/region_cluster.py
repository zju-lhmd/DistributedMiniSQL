import heapq

from .entity import Region, Table


class RegionCluster:
    def __init__(self, copy_num):
        self.copy_num = copy_num

        self._regions = {}
        self._tables = {}

    @property
    def regions(self):
        return list(self._regions.keys())

    @property
    def tables(self):
        return list(self._tables.keys())

    def region(self, reg):
        return self._regions.get(reg)

    def add_region(self, reg):
        region = Region(reg)
        self._regions[reg] = region
        return region

    def remove_region(self, reg):
        region = self.region(reg)
        self._regions.pop(reg)
        return region

    def table(self, tbl):
        return self._tables.get(tbl)

    def add_table(self, tbl):
        table = Table(tbl)
        self._tables[tbl] = table
        return table

    def remove_table(self, tbl):
        table = self.table(tbl)
        self._tables.pop(tbl)
        return table

    def find_min_load_among(self, regs):
        return min(regs, key=lambda x: self._regions[x].load)

    def find_min_load_without(self, regs):
        left = list(set(self.regions) - set(regs))
        return self.find_min_load_among(left)

    def find_n_min_load(self):
        heap = [(self.region(reg).load, reg) for reg in self.regions]
        heapq.heapify(heap)
        return [heapq.heappop(heap)[1] for _ in range(self.copy_num)]

    def print(self):
        print(self._regions)
        print(self._tables)
