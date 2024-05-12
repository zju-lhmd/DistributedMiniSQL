from .region_cluster import RegionCluster


class BaseAssign:
    def __init__(self, cluster, region_addr, table):
        assert isinstance(cluster, RegionCluster)
        self.cluster = cluster

        self.region = region_addr
        self.table = table

    def apply(self):
        raise NotImplementedError('Subclasses must implement apply method')


class CreateAssign(BaseAssign):
    def __init__(self, cluster, region_addr, table, master=False):
        super().__init__(cluster, region_addr, table)
        self.master = master

    def apply(self):
        region = self.region(self.region)
        region.tables.append(self.table)

        table = self.cluster.table(self.table)
        if table is None:
            table = self.cluster.add_table(self.table)
        if self.master is True:
            table.master = self.region
        else:
            table.slaves.append(self.region)


class DropAssign(BaseAssign):
    def __init__(self, cluster, region_addr, table, master=False):
        super().__init__(cluster, region_addr, table)
        self.master = master

    def apply(self):
        region = self.region(self.region)
        region.tables.remove(self.table)

        table = self.cluster.table(self.table)
        if self.master is True:
            table.master = ''
        else:
            table.slaves.remove(self.region)
        if table.isorphan:
            self.cluster.remove_table(self.table)


class RecoverAssign(BaseAssign):
    def apply(self):
        region = self.region(self.region)
        region.tables.append(self.table)

        table = self.cluster.table(self.table)
        table.slaves.append(self.region)


class UpgradeAssign(BaseAssign):
    def apply(self):
        region = self.cluster.region(self.region)
        region.masters += 1

        table = self.cluster.table(self.table)
        table.upgrade(self.region)
