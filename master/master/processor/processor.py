from .util import remote_call
from ..api.m2r import m2r
from ..api.c2m import m2c
from ..task_queue.task import *


class ProcessorDict:
    def __init__(self, cluster):
        self._dict = {
            ClientQueryTask.__name__: ClientQueryProcessor(cluster),
            ClientCreateTask.__name__: ClientCreateProcessor(cluster),
            ClientDropTask.__name__: ClientDropProcessor(cluster),
            RegionOnTask.__name__: RegionOnProcessor(cluster),
            RegionOffTask.__name__: RegionOffProcessor(cluster),
        }

    def process(self, task):
        print('[Master] Process ' + task.__class__.__name__)
        print('Args: ' + str(task.__dict__))
        self._dict[task.__class__.__name__].process(task)


class BaseProcessor:
    def process(self, task):
        raise NotImplementedError('Subclasses must implement process method')


class ClientQueryProcessor(BaseProcessor):
    def __init__(self, cluster):
        self._cluster = cluster

    def process(self, task):
        table = self._cluster.table(task.table)
        if table is None:
            remote_call(task.client, m2c.Client, 'queryResp', 1, [])
        else:
            remote_call(task.client, m2c.Client, 'queryResp', 0, table.regions)

        print('Result: ' + str(table))


class ClientCreateProcessor(BaseProcessor):
    def __init__(self, cluster):
        self._cluster = cluster

    def process(self, task):
        table = self._cluster.table(task.table)
        if table is not None:
            remote_call(task.client, m2c.Client, 'createResp', 1, [])
        else:
            master, *slaves = self._cluster.find_n_min_load()
            region = self._cluster.region(master)
            region.tables.append(task.table)
            region.masters += 1
            for slave in slaves:
                region = self._cluster.region(slave)
                region.tables.append(task.table)

            table = self._cluster.add_table(task.table)
            table.master = master
            table.slaves = slaves

            remote_call(master, m2r.Client, 'create', task.table, task.sql, slaves)

            remote_call(task.client, m2c.Client, 'createResp', 0, table.regions)

        print('Result: ' + str(table))


class ClientDropProcessor(BaseProcessor):
    def __init__(self, cluster):
        self._cluster = cluster

    def process(self, task):
        table = self._cluster.table(task.table)
        if table is None:
            remote_call(task.client, m2c.Client, 'dropResp', 1)
        else:
            self._cluster.remove_table(task.table)

            region = self._cluster.region(table.master)
            region.tables.remove(task.table)
            region.masters -= 1
            for slave in table.slaves:
                region = self._cluster.region(slave)
                region.tables.remove(task.table)

            remote_call(table.master, m2r.Client, 'drop', task.table)

            remote_call(task.client, m2c.Client, 'dropResp', 0)

        print('Result: ' + str(table))


class RegionOnProcessor(BaseProcessor):
    def __init__(self, cluster):
        self._cluster = cluster

    def process(self, task):
        region = self._cluster.add_region(task.region)

        print('Result: ' + str(region))


class RegionOffProcessor(BaseProcessor):
    def __init__(self, cluster):
        self._cluster = cluster

    def process(self, task):
        region = self._cluster.remove_region(task.region)
        for tbl in region.tables:
            table = self._cluster.table(tbl)

            if table.master == task.region:
                table.master = ''

                slave = self._cluster.find_min_load_without(table.slaves)
                dumper = self._cluster.find_min_load_among(table.slaves)

                remote_call(dumper, m2r.Client, 'recover', tbl, slave)

                master = self._cluster.find_min_load_among(table.slaves)

                table.slaves.append(slave)
                region = self._cluster.region(slave)
                region.tables.append(tbl)

                table.upgrade(master)
                region = self._cluster.region(master)
                region.masters += 1

                remote_call(master, m2r.Client, 'upgrade', tbl, table.slaves)
            else:
                table.slaves.remove(task.region)

                slave = self._cluster.find_min_load_without(table.regions)
                dumper = self._cluster.find_min_load_among(table.regions)

                remote_call(dumper, m2r.Client, 'recover', tbl, slave)

                table.slaves.append(slave)
                region = self._cluster.region(slave)
                region.tables.append(tbl)

            print('Result: ' + str(table))
