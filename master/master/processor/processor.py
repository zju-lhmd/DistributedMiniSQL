from .util import remote_call
from ..api.m2r import m2r
from ..api.c2m import m2c
from ..task_queue.task import *
from ..waiter_dict.waiter import *


class ProcessorDict:
    def __init__(self, cluster, queue, waiter):
        self._dict = {
            ClientQueryTask.__name__: ClientQueryProcessor(cluster),
            ClientCreateTask.__name__: ClientCreateProcessor(cluster, waiter),
            ClientCreateDoneTask.__name__: ClientCreateDoneProcessor(),
            ClientDropTask.__name__: ClientDropProcessor(cluster, waiter),
            ClientDropDoneTask.__name__: ClientDropDoneProcessor(),
            RegionCreateDoneTask.__name__: RegionCreateDoneProcessor(cluster, queue),
            RegionDropDoneTask.__name__: RegionDropDoneProcessor(cluster, queue),
            RegionRecoverDoneTask.__name__: RegionRecoverDoneProcessor(cluster),
            RegionUpgradeDoneTask.__name__: RegionUpgradeDoneProcessor(cluster),
            RegionOnTask.__name__: RegionOnProcessor(cluster),
            RegionOffTask.__name__: RegionOffProcessor(cluster, waiter),
        }

    def process(self, task):
        print('[Master] Process ' + task.__class__.__name__)
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
            return
        remote_call(task.client, m2c.Client, 'queryResp', 0, table.regions)


class ClientCreateProcessor(BaseProcessor):
    def __init__(self, cluster, waiter):
        self._cluster = cluster
        self._waiter = waiter

    def process(self, task):
        table = self._cluster.table(task.table)
        if table is not None:
            remote_call(task.client, m2c.Client, 'createResp', 1, [])
            return

        regs = self._cluster.find_n_min_load()

        wid = self._waiter.gen_id()
        waiter = RegionCreateWaiter(wid, task.client, regs, table)
        self._waiter.add(waiter)

        remote_call(regs[0], m2r.Client, 'create', task.table, task.sql, regs[1:], wid)


class ClientCreateDoneProcessor(BaseProcessor):
    def process(self, task):
        remote_call(task.client, m2c.Client, 'createResp', 0, task.regions)


class ClientDropProcessor(BaseProcessor):
    def __init__(self, cluster, waiter):
        self._cluster = cluster
        self._waiter = waiter

    def process(self, task):
        table = self._cluster.table(task.table)
        if table is None:
            remote_call(task.client, m2c.Client, 'dropResp', 1)
            return

        wid = self._waiter.gen_id()
        waiter = RegionDropWaiter(wid, task.client, table)
        self._waiter.add(waiter)

        remote_call(table.master, m2r.Client, 'drop', task.table, wid)


class ClientDropDoneProcessor(BaseProcessor):
    def process(self, task):
        remote_call(task.client, m2c.Client, 'dropResp', 0)


class RegionOnProcessor(BaseProcessor):
    def __init__(self, cluster):
        self._cluster = cluster

    def process(self, task):
        self._cluster.add_region(task.region)


class RegionOffProcessor(BaseProcessor):
    def __init__(self, cluster, waiter):
        self._cluster = cluster
        self._waiter = waiter

    def process(self, task):
        region = self._cluster.remove_region(task.region)
        for tbl in region.tables:
            table = self._cluster.table(tbl)
            table.remove(task.region)

            new_slave = self._cluster.find_min_load_without(table.valid_regions)

            wid = self._waiter.gen_id()
            waiter = RegionRecoverWaiter(wid, new_slave, tbl)
            self._waiter.add(waiter)

            remote_call(new_slave, m2r.Client, 'recover', tbl, table.valid_regions, wid)

            if table.master == '':
                master = self._cluster.find_min_load_among(table.slaves)

                wid = self._waiter.gen_id()
                waiter = RegionUpgradeWaiter(wid, master, tbl)
                self._waiter.add(waiter)

                new_slaves = [new_slave] + table.slaves
                new_slaves.remove(master)
                remote_call(master, m2r.Client, 'upgrade', tbl, new_slaves, wid)


class RegionCreateDoneProcessor(BaseProcessor):
    def __init__(self, cluster, queue):
        self._cluster = cluster
        self._queue = queue

    def process(self, task):
        master = task.regions[0]
        slaves = task.regions[1:]

        region = self._cluster.region(master)
        region.tables.append(task.table)
        region.masters += 1
        for reg in slaves:
            region = self._cluster.region(reg)
            region.tables.append(task.table)

        table = self._cluster.add_table(task.table)
        table.master = master
        table.slaves = slaves

        new_task = ClientCreateDoneTask(task.client, task.regions)
        self._queue.put(new_task)


class RegionDropDoneProcessor(BaseProcessor):
    def __init__(self, cluster, queue):
        self._cluster = cluster
        self._queue = queue

    def process(self, task):
        table = self._cluster.remove_table(task.table)

        region = self._cluster.region(table.master)
        region.tables.remove(task.table)
        region.masters -= 1
        for reg in table.slaves:
            region = self._cluster.region(reg)
            region.tables.remove(task.table)

        new_task = ClientDropDoneTask(task.client)
        self._queue.put(new_task)


class RegionRecoverDoneProcessor(BaseProcessor):
    def __init__(self, cluster):
        self._cluster = cluster

    def process(self, task):
        region = self._cluster.region(task.region)
        region.tables.append(task.table)

        table = self._cluster.table(task.table)
        table.slaves.append(task.region)


class RegionUpgradeDoneProcessor(BaseProcessor):
    def __init__(self, cluster):
        self._cluster = cluster

    def process(self, task):
        region = self._cluster.region(task.region)
        region.masters += 1

        table = self._cluster.table(task.table)
        table.upgrade(task.reigon)
