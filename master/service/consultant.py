class ConsultantHandler:
    def __init__(self, cluster):
        self._cluster = cluster

    def query(self, table):
        res_table = self._cluster.get_table(table)

        res_table.acquire_read()
        addrs = [res_table.master] + res_table.slaves
        res_table.release_read()

        return addrs

    def create(self, table, sql):
        self._cluster.acquire()
        regs = self._cluster.find_n_min_load()
        for reg in regs:

            # TODO: call thrift to create table

            region = self._cluster.get_region(reg)
            region.tables.append(table)
            if reg == regs[0]:
                region.increase_masters(1)

        self._cluster.release()

    def drop(self, table):
        res_table = self._cluster.get_table(table)
        res_table.acquire_write()
        self._cluster.remove_table(table)
        regs = [res_table.master] + res_table.slaves
        res_table.acquire_write()

        self._cluster.acquire()

        for reg in regs:

            # TODO: call thrift to drop table

            region = self._cluster.get_region(reg)
            region.tables.remove(table)
            if reg == regs[0]:
                region.increase_masters(-1)

        self._cluster.release()
