from .basewt import BaseWatcher


class RegionWatcher(BaseWatcher):
    def __init__(self, cluster):
        self._cluster = cluster
        self._ignore = True  # simply ignore the very first time call

    def __call__(self, children):
        if self._ignore is True:
            print('RegionWatcher REGISTERED')
            self._ignore = False
            return

        print('RegionWatcher CALLED')
        self._cluster.acquire()

        added_children = set(children) - set(self._cluster.region_list())
        removed_children = set(self._cluster.region_list()) - set(children)

        for reg in added_children:
            zk = self._cluster.zk()
            data, stat = zk.get(f'{self._cluster.ZK_RS_DIR}/{reg}')
            if data is not None:
                tbls = data.decode('utf-8').split(self._cluster.ZK_SEPARATOR)

                # drop all tables
                for tbl in tbls:

                    # TODO: call thrift to drop tables

                    pass

            self._cluster.add_region(reg)

        for reg in removed_children:
            region = self._cluster.get_region(reg)
            self._cluster.remove_region(reg)

            # transfer tables to other regions
            for tbl in region.tables:
                table = self._cluster.get_table(tbl)

                table.acquire_write()

                if reg in table.slaves:
                    table.slaves.remove(reg)

                if table.master == reg:
                    slave = self._cluster.find_min_load_among(table.slaves)

                    # TODO: call thrift to upgrade

                    table.upgrade(slave)
                    self._cluster.get_region(slave).increase_masters(1)

                new_reg = self._cluster.find_min_load_without(table.slaves + [table.master])

                # TODO: call thrift to transfer table (new_reg, tbl)

                self._cluster.get_region(new_reg).tables.append(tbl)

                table.release_write()

        self._cluster.release()

        self._cluster.print()
