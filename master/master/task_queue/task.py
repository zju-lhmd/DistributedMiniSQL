class BaseTask:
    def __init__(self):
        pass


class ClientTask(BaseTask):
    def __init__(self, client_addr):
        super().__init__()
        self.addr = client_addr


class ClientQueryTask(ClientTask):
    def __init__(self, client_addr, table):
        super().__init__(client_addr)
        self.table = table


class ClientCreateTask(ClientTask):
    def __init__(self, client_addr, table, sql):
        super().__init__(client_addr)
        self.table = table
        self.sql = sql


class ClientDropTask(ClientTask):
    def __init__(self, client_addr, table):
        super().__init__(client_addr)
        self.table = table


class RegionChangeTask(BaseTask):
    def __init__(self, region_addrs):
        super().__init__()
        self.regions = region_addrs
