class BaseTask:
    def __init__(self):
        pass


class ClientQueryTask(BaseTask):
    def __init__(self, client_addr, table):
        super().__init__()
        self.client = client_addr
        self.table = table


class ClientCreateTask(BaseTask):
    def __init__(self, client_addr, table, sql):
        super().__init__()
        self.client = client_addr
        self.table = table
        self.sql = sql


class ClientDropTask(BaseTask):
    def __init__(self, client_addr, table):
        super().__init__()
        self.client = client_addr
        self.table = table


class RegionOnTask(BaseTask):
    def __init__(self, region_addr):
        super().__init__()
        self.region = region_addr


class RegionOffTask(BaseTask):
    def __init__(self, region_addr):
        super().__init__()
        self.region = region_addr
