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


class ClientCreateDoneTask(BaseTask):
    def __init__(self, client_addr, region_addrs):
        super().__init__()
        self.client = client_addr
        self.regions = region_addrs


class ClientDropTask(BaseTask):
    def __init__(self, client_addr, table):
        super().__init__()
        self.client = client_addr
        self.table = table


class ClientDropDoneTask(BaseTask):
    def __init__(self, client_addr):
        super().__init__()
        self.client = client_addr


class RegionOnTask(BaseTask):
    def __init__(self, region_addr):
        super().__init__()
        self.region = region_addr


class RegionOffTask(BaseTask):
    def __init__(self, region_addr):
        super().__init__()
        self.region = region_addr


class RegionCreateDoneTask(BaseTask):
    def __init__(self, client_addr, region_addrs, table):
        super().__init__()
        self.client = client_addr
        self.regions = region_addrs
        self.table = table


class RegionDropDoneTask(BaseTask):
    def __init__(self, client_addr, table):
        super().__init__()
        self.client = client_addr
        self.table = table


class RegionRecoverDoneTask(BaseTask):
    def __init__(self, region_addr, table):
        super().__init__()
        self.region = region_addr
        self.table = table


class RegionUpgradeDoneTask(BaseTask):
    def __init__(self, region_addr, table):
        super().__init__()
        self.region = region_addr
        self.table = table
