class BaseWaiter:
    def __init__(self, wait_id):
        self.id = wait_id


class RegionCreateWaiter(BaseWaiter):
    def __init__(self, wait_id, client_addr, region_addrs, table):
        super().__init__(wait_id)
        self.client = client_addr
        self.regions = region_addrs
        self.table = table


class RegionDropWaiter(BaseWaiter):
    def __init__(self, wait_id, client_addr, table):
        super().__init__(wait_id)
        self.client = client_addr
        self.table = table


class RegionRecoverWaiter(BaseWaiter):
    def __init__(self, wait_id, region_addr, table):
        super().__init__(wait_id)
        self.region = region_addr
        self.table = table


class RegionUpgradeWaiter(BaseWaiter):
    def __init__(self, wait_id, region_addr, table):
        super().__init__(wait_id)
        self.region = region_addr
        self.table = table
