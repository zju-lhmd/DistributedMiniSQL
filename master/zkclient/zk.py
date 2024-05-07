from kazoo.client import KazooClient


class ZkClient:
    # singleton
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ZkClient, cls).__new__(cls)
        return cls._instance

    def __init__(self, hosts):
        self.zk = KazooClient(hosts=hosts)

    @staticmethod
    def get():
        assert(ZkClient._instance is not None)
        return ZkClient._instance.zk
