from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.client import ChildrenWatch
from kazoo.client import DataWatch
from kazoo.exceptions import NodeExistsError


class ZkClient:
    def __init__(self, hosts):
        self._zk = KazooClient(hosts=hosts)

        @self._zk.add_listener
        def state_check(state):
            if state == KazooState.LOST:
                print('ZkClient . -> LOST')
            elif state == KazooState.CONNECTED:
                print('ZkClient . -> CONNECTED')
            elif state == KazooState.SUSPENDED:
                print('ZkClient . -> SUSPENDED')

    def start(self):
        self._zk.start()

    def stop(self):
        self._zk.stop()

    def create(self, path, data=b'', ephemeral=False, sequence=False):
        try:
            p = self._zk.create(path, value=data, ephemeral=ephemeral, sequence=sequence)
            return True, p
        except NodeExistsError:
            return False, None

    def delete(self, path, recursive=False):
        self._zk.delete(path, recursive=recursive)

    def get(self, path, watch=None):
        return self._zk.get(path, watch=watch)

    def get_children(self, path, watch=None):
        return self._zk.get_children(path, watch=watch)

    def add_data_watch(self, path, callback):
        DataWatch(self._zk, path, func=callback)

    def add_children_watch(self, path, callback):
        ChildrenWatch(self._zk, path, func=callback)
