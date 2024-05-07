from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.client import ChildrenWatch
from kazoo.client import DataWatch


class ZkClient:
    def __init__(self, hosts):
        self._zk = KazooClient(hosts=hosts)

        @self._zk.add_listener
        def state_check(state):
            if state == KazooState.LOST:
                print('ZkClient LOST')
            elif state == KazooState.CONNECTED:
                print('ZkClient CONNECTED')
            else:
                print('ZkClient SUSPENDED')

    def start(self):
        self._zk.start()

    def stop(self):
        self._zk.stop()

    def create(self, path, data=b'', ephemeral=False, sequence=False):
        return self._zk.create(path, value=data, ephemeral=ephemeral, sequence=sequence)

    def delete(self, path, recursive=False):
        self._zk.delete(path, recursive=recursive)

    def get(self, path):
        return self._zk.get(path)

    def get_children(self, path, include_data=False):
        return self._zk.get_children(path, include_data=include_data)

    def add_data_watch(self, path, callback):
        DataWatch(self._zk, path, func=callback)

    def add_children_watch(self, path, callback):
        ChildrenWatch(self._zk, path, func=callback)
