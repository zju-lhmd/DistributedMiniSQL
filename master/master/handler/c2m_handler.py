from api.c2m import c2m
from task_queue.task import ClientQueryTask, ClientCreateTask, ClientDropTask


class C2MHandler(c2m.Iface):
    def __init__(self, queue):
        self._queue = queue

    def query(self, client_addr, table):
        task = ClientQueryTask(client_addr, table)
        self._queue.put(task)

    def create(self, client_addr, table, sql):
        task = ClientCreateTask(client_addr, table, sql)
        self._queue.put(task)

    def drop(self, client_addr, table):
        task = ClientDropTask(client_addr, table)
        self._queue.put(task)
