from api.m2r import r2m
from task_queue.task import RegionCreateDoneTask
from task_queue.task import RegionDropDoneTask
from task_queue.task import RegionRecoverDoneTask
from task_queue.task import RegionUpgradeDoneTask


class R2MHandler(r2m.Iface):
    def __init__(self, queue, waiter):
        self._queue = queue
        self._waiter = waiter

    def createResp(self, state, aid):
        waiter = self._waiter.remove(aid)
        if state == 0:
            task = RegionCreateDoneTask(waiter.client, waiter.regions, waiter.table)
            self._queue.put_ahead(task)

    def dropResp(self, state, aid):
        waiter = self._waiter.remove(aid)
        if state == 0:
            task = RegionDropDoneTask(waiter.client, waiter.table)
            self._queue.put_ahead(task)

    def recoverResp(self, state, aid):
        waiter = self._waiter.remove(aid)
        if state == 0:
            task = RegionRecoverDoneTask(waiter.addr, waiter.table)
            self._queue.put_ahead(task)

    def upgradeResp(self, state, aid):
        waiter = self._waiter.remove(aid)
        if state == 0:
            task = RegionUpgradeDoneTask(waiter.addr, waiter.table)
            self._queue.put_ahead(task)
