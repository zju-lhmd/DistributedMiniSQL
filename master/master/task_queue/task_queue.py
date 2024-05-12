from threading import Semaphore, Lock
from collections import deque

from .task import BaseTask


class QueueLock:
    def __init__(self, n=1000):
        self._empty_slots = Semaphore(n)
        self._full_slots = Semaphore(0)
        self._resource = Lock()

    def wait_empty(self):
        self._empty_slots.acquire()

    def wait_full(self):
        self._full_slots.acquire()

    def signal_empty(self):
        self._empty_slots.release()

    def signal_full(self):
        self._full_slots.release()

    def acquire(self):
        self._resource.acquire()

    def release(self):
        self._resource.release()


class TaskQueue(QueueLock):
    def __init__(self):
        super().__init__()
        self._queue = deque()

    def put(self, task):
        assert isinstance(task, BaseTask)

        self.wait_empty()
        self.acquire()
        self._queue.append(task)
        self.release()
        self.signal_full()

    def put_ahead(self, task):
        assert isinstance(task, BaseTask)

        self.wait_empty()
        self.acquire()
        self._queue.appendleft(task)
        self.release()
        self.signal_full()

    def get(self):
        self.wait_full()
        self.acquire()
        task = self._queue.popleft()
        self.release()
        self.signal_empty()
        return task
