from threading import Semaphore, Lock


# writer-preferring
class TableLock:
    def __init__(self):
        self._rmutex = Lock()
        self._rcount = 0
        self._wmutex = Lock()
        self._wcount = 0
        self._readtry = Lock()
        self._resource = Semaphore(1)

    def acquire_read(self):
        with self._readtry:
            with self._rmutex:
                self._rcount += 1
                if self._rcount == 1:
                    self._resource.acquire()

    def release_read(self):
        with self._rmutex:
            self._rcount -= 1
            if self._rcount == 0:
                self._resource.release()

    def acquire_write(self):
        with self._wmutex:
            self._wcount += 1
            if self._wcount == 1:
                self._readtry.acquire()
        self._resource.acquire()

    def release_write(self):
        self._resource.release()
        with self._rmutex:
            self._wcount -= 1
            if self._wcount == 0:
                self._readtry.release()


class Table(TableLock):
    def __init__(self, name):
        super(Table, self).__init__()
        self.name = name
        self.master = None
        self.slaves = []

    def upgrade(self, slave):
        self.slaves.remove(slave)
        down = self.downgrade()
        self.master = slave
        return down

    def downgrade(self):
        swapper = None
        if self.master is not None:
            swapper = self.master
            self.slaves.append(self.master)
        return swapper

    def __str__(self):
        return f'<Table name:{self.name}, master:{self.master}, slaves:{self.slaves}>'

    __repr__ = __str__
