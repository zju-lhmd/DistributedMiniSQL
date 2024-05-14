class WaiterDict:
    def __init__(self):
        self._wait_id = 1
        self._dict = {}

    def gen_id(self):
        wait_id = self._wait_id
        self._wait_id += 1
        return wait_id

    def add(self, waiter):
        self._dict[waiter.id] = waiter

    def remove(self, wait_id):
        waiter = self._dict[wait_id]
        self._dict.pop(wait_id)
        return waiter
