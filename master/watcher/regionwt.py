from .basewt import BaseWatcher


class RsWatcher(BaseWatcher):
    def __init__(self, cluster):
        self._cluster = cluster
        self.ignore = True  # simply ignore the very first time call

    def __call__(self, children):
        if self.ignore is True:
            print('RsWatcher REGISTERED')
            self.ignore = False
            return

        print('RsWatcher CALLED')
        # TODO: adjust our regions as well as tables
        pass
