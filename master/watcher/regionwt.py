from .basewt import BaseWatcher


class RegionWatcher(BaseWatcher):
    def __init__(self, cluster):
        self._cluster = cluster
        self._ignore = True  # simply ignore the very first time call

    def __call__(self, children):
        if self._ignore is True:
            print('RsWatcher REGISTERED')
            self._ignore = False
            return

        print('RsWatcher CALLED')
        # TODO: adjust our regions as well as tables
        pass
