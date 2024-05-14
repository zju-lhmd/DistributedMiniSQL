from .task_queue.task import RegionOnTask
from .task_queue.task import RegionOffTask


class BaseWatcher:
    def __call__(self, *args, **kwargs):
        raise NotImplementedError('Subclasses must implement __call__ method')


class RegionsWatcher(BaseWatcher):
    def __init__(self, queue, regions):
        self._queue = queue
        self._regions = regions
        self._ignore = True  # simply ignore the very first time call

    def __call__(self, children):
        # print some useful information
        if self._ignore is True:
            print('[Watcher] Region watcher register')
            self._ignore = False
            return

        # assume only one region fails
        removed_regions = list(set(self._regions) - set(children))
        added_regions = list(set(children) - set(self._regions))

        for reg in added_regions:
            task = RegionOnTask(reg)
            self._queue.put_ahead(task)

        for reg in removed_regions:
            task = RegionOffTask(reg)
            self._queue.put_ahead(task)

        self._regions = children
