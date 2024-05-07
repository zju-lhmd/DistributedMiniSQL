class BaseWatcher:
    def __call__(self, *args, **kwargs):
        raise NotImplementedError("Subclasses must implement __call__ method")
