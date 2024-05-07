class Region:
    def __init__(self, addr, tables=None):
        self.addr = addr
        self.tables = []
        self.masters = 0
        if tables is not None:
            self.tables = tables

    def load(self):
        return len(self.tables) + self.masters

    def inc(self):
        self.masters += 1

    def dec(self):
        self.masters -= 1

    def __str__(self):
        return f'<Region addr:{self.addr}, tbls:{self.tables}>'

    __repr__ = __str__
