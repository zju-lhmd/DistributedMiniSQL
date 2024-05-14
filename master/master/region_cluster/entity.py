class Region:
    def __init__(self, addr):
        self.addr = addr
        self.masters = 0
        self.tables = []

    @property
    def load(self):
        return len(self.tables) + self.masters

    def __str__(self):
        return f'<Region addr:{self.addr}, tbls:{self.tables}>'

    __repr__ = __str__


class Table:
    def __init__(self, name):
        self.name = name
        self.master = ''
        self.slaves = []

    @property
    def regions(self):
        return [self.master] + self.slaves

    @property
    def valid_regions(self):
        return ([self.master] if self.master else []) + self.slaves

    def remove(self, reg):
        if self.master == reg:
            self.master = ''
        else:
            self.slaves.remove(reg)

    def upgrade(self, slave):
        assert slave in self.slaves

        self.slaves.remove(slave)
        self.master = slave

    def __str__(self):
        return f'<Table name:{self.name}, master:{self.master}, slaves:{self.slaves}>'

    __repr__ = __str__
