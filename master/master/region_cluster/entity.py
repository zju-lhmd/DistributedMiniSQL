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
    def isorphan(self):
        return self.master == '' and len(self.slaves) == 0

    @property
    def regions(self):
        return [self.master] + self.slaves

    def upgrade(self, slave):
        assert slave in self.slaves

        self.slaves.remove(slave)
        down = self.downgrade()
        self.master = slave
        return down

    def downgrade(self):
        swapper = ''
        if self.master != '':
            swapper = self.master
            self.slaves.append(self.master)
            self.master = ''
        return swapper

    def __str__(self):
        return f'<Table name:{self.name}, master:{self.master}, slaves:{self.slaves}>'

    __repr__ = __str__
