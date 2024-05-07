class Table:
    def __init__(self, name):
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
