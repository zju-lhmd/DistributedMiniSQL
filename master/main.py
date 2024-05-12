from config import config
from master import Master

if __name__ == '__main__':
    master = Master(config)
    master.start()
