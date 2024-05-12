from enum import Enum, unique


@unique
class MasterState(Enum):
    Idle = 0
    Active = 1
