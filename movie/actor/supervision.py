from enum import Enum, auto


class SupervisorDirective(Enum):
    RESTART = auto()
    STOP = auto()
    ESCALATE = auto()
