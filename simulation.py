from dataclasses import dataclass
from typing import Any, List


@dataclass
class Event():
    tick: int
    kind: int
    data: Any


class Action:
    @staticmethod
    def run(tick: int) -> List[Event]:
        return []


class Simulation:
    def __init__(self, num_ticks: int):
        self.num_ticks = num_ticks
        self.events: List[Event] = []

    def run(self) -> List[Event]:
        for tick in range(self.num_ticks):
            pass
        return self.events
