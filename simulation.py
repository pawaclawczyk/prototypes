from collections import defaultdict
from dataclasses import dataclass
from typing import Any, List

Tick = int
Kind = str


@dataclass
class Event:
    tick: Tick
    kind: Kind
    data: Any


class EventCollector:
    def __init__(self):
        self._queues = defaultdict(list)

    def publish(self, ev: Event):
        self._queues[ev.kind].append(ev)

    def fetch(self, kind: int) -> List[Event]:
        return self._queues[kind]


class Action:
    @staticmethod
    def run(ec: EventCollector, tick: int): pass


class Simulation:
    def __init__(self, ticks: int, action: Action):
        self.ticks = ticks
        self.action = action

    def run(self, ec: EventCollector):
        for tick in range(self.ticks):
            self.action.run(ec, tick)
