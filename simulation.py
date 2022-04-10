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


class Process:
    def run(self, t: Tick): pass


class Simulation:
    def __init__(self, ticks: Tick, process: Process):
        self.ticks = ticks
        self.process = process

    def run(self):
        for t in range(self.ticks):
            self.process.run(t)
