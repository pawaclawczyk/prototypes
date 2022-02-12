from collections import defaultdict
from dataclasses import dataclass
from typing import Any, List, Generator, KT, VT

Tick = int
Kind = str


class Clock:
    def __init__(self, ticks: int):
        self.ticks = ticks
        self._now = 0

    def start(self) -> Generator[Tick, None, None]:
        for t in range(self.ticks):
            self._now = t
            yield t

    @property
    def now(self) -> Tick:
        return self._now


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


class Context:
    def __init__(self, base: 'Context' = None, **kwargs):
        self._base = base
        self._data = kwargs

    def get(self, key: KT, default: VT = None) -> VT:
        if key in self._data:
            return self._data[key]

        return self._base.get(key, default) if self._base else default

    def __getitem__(self, key: KT):
        return self.get(key)


class Process:
    def run(self, ctx: Context): pass


class Simulation:
    def __init__(self, clock: Clock):
        self.clock = clock
        self.processes: List[Process] = []

    def register_process(self, p: Process):
        self.processes.append(p)

    def run(self):
        for t in self.clock.start():
            for p in self.processes:
                p.run(Context(tick=t))
