from collections import defaultdict
from dataclasses import dataclass
from typing import Any, List, Iterable

import numpy as np

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
    """Process handles a single request.

    It receives the number of the window and the number of the request within the window."""

    def run(self, window: int, request: int) -> Iterable: pass


class DummyProcess(Process):
    def run(self, window: int, request: int):
        yield True


class Simulation:
    def __init__(self, ticks: Tick, process: Process):
        self.ticks = ticks
        self.process = process

    def run(self):
        for t in range(self.ticks):
            self.process.run(t)


class SimulationV2:
    """Runs traffic simulation based on provided traffic distribution.

    The traffic distribution is represented as 1-D array, each value represents number of requests within a time window.

    On each request the `process` is called. A process is expected to return events as an iterable data structure.
    """

    def __init__(self, dist: np.ndarray, process: Process):
        self.dist = dist
        self.process = process

    def run(self):
        for window, requests in enumerate(self.dist):
            for request in range(requests):
                yield from self.process.run(window, request)
