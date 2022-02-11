from dataclasses import dataclass
from typing import Any, List


@dataclass
class Message:
    tick: int
    kind: int
    data: Any


@dataclass
class Event(Message):
    pass


@dataclass
class Future(Message):
    pass


class Action:
    @staticmethod
    def run(tick: int, messages: List[Future]) -> (List[Event], List[Future]):
        return [], []


class Simulation:
    def __init__(self, num_ticks: int, action: Action):
        self.num_ticks = num_ticks
        self.action = action

    def run(self):
        events: List[Event] = []
        futures: List[Future] = []

        for tick in range(self.num_ticks):
            messages = [f for f in futures if f.tick == tick]
            futures = [f for f in futures if f.tick > tick]

            evs, fts = self.action.run(tick, messages)

            events.extend(evs)
            futures.extend(fts)

        return events
