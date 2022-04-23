from typing import Iterable

import numpy as np

Event = dict


class Request:
    __slots__ = ["request", "window", "request_in_window"]

    def __init__(self, request: int, window: int, request_in_window: int):
        self.request = request
        self.window = window
        self.request_in_window = request_in_window


class Process:
    """Process handles a single request.

    It's run with the number of the current window,
     and the number of the request within that window.

    It's notified on the end of a time window with the window's number.
    """

    def run(self, request: Request) -> Iterable: pass

    def notify_window_end(self, window: int) -> None: pass


class Simulation:
    """Runs traffic simulation based on provided traffic distribution.

    The traffic distribution is represented as 1-D array, each value represents number of requests within a time window.

    It calls a process on each request.
    """

    def __init__(self, dist: np.ndarray, process: Process):
        self.dist = dist
        self.process = process

    def run(self):
        request = 0
        for window, requests in enumerate(self.dist):
            for request_in_window in range(requests):
                yield from self.process.run(Request(request, window, request_in_window))
                request += 1
            self.process.notify_window_end(window)
