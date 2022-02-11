import math
import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict

import numpy as np

from simulation import Action, Event, Future


@dataclass
class Bid:
    campaign_id: int
    value: int


class Pacing:
    budget_daily: int

    def should_bid(self, value) -> bool: pass

    def charge(self, value): pass

    def adjust(self): pass


class AsapPacing(Pacing):
    def __init__(self, budget_daily):
        self.budget_daily = budget_daily
        self.budget_spent = 0

    def should_bid(self, value) -> bool:
        return (self.budget_daily - self.budget_spent) >= value

    def charge(self, value):
        self.budget_spent += value

    def adjust(self): pass


class CumulativeEqualPacing(Pacing):
    def __init__(self, budget_daily, periods):
        self.budget_daily = budget_daily
        self.budget_spent = 0
        self.budget_period = math.ceil(budget_daily / periods)
        self.budget_active = 0

    def should_bid(self, value) -> bool:
        return self.budget_spent < self.budget_daily and self.budget_active > value

    def charge(self, value):
        self.budget_spent += value
        self.budget_active -= value

    def adjust(self):
        self.budget_active += self.budget_period


class RecomputedEqualPacing(Pacing):
    def __init__(self, budget_daily, periods):
        self.budget_daily = budget_daily
        self.budget_spent = 0
        self.budget_active = 0
        self.periods_remaining = periods

    def should_bid(self, value) -> bool:
        return self.budget_spent < self.budget_daily and self.budget_active > value

    def charge(self, value):
        self.budget_spent += value
        self.budget_active -= value

    def adjust(self):
        budget_remaining = self.budget_daily - self.budget_spent
        self.budget_active = math.ceil(budget_remaining / self.periods_remaining)
        self.periods_remaining -= 1


def redistribute(forecast: np.ndarray, hot_end: int) -> np.ndarray:
    forecast = forecast.copy()

    s_before = forecast.sum()

    n = forecast[len(forecast) - hot_end - 1:].sum()

    forecast[len(forecast) - hot_end - 1:] = 0
    for _ in range(n):
        i = random.randint(len(forecast) - 2 * hot_end - 1, len(forecast) - hot_end - 1)
        forecast[i] += 1

    s_after = forecast.sum()

    assert s_before == s_after

    return forecast


class LinkedInPacing(Pacing):
    def __init__(self, budget_daily: int, forecast: np.ndarray, hot_end: int = 0, ptr: float = 0.1, ar: float = 0.1):
        self.budget_daily = budget_daily
        self.budget_spent = 0
        fct = redistribute(forecast, hot_end)
        self.forecast = fct.cumsum()
        self.forecast_total = fct.sum()
        self.ptr = ptr
        self.ar = ar
        self.rng = random.Random(x=datetime.utcnow().microsecond * budget_daily)
        self.period = 0

    def should_bid(self, value) -> bool:
        return self.budget_spent < self.budget_daily and self.rng.random() < self.ptr

    def charge(self, value):
        self.budget_spent += value

    def adjust(self):
        if self.period == 0:
            self.period += 1
            return

        a = (self.forecast[self.period] / self.forecast_total) * self.budget_daily



        if self.budget_spent > a:
            new_ptr = self.ptr * (1.0 - self.ar)
            self.ptr = max(new_ptr, 0.0)
        elif self.budget_spent < a:
            new_ptr = self.ptr * (1.0 + self.ar)
            self.ptr = min(new_ptr, 1.0)

        self.period += 1


class Campaign:
    def __init__(self, id_: int, bid_value: int, pacing: Pacing):
        self.id_ = id_
        self.bid_value = bid_value
        self.pacing = pacing

    def bid(self) -> Optional[Bid]:
        if self.pacing.should_bid(self.bid_value):
            return Bid(self.id_, self.bid_value)

    def notify_win(self, win: Bid):
        self.pacing.charge(win.value)

    @property
    def budget_daily(self) -> int:
        return self.pacing.budget_daily


class Auction:
    def __init__(self, campaigns: List[Campaign]):
        self.campaigns = campaigns

    def run(self) -> Optional[Bid]:
        bids = sorted(
            list(filter(None, (c.bid() for c in self.campaigns))),
            key=lambda b: b.value,
            reverse=True
        )
        return bids[0] if bids else None


class AdServer(Action):
    EVENT_KIND_WIN = 1
    EVENT_KIND_NO_WIN = 2

    def __init__(self, traffic_distribution: List[int], auction: Auction, campaigns: List[Campaign]):
        self.traffic_distribution = traffic_distribution
        self.auction = auction
        self.campaigns: Dict[int, Campaign] = {c.id_: c for c in campaigns}

    def run(self, tick: int, messages: List[Future]) -> (List[Event], List[Future]):
        events: List[Event] = []
        wins: List[Bid] = []

        # adjust budgets
        for c in self.campaigns.values():
            c.pacing.adjust()

        # run all auctions in period
        for _ in range(self.traffic_distribution[tick]):
            win = self.auction.run()
            if win:
                wins.append(win)
                events.append(Event(tick, self.EVENT_KIND_WIN, win))
            else:
                events.append(Event(tick, self.EVENT_KIND_NO_WIN, None))

        # charge for all auctions in period
        for w in wins:
            self.campaigns[w.campaign_id].notify_win(w)

        return events, []


def budget_spending_over_time(win_events: List[Event]):
    results: Dict[int, Dict[int, int]] = defaultdict(lambda: defaultdict(lambda: 0))

    for e in win_events:
        w: Bid = e.data
        results[w.campaign_id][e.tick] += w.value

    return results
