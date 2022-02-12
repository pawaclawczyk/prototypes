import math
import random
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict

import numpy as np

from simulation import Process, Event, Context, EventCollector


class AuctionEvents:
    WIN = "win"
    NO_WIN = "no-win"


@dataclass
class Bid:
    campaign_id: int
    value: int


class Pacing:
    budget_daily: int

    def should_bid(self, value: int, ctx: Context) -> bool: pass

    def charge(self, bid: Bid, ctx: Context): pass

    def adjust(self, ctx: Context): pass


class AsapPacing(Pacing):
    def __init__(self, budget_daily):
        self.budget_daily = budget_daily
        self.budget_spent = 0

    def should_bid(self, value: int, ctx: Context) -> bool:
        return (self.budget_daily - self.budget_spent) >= value

    def charge(self, bid: Bid, ctx: Context):
        self.budget_spent += bid.value


class CumulativeEqualPacing(Pacing):
    def __init__(self, budget_daily, periods):
        self.budget_daily = budget_daily
        self.budget_spent = 0
        self.budget_period = math.ceil(budget_daily / periods)
        self.budget_active = 0

    def should_bid(self, value: int, ctx: Context) -> bool:
        return self.budget_spent < self.budget_daily and self.budget_active > value

    def charge(self, bid: Bid, ctx: Context):
        self.budget_spent += bid.value
        self.budget_active -= bid.value

    def adjust(self, ctx: Context):
        self.budget_active += self.budget_period


class RecomputedEqualPacing(Pacing):
    def __init__(self, budget_daily, periods):
        self.budget_daily = budget_daily
        self.budget_spent = 0
        self.budget_active = 0
        self.periods_remaining = periods

    def should_bid(self, value: int, ctx: Context) -> bool:
        return self.budget_spent < self.budget_daily and self.budget_active > value

    def charge(self, bid: Bid, ctx: Context):
        self.budget_spent += bid.value
        self.budget_active -= bid.value

    def adjust(self, ctx: Context):
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

    def should_bid(self, value: int, ctx: Context) -> bool:
        return self.budget_spent < self.budget_daily and self.rng.random() < self.ptr

    def charge(self, bid: Bid, ctx: Context):
        self.budget_spent += bid.value

    def adjust(self, ctx: Context):
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

    def bid(self, ctx: Context) -> Optional[Bid]:
        if self.pacing.should_bid(self.bid_value, ctx):
            return Bid(self.id_, self.bid_value)

    @property
    def budget_daily(self) -> int:
        return self.pacing.budget_daily


class Auction:
    def __init__(self, campaigns: List[Campaign], ec: EventCollector):
        self.campaigns = campaigns
        self.ec = ec

    def run(self, ctx: Context) -> Optional[Bid]:
        bids = sorted(
            list(filter(None, (c.bid(ctx) for c in self.campaigns))),
            key=lambda b: b.value,
            reverse=True
        )

        win = bids[0] if bids else None
        kind = AuctionEvents.WIN if win else AuctionEvents.NO_WIN

        self.ec.publish(Event(ctx["tick"], kind, win))

        return win


class AdServer(Process):
    def __init__(self, traffic_distribution: List[int], auction: Auction, campaigns: List[Campaign]):
        self.traffic_distribution = traffic_distribution
        self.auction = auction
        self.campaigns: Dict[int, Campaign] = {c.id_: c for c in campaigns}

    def run(self, ctx: Context):
        wins: List[Bid] = []

        for c in self.campaigns.values():
            c.pacing.adjust(ctx)

        # run all auctions in period
        for _ in range(self.traffic_distribution[ctx["tick"]]):
            win = self.auction.run(ctx)
            if win:
                wins.append(win)

        # charge for all auctions in period
        for w in wins:
            self.campaigns[w.campaign_id].pacing.charge(w, ctx)
