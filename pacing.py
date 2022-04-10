import random
from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import pandas as pd

from simulation import Process, Event, EventCollector, Tick


class AuctionEvents:
    WIN = "win"
    NO_WIN = "no-win"


@dataclass
class Bid:
    campaign_id: int
    value: int


@dataclass
class CampaignParams:
    ID: int
    bid_val: int
    imps_target: int
    budget_limit: int


class Campaign:
    def __init__(self, params: CampaignParams):
        self.params = params
        self.spent: int = 0
        self.chunk: int = 0
        self.ptr: float = 0.0


class Pacer:
    def server_process(self, t: Tick, c: Campaign, bids: List[Bid]):
        pass

    def bidder_process(self, c: Campaign) -> Optional[Bid]:
        pass


class AsapPacer(Pacer):
    def server_process(self, t: Tick, c: Campaign, bids: List[Bid]):
        val = sum(b.value for b in bids if b.campaign_id == c.params.ID)
        c.spent += val

    def bidder_process(self, c: Campaign):
        if (c.params.budget_limit - c.spent) >= c.params.bid_val:
            return Bid(c.params.ID, c.params.bid_val)


class EqualPacer(Pacer):
    def __init__(self, periods: int):
        self.periods = periods

    def server_process(self, t: Tick, c: Campaign, bids: List[Bid]):
        val = sum(b.value for b in bids if b.campaign_id == c.params.ID)
        c.spent += val
        c.chunk += c.params.budget_limit // self.periods - val

    def bidder_process(self, c: Campaign):
        if c.chunk >= c.params.bid_val:
            return Bid(c.params.ID, c.params.bid_val)


class ThrottledPacer(Pacer):
    ptr: float = 0.1
    ar: float = 0.1
    ff_end: int = 22 * 60

    def __init__(self, forecast: np.ndarray, fast_finish: bool = False):
        if fast_finish:
            forecast = self._apply_fast_finish(forecast, self.ff_end)
        n = forecast.sum()
        f = forecast.cumsum()
        self.forecast = f / n

    def server_process(self, t: Tick, c: Campaign, bids: List[Bid]):
        val = sum(b.value for b in bids if b.campaign_id == c.params.ID)
        c.spent += val
        if t == 0:
            c.ptr = self.ptr
        else:
            a = self.forecast[t] * c.params.budget_limit
            if c.spent > a:
                c.ptr = max(c.ptr * (1 - self.ar), 0)
            elif c.spent < a:
                c.ptr = min(c.ptr * (1 + self.ar), 1)

    def bidder_process(self, c: Campaign):
        if random.random() < c.ptr:
            if (c.params.budget_limit - c.spent) >= c.params.bid_val:
                return Bid(c.params.ID, c.params.bid_val)

    @staticmethod
    def _apply_fast_finish(forecast: np.ndarray, end_at: Tick):
        f = pd.Series(forecast.copy())
        n = forecast[end_at:].sum()
        f[end_at:] = 0
        r = np.random.randint(0, end_at, int(n))
        p = pd.Series(r).groupby(by=r).aggregate(np.size)
        p = p.reindex(f.index.values, fill_value=0)

        return f + p


# class Pacing:
#     budget_daily: int
#
#     def should_bid(self, value: int, ctx: Context) -> bool: pass
#
#     def charge(self, bid: Bid, ctx: Context): pass
#
#     def adjust(self, ctx: Context): pass
#
#
# class AsapPacing(Pacing):
#     def __init__(self, budget_daily):
#         self.budget_daily = budget_daily
#         self.budget_spent = 0
#
#     def should_bid(self, value: int, ctx: Context) -> bool:
#         return (self.budget_daily - self.budget_spent) >= value
#
#     def charge(self, bid: Bid, ctx: Context):
#         self.budget_spent += bid.value
#
#
# class CumulativeEqualPacing(Pacing):
#     def __init__(self, budget_daily, periods):
#         self.budget_daily = budget_daily
#         self.budget_spent = 0
#         self.budget_period = math.ceil(budget_daily / periods)
#         self.budget_active = 0
#
#     def should_bid(self, value: int, ctx: Context) -> bool:
#         return self.budget_spent < self.budget_daily and self.budget_active > value
#
#     def charge(self, bid: Bid, ctx: Context):
#         self.budget_spent += bid.value
#         self.budget_active -= bid.value
#
#     def adjust(self, ctx: Context):
#         self.budget_active += self.budget_period
#
#
# class RecomputedEqualPacing(Pacing):
#     def __init__(self, budget_daily, periods):
#         self.budget_daily = budget_daily
#         self.budget_spent = 0
#         self.budget_active = 0
#         self.periods_remaining = periods
#
#     def should_bid(self, value: int, ctx: Context) -> bool:
#         return self.budget_spent < self.budget_daily and self.budget_active > value
#
#     def charge(self, bid: Bid, ctx: Context):
#         self.budget_spent += bid.value
#         self.budget_active -= bid.value
#
#     def adjust(self, ctx: Context):
#         budget_remaining = self.budget_daily - self.budget_spent
#         self.budget_active = math.ceil(budget_remaining / self.periods_remaining)
#         self.periods_remaining -= 1
#
#
# def redistribute(forecast: np.ndarray, hot_end: int) -> np.ndarray:
#     forecast = forecast.copy()
#
#     s_before = forecast.sum()
#
#     n = forecast[len(forecast) - hot_end - 1:].sum()
#
#     forecast[len(forecast) - hot_end - 1:] = 0
#     for _ in range(n):
#         i = random.randint(len(forecast) - 2 * hot_end - 1, len(forecast) - hot_end - 1)
#         forecast[i] += 1
#
#     s_after = forecast.sum()
#
#     assert s_before == s_after
#
#     return forecast
#
#
# class LinkedInPacing(Pacing):
#     def __init__(self, budget_daily: int, forecast: np.ndarray, hot_end: int = 0, ptr: float = 0.1, ar: float = 0.1):
#         self.budget_daily = budget_daily
#         self.budget_spent = 0
#         fct = redistribute(forecast, hot_end)
#         self.forecast = fct.cumsum()
#         self.forecast_total = fct.sum()
#         self.ptr = ptr
#         self.ar = ar
#         self.rng = random.Random(x=datetime.utcnow().microsecond * budget_daily)
#         self.period = 0
#
#     def should_bid(self, value: int, ctx: Context) -> bool:
#         return self.budget_spent < self.budget_daily and self.rng.random() < self.ptr
#
#     def charge(self, bid: Bid, ctx: Context):
#         self.budget_spent += bid.value
#
#     def adjust(self, ctx: Context):
#         if self.period == 0:
#             self.period += 1
#             return
#
#         a = (self.forecast[self.period] / self.forecast_total) * self.budget_daily
#
#         if self.budget_spent > a:
#             new_ptr = self.ptr * (1.0 - self.ar)
#             self.ptr = max(new_ptr, 0.0)
#         elif self.budget_spent < a:
#             new_ptr = self.ptr * (1.0 + self.ar)
#             self.ptr = min(new_ptr, 1.0)
#
#         self.period += 1
#
#
# class Campaign:
#     def __init__(self, id_: int, bid_value: int, pacing: Pacing):
#         self.id_ = id_
#         self.bid_value = bid_value
#         self.pacing = pacing
#
#     def bid(self, ctx: Context) -> Optional[Bid]:
#         if self.pacing.should_bid(self.bid_value, ctx):
#             return Bid(self.id_, self.bid_value)
#
#     @property
#     def budget_daily(self) -> int:
#         return self.pacing.budget_daily


class Auction:
    def __init__(self, ec: EventCollector, pacer: Pacer, camps: List[Campaign], ):
        self.ec = ec
        self.pacer = pacer
        self.campaigns = camps

    def run(self, t: Tick) -> Optional[Bid]:
        win: Optional[Bid] = None

        for c in self.campaigns:
            b = self.pacer.bidder_process(c)
            if not win or (b and b.value > win.value):
                win = b

        if win:
            self.ec.publish(Event(t, AuctionEvents.WIN, win))
        else:
            self.ec.publish(Event(t, AuctionEvents.NO_WIN, win))

        return win


class AdServer(Process):
    def __init__(self, ec: EventCollector, traffic_dist: np.ndarray, pacer: Pacer, camps_params: List[CampaignParams]):
        self.ec = ec
        self.traffic_dist = traffic_dist
        self.pacer = pacer
        self.camps = [Campaign(cp) for cp in camps_params]

        self.auction = Auction(self.ec, self.pacer, self.camps)

        self.wins: List[Bid] = []

    def run(self, t: Tick):
        for c in self.camps:
            self.pacer.server_process(t, c, self.wins)
        self.wins = []

        for _ in range(self.traffic_dist[t]):
            win = self.auction.run(t)
            if win:
                self.wins.append(win)
