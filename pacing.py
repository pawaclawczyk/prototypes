import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Iterable

import numpy as np

from simulation import Process, Request, Response


# class Pacer:
#     def server_process(self, t: Tick, c: Campaign, bids: List[Bid]):
#         pass
#
#     def bidder_process(self, c: Campaign) -> Optional[Bid]:
#         pass
#
#
# class AsapPacer(Pacer):
#     def server_process(self, t: Tick, c: Campaign, bids: List[Bid]):
#         val = sum(b.value for b in bids if b.campaign_id == c.params.ID)
#         c.spent += val
#
#     def bidder_process(self, c: Campaign):
#         if (c.params.budget_limit - c.spent) >= c.params.bid_val:
#             return Bid(c.params.ID, c.params.bid_val)
#
#
# class EqualPacer(Pacer):
#     def __init__(self, periods: int):
#         self.periods = periods
#
#     def server_process(self, t: Tick, c: Campaign, bids: List[Bid]):
#         val = sum(b.value for b in bids if b.campaign_id == c.params.ID)
#         c.spent += val
#         c.chunk += c.params.budget_limit // self.periods - val
#
#     def bidder_process(self, c: Campaign):
#         if c.chunk >= c.params.bid_val:
#             return Bid(c.params.ID, c.params.bid_val)
#
#
# class ThrottledPacer(Pacer):
#     ptr: float = 0.1
#     ar: float = 0.1
#     ff_end: int = 22 * 60
#
#     def __init__(self, forecast: np.ndarray, fast_finish: bool = False):
#         if fast_finish:
#             forecast = self._apply_fast_finish(forecast, self.ff_end)
#         n = forecast.sum()
#         f = forecast.cumsum()
#         self.forecast = f / n
#
#     def server_process(self, t: Tick, c: Campaign, bids: List[Bid]):
#         val = sum(b.value for b in bids if b.campaign_id == c.params.ID)
#         c.spent += val
#         if t == 0:
#             c.ptr = self.ptr
#         else:
#             a = self.forecast[t] * c.params.budget_limit
#             if c.spent > a:
#                 c.ptr = max(c.ptr * (1 - self.ar), 0)
#             elif c.spent < a:
#                 c.ptr = min(c.ptr * (1 + self.ar), 1)
#
#     def bidder_process(self, c: Campaign):
#         if random.random() < c.ptr:
#             if (c.params.budget_limit - c.spent) >= c.params.bid_val:
#                 return Bid(c.params.ID, c.params.bid_val)
#
#     @staticmethod
#     def _apply_fast_finish(forecast: np.ndarray, end_at: Tick):
#         f = pd.Series(forecast.copy())
#         n = forecast[end_at:].sum()
#         f[end_at:] = 0
#         r = np.random.randint(0, end_at, int(n))
#         p = pd.Series(r).groupby(by=r).aggregate(np.size)
#         p = p.reindex(f.index.values, fill_value=0)
#
#         return f + p


@dataclass
class CampaignState:
    budget: int = 0
    alloc: int = 0
    ptr: float = 0
    rng: random.Random = None


@dataclass
class Campaign:
    campaign_id: int
    planned_budget: int
    bid_value: int
    state: CampaignState = field(default_factory=CampaignState)


class AdServerResponse(Response):
    __slots__ = Response.__slots__ + ["kind", "campaign_id", "bid_value"]

    def __init__(self, request: Request):
        super().__init__(request)
        self.kind = None
        self.campaign_id = None
        self.bid_value = None

    @staticmethod
    def bid(request: Request, campaign_id: int, bid_value: int) -> 'AdServerResponse':
        e = AdServerResponse(request)
        e.kind = "bid"
        e.campaign_id = campaign_id
        e.bid_value = bid_value
        return e

    @staticmethod
    def no_bid(request: Request, campaign_id: int) -> 'AdServerResponse':
        e = AdServerResponse(request)
        e.kind = "no bid"
        e.campaign_id = campaign_id
        return e

    @staticmethod
    def win(request: Request, bid: 'AdServerResponse') -> 'AdServerResponse':
        e = AdServerResponse(request)
        e.kind = "win"
        e.campaign_id = bid.campaign_id
        e.bid_value = bid.bid_value
        return e

    @staticmethod
    def no_win(request: Request) -> 'AdServerResponse':
        e = AdServerResponse(request)
        e.kind = "no win"
        return e


class Pacing:
    @staticmethod
    def init(campaign: Campaign):
        campaign.state.budget = campaign.planned_budget

    @staticmethod
    def bid(campaign: Campaign, request: Request) -> AdServerResponse:
        if campaign.bid_value <= (campaign.state.budget - campaign.state.alloc):
            campaign.state.alloc += campaign.bid_value
            return AdServerResponse.bid(request, campaign.campaign_id, campaign.bid_value)
        else:
            return AdServerResponse.no_bid(request, campaign.campaign_id)

    @staticmethod
    def notify(window: int, campaign: Campaign, wins: List[AdServerResponse]):
        for win in wins:
            campaign.state.budget -= win.bid_value
        campaign.state.alloc = 0


# class ThrottledPacer(Pacer):
#     ptr: float = 0.1
#     ar: float = 0.1
#     ff_end: int = 22 * 60
#
#     def __init__(self, forecast: np.ndarray, fast_finish: bool = False):
#         if fast_finish:
#             forecast = self._apply_fast_finish(forecast, self.ff_end)
#         n = forecast.sum()
#         f = forecast.cumsum()
#         self.forecast = f / n
#
#     def server_process(self, t: Tick, c: Campaign, bids: List[Bid]):
#         val = sum(b.value for b in bids if b.campaign_id == c.params.ID)
#         c.spent += val
#         if t == 0:
#             c.ptr = self.ptr
#         else:
#             a = self.forecast[t] * c.params.budget_limit
#             if c.spent > a:
#                 c.ptr = max(c.ptr * (1 - self.ar), 0)
#             elif c.spent < a:
#                 c.ptr = min(c.ptr * (1 + self.ar), 1)
#
#     def bidder_process(self, c: Campaign):
#         if random.random() < c.ptr:
#             if (c.params.budget_limit - c.spent) >= c.params.bid_val:
#                 return Bid(c.params.ID, c.params.bid_val)
#
#     @staticmethod
#     def _apply_fast_finish(forecast: np.ndarray, end_at: Tick):
#         f = pd.Series(forecast.copy())
#         n = forecast[end_at:].sum()
#         f[end_at:] = 0
#         r = np.random.randint(0, end_at, int(n))
#         p = pd.Series(r).groupby(by=r).aggregate(np.size)
#         p = p.reindex(f.index.values, fill_value=0)
#
#         return f + p

class ThrottledPacing(Pacing):
    ar = 0.1
    ptr = 0.1

    def __init__(self, forecast: np.ndarray):
        # convert request distribution to cumulative probability distribution
        # in real scenario, every campaign would have its own forecast
        self.forecast = forecast.cumsum() / forecast.sum()

    def init(self, campaign: Campaign):
        campaign.state.budget = campaign.planned_budget
        campaign.state.ptr = self.ptr
        campaign.state.rng = random.Random(random.randbytes(16))

    @staticmethod
    def bid(campaign: Campaign, request: Request) -> AdServerResponse:
        if campaign.state.rng.random() <= campaign.state.ptr:
            if campaign.bid_value <= (campaign.state.budget - campaign.state.alloc):
                campaign.state.alloc += campaign.bid_value
                return AdServerResponse.bid(request, campaign.campaign_id, campaign.bid_value)

        return AdServerResponse.no_bid(request, campaign.campaign_id)

    def notify(self, window: int, campaign: Campaign, wins: List[AdServerResponse]):
        for win in wins:
            campaign.state.budget -= win.bid_value
        campaign.state.alloc = 0
        self.adjust_ptr(window, campaign)

    def adjust_ptr(self, window: int, campaign: Campaign):
        a = self.forecast[window] * campaign.planned_budget
        if campaign.state.budget > a:
            campaign.state.ptr = max(campaign.state.ptr * (1 - self.ar), 0)
        elif campaign.state.budget < a:
            campaign.state.ptr = min(campaign.state.ptr * (1 + self.ar), 1)


class AdServer(Process):
    """The ad server process simulates a single auction.

    It collects bids from all campaigns and selects the winning bid.
    It consolidates the campaigns budgets at the beginning of each time window
     (equivalent for ending of the previous time window).

    """

    def __init__(self, pacing: Pacing, campaigns: List[Campaign]):
        self.pacing = pacing
        self.campaigns = dict((c.campaign_id, c) for c in campaigns)
        self.wins: List[AdServerResponse] = []

        for campaign in self.campaigns.values():
            pacing.init(campaign)

    def run(self, request: Request) -> Iterable[AdServerResponse]:
        bids = self.collect_bids(request)
        win = self.auction(request, bids)
        self.wins.append(win)
        yield from bids
        yield win

    def notify_window_end(self, window: int) -> None:
        self.process_wins(window)

    def collect_bids(self, request: Request) -> List[AdServerResponse]:
        return [self.pacing.bid(c, request) for c in self.campaigns.values()]

    @staticmethod
    def auction(request: Request, bids: List[AdServerResponse]):
        bids = [b for b in bids if b.kind == "bid"]
        bids = sorted(bids, key=lambda b: b.bid_value, reverse=True)

        return AdServerResponse.win(request, bids[0]) if bids else AdServerResponse.no_win(request)

    def process_wins(self, window: int):
        wins_by_camp = defaultdict(list)
        for win in self.wins:
            if win.kind != "win":
                continue
            wins_by_camp[win.campaign_id].append(win)
        for cid, wins in wins_by_camp.items():
            self.pacing.notify(window, self.campaigns[cid], wins)
        self.wins = []
