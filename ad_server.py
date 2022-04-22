import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Iterable, Dict, Callable

import numpy as np

from simulation import Process, Request, Event


@dataclass
class CampaignState:
    budget: int = 0
    alloc: int = 0
    ptr: float = 0
    rng: random.Random = None

    @property
    def available_budget(self) -> int:
        """The available budget is the difference between consolidated budget and the budget allocation
        (not yet consolidated)"""
        return self.budget - self.alloc


@dataclass
class Campaign:
    campaign_id: int
    planned_budget: int
    bid_value: int
    state: CampaignState = field(default_factory=CampaignState)

    def to_dict(self):
        return {"campaign_id": self.campaign_id, "planned_budget": self.planned_budget, "bid_value": self.bid_value}


class EventFactory:
    @staticmethod
    def bid(request: Request, campaign_id: int, bid_value: int):
        return {"kind": "bid", "request": request.request, "window": request.window,
                "request_in_window": request.request_in_window,
                "campaign_id": campaign_id, "bid_value": bid_value}

    @staticmethod
    def no_bid(request: Request, campaign_id: int):
        return {"kind": "no bid", "request": request.request, "window": request.window,
                "request_in_window": request.request_in_window,
                "campaign_id": campaign_id, "bid_value": 0}

    @staticmethod
    def win(request: Request, campaign_id: int, bid_value: int):
        return {"kind": "win", "request": request.request, "window": request.window,
                "request_in_window": request.request_in_window,
                "campaign_id": campaign_id, "bid_value": bid_value}

    @staticmethod
    def no_win(request: Request):
        return {"kind": "no win", "request": request.request, "window": request.window,
                "request_in_window": request.request_in_window,
                "campaign_id": 0, "bid_value": 0}


class Pacing:
    def init(self, campaign: Campaign): pass

    def bid(self, campaign: Campaign, request: Request) -> Event: pass

    def consolidate_budget(self, window: int, campaign: Campaign, wins: List[Event]): pass


class AsapPacing(Pacing):
    """ASAP is the most basic pacing technique.

    It always places a bid if there is available budget to spend.
    """

    def init(self, campaign: Campaign):
        """Initialize campaign state with daily planned budget.

        The allocation is set to 0 as there should be no bids placed before the initialization.
        """
        campaign.state.budget = campaign.planned_budget
        campaign.state.alloc = 0

    def bid(self, campaign: Campaign, request: Request) -> Event:
        """Places a bid whenever the bid value is within the available budget.

        Once the bid is placed, its value is added to the allocated budget.
        """
        if campaign.bid_value <= campaign.state.available_budget:
            campaign.state.alloc += campaign.bid_value
            return EventFactory.bid(request, campaign.campaign_id, campaign.bid_value)
        else:
            return EventFactory.no_bid(request, campaign.campaign_id)

    def consolidate_budget(self, window: int, campaign: Campaign, wins: List[Event]):
        """Consolidates the budget based on winning notifications.

        The winning bid values are subtracted from the budget. The allocation budget set back to 0 as it contains both:
        the winning bids and the lost opportunities. The assumption here is that all auctions in which a bid was placed
        (and included into allocated budget) are completed (no delays).
        """
        for win in wins:
            campaign.state.budget -= win["bid_value"]
        campaign.state.alloc = 0


class ThrottledPacing(AsapPacing):
    """Throttled pacing technique - presented in the LinkedIn's article.

    It introduces threshold value for the percentage of auctions where the bid is placed.
    Periodically the threshold value is updated (up or down) by the adjustment rate.

    The implementation assumes identical forecast for each campaign.
    The base forecast distribution is kept, and budget spending forecast is computed on-the-fly during bid placing.
    The "fast finnish" forces algorithm to spend budgets more aggressively at the end of the day.
    It is done by changing the forecast information such as there will be no more ad requests in that period.
    Consts
    ======
    PTR: float
        The threshold value.
    AR: float
        The adjustment rate value.
    """
    PTR = 0.1
    AR = 0.1

    def __init__(self, base_distribution: np.ndarray, fast_finish: int = 0):
        self.forecast = base_distribution.copy().cumsum()
        self.forecast[len(self.forecast) - fast_finish:] = 1.0

    def init(self, campaign: Campaign):
        """Initializes budgets and PTR. Creates independent PRNG."""
        super().init(campaign)
        campaign.state.ptr = self.PTR
        campaign.state.rng = random.Random(random.randbytes(16))

    def bid(self, campaign: Campaign, request: Request) -> Event:
        """Places a bid (according to standard budget rules) for randomly selected requests.

        It uses uniform PRNG to test if bid should be placed. It spreads the bids across the whole time window.
        """
        if campaign.state.rng.random() <= campaign.state.ptr:
            return super().bid(campaign, request)
        return EventFactory.no_bid(request, campaign.campaign_id)

    def consolidate_budget(self, window: int, campaign: Campaign, wins: List[Event]):
        """Consolidates the budget and adjusts the PTR value."""
        super().consolidate_budget(window, campaign, wins)
        self.adjust_ptr(window, campaign)

    def adjust_ptr(self, window: int, campaign: Campaign):
        """Increases or decreases the PTR value.

        The PTR change depends on the budget consumption compared to forecasted budget consumption.
        """
        a = self.forecast[window] * campaign.planned_budget
        s = campaign.planned_budget - campaign.state.budget
        if s > a:
            campaign.state.ptr = max(campaign.state.ptr * (1 - self.AR), 0)
        elif s < a:
            campaign.state.ptr = min(campaign.state.ptr * (1 + self.AR), 1)


def first_price_auction(request: Request, bids: List[Event]) -> Event:
    """Select highest bid as the win."""
    bids = [b for b in bids if b["kind"] == "bid"]
    if not bids:
        return EventFactory.no_win(request)
    bid = sorted(bids, key=lambda b: b.bid_value, reverse=True)[0]
    return EventFactory.win(request, bid["campaign_id"], bid["bid_value"])


def second_price_auction(request: Request, bids: List[Event]) -> Event:
    """Select highest bid as the win, but use the bid value from the second one."""
    bids = [b for b in bids if b["kind"] == "bid"]
    if not bids:
        return EventFactory.no_win(request)
    bids = sorted(bids, key=lambda b: b["bid_value"], reverse=True)
    if len(bids) == 1:
        return EventFactory.win(request, bids[0]["campaign_id"], bids[0]["bid_value"])
    return EventFactory.win(request, bids[0]["campaign_id"], bids[1]["bid_value"])


class AdServer(Process):
    """The ad server process simulates a single auction.

    It collects bids from all campaigns and selects the winning bid for every request.

    At the end of time window it consolidates the campaigns budgets.
    """

    def __init__(self, pacing: Pacing, select_win: Callable[[Request, List[Event]], Event],
                 campaigns: List[Campaign]):
        self.pacing = pacing
        self.select_win = select_win
        self.campaigns = dict((c.campaign_id, c) for c in campaigns)
        self.wins: List[Event] = []

        for campaign in self.campaigns.values():
            pacing.init(campaign)

    def run(self, request: Request) -> Iterable[Event]:
        """Runs an auction."""
        bids = self.collect_bids(request)
        win = self.select_win(request, bids)
        self.wins.append(win)
        # yield from bids
        yield win

    def notify_window_end(self, window: int) -> None:
        """Processes wins and consolidates budgets."""
        wins_by_campaign = self.process_wins()
        self.consolidate_budgets(window, wins_by_campaign)

    def collect_bids(self, request: Request) -> List[Event]:
        """Collects bids from each campaign."""
        return [self.pacing.bid(c, request) for c in self.campaigns.values()]

    def process_wins(self) -> Dict[int, List[Event]]:
        """Groups win events by campaign and clears the local state."""
        wins_by_camp = defaultdict(list)
        for win in self.wins:
            if win["kind"] != "win":
                continue
            wins_by_camp[win["campaign_id"]].append(win)
        self.wins = []
        return wins_by_camp

    def consolidate_budgets(self, window: int, wins_by_campaign: Dict[int, List[Event]]):
        """Consolidates budget of each campaign."""
        for campaign in self.campaigns.values():
            self.pacing.consolidate_budget(window, campaign, wins_by_campaign[campaign.campaign_id])
