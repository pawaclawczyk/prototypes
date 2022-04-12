import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Iterable, Dict

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


class AdServerEvent(Event):
    __slots__ = Event.__slots__ + ["kind", "campaign_id", "bid_value"]

    def __init__(self, request: Request):
        super().__init__(request)
        self.kind = ""
        self.campaign_id = 0
        self.bid_value = 0

    @staticmethod
    def bid(request: Request, campaign_id: int, bid_value: int) -> 'AdServerEvent':
        e = AdServerEvent(request)
        e.kind = "bid"
        e.campaign_id = campaign_id
        e.bid_value = bid_value
        return e

    @staticmethod
    def no_bid(request: Request, campaign_id: int) -> 'AdServerEvent':
        e = AdServerEvent(request)
        e.kind = "no bid"
        e.campaign_id = campaign_id
        return e

    @staticmethod
    def win(request: Request, bid: 'AdServerEvent') -> 'AdServerEvent':
        e = AdServerEvent(request)
        e.kind = "win"
        e.campaign_id = bid.campaign_id
        e.bid_value = bid.bid_value
        return e

    @staticmethod
    def no_win(request: Request) -> 'AdServerEvent':
        e = AdServerEvent(request)
        e.kind = "no win"
        return e


class AsapPacing:
    """ASAP is the most basic pacing technique.

    It always places a bid if there is available budget to spend.
    """

    @staticmethod
    def init(campaign: Campaign):
        """Initialize campaign state with daily planned budget.

        The allocation is set to 0 as there should be no bids placed before the initialization.
        """
        campaign.state.budget = campaign.planned_budget
        campaign.state.alloc = 0

    @staticmethod
    def bid(campaign: Campaign, request: Request) -> AdServerEvent:
        """Places a bid whenever the bid value is within the available budget.

        Once the bid is placed, its value is added to the allocated budget.
        """
        if campaign.bid_value <= campaign.state.available_budget:
            campaign.state.alloc += campaign.bid_value
            return AdServerEvent.bid(request, campaign.campaign_id, campaign.bid_value)
        else:
            return AdServerEvent.no_bid(request, campaign.campaign_id)

    @staticmethod
    def consolidate_budget(window: int, campaign: Campaign, wins: List[AdServerEvent]):
        """Consolidates the budget based on winning notifications.

        The winning bid values are subtracted from the budget. The allocation budget set back to 0 as it contains both:
        the winning bids and the lost opportunities. The assumption here is that all auctions in which a bid was placed
        (and included into allocated budget) are completed (no delays).
        """
        for win in wins:
            campaign.state.budget -= win.bid_value
        campaign.state.alloc = 0


class ThrottledPacing(AsapPacing):
    """Throttled pacing technique - presented in the LinkedIn's article.

    It introduces threshold value for the percentage of auctions where the bid is placed.
    Periodically the threshold value is updated (up or down) by the adjustment rate.

    Consts
    ======
    PTR: float
        The threshold value.
    AR: float
        The adjustment rate value.
    """
    PTR = 0.1
    AR = 0.1

    def __init__(self, forecast: np.ndarray):
        # @todo: prepare the forecast: get normalized distribution (not the traffic distribution)
        #        and multiply by the planned daily budget. Then convert to cumulative.
        self.forecast = forecast.cumsum() / forecast.sum()

    def init(self, campaign: Campaign):
        """Initializes budgets and PTR. Creates independent PRNG."""
        super().init(campaign)
        campaign.state.ptr = self.PTR
        campaign.state.rng = random.Random(random.randbytes(16))

    def bid(self, campaign: Campaign, request: Request) -> AdServerEvent:
        """Places a bid (according to standard budget rules) for randomly selected requests.

        It uses uniform PRNG to test if bid should be placed. It spreads the bids across the whole time window.
        """
        if campaign.state.rng.random() <= campaign.state.ptr:
            return super().bid(campaign, request)
        return AdServerEvent.no_bid(request, campaign.campaign_id)

    def consolidate_budget(self, window: int, campaign: Campaign, wins: List[AdServerEvent]):
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


class AdServer(Process):
    """The ad server process simulates a single auction.

    It collects bids from all campaigns and selects the winning bid for every request.

    At the end of time window it consolidates the campaigns budgets.
    """

    def __init__(self, pacing: AsapPacing, campaigns: List[Campaign]):
        self.pacing = pacing
        self.campaigns = dict((c.campaign_id, c) for c in campaigns)
        self.wins: List[AdServerEvent] = []

        for campaign in self.campaigns.values():
            pacing.init(campaign)

    def run(self, request: Request) -> Iterable[AdServerEvent]:
        """Runs an auction."""
        bids = self.collect_bids(request)
        win = self.auction(request, bids)
        self.wins.append(win)
        yield from bids
        yield win

    def notify_window_end(self, window: int) -> None:
        """Processes wins and consolidates budgets."""
        wins_by_campaign = self.process_wins()
        self.consolidate_budgets(window, wins_by_campaign)

    def collect_bids(self, request: Request) -> List[AdServerEvent]:
        """Collects bids from each campaign."""
        return [self.pacing.bid(c, request) for c in self.campaigns.values()]

    @staticmethod
    def auction(request: Request, bids: List[AdServerEvent]):
        """First price auction."""
        bids = [b for b in bids if b.kind == "bid"]
        bids = sorted(bids, key=lambda b: b.bid_value, reverse=True)

        return AdServerEvent.win(request, bids[0]) if bids else AdServerEvent.no_win(request)

    def process_wins(self) -> Dict[int, List[AdServerEvent]]:
        """Groups win events by campaign and clears the local state."""
        wins_by_camp = defaultdict(list)
        for win in self.wins:
            if win.kind != "win":
                continue
            wins_by_camp[win.campaign_id].append(win)
        self.wins = []
        return wins_by_camp

    def consolidate_budgets(self, window: int, wins_by_campaign: Dict[int, List[AdServerEvent]]):
        """Consolidates budget of each campaign."""
        for campaign in self.campaigns.values():
            self.pacing.consolidate_budget(window, campaign, wins_by_campaign[campaign.campaign_id])
