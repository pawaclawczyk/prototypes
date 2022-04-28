import math
import os.path

import numpy as np

import distributions
import load_save
from ad_server import AsapPacing, Campaign, AdServer, second_price_auction, ThrottledPacing
from simulation import Simulation


class Scenario:
    windows = 24 * 60
    fast_finish = 2 * 60

    def __init__(self, name: str, output_dir: str, requests: int, campaigns: int, budget_to_requests_rate: float):
        self.output_dir = os.path.join(output_dir, name)
        self.requests = requests
        self.campaigns = campaigns
        self.budget_to_requests_rate = budget_to_requests_rate

        self.base_dist = self.make_base_distribution(self.windows, self.output_dir)
        self.reqs_dist = self.make_requests_distribution(self.base_dist, self.requests)
        self.campaigns = self.make_campaigns(campaigns, requests, budget_to_requests_rate, self.output_dir)

    @staticmethod
    def make_base_distribution(windows: int, output_dir: str) -> np.ndarray:
        dist = distributions.custom_distribution(windows)
        load_save.save_base_distribution(output_dir, dist)
        return dist

    @staticmethod
    def make_requests_distribution(base_dist: np.ndarray, requests: int) -> np.ndarray:
        return distributions.traffic_distribution(base_dist, requests)

    @staticmethod
    def make_campaigns(
            n: int, total_requests: int, budget_to_requests_rate: float, output_dir: str
    ) -> list[Campaign]:
        campaigns = []
        for campaign_id in range(1, n + 1):
            bid_value = 1000 + campaign_id
            planned_budget = math.ceil(bid_value * total_requests * budget_to_requests_rate / n)
            campaigns.append(Campaign(campaign_id, planned_budget, bid_value))
        load_save.save_campaigns(output_dir, campaigns)
        return campaigns

    @staticmethod
    def make_and_run_asap_case(name: str, output_dir: str, reqs_dist: np.ndarray, campaigns: list[Campaign]):
        pacing = AsapPacing()
        process = AdServer(pacing, second_price_auction, campaigns)
        sim = Simulation(reqs_dist, process)
        events = sim.run()
        load_save.save_events(output_dir, name, events)

    @staticmethod
    def make_and_run_throttled_case(name: str, output_dir: str, fast_finnish: int, base_dist: np.ndarray,
                                    reqs_dist: np.ndarray, campaigns: list[Campaign]):
        pacing = ThrottledPacing(base_dist, fast_finnish)
        process = AdServer(pacing, second_price_auction, campaigns)
        sim = Simulation(reqs_dist, process)
        events = sim.run()
        load_save.save_events(output_dir, name, events)

    def run(self):
        self.make_and_run_asap_case("ASAP", self.output_dir, self.reqs_dist, self.campaigns)
        self.make_and_run_throttled_case("Throttled", self.output_dir, self.fast_finish, self.base_dist, self.reqs_dist,
                                         self.campaigns)
