import math
from pathlib import Path

import ad_server
import distributions
import load_save
import simulation


class Scenario:
    windows = 24 * 60
    fast_finish = 2 * 60

    def __init__(self, name: str, output_dir: str, number_of_requests: int, number_of_campaigns: int,
                 budget_to_requests_rate: float):
        self.output_dir = Path(output_dir) / name
        self.number_of_requests = number_of_requests
        self.number_of_campaigns = number_of_campaigns
        self.budget_to_requests_rate = budget_to_requests_rate

        self.make_base_distribution()
        self.requests_distribution = distributions.traffic_distribution(self.base_distribution, self.number_of_requests)

        self.campaigns = self.make_campaigns(number_of_campaigns, number_of_requests, budget_to_requests_rate)
        self.make_cases()
        self.select_win = ad_server.second_price_auction

        load_save.save_campaigns(self.output_dir, self.campaigns)

    def make_base_distribution(self):
        self.base_distribution = distributions.custom_distribution(self.windows)
        load_save.save_base_distribution(self.output_dir, self.base_distribution)

    @staticmethod
    def make_campaigns(n: int, total_requests: int, budget_to_requests_rate: float):
        campaigns = []
        for campaign_id in range(1, n + 1):
            bid_value = 1000 + campaign_id
            planned_budget = math.ceil(bid_value * total_requests * budget_to_requests_rate / n)
            campaigns.append(ad_server.Campaign(campaign_id, planned_budget, bid_value))
        return campaigns

    def make_cases(self):
        self.cases = dict()
        self.cases["ASAP"] = ad_server.AsapPacing()
        self.cases["Throttled"] = ad_server.ThrottledPacing(self.base_distribution, self.fast_finish)

    def run(self):
        for case_name, pacing in self.cases.items():
            self.run_case(case_name, pacing)

    def run_case(self, case_name: str, pacing: ad_server.Pacing):
        process = ad_server.AdServer(pacing, self.select_win, self.campaigns)
        sim = simulation.Simulation(self.requests_distribution, process)
        events = sim.run()
        load_save.save_events(self.output_dir, case_name, events)
