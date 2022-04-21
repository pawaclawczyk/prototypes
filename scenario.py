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
        self.requests_distribution = distributions.traffic_dist(self.base_distribution, self.number_of_requests)

        self.make_campaigns()
        self.make_cases()
        self.select_win = ad_server.second_price_auction

    def make_base_distribution(self):
        self.base_distribution = distributions.custom_dist(self.windows)
        load_save.save_base_distribution(self.output_dir, self.base_distribution)

    def make_campaigns(self):
        self.campaigns = []
        for campaign_id in range(1, self.number_of_campaigns + 1):
            bid_value = 1000 + campaign_id
            planned_budget = math.ceil(
                bid_value * self.number_of_requests * self.budget_to_requests_rate / self.number_of_campaigns)
            self.campaigns.append(ad_server.Campaign(campaign_id, planned_budget, bid_value))
        load_save.save_campaigns(self.output_dir, self.campaigns)

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
