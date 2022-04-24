from pathlib import Path
from typing import List

import numpy as np
from dagster import OpExecutionContext, op, config_mapping, job

from ad_server import AdServer, ThrottledPacing, second_price_auction, Campaign, AsapPacing
from distributions import custom_distribution, traffic_distribution
from load_save import save_events
from scenario import Scenario
from simulation import Simulation


@op(config_schema={"windows": int})
def make_base_distribution(context: OpExecutionContext) -> np.ndarray:
    return custom_distribution(context.op_config["windows"])


@op(config_schema={"requests": int})
def make_requests_distribution(context: OpExecutionContext, base_dist: np.ndarray) -> np.ndarray:
    return traffic_distribution(base_dist, context.op_config["requests"])


@op(config_schema={"campaigns": int, "requests": int, "budget_to_requests_rate": float})
def make_campaigns(context: OpExecutionContext) -> List[Campaign]:
    return Scenario.make_campaigns(
        context.op_config["campaigns"],
        context.op_config["requests"],
        context.op_config["budget_to_requests_rate"],
    )


@op(config_schema={"name": str, "output_dir": str})
def run_asap_case(context: OpExecutionContext, reqs_dist: np.ndarray, campaigns: List[Campaign]):
    proc = AdServer(AsapPacing(), second_price_auction, campaigns)
    sim = Simulation(reqs_dist, proc)
    save_events(context.op_config["output_dir"], context.op_config["name"], sim.run())


@op(config_schema={"name": str, "output_dir": str, "fast_finnish": int})
def run_throttled_case(context: OpExecutionContext, base_dist: np.ndarray, reqs_dist: np.ndarray,
                       campaigns: List[Campaign]):
    proc = AdServer(ThrottledPacing(base_dist, context.op_config["fast_finnish"]), second_price_auction, campaigns)
    sim = Simulation(reqs_dist, proc)
    save_events(context.op_config["output_dir"], context.op_config["name"], sim.run())


@config_mapping(config_schema={
    "name": str,
    "output_dir": str,
    "windows": int,
    "requests": int,
    "campaigns": int,
    "budget_to_requests_rate": float,
    "fast_finnish": int
})
def scenario_config(values: dict) -> dict:
    output_dir = str(Path(values["output_dir"]) / values["name"])

    return {"ops": {
        "make_base_distribution": {"config": {"windows": values["windows"]}},
        "make_requests_distribution": {"config": {"requests": values["requests"]}},
        "make_campaigns": {"config": {
            "campaigns": values["campaigns"], "requests": values["requests"],
            "budget_to_requests_rate": values["budget_to_requests_rate"]
        }},
        "run_asap_case": {"config": {"name": "ASAP", "output_dir": output_dir}},
        "run_throttled_case": {"config": {
            "name": "Throttled", "output_dir": output_dir,
            "fast_finnish": values["fast_finnish"]
        }}
    }}


@job(config=scenario_config)
def run_scenario():
    base_dist = make_base_distribution()
    reqs_dist = make_requests_distribution(base_dist)
    campaigns = make_campaigns()
    run_asap_case(reqs_dist, campaigns)
    run_throttled_case(base_dist, reqs_dist, campaigns)


if __name__ == "__main__":
    pass
