from pathlib import Path
from typing import List

import nbconvert
import numpy as np
import papermill as pm
import yaml
from dagster import OpExecutionContext, op, config_mapping, job, In, Nothing

import load_save
from ad_server import AdServer, ThrottledPacing, second_price_auction, Campaign, AsapPacing
from distributions import custom_distribution, traffic_distribution
from load_save import save_events
from scenario import Scenario
from simulation import Simulation


@op(config_schema={"windows": int, "output_dir": str})
def make_base_distribution(context: OpExecutionContext) -> np.ndarray:
    dist = custom_distribution(context.op_config["windows"])
    load_save.save_base_distribution(context.op_config["output_dir"], dist)
    return dist


@op(config_schema={"requests": int})
def make_requests_distribution(context: OpExecutionContext, base_dist: np.ndarray) -> np.ndarray:
    return traffic_distribution(base_dist, context.op_config["requests"])


@op(config_schema={"campaigns": int, "requests": int, "budget_to_requests_rate": float, "output_dir": str})
def make_campaigns(context: OpExecutionContext) -> List[Campaign]:
    campaigns = Scenario.make_campaigns(
        context.op_config["campaigns"],
        context.op_config["requests"],
        context.op_config["budget_to_requests_rate"],
    )
    load_save.save_campaigns(context.op_config["output_dir"], campaigns)
    return campaigns


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


@op(config_schema={"scenario": str, "template": str, "output_dir": str},
    ins={"asap": In(Nothing), "throttled": In(Nothing)})
def generate_report(context: OpExecutionContext):
    path_nb = Path(context.op_config["output_dir"]) / "report.ipynb"
    path_html = Path(context.op_config["output_dir"]) / "report.html"

    pm.execute_notebook(context.op_config["template"], path_nb, {"simulation": context.op_config["scenario"]})

    exporter = nbconvert.HTMLExporter()
    body, _ = exporter.from_filename(path_nb)
    with open(path_html, "w") as fp:
        fp.write(body)


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
        "make_base_distribution": {"config": {"windows": values["windows"], "output_dir": output_dir}},
        "make_requests_distribution": {"config": {"requests": values["requests"]}},
        "make_campaigns": {"config": {
            "campaigns": values["campaigns"], "requests": values["requests"],
            "budget_to_requests_rate": values["budget_to_requests_rate"],
            "output_dir": output_dir
        }},
        "run_asap_case": {"config": {"name": "ASAP", "output_dir": output_dir}},
        "run_throttled_case": {"config": {
            "name": "Throttled", "output_dir": output_dir,
            "fast_finnish": values["fast_finnish"]
        }},
        "generate_report": {"config": {
            "scenario": values["name"], "template": "report.template.ipynb", "output_dir": output_dir
        }}
    }}


@job(config=scenario_config)
def run_scenario():
    base_dist = make_base_distribution()
    reqs_dist = make_requests_distribution(base_dist)
    campaigns = make_campaigns()
    asap = run_asap_case(reqs_dist, campaigns)
    throttled = run_throttled_case(base_dist, reqs_dist, campaigns)
    generate_report(asap=asap, throttled=throttled)


if __name__ == "__main__":
    with open("config.yaml", "r") as fp:
        config = yaml.safe_load(fp)
    run_scenario.execute_in_process()
