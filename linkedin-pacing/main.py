from pathlib import Path
from typing import List

import numpy as np
import yaml
from dagster import OpExecutionContext, op, config_mapping, job, In, Nothing

from ad_server import Campaign
from analysis import make_report
from scenario import Scenario


@op(config_schema={"windows": int, "output_dir": str})
def make_base_distribution(context: OpExecutionContext) -> np.ndarray:
    return Scenario.make_base_distribution(context.op_config["windows"], context.op_config["output_dir"])


@op(config_schema={"requests": int})
def make_requests_distribution(context: OpExecutionContext, base: np.ndarray) -> np.ndarray:
    return Scenario.make_requests_distribution(base, context.op_config["requests"])


@op(config_schema={"campaigns": int, "requests": int, "budget_to_requests_rate": float, "output_dir": str})
def make_campaigns(context: OpExecutionContext) -> List[Campaign]:
    return Scenario.make_campaigns(
        context.op_config["campaigns"],
        context.op_config["requests"],
        context.op_config["budget_to_requests_rate"],
        context.op_config["output_dir"],
    )


@op(config_schema={"name": str, "output_dir": str})
def run_asap_case(context: OpExecutionContext, reqs_dist: np.ndarray, campaigns: List[Campaign]):
    Scenario.make_and_run_asap_case(context.op_config["name"], context.op_config["output_dir"], reqs_dist, campaigns)


@op(config_schema={"name": str, "output_dir": str, "fast_finnish": int})
def run_throttled_case(context: OpExecutionContext, base_dist: np.ndarray, reqs_dist: np.ndarray,
                       campaigns: List[Campaign]):
    Scenario.make_and_run_throttled_case(context.op_config["name"], context.op_config["output_dir"],
                                         context.op_config["fast_finnish"], base_dist, reqs_dist, campaigns)


@op(config_schema={"scenario": str, "template": str, "output_dir": str},
    ins={"asap": In(Nothing), "throttled": In(Nothing)})
def generate_report(context: OpExecutionContext):
    make_report(context.op_config["scenario"], context.op_config["template"], context.op_config["output_dir"])


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
            "scenario": values["name"], "template": "analysis.template.ipynb", "output_dir": output_dir
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
