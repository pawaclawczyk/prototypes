import numpy as np
from dagster import op, OpExecutionContext, List, In, Nothing, Array

from ad_server import Campaign
from analysis import make_report
from distributions import custom_distribution
from load_save import save_base_distribution
from scenario import Scenario


@op(config_schema={"windows": int, "output_dir": str})
def make_custom_distribution(context: OpExecutionContext) -> np.ndarray:
    dist = custom_distribution(context.op_config["windows"])
    save_base_distribution(context.op_config["output_dir"], dist)
    return dist


@op(config_schema={"windows": int})
def make_uniform_distribution(context: OpExecutionContext) -> np.ndarray:
    return custom_distribution(context.op_config["windows"])


@op(config_schema={"requests": int})
def make_requests_distribution(context: OpExecutionContext, base_dist: np.ndarray) -> np.ndarray:
    return Scenario.make_requests_distribution(base_dist, context.op_config["requests"])


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


@op(config_schema={"scenario": str, "template": str, "output_dir": str}, ins={"wait_for": In(Nothing)})
def make_analysis(context: OpExecutionContext):
    make_report(context.op_config["scenario"], context.op_config["template"], context.op_config["output_dir"])
