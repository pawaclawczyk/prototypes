import math

import numpy as np
from dagster import op, OpExecutionContext, List, In, Nothing, job, repository, Out, Output

from pacing_simulation.ad_server.simulation import Simulation, Event
from pacing_simulation.distributions import custom_distribution, traffic_distribution
from pacing_simulation.load_save import save_base_distribution
from scenario import Scenario
from src.pacing_simulation.ad_server.ad_server import Campaign, AsapPacing, AdServer, second_price_auction
from src.pacing_simulation.analysis import make_report

WINDOWS = (24 * 60)
BID_VALUE_OFFSET = 1_000


@op(
    config_schema={
        "num_of_camps": int,
        "num_of_imps": int,
        "delivery": float,
    },
    out={
        "camps": Out(),
        "traffic": Out(),
    }
)
def prepare_input_data(context: OpExecutionContext):
    num_of_camps = context.op_config["num_of_camps"]
    num_of_imps = context.op_config["num_of_imps"]
    delivery = context.op_config["delivery"]

    camps = []
    for i in range(1, num_of_camps + 1):
        bv = (BID_VALUE_OFFSET + i)
        camps.append(Campaign(i, num_of_imps * bv, bv))

    reqs = math.ceil(num_of_camps * num_of_imps * delivery)

    dist = custom_distribution(WINDOWS)
    traffic = traffic_distribution(dist, reqs)

    return Output(camps, "camps"), Output(traffic, "traffic")


@op
def run_asap_case(camps: list[Campaign], traffic: np.ndarray) -> list[Event]:
    pacing = AsapPacing()
    process = AdServer(pacing, second_price_auction, camps)
    sim = Simulation(traffic, process)
    return list(sim.run())


@job
def run_all():
    camps, traffic = prepare_input_data()
    run_asap_case(camps, traffic)


@repository
def simulations():
    return [run_all]


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
    Scenario.make_and_run_asap_case(context.op_config["name"], context.op_config["output_dir"], reqs_dist,
                                    campaigns)


@op(config_schema={"name": str, "output_dir": str, "fast_finnish": int})
def run_throttled_case(context: OpExecutionContext, base_dist: np.ndarray, reqs_dist: np.ndarray,
                       campaigns: List[Campaign]):
    Scenario.make_and_run_throttled_case(context.op_config["name"], context.op_config["output_dir"],
                                         context.op_config["fast_finnish"], base_dist, reqs_dist, campaigns)


@op(config_schema={"scenario": str, "template": str, "output_dir": str}, ins={"wait_for": In(Nothing)})
def make_analysis(context: OpExecutionContext):
    make_report(context.op_config["scenario"], context.op_config["template"], context.op_config["output_dir"])


if __name__ == "__main__":
    run_all.execute_in_process()
