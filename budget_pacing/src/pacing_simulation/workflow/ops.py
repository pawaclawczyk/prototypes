import math
import os.path

import numpy as np
import pandas as pd
from dagster import op, OpExecutionContext, List, In, Nothing, job, repository, Out, Output, IOManager, InputContext, \
    OutputContext, io_manager, InitResourceContext

from pacing_simulation.ad_server.simulation import Simulation
from pacing_simulation.distributions import custom_distribution, traffic_distribution, uniform_distribution
from pacing_simulation.load_save import save_base_distribution
from scenario import Scenario
from src.pacing_simulation.ad_server.ad_server import Campaign, AsapPacing, AdServer, second_price_auction, \
    ThrottledPacing
from src.pacing_simulation.analysis import make_report, aggregate_case_events


class PandasCSVIOManager(IOManager):
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.file_postfix = ".csv"

    def handle_output(self, context: OutputContext, df: pd.DataFrame):
        path = os.path.join(self.base_dir, context.metadata["path"] + self.file_postfix)
        df.to_csv(path)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        path = os.path.join(self.base_dir, context.upstream_output.metadata["path"] + self.file_postfix)
        return pd.read_csv(path)


@io_manager(config_schema={"base_dir": str})
def pandas_csv_io_manager(init_context: InitResourceContext) -> IOManager:
    return PandasCSVIOManager(init_context.resource_config["base_dir"])


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
        "dist": Out(),
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

    return Output(camps, "camps"), dist, Output(traffic, "traffic")


@op(out=Out(io_manager_key="pandas_io_manager", metadata={"path": "asap"}))
def run_asap_case(camps: list[Campaign], traffic: np.ndarray) -> pd.DataFrame:
    pacing = AsapPacing()
    process = AdServer(pacing, second_price_auction, camps)
    sim = Simulation(traffic, process)
    return aggregate_case_events(sim.run())


@op(out=Out(io_manager_key="pandas_io_manager", metadata={"path": "throttled"}))
def run_throttled_case(dist: np.ndarray, camps: list[Campaign], traffic: np.ndarray) -> pd.DataFrame:
    pacing = ThrottledPacing(dist, (2 * 60))
    process = AdServer(pacing, second_price_auction, camps)
    sim = Simulation(traffic, process)
    return aggregate_case_events(sim.run())


@op(out=Out(io_manager_key="pandas_io_manager", metadata={"path": "throttled_wo_ff"}))
def run_throttled_without_fast_finnish_case(dist: np.ndarray, camps: list[Campaign],
                                            traffic: np.ndarray) -> pd.DataFrame:
    pacing = ThrottledPacing(dist)
    process = AdServer(pacing, second_price_auction, camps)
    sim = Simulation(traffic, process)
    return aggregate_case_events(sim.run())


@op(out=Out(io_manager_key="pandas_io_manager", metadata={"path": "throttled_uniform"}))
def run_throttled_with_uniform_forecast(camps: list[Campaign], traffic: np.ndarray) -> pd.DataFrame:
    pacing = ThrottledPacing(uniform_distribution(WINDOWS))
    process = AdServer(pacing, second_price_auction, camps)
    sim = Simulation(traffic, process)
    return aggregate_case_events(sim.run())


@job(resource_defs={
    "pandas_io_manager": pandas_csv_io_manager.configured({"base_dir": "data"})
})
def run_all():
    camps, dist, traffic = prepare_input_data()
    run_asap_case(camps, traffic)
    run_throttled_case(dist, camps, traffic)
    run_throttled_without_fast_finnish_case(dist, camps, traffic)
    run_throttled_with_uniform_forecast(camps, traffic)


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
