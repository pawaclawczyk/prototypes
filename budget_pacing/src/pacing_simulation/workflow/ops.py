import math
import os.path

import numpy as np
import pandas as pd
from dagster import op, OpExecutionContext, job, repository, Out, Output, IOManager, InputContext, \
    OutputContext, io_manager, InitResourceContext

from pacing_simulation.ad_server.simulation import Simulation
from pacing_simulation.distributions import custom_distribution, traffic_distribution, uniform_distribution
from src.pacing_simulation.ad_server.ad_server import Campaign, AsapPacing, AdServer, second_price_auction, \
    ThrottledPacing
from src.pacing_simulation.analysis import aggregate_case_events


class PandasCSVIOManager(IOManager):
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.file_postfix = ".csv"

    def handle_output(self, context: OutputContext, df: pd.DataFrame):
        path = os.path.join(self.base_dir, context.metadata["path"] + self.file_postfix)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        path = os.path.join(self.base_dir, context.upstream_output.metadata["path"] + self.file_postfix)
        return pd.read_csv(path)


class CampaignsIOManager(PandasCSVIOManager):
    def handle_output(self, context: OutputContext, camps: list[Campaign]):
        df = pd.DataFrame((c.to_dict() for c in camps)).set_index("campaign_id")
        super().handle_output(context, df)

    def load_input(self, context: InputContext) -> list[Campaign]:
        df = super().load_input(context)
        return [Campaign(c["campaign_id"], c["planned_budget"], bid_value=c["bid_value"]) for _, c in df.iterrows()]


@io_manager(config_schema={"base_dir": str})
def pandas_csv_io_manager(init_context: InitResourceContext) -> IOManager:
    return PandasCSVIOManager(init_context.resource_config["base_dir"])


@io_manager(config_schema={"base_dir": str})
def campaigns_io_manager(init_context: InitResourceContext) -> IOManager:
    return CampaignsIOManager(init_context.resource_config["base_dir"])


WINDOWS = (24 * 60)
BID_VALUE_OFFSET = 1_000


@op(
    config_schema={
        "num_of_camps": int,
        "num_of_imps": int,
        "delivery": float,
    },
    out={
        "camps": Out(io_manager_key="campaigns_io_manager", metadata={"path": "input/campaigns"}),
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


@op(out=Out(io_manager_key="pandas_io_manager", metadata={"path": "result/asap"}))
def run_asap_case(camps: list[Campaign], traffic: np.ndarray) -> pd.DataFrame:
    pacing = AsapPacing()
    process = AdServer(pacing, second_price_auction, camps)
    sim = Simulation(traffic, process)
    return aggregate_case_events(sim.run())


@op(out=Out(io_manager_key="pandas_io_manager", metadata={"path": "result/throttled"}))
def run_throttled_case(dist: np.ndarray, camps: list[Campaign], traffic: np.ndarray) -> pd.DataFrame:
    pacing = ThrottledPacing(dist, (2 * 60))
    process = AdServer(pacing, second_price_auction, camps)
    sim = Simulation(traffic, process)
    return aggregate_case_events(sim.run())


@op(out=Out(io_manager_key="pandas_io_manager", metadata={"path": "result/throttled_wo_ff"}))
def run_throttled_without_fast_finnish_case(dist: np.ndarray, camps: list[Campaign],
                                            traffic: np.ndarray) -> pd.DataFrame:
    pacing = ThrottledPacing(dist)
    process = AdServer(pacing, second_price_auction, camps)
    sim = Simulation(traffic, process)
    return aggregate_case_events(sim.run())


@op(out=Out(io_manager_key="pandas_io_manager", metadata={"path": "result/throttled_uniform"}))
def run_throttled_with_uniform_forecast(camps: list[Campaign], traffic: np.ndarray) -> pd.DataFrame:
    pacing = ThrottledPacing(uniform_distribution(WINDOWS))
    process = AdServer(pacing, second_price_auction, camps)
    sim = Simulation(traffic, process)
    return aggregate_case_events(sim.run())


@job(resource_defs={
    "pandas_io_manager": pandas_csv_io_manager.configured({"base_dir": "data"}),
    "campaigns_io_manager": campaigns_io_manager.configured({"base_dir": "data"})
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


if __name__ == "__main__":
    run_all.execute_in_process()
