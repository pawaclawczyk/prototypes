from dagster import OpExecutionContext, op, job

from ad_server import AdServer, ThrottledPacing, second_price_auction
from distributions import custom_distribution, traffic_distribution
from load_save import save_events
from scenario import Scenario
from simulation import Simulation


@op(config_schema={
    "case_name": str,
    "windows": int,
    "number_of_campaigns": int,
    "total_requests": int,
    "budget_to_requests_rate": float,
    "fast_finnish": int,
    "output_dir": str,
})
def run_simulation(context: OpExecutionContext):
    # make base distribution
    base_dist = custom_distribution(context.op_config["windows"])
    # make traffic distribution
    reqs_dist = traffic_distribution(base_dist, context.op_config["total_requests"])
    # make campaigns
    campaigns = Scenario.make_campaigns(
        context.op_config["number_of_campaigns"],
        context.op_config["total_requests"],
        context.op_config["budget_to_requests_rate"],
    )
    # instantiate simulation
    proc = AdServer(ThrottledPacing(base_dist, context.op_config["fast_finnish"]), second_price_auction, campaigns)
    sim = Simulation(reqs_dist, proc)
    # run simulation
    save_events(context.op_config["output_dir"], context.op_config["case_name"], sim.run())


@job
def run_all():
    run_simulation()


if __name__ == "__main__":
    pass
