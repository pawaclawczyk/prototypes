import inspect
import os.path

import yaml
from dagster import config_mapping, job, GraphDefinition, repository, NodeInvocation, DependencyDefinition, \
    OpDefinition, MultiDependencyDefinition
from dagster.core.definitions import NodeDefinition

from pacing_simulation.workflow import ops


@config_mapping(config_schema={
    "general": {
        "output_dir": str,
    },
    "scenario": {
        "name": str,
        "windows": int,
        "requests": int,
    },
    "campaigns": {
        "count": int,
        "budget_to_requests_rate": float,
    },
    "cases": {
        "throttled": {
            "fast_finnish": int
        }
    },
    "analysis": {
        "template": str,
    }
})
def scenario_config(values: dict) -> dict:
    name, windows, requests = values["scenario"].values()
    output_dir = os.path.join(values["general"]["output_dir"], name)
    camps, btr = values["campaigns"].values()
    cases = values["cases"]
    analysis = values["analysis"]

    return {"ops": {
        "make_custom_distribution": {"config": {
            "windows": windows,
            "output_dir": output_dir
        }},
        "make_uniform_distribution": {"config": {
            "windows": windows
        }},
        "make_requests_distribution": {"config": {
            "requests": requests
        }},
        "make_campaigns": {"config": {
            "campaigns": camps,
            "requests": requests,
            "budget_to_requests_rate": btr,
            "output_dir": output_dir
        }},
        "run_asap_case": {"config": {
            "name": "asap",
            "output_dir": output_dir
        }},
        "run_throttled_case": {"config": {
            "name": "throttled",
            "output_dir": output_dir,
            "fast_finnish": cases["throttled"]["fast_finnish"]
        }},
        "throttled_no_forecast": {"config": {
            "name": "throttled_no_forecast",
            "output_dir": output_dir,
            "fast_finnish": cases["throttled"]["fast_finnish"]
        }},
        "make_analysis": {"config": {
            "scenario": name,
            "template": analysis["template"],
            "output_dir": output_dir
        }}
    }}


@job(config=scenario_config)
def run_scenario():
    custom_dist = ops.make_custom_distribution()
    uniform_dist = ops.make_uniform_distribution()
    reqs_dist = ops.make_requests_distribution(custom_dist)
    campaigns = ops.make_campaigns()

    asap = ops.run_asap_case(reqs_dist, campaigns)
    throttled = ops.run_throttled_case(custom_dist, reqs_dist, campaigns)
    throttled_no_forecast = ops.run_throttled_case.alias("throttled_no_forecast")(uniform_dist, reqs_dist, campaigns)
    ops.make_analysis(wait_for=[asap, throttled, throttled_no_forecast])


def build_graph(config_path: str, nodes: list[NodeDefinition]) -> GraphDefinition:
    with open(config_path, "r") as f:
        definition = yaml.safe_load(f)

    graph_deps = {
        "make_requests_distribution": {
            "base_dist": DependencyDefinition("make_custom_distribution")
        }
    }

    analysis_deps = []
    for case in definition["cases"]:
        case_deps = {
            "reqs_dist": DependencyDefinition("make_requests_distribution"),
            "campaigns": DependencyDefinition("make_campaigns")
        }

        for name, op in case["extra_deps"].items():
            case_deps[name] = DependencyDefinition(op)
        graph_deps[NodeInvocation(case["op"], case["name"])] = case_deps

        analysis_deps.append(DependencyDefinition(case["name"]))

    graph_deps[NodeInvocation("make_analysis")] = {
        "wait_for": MultiDependencyDefinition(analysis_deps)
    }

    return GraphDefinition("scenario", None, nodes, graph_deps)


@repository
def _repository():
    nodes = [f for _, f in inspect.getmembers(ops, lambda m: isinstance(m, OpDefinition))]
    return [
        run_scenario,
        build_graph("graph.yaml", nodes).to_job()
    ]


if __name__ == "__main__":
    with open("config.yaml", "r") as fp:
        config = yaml.safe_load(fp)
    run_scenario.execute_in_process()
