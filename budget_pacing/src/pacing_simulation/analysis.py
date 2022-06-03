import os
from collections import defaultdict
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import papermill as pm
import seaborn as sns
from nbconvert import HTMLExporter

from pacing_simulation.ad_server.simulation import Event
from src.pacing_simulation.ad_server.ad_server import KIND_WIN, EVENT_FIELD_NAMES


def aggregate_case_events(case_events: Iterable[Event]) -> pd.DataFrame:
    df = pd.DataFrame.from_records(case_events, columns=EVENT_FIELD_NAMES)
    return df[df["kind"] == KIND_WIN].groupby(["window", "campaign_id"]).agg(spent=("bid_value", np.sum))


def compare_budget_spending(campaigns: pd.DataFrame, base_distribution: pd.DataFrame, events: dict[str, pd.DataFrame]):
    campaign_planned_budgets = campaigns["planned_budget"]
    cumulative_base_distribution = base_distribution.cumsum()

    fig, axs = plt.subplots(len(events) + 1, 1, sharex="col", figsize=(20, 7.5 * (len(events) + 1)))
    axs = axs.reshape(-1)
    current_ax = 0

    axs[current_ax].set_title("Perfect")
    sns.lineplot(data=cumulative_base_distribution, ax=axs[current_ax])
    current_ax += 1

    for name, df in events.items():
        budget_spending = (df.groupby(["window", "campaign_id"])
                           .agg(spending=("bid_value", np.sum))
                           .unstack(fill_value=.0)
                           .cumsum())
        normalized_budget_spending = budget_spending["spending"] / campaign_planned_budgets

        axs[current_ax].set_title(name.capitalize())
        sns.lineplot(data=normalized_budget_spending, ax=axs[current_ax])
        current_ax += 1


def compare_lifespan(events: dict[str, pd.DataFrame]):
    fig, axs = plt.subplots(len(events), 2, sharex="all", figsize=(20, 7.5 * len(events)))
    axs = axs.reshape(-1)
    current_ax = 0

    for name, df in events.items():
        df = df[df["kind"] == KIND_WIN]
        windows = df.groupby(["campaign_id"]).agg(
            window_width=("window", lambda w: np.max(w) - np.min(w)),
            window_unique=("window", pd.Series.nunique)
        )

        axs[current_ax].set_title("Window width: " + name.capitalize())
        sns.boxplot(x="window_width", data=windows, ax=axs[current_ax])
        current_ax += 1

        axs[current_ax].set_title("Unique windows: " + name.capitalize())
        sns.boxplot(x="window_unique", data=windows, ax=axs[current_ax])
        current_ax += 1


def compare_fill_rate(events: dict[str, pd.DataFrame]):
    fig, axs = plt.subplots(len(events), 1, figsize=(20, 7.5 * len(events)))
    axs = axs.reshape(-1)
    current_ax = 0

    for name, df in events.items():
        data = df.groupby("window").agg(
            wins=("kind", lambda s: s[s == KIND_WIN].size),
            count=("kind", np.size)
        )
        data["fill_rate"] = data["wins"] / data["count"]

        axs[current_ax].set_title(name.capitalize())
        sns.lineplot(x=data.index, y=data["fill_rate"], ax=axs[current_ax])
        current_ax += 1


def compare_bid_value(events: dict[str, pd.DataFrame]):
    fig, axs = plt.subplots(len(events), 2, sharex="col", sharey="col", figsize=(20, 7.5 * len(events)))
    axs = axs.reshape(-1)
    current_ax = 0

    for name, df in events.items():
        df = df[df["kind"] == KIND_WIN]
        data = df.groupby("window").agg(
            wins=("kind", np.size),
            value=("bid_value", np.sum)
        )
        data["avg_bid_value"] = data["value"] / data["wins"]

        axs[current_ax].set_title(name.capitalize())
        sns.lineplot(x=data.index, y=data["avg_bid_value"], ax=axs[current_ax])
        current_ax += 1

        axs[current_ax].set_title(name.capitalize())
        sns.boxplot(x=df["bid_value"], ax=axs[current_ax])
        current_ax += 1


def summary_comparison(events: dict[str, pd.DataFrame], quantile: float = .9, normalize: bool = True) -> pd.DataFrame:
    res = defaultdict(dict)
    for name, df in events.items():
        win = df[df["kind"] == KIND_WIN]
        res["window_width"][name] = (win.groupby("campaign_id")
        .aggregate({"window": lambda w: np.max(w) - np.min(w)})
        .quantile(quantile)[0])
        res["unique_windows"][name] = (win.groupby("campaign_id")
        .aggregate({"window": pd.Series.nunique})
        .quantile(quantile)[0])
        res["fill_rate"][name] = win.shape[0] / df.shape[0]
        res["revenue"][name] = win["bid_value"].sum()

    df = pd.DataFrame(res)

    if normalize:
        base = df.max()
        df /= base

    return df


def make_report(scenario: str, template: str, output_dir: str):
    out_nb = os.path.join(output_dir, "../../notebooks/analysis.ipynb")
    out_html = os.path.join(output_dir, "analysis.html")
    pm.execute_notebook(template, out_nb, {"scenario": scenario})
    body, _ = HTMLExporter().from_filename(out_nb)
    with open(out_html, "w") as fp:
        fp.write(body)
