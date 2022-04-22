from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


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
        df = df[df["kind"] == "win"]
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
            wins=("kind", lambda s: s[s == "win"].size),
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
        df = df[df["kind"] == "win"]
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
        win = df[df["kind"] == "win"]
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
