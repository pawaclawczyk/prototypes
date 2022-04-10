from typing import List

import numpy as np
import pandas as pd
import seaborn as sns

from pacing import Campaign
from simulation import Event, Tick


def plot_line(arr):
    sns.relplot(data=arr, kind="line", height=10, aspect=3)


def prepare_data(events: List[Event], ticks: Tick, camps: List[Campaign]):
    records = pd.DataFrame.from_records([(e.tick, e.data.campaign_id, e.data.value) for e in events],
                                        columns=["time", "campaign", "value"])
    agg = records.agg(["time", "campaign"]).sum()



def analyse(wins: List[Event], campaigns: List[Campaign], ticks):
    base_df = pd.DataFrame.from_records([(w.tick, w.data.campaign_id, w.data.value) for w in wins],
                                        columns=["time", "campaign", "value"])
    agg_base_df = base_df.groupby(by=["time", "campaign"]).sum()
    vals = (agg_base_df
            # use "campaign" as columns
            .unstack(level=1, fill_value=0)
            # drop nested column "value"
            .droplevel(level=0, axis=1)
            # fill missing values in "time" index
            .reindex([i for i in range(ticks)], fill_value=0)
            # add missing "campaign" columns
            .reindex([c.ID for c in campaigns], axis=1, fill_value=0))
    prog = vals.cumsum()
    budgets = pd.DataFrame(data=[[c.budget_limit for c in campaigns]], columns=[c.ID for c in campaigns])
    norm_prog = prog / budgets.iloc[0]
    plot_line(norm_prog)


def stats(wins: List[Event]):
    base_df = pd.DataFrame.from_records([(w.tick, w.data.campaign_id, w.data.value) for w in wins],
                                        columns=["time", "campaign", "value"])
    return base_df.groupby(by=["campaign"]).agg(
        t_min=("time", np.min),
        t_max=("time", np.max),
        t_size=("time", lambda t: np.max(t) - np.min(t) + 1),
        t_uniq=("time", pd.Series.nunique),
        imps=("time", np.size)
    )
