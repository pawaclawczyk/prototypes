from collections import defaultdict
from typing import List

import numpy as np
import pandas as pd
import seaborn as sns

from pacing import AdServer
from simulation import Message


def split_messages_by_kind(messages: List[Message]):
    result = defaultdict(list)
    for m in messages:
        result[m.kind].append(m)
    return result


def line_plot(arr):
    sns.relplot(data=arr, kind="line", height=10, aspect=3)


def analyse(events, campaigns, ticks):
    wins = split_messages_by_kind(events)[AdServer.EVENT_KIND_WIN]
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
            .reindex([c.id_ for c in campaigns], axis=1, fill_value=0))
    prog = vals.cumsum()
    budgets = pd.DataFrame(data=[[c.pacing.budget_daily for c in campaigns]], columns=[c.id_ for c in campaigns])
    norm_prog = prog / budgets.iloc[0]
    line_plot(norm_prog)


def stats(events):
    wins = split_messages_by_kind(events)[AdServer.EVENT_KIND_WIN]
    base_df = pd.DataFrame.from_records([(w.tick, w.data.campaign_id, w.data.value) for w in wins],
                                        columns=["time", "campaign", "value"])
    return base_df.groupby(by=["campaign"]).agg(
        t_min=("time", np.min),
        t_max=("time", np.max),
        t_size=("time", lambda t: np.max(t) - np.min(t) + 1),
        t_uniq=("time", pd.Series.nunique),
        imps=("time", np.size)
    )
