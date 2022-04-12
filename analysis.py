import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

import distributions


def budget_consumption(df: pd.DataFrame, requests_distribution: np.ndarray):
    values_df = df[df["kind"] == "win"] \
        .groupby(["window", "campaign_id"]) \
        .aggregate(value=("bid_value", np.sum)) \
        .unstack(fill_value=0)

    budget_spending_df = values_df.cumsum() / values_df.sum()
    return budget_spending_df
    ideal_budget_spending_df = distributions.normalize(requests_distribution).cumsum()

    fig, (ax1, ax2) = plt.subplots(1, 2, sharey="row", figsize=(20, 10))
    sns.lineplot(data=ideal_budget_spending_df, ax=ax1)
    sns.lineplot(data=budget_spending_df, ax=ax2)
