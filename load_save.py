import os
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd

from ad_server import Campaign
from simulation import Event


def load_base_distribution(output_dir: Path) -> pd.Series:
    return pd.read_csv(output_dir / "base_distribution.csv", index_col="window")


def save_base_distribution(output_dir: Path, base_distribution: np.ndarray):
    os.makedirs(output_dir, exist_ok=True)
    path = output_dir / "base_distribution.csv"
    s = pd.Series(base_distribution)
    s.index.name = "window"
    s.name = "requests"
    s.to_csv(path)


def load_campaigns(output_dir: Path) -> pd.DataFrame:
    return pd.read_csv(output_dir / "campaigns.csv", index_col="campaign_id")


def save_campaigns(output_dir: Path, campaigns: list[Campaign]):
    os.makedirs(output_dir, exist_ok=True)
    path = output_dir / "campaigns.csv"
    df = pd.DataFrame(c.to_dict() for c in campaigns)
    df.set_index("campaign_id")
    df.to_csv(path)


def load_events(output_dir: Path, case_name: str, kinds: Optional[list[str]] = None) -> pd.DataFrame:
    df = pd.read_parquet(output_dir / f"{case_name}.parquet")
    if kinds:
        df = df[df["kind"].isin(kinds)]
    return df



def load_events_from_multiple_cases(output_dir: Path, cases: list[str], kinds: Optional[list[str]] = None) \
        -> dict[str, pd.DataFrame]:
    return {case_name: load_events(output_dir, case_name, kinds) for case_name in cases}


def save_events(output_dir: Path, case_name: str, events: Iterable[Event]):
    os.makedirs(output_dir, exist_ok=True)
    path = output_dir / f"{case_name}.parquet"
    df = pd.DataFrame(e.to_dict() for e in events)
    df.index.name = "ord"
    df.to_parquet(path)
