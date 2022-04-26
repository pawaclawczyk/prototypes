import json
import os
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd

from ad_server import Campaign
from simulation import Event


def load_base_distribution(output_dir: str) -> pd.Series:
    output_dir = Path(output_dir)
    return pd.read_csv(output_dir / "base_distribution.csv", index_col="window")


def save_base_distribution(output_dir: str, base_distribution: np.ndarray):
    output_dir = Path(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    path = output_dir / "base_distribution.csv"
    s = pd.Series(base_distribution)
    s.index.name = "window"
    s.name = "requests"
    s.to_csv(path)


def load_campaigns(output_dir: str) -> pd.DataFrame:
    output_dir = Path(output_dir)
    return pd.read_csv(output_dir / "campaigns.csv", index_col="campaign_id")


def save_campaigns(output_dir: str, campaigns: list[Campaign]):
    output_dir = Path(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    path = output_dir / "campaigns.csv"
    df = pd.DataFrame(c.to_dict() for c in campaigns)
    df.set_index("campaign_id")
    df.to_csv(path)


def load_events(output_dir: str, case_name: str, kinds: Optional[list[str]] = None) -> pd.DataFrame:
    output_dir = Path(output_dir)
    df = pd.read_json(output_dir / f"{case_name}.jsonl", lines=True)
    df.set_index("request")
    if kinds:
        df = df[df["kind"].isin(kinds)]
    return df


def load_events_from_multiple_cases(output_dir: str, cases: list[str], kinds: Optional[list[str]] = None) \
        -> dict[str, pd.DataFrame]:
    return {case_name: load_events(output_dir, case_name, kinds) for case_name in cases}


def save_events(output_dir: str, case_name: str, events: Iterable[Event]):
    output_dir = Path(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    jsonl_path = output_dir / f"{case_name}.jsonl"
    with open(jsonl_path, "w") as fp:
        for event in events:
            fp.write(json.dumps(event) + "\n")
