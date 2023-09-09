from pathlib import Path

import numpy as np
import pandas as pd


def read_agents(persons_path: Path) -> pd.DataFrame:
    return pd.read_csv(
        persons_path,
        sep=";",
        index_col="person",
        dtype={
            "person": str,
            "first_act_type": "category",
            "subpopulation": "category",
            "home-activity-zone": "category",
        },
    )


def read_trips(trips_path: Path) -> pd.DataFrame:
    return pd.read_csv(
        trips_path,
        sep=";",
        index_col="trip_id",
        dtype={
            "trip_id": str,
            "person": str,
            "trip_number": np.int8,
            "main_mode": "category",
            "longest_distance_mode": "category",
            "modes": "category",
            "start_activity_type": "category",
            "end_activity_type": "category",
            "start_link": "category",
            "end_link": "category",
        },
    )


def read_trips_berlinwise(trips_berlinwise_path: Path) -> pd.DataFrame:
    return pd.read_csv(
        trips_berlinwise_path,
        sep=";",
        index_col="trip_id",
        dtype={
            "trip_id": str,
            "person": str,
            "trip_number": np.int8,
            "start_link": "category",
            "end_link": "category",
            "berlinwise": "category",
        },
    )
