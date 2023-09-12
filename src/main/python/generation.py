import functools
from dataclasses import dataclass
from typing import Dict

import numpy as np
import pandas as pd

from scenarios import BerlinScenario


@dataclass
class AgentSubpopulations:
    freight: pd.DataFrame
    ride: pd.DataFrame
    relevant: pd.DataFrame


def generate_agents_subpopulations(
    agents: pd.DataFrame, trips: pd.DataFrame
) -> AgentSubpopulations:
    main_modes_by_agent = trips.groupby("person")["main_mode"].agg(main_modes=set)
    joined_agents = agents.join(main_modes_by_agent, "person", "outer")
    joined_agents["main_modes"] = joined_agents["main_modes"].map(
        lambda modes: modes if type(modes) == set else set()
    )
    excluded_freight = joined_agents["subpopulation"] == "freight"
    excluded_ride = joined_agents["main_modes"].map(lambda modes: "ride" in modes)
    return AgentSubpopulations(
        freight=joined_agents[excluded_freight],
        ride=joined_agents[excluded_ride],
        relevant=joined_agents[~excluded_freight & ~excluded_ride],
    )


def add_person_and_trip_number_from_trip_id(data: pd.DataFrame):
    data.insert(
        0,
        "person",
        data.index.map(lambda trip_id: trip_id.rsplit("_", 1)[0]).astype("category"),
    )
    data.insert(
        1,
        "trip_number",
        data.index.map(lambda trip_id: np.int8(trip_id.rsplit("_", 1)[1])),
    )


def berlinwise_col(scenario: BerlinScenario) -> str:
    return f"{scenario}.berlinwise"


BERLINWISE_CAT = pd.CategoricalDtype(
    ["berlin_orig", "berlin_dest", "berlin_inner", "non_berlin", "cancelled"]
)


def to_berlinwise(berlinwise):
    if berlinwise == "berlin_transit":
        return "non_berlin"
    else:
        return berlinwise


def generate_trips_berlinwise(
    agents: pd.DataFrame,
    trips_berlinwise_by_scenario: Dict[BerlinScenario, pd.DataFrame],
) -> pd.DataFrame:
    trips_berlinwise = trips_berlinwise_by_scenario[BerlinScenario.BASE][[]]
    for scenario, trips_berlinwise_of_scenario in trips_berlinwise_by_scenario.items():
        trips_berlinwise_of_scenario = (
            trips_berlinwise_of_scenario["berlinwise"]
            .rename(berlinwise_col(scenario))
            .map(to_berlinwise)
            .astype(BERLINWISE_CAT)
        )
        trips_berlinwise = trips_berlinwise.join(
            trips_berlinwise_of_scenario, how="outer"
        )
    # fill NaNs in a second pass, as the outer join could add NaNs after each scenario
    for scenario in trips_berlinwise_by_scenario.keys():
        trips_berlinwise[berlinwise_col(scenario)].fillna("cancelled", inplace=True)
    # As an outer join is used, I could not find an option for "take person from whatever data frame where it exists",
    # so recreate person and trip_number columns
    add_person_and_trip_number_from_trip_id(trips_berlinwise)
    return trips_berlinwise.join(agents[[]], "person", "inner")


def berlin_transit_col(scenario: BerlinScenario) -> str:
    return f"{scenario}.berlin_transit"


modes = ["freight", "pt", "car", "ride", "bicycle", "walk"]


BERLIN_TRANSIT_CAT = pd.CategoricalDtype(
    [f"transit_{mode}" for mode in modes]
    + [
        "other",
        "berlin",
        "cancelled",
    ]
)


def to_berlin_transit(row):
    if row["berlinwise"] == "berlin_transit":
        for mode in modes:
            if mode in row["modes"]:
                return f"transit_{mode}"
        raise NotImplementedError
    elif row["berlinwise"] == "non_berlin":
        return "other"
    else:
        return "berlin"


def is_in_all_scenarios_berlin(trips):
    return functools.reduce(
        lambda a, b: a & b,
        (
            trips[berlin_transit_col(scenario)] == "berlin"
            for scenario in BerlinScenario
        ),
    )


def generate_nonberlin_trips(
    agents: pd.DataFrame,
    trips_berlinwise_by_scenario: Dict[BerlinScenario, pd.DataFrame],
) -> pd.DataFrame:
    nonberlin_trips = trips_berlinwise_by_scenario[BerlinScenario.BASE][[]]
    for (
        scenario,
        trips_berlinwise_of_scenario,
    ) in trips_berlinwise_by_scenario.items():
        trips_berlin_transit = (
            trips_berlinwise_of_scenario.apply(to_berlin_transit, axis=1)
            .rename(berlin_transit_col(scenario))
            .astype(BERLIN_TRANSIT_CAT)
        )
        nonberlin_trips = nonberlin_trips.join(trips_berlin_transit, how="outer")
    nonberlin_trips = nonberlin_trips[~is_in_all_scenarios_berlin(nonberlin_trips)]
    # fill NaNs in a second pass, as the outer join could add NaNs after each scenario
    for scenario in trips_berlinwise_by_scenario.keys():
        nonberlin_trips[berlin_transit_col(scenario)].fillna("cancelled", inplace=True)
    # As an outer join is used, I could not find an option for "take person from whatever data frame where it exists",
    # so recreate person and trip_number columns
    add_person_and_trip_number_from_trip_id(nonberlin_trips)
    return nonberlin_trips.join(agents[[]], "person", "inner")


def at_least_one_berlin_start_or_end(berlinwise):
    return any(
        value in {"berlin_orig", "berlin_dest", "berlin_inner"} for value in berlinwise
    )


def generate_agents_berlin_trips(
    agents: pd.DataFrame, trips_berlinwise: pd.DataFrame
) -> pd.DataFrame:
    berlin_trips = trips_berlinwise.groupby("person")["berlinwise"].agg(
        berlin_trip=at_least_one_berlin_start_or_end
    )
    joined_agents = agents.join(berlin_trips, "person")
    joined_agents["berlin_trip"].fillna(False, inplace=True)
    joined_agents["berlin_trip"] = joined_agents["berlin_trip"].map(
        lambda berlin_trip: "yes" if berlin_trip else "no"
    )
    return joined_agents
