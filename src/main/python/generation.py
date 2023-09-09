from dataclasses import dataclass
from typing import Dict

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


def berlinwise_col(scenario: BerlinScenario) -> str:
    return f"{scenario.value}.berlinwise"


def to_berlinwise(berlinwise):
    if berlinwise == "berlin_transit":
        return "non_berlin"
    else:
        return berlinwise


def generate_trips_berlinwise(
    agents: pd.DataFrame,
    trips_berlinwise_by_scenario: Dict[BerlinScenario, pd.DataFrame],
) -> pd.DataFrame:
    trips_berlinwise = trips_berlinwise_by_scenario[BerlinScenario.BASE][
        ["person", "trip_number"]
    ]
    for scenario, trips_berlinwise_of_scenario in trips_berlinwise_by_scenario.items():
        trips_berlinwise_of_scenario = (
            trips_berlinwise_of_scenario["berlinwise"]
            .rename(berlinwise_col(scenario))
            .map(to_berlinwise)
        )
        trips_berlinwise = trips_berlinwise.join(
            trips_berlinwise_of_scenario, "trip_id", "outer"
        )
    # fill NaNs in a second pass, as the outer join could add NaNs after each scenario
    for scenario in trips_berlinwise_by_scenario.keys():
        trips_berlinwise[berlinwise_col(scenario)].fillna("cancelled", inplace=True)
    return trips_berlinwise.join(agents[[]], "person", "inner")


def berlin_transit_col(scenario: BerlinScenario) -> str:
    return f"{scenario.value}.berlin_transit"


def to_berlin_transit(row):
    if row["berlinwise"] == "berlin_transit":
        return f"transit_{row['main_mode']}"
    elif row["berlinwise"] == "non_berlin":
        return "other"
    else:
        return "berlin"


def is_in_all_scenarios_berlin(row):
    return all(
        row[berlin_transit_col(scenario)] == "berlin" for scenario in BerlinScenario
    )


def generate_nonberlin_trips(
    agents: pd.DataFrame,
    trips_by_scenario: Dict[BerlinScenario, pd.DataFrame],
    trips_berlinwise_by_scenario: Dict[BerlinScenario, pd.DataFrame],
) -> pd.DataFrame:
    nonberlin_trips = trips_berlinwise_by_scenario[BerlinScenario.BASE][
        ["person", "trip_number"]
    ]
    for (
        scenario,
        trips_of_scenario,
    ) in trips_by_scenario.items():
        trips_berlinwise_of_scenario = trips_berlinwise_by_scenario[scenario]
        trips = trips_of_scenario[["main_mode"]].join(
            trips_berlinwise_of_scenario[["berlinwise"]], "trip_id"
        )
        trips_berlin_transit = trips.apply(to_berlin_transit, axis=1).rename(
            berlin_transit_col(scenario)
        )
        nonberlin_trips = nonberlin_trips.join(trips_berlin_transit, "trip_id", "outer")
    nonberlin_trips = nonberlin_trips[
        ~nonberlin_trips.apply(is_in_all_scenarios_berlin, axis=1)
    ]
    # fill NaNs in a second pass, as the outer join could add NaNs after each scenario
    for scenario in trips_by_scenario.keys():
        nonberlin_trips[berlin_transit_col(scenario)].fillna("cancelled", inplace=True)
    return nonberlin_trips.join(agents[[]], "person", "inner")


def at_least_one_berlin_start_or_end(berlinwise):
    return any(
        value in {"berlin_orig", "berlin_dest", "berlin_inner"} for value in berlinwise
    )


def generate_agents_berlin_trips(
    agents: pd.DataFrame, trips_berlinwise: pd.DataFrame
) -> pd.DataFrame:
    at_least_one_berlin_trip_by_person = trips_berlinwise.groupby("person")[
        "berlinwise"
    ].agg(at_least_one_berlin_trip=at_least_one_berlin_start_or_end)
    joined_agents = agents.join(at_least_one_berlin_trip_by_person, "person")
    joined_agents["at_least_one_berlin_trip"].fillna(False, inplace=True)
    return joined_agents
