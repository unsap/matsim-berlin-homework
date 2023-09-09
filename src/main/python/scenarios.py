from enum import Enum
from pathlib import Path


class BerlinScenario(Enum):
    BASE = "0.BASE"
    GR_HS = "1.GR-HS"
    GR_WS = "2.GR-WS"
    KR_HS = "3.KR-HS"
    KB = "4.KB"
    MV = "5.MV"
    S1 = "6.S1"
    S2 = "7.S2"
    S3 = "8.S3"


SCENARIOS_PATH = Path(__file__).parents[3] / "scenarios"
