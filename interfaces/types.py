import datetime
from dataclasses import dataclass
from math import inf


@dataclass(eq=True, frozen=True)
class TrajectoryPoint:
    id: str = ""
    time: datetime.datetime = datetime.datetime.now()
    lng: float = inf
    lat: float = inf


@dataclass(eq=True, frozen=True)
class TrajectorySegment:
    id: str = ""
    start: TrajectoryPoint = None
    end: TrajectoryPoint = None
