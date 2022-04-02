import datetime
from dataclasses import dataclass
from math import inf


@dataclass(eq=True, frozen=True)
class TrajectoryPoint:
    id: int = inf
    time: datetime.datetime = datetime.datetime.now()
    lng: float = inf
    lat: float = inf


@dataclass(eq=True, frozen=True)
class TrajectorySegment:
    id: int = inf
    start: TrajectoryPoint = None
    end: TrajectoryPoint = None


@dataclass(eq=True, frozen=True)
class MBR:
    minX = inf
    minY = inf
    maxX = inf
    maxY = inf
