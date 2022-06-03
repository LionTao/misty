from dataclasses import dataclass


@dataclass(frozen=True)
class Point:
    id: str
    date: int
    lon: float
    lat: float
