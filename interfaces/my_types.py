from dataclasses import dataclass
import datetime
from math import inf

@dataclass
class TrajectoryPoint:
    id: int=0
    time:datetime.datetime=datetime.datetime.now()
    x:float=inf
    y:float=inf
