import numpy as np
import traj_dist.distance as tdist
from dataclasses import dataclass

TARGET = np.array([
    [116.49625, 39.9146],
    [116.50962, 39.91071],
    [116.52231, 39.91588],
    [116.52231, 39.91588],
    [116.56444, 39.91445],
    [116.59512, 39.90798],
    [116.65522, 39.8622],
])


@dataclass(frozen=True)
class Point:
    id: str
    date: int
    lon: float
    lat: float


if __name__ == '__main__':
    # res = tdist.hausdorff(TARGET, np.array([[116.49625, 39.9146], [116.49625, 39.9146]]), "spherical")
    # print(res)
    import pickle

    pickle.dump(Point("1", 1, 1, 1))
