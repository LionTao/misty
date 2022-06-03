import itertools
import time
from typing import Tuple, Iterable, List, Dict

import numpy as np
import traj_dist.distance as tdist
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from mytest import Point

TARGET = np.array([
    [116.49625, 39.9146],
    [116.50962, 39.91071],
    [116.52231, 39.91588],
    [116.52231, 39.91588],
    [116.56444, 39.91445],
    [116.59512, 39.90798],
    [116.65522, 39.8622],
])
# DATA_PATH = "/home/liontao/work/spark-trajectory/data/small.txt"
DATA_PATH2 = "/home/liontao/work/spark-trajectory/data/all.txt"

PARTITIONS = 100


def point_to_target(s: str) -> Tuple[str, float]:
    l = s.split(",")
    return l[0], tdist.hausdorff(TARGET, np.array([[float(l[2]), float(l[3])], [float(l[2]), float(l[3])]]),
                                 "spherical")


def data_to_point(s: str):
    l = s.split(",")
    return l[0], Point(l[0], int(time.mktime(time.strptime(l[1], "%Y-%m-%d %H:%M:%S"))), float(l[2]), float(l[3]))


def copy_points(r: Tuple[str, Point]) -> List[Tuple[int, Point]]:
    res = list()
    for i in range(PARTITIONS):
        res.append((i, r[1]))
    return res


def sort_points(partitions: Iterable[Tuple[str, Iterable[Point]]]):
    for p in partitions:
        i = list(p[1])
        i.sort(key=lambda x: x.date)
        yield p[0], i


def trajectrory_to_segments(t):
    return zip(t[:-1], t[1:])


def segment_to_trajectory_hausdorff(s: Tuple[Point, Point]) -> float:
    dis = tdist.hausdorff(TARGET, np.array([[s[0].lon, s[0].lat], [s[1].lon, s[1].lat]]), "spherical")
    return dis


def reduce_max(a: float, b: float) -> float:
    return max(a, b)


def do_calculation_within_partition_with_index(idx: int, partitions: Iterable[Tuple[int, Iterable[Point]]]) -> Iterable:
    for p in partitions:
        bucket: Dict[str, List[Point]] = dict()
        for point in p[1]:
            if point.id in bucket:
                bucket[point.id].append(point)
            else:
                bucket[point.id] = [point]
        trajs = []
        for k, v in bucket.items():
            if len(v) < 2:
                pass
            traj = v
            traj.sort(key=lambda x: x.date)
            trajs.append(traj)
        yield list(filter(lambda x: int(x[0][0].id) % PARTITIONS == idx, itertools.product(trajs, trajs)))


def trajectory_to_trajectory_hausdorff(p: Tuple[List[Point], List[Point]]) -> Tuple[str,Tuple[str, float]]:
    target = p[0]
    target_id = target[0].id
    candidate = p[1]
    candidate_id = p[1][0].id
    target_array = np.array([[i.lon, i.lat] for i in target])
    candidate_array = np.array([[i.lon, i.lat] for i in candidate])
    return target_id,(candidate_id, tdist.hausdorff(target_array, candidate_array, "spherical"))


def main():
    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext(f"local[{PARTITIONS}]", "NetworkWordCount")
    ssc = StreamingContext(sc, batchDuration=1)

    # data_rdd = sc.textFile(DATA_PATH)
    rdd2 = sc.textFile(DATA_PATH2)

    # ssc \
    #     .queueStream([data_rdd, rdd2]) \
    #     .map(data_to_point) \
    #     .groupByKey() \
    #     .mapPartitions(sort_points) \
    #     .flatMapValues(trajectrory_to_segments) \
    #     .mapValues(segment_to_trajectory_hausdorff) \
    #     .reduceByKey(reduce_max) \
    #     .pprint()

    ssc \
        .queueStream([rdd2]) \
        .map(data_to_point) \
        .flatMap(copy_points) \
        .groupByKey(PARTITIONS) \
        .mapPartitionsWithIndex(do_calculation_within_partition_with_index) \
        .flatMap(lambda x: x) \
        .map(trajectory_to_trajectory_hausdorff) \
        .pprint()

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()
