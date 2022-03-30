import asyncio
import multiprocessing
from dataclasses import asdict
from datetime import datetime
from typing import List

from dapr.actor import ActorProxy, ActorId
from split import chop

from interfaces.trajectory_assembler_interface import TrajectoryAssemblerInterface
from interfaces.types import TrajectoryPoint


def str_to_TrajectoryPoint(s: str) -> TrajectoryPoint:
    data_list = s.strip().split(",")
    try:
        p = TrajectoryPoint(
            id=int(data_list[0]),
            time=datetime.strptime(data_list[1], "%Y-%m-%d %H:%M:%S"),
            lng=float(data_list[2]),
            lat=float(data_list[3])
        )
        return p
    except Exception as e:
        print("current:", s, flush=True)
        return TrajectoryPoint(
            0, datetime.now(), 0, 0
        )


async def send_one_point(s: str, proxy: ActorProxy):
    p: TrajectoryPoint = str_to_TrajectoryPoint(s)
    await proxy.AcceptNewPoint(asdict(p))


async def worker(id: int):
    try:
        proxy = ActorProxy.create('TrajectoryAssemblerActor', ActorId(str(id)), TrajectoryAssemblerInterface)
        with open(f"/mnt/g/Data/tdrive/taxi_log_2008_by_id/{id}.txt", 'r', encoding="utf-8") as f:
            s = f.readline()
            while s:
                await send_one_point(s, proxy)
                s = f.readline()
        return 0
    except Exception as e:
        print(id, "failed", flush=True)
        print(e, flush=True)
        return 1


def run_loop(ids: List[int]):
    loop = asyncio.get_event_loop()
    loops = [loop.create_task(worker(id)) for id in ids]
    loop.run_until_complete(asyncio.wait(loops))


def main():
    pool = multiprocessing.Pool(processes=5)
    pool.imap_unordered(run_loop, list(chop(10, range(1, 10358))))
    pool.close()
    pool.join()


main()
