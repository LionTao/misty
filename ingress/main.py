import asyncio
import multiprocessing
import os
import sys
from dataclasses import asdict
from datetime import datetime
import time
from typing import List

from asyncio_throttle import Throttler
from dapr.actor import ActorProxy, ActorId
from split import chop

sys.path.append(os.curdir)
print(sys.path)
from interfaces.trajectory_assembler_interface import TrajectoryAssemblerInterface
from interfaces.types import TrajectoryPoint


def str_to_TrajectoryPoint(s: str) -> TrajectoryPoint:
    data_list = s.strip().split(",")
    try:
        p = TrajectoryPoint(
            id=data_list[0],
            time=datetime.strptime(data_list[1], "%Y-%m-%d %H:%M:%S"),
            lng=float(data_list[2]),
            lat=float(data_list[3])
        )
        return p
    except Exception as e:
        print("current:", s, flush=True)
        return TrajectoryPoint(
            "-1", datetime.now(), 0, 0
        )


async def send_one_point(s: str, id: int) -> bool:
    p: TrajectoryPoint = str_to_TrajectoryPoint(s)
    proxy = ActorProxy.create('TrajectoryAssemblerActor', ActorId(str(id)), TrajectoryAssemblerInterface)
    return await proxy.AcceptNewPoint(asdict(p))


async def worker(id: int, throttler: Throttler):
    try:
        print("sending", id, flush=True)

        with open(f"/home/liontao/data/taxi_log_2008_by_id/{id}.txt", 'r', encoding="utf-8") as f:
            s = f.readline()
            while s:
                # async with throttler:
                resp = await send_one_point(s, id)
                c = 1
                while not resp:
                    await asyncio.sleep(0.01)
                    resp = await send_one_point(s, id)
                    c += 1
                    if c % 10 == 0:
                        print(f"{id}: tried for {c} times!", flush=True)
                s = f.readline()
        print(id, " done", flush=True)
        return 0
    except Exception as e:
        print(id, "failed", flush=True)
        print(e, flush=True)
        return 1


def run_loop(ids: List[int]):
    throttler = Throttler(rate_limit=1000, period=1, retry_interval=0.001)
    loop = asyncio.get_event_loop()
    loops = [loop.create_task(worker(i, throttler)) for i in ids]
    loop.run_until_complete(asyncio.wait(loops))


def main():
    start = time.perf_counter()
    pool = multiprocessing.Pool(processes=2)
    pool.imap_unordered(run_loop, list(chop(2, range(1, 5))))
    pool.close()
    pool.join()
    end = time.perf_counter()
    print(f"using: {end-start}s")
    # for i in range(1, 10358):
    #     await worker(i)


main()
# asyncio.get_event_loop().run_until_complete()
