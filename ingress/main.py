import asyncio
import json
import os
import sys
import time
import traceback
from dataclasses import asdict
from datetime import datetime
from typing import List, Dict

from asyncio_throttle import Throttler
from dapr.actor import ActorProxy, ActorId
from requests import get
from split import chop

sys.path.append(os.curdir)
from interfaces.accumulator_interface import AccumulatorInterface

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
    while True:
        try:
            p: TrajectoryPoint = str_to_TrajectoryPoint(s)
            proxy = ActorProxy.create('TrajectoryAssemblerActor', ActorId(str(id)), TrajectoryAssemblerInterface)
            return await proxy.AcceptNewPoint(asdict(p))

        except Exception as e:
            traceback.print_exc()
            print(f"sleeping: {e}", flush=True)
            time.sleep(1)
            continue


async def worker(id: int, throttler: Throttler):
    try:
        print("sending", id, flush=True)
        with open(f"data/filtered/{id}.txt", 'r', encoding="utf-8") as f:
            s = f.readline()
            while s:
                async with throttler:
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
    throttler = Throttler(rate_limit=60000, period=60, retry_interval=0.0001)
    loop = asyncio.get_event_loop()
    loops = [loop.create_task(worker(i, throttler)) for i in ids]
    loop.run_until_complete(asyncio.wait(loops))


async def single_process():
    throttler = Throttler(rate_limit=1000, period=1, retry_interval=0.0001)
    with open("data/four.txt", 'r', encoding="utf-8") as f:
        start = time.perf_counter()
        lines = f.readlines()
        for line in lines:
            p: TrajectoryPoint = str_to_TrajectoryPoint(line)
            async with throttler:
                proxy = ActorProxy.create('TrajectoryAssemblerActor', ActorId(str(p.id)), TrajectoryAssemblerInterface)
                await proxy.AcceptNewPoint(asdict(p))
        end = time.perf_counter()
        print(
            f"using: {end - start}s,{len(lines)} p,{len(lines) / (end - start)} p/s, {(end - start) / len(lines)} s/p")


async def run_a_batch(fnames: List[int], res_fname: str, start: float):
    throttler = Throttler(rate_limit=60000, period=60, retry_interval=0.0001)
    # pool = multiprocessing.Pool(processes=20)
    # pool.imap_unordered(run_loop, chop(1, fnames))
    # p = subprocess.Popen(f"python3 ingress/query_sampler.py {res_fname}_{min(fnames)}_{max(fnames)}", shell=True)
    # p2 = subprocess.Popen(f"python3 ingress/mytop.py {res_fname}_{min(fnames)}_{max(fnames)}", shell=True)
    await asyncio.gather(*[worker(i, throttler) for i in fnames])
    # pool.close()
    # pool.join()
    # p.kill()
    # p2.kill()
    end = time.perf_counter()
    print(f"inserting using: {end - start}s")
    t1 = end - start
    # for i in range(1, 10358):
    #     await worker(i)
    # start2 = time.perf_counter()
    accumulator_proxy = ActorProxy.create('AccumulatorActor', ActorId("0"), AccumulatorInterface)
    counter: Dict[str, int] = await accumulator_proxy.Get()
    tree_counter, meta_counter = counter["tree"], counter["meta"]
    # res = []
    # for i in fnames:
    #     resp = get(f"http://localhost:3302/query-with-id/{i}?threshold=100000&batch_size=2")
    #     res.append(resp.json())
    # end2 = time.perf_counter()
    # print(f"query using: {end - start}s")
    # t2 = end2 - start2

    total_points = 0
    for f in fnames:
        with open(f"data/filtered/{f}.txt") as fp:
            total_points += len(fp.readlines())
    with open(f"tests/results/{res_fname}_{min(fnames)}_{max(fnames)}.json", "w") as f:
        json.dump({"insertion_time": t1, "tps": total_points / t1,
                   "latency": t1 / total_points, "tree_counter": tree_counter, "meta_counter": meta_counter}, f)
    return t1, tree_counter, meta_counter, time.time()


async def run_test(fnames: List[int], res_fname: str):
    batch_size = 20
    batches = chop(batch_size, fnames)
    start = time.perf_counter()
    res = dict()
    for idx, batch in enumerate(batches):
        t1, tree_counter, meta_counter, t = await run_a_batch(batch, res_fname, start)
        # query test
        resp = get(f"http://localhost:3302/query-with-id/1?threshold=10000&batch_size=2")
        res[(idx + 1) * batch_size] = {"insertion_time": t1, "tree_counter": tree_counter,
                                       "meta_counter": meta_counter, "resp": resp.json(), "t": t}
    return res


async def main():
    with open("tests/parameters.json") as f:
        para: dict = json.load(f)
    res_fname: str = para["result_file_name"]
    target_trajectories: List[int] = para["trajectories"]
    res = await run_test(target_trajectories, res_fname)
    with open(f"tests/results/{res_fname}.json", "w") as f:
        json.dump(res, f)
    # asyncio.get_event_loop().run_until_complete(single_process())


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
