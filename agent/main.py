import asyncio
import time
import traceback
from queue import Queue
from typing import List, Tuple, Set

from aiologger import Logger
from aiologger.formatters.base import Formatter
from aiologger.levels import LogLevel
from dacite import from_dict
from dapr.actor import ActorProxy, ActorId
from dapr.ext.fastapi import DaprApp
from fastapi import FastAPI, HTTPException
from pyproj import Transformer
from shapely.geometry import LineString, Polygon
from split import chop

from index.actor import DistributedIndexActor
from interfaces.distance_compute_interface import DistanceComputeInterface
from interfaces.distributed_index_interface import DistributedIndexInterface
from interfaces.index_meta_interface import IndexMetaInterface
from interfaces.trajectory_assembler_interface import TrajectoryAssemblerInterface
from interfaces.types import TrajectoryPoint

app = FastAPI(title=f'Continuous query agent Service')

dapr = DaprApp(app)

transformer = Transformer.from_crs(4326, 3857, always_xy=True)

logger = Logger.with_default_handlers(name=f"Query agent", level=LogLevel.INFO,
                                      formatter=Formatter("%(name)s-%(asctime)s: %(message)s"))


@app.get("/query-with-id/{target_trajectory_id}")
async def query_with_id(target_trajectory_id: str, threshold: float, batch_size=3):
    try:
        # 拿到轨迹所有的点
        before_target = time.perf_counter()
        target_home = ActorProxy.create('TrajectoryAssemblerActor', ActorId(target_trajectory_id),
                                        TrajectoryAssemblerInterface)
        target: List[TrajectoryPoint] = [from_dict(TrajectoryPoint, i) for i in await target_home.Query()]
        points: List[Tuple[float, float]] = [transformer.transform(i.lng, i.lat) for i in target]
        # 组成复合polygon
        areas: Polygon = LineString(points).buffer(threshold)
        after_target = time.perf_counter()
        await logger.info(f"target: {after_target - before_target}s, areas: {areas.wkt}")
        # 送meta查询rtree
        before_query = time.perf_counter()
        meta_proxy = ActorProxy.create('IndexMetaActor', ActorId("0"), IndexMetaInterface)
        candidate_regions: List[str] = await meta_proxy.AgentQuery(areas.wkt)
        after_query = time.perf_counter()
        await logger.info(f"regions tims:{after_query - before_query}s,  regions: {candidate_regions}")
        # 根据meta返回的index id调用query取得候选轨迹id
        before_candidate = time.perf_counter()
        candidates_ids = set()
        q = Queue()
        [q.put(c) for c in candidate_regions]
        while not q.empty():
            r = q.get()
            candidate_region_proxy = ActorProxy.create('DistributedIndexActor', ActorId(r), DistributedIndexInterface)
            has_value, partial_candidates = await candidate_region_proxy.Query(areas.wkt)
            if has_value:
                candidates_ids.update(partial_candidates)
            else:
                await logger.info(f"Failed, adding more cells")
                more_cells: Set[str] = DistributedIndexActor.split_h3_area(r, partial_candidates[0])
                [q.put(c) for c in more_cells]
        after_candidate = time.perf_counter()
        await logger.info(f"tid time: {after_candidate - before_candidate}s, tids: {candidates_ids}")
        # 发送compute计算距离
        before_compute = time.perf_counter()
        coroutines = []
        # res=[]
        for idx, c in enumerate(chop(int(batch_size), candidates_ids)):
            compute_proxy = ActorProxy.create("DistanceComputeActor", ActorId(str(idx)), DistanceComputeInterface)
            coroutines.append(compute_proxy.ComputeHausdorffWithID({
                "target_trajectory_id": target_trajectory_id,
                "candidates": c
            }))
        res = await asyncio.gather(*coroutines)
        after_compute = time.perf_counter()
        await logger.info(
            f"compute: {after_compute - before_compute}s, total: {after_compute - before_target}s")
        # 返回结果
        return res
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/query-with-track")
async def query_with_track():
    pass
