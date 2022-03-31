import asyncio
import os
import traceback
from collections import defaultdict
from dataclasses import asdict
from typing import Optional, List, Dict

import h3
from aiologger import Logger
from aiologger.levels import LogLevel
from dacite import from_dict
from dapr.actor import Actor, ActorId, ActorProxy
from dapr.actor.runtime.context import ActorRuntimeContext
from geopandas import GeoDataFrame
from shapely.geometry import LineString, box

from interfaces.distributed_index_interface import DistributedIndexInterface
from interfaces.index_meta_interface import IndexMetaInterface
from interfaces.types import TrajectorySegment, MBR, TrajectoryPoint

# buffer里最大存储数量
MAX_BUFFER_SIZE = os.getenv("MAX_BUFFER_SIZE", 10)
# buffer里数量:树里的数量
TREE_INSERTION_THRESHOLD = os.getenv("TREE_INSERTION_THRESHOLD", 0.5)
# 分裂所需的树索引阈值
SPLIT_THRESHOLD = os.getenv("SPLIT_THRESHOLD", 20)


class DistributedIndexActor(Actor, DistributedIndexInterface):
    """
    自动分裂的分布式轨迹段空间索引
    """

    def __init__(self, ctx: ActorRuntimeContext, actor_id: ActorId):
        super().__init__(ctx, actor_id)

        self.STATE_KEY = f"DistributedIndexActor_{self.id}"
        self.RETIRED_KRY = f"DistributedIndexActor_{self.id}_retired"

        self.retired: bool = False

        # h3 grid id
        self.h: str = self.id.id
        # 当前的分辨率
        self.resolution: int = h3.h3_get_resolution(self.h)

        # str-tree索引
        self.segments: Optional[GeoDataFrame] = None
        # Buffer
        self.cache: Optional[List[TrajectorySegment]] = None

        self.logger = Logger.with_default_handlers(name=f"DistributedIndex_{self.id}", level=LogLevel.INFO)

    async def _on_activate(self) -> None:
        has_value, p = await self._state_manager.try_get_state(self.STATE_KEY)
        if has_value:
            val: List[TrajectorySegment] = list(map(lambda x: from_dict(TrajectorySegment, x), p))
            self.cache = val
            self.logger.info("Got segments restored")
        else:
            self.logger.info("No previous_point available")
        has_value, p = await self._state_manager.try_get_state(self.RETIRED_KRY)
        if has_value:
            val: bool = p
            self.retired = val
            if self.retired: self.logger.info(f"Why wake {self.id}_region up?")
        self.logger.info(f"{self.id}_region activated")

    async def _on_deactivate(self) -> None:
        self._do_insertion()
        await self._state_manager.set_state(self.STATE_KEY, [asdict(i) for i in self.segments["obj"]])
        await self._state_manager.set_state(self.RETIRED_KRY, self.retired)
        await self._state_manager.save_state()
        self.logger.info("State stored")
        await self.logger.shutdown()

    async def accept_new_segment(self, segment: dict) -> bool:
        """
        接受一个轨迹段并先插入buffer
        """
        try:
            if self.retired:
                return False
            s: TrajectorySegment = from_dict(TrajectorySegment, segment)
            if self.cache:
                self.cache.append(s)
            else:
                self.cache = [s]
            self._check_insertion()
            self.logger.info(
                f"Buffer: {len(self.cache) if self.cache else 0}, Tree: {len(self.segments) if self.segments is not None else 0}")
            await self._check_split()
            return True
        except Exception as e:
            traceback.print_tb(e.__traceback__)
            print("error:", str(e), flush=True)
            return False

    def _check_insertion(self):
        """
        看看是不是要进行合并
        """
        if self._need_insertion():
            self._do_insertion()

    def _do_insertion(self):
        """
        进行合并
        """
        gdf = self._cache_to_gdf()
        if self.segments is not None:
            self.segments = self.segments.append(gdf, ignore_index=True)
        else:
            self.segments = gdf
        self.cache = None

    def _cache_to_gdf(self) -> GeoDataFrame:
        """
        buffer转成gdf
        """
        # TODO: 转换墨卡托投影
        d = [{"id": i.id, "obj": i, "geometry": LineString([(i.start.lng, i.start.lat), (i.end.lng, i.end.lat)])} for i
             in
             self.cache]
        return GeoDataFrame(d, crs="EPSG:4326")

    def _need_insertion(self) -> bool:
        return len(self.cache) > MAX_BUFFER_SIZE or (
                self.segments is not None and (len(self.cache) / len(self.segments)) > TREE_INSERTION_THRESHOLD)

    async def initialize_as_a_new_child_region(self, segments: List[dict]) -> bool:
        """
        接受母亲那来的一堆轨迹段进行初始化
        """
        self.cache = list(map(lambda x: from_dict(TrajectorySegment, x), segments))
        self._check_insertion()
        return True

    async def query(self, mbr: dict, threshold: float) -> List[int]:
        """
        根据给定的mbr进行查询
        """
        # TODO: add check retired?
        # TODO: use threshold
        # TODO: 转换墨卡托投影
        res = set()
        mbr = from_dict(MBR, mbr)
        if self.segments is not None:
            ids = self.segments.sindex.query(box(mbr.minX, mbr.minY, mbr.maxX, mbr.maxY))
            if ids:
                res.update(ids)
        if self.cache:
            for i in self.cache:
                if box(mbr.minX, mbr.minY, mbr.maxX, mbr.maxY).intersects(
                        LineString([(i.start.lng, i.start.lat), (i.end.lng, i.end.lat)])):
                    res.add(i.id)
        return list(res)

    async def _check_split(self):
        """
        查看是否要分裂
        """
        if self._need_split():
            self._do_insertion()
            # 1. 切分数据到7个子区块
            children_resolution: int = self.resolution + 1
            children: set = h3.h3_to_children(self.h, children_resolution)
            buckets: Dict[str, List[TrajectorySegment]] = defaultdict(lambda: list())
            for s in self.segments["obj"]:
                start: TrajectoryPoint = s.start
                end: TrajectoryPoint = s.end
                start_h = h3.geo_to_h3(start.lat, start.lng, children_resolution)
                end_h = h3.geo_to_h3(end.lat, end.lng, children_resolution)
                if start_h in children:  # lat,lng
                    buckets[start_h].append(s)
                if end_h in children:
                    buckets[end_h].append(s)

            # 2. 初始化7个子区块
            coroutines = [self._childbirth(k, v) for k, v in buckets.items()]
            await asyncio.gather(*coroutines)

            # 3. 更新meta服务
            data = {
                "mother": self.id.id,
                "children": list(children)
            }
            meta_proxy = ActorProxy.create('IndexMetaActor', ActorId("0"), IndexMetaInterface)
            resp: str = await meta_proxy.RegionSplit(data)
            self.logger.info(resp)
            self.retired = True

    @staticmethod
    async def _childbirth(h: str, segments: List[TrajectorySegment]) -> bool:
        proxy = ActorProxy.create('DistributedIndexActor', ActorId(h), DistributedIndexInterface)
        return proxy.InitializeAsANewChildRegion(list(map(asdict, segments)))

    def _need_split(self) -> bool:
        l = len(self.segments) if self.segments is not None else 0
        return self.resolution < 15 and l > SPLIT_THRESHOLD
