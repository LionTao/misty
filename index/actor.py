import json
import traceback
import warnings
from collections import defaultdict
from dataclasses import asdict
from typing import Optional, List, Dict, Set, Tuple

import aiorwlock
import h3
from aiologger import Logger
from aiologger.formatters.base import Formatter
from aiologger.levels import LogLevel
from dacite import from_dict
from dapr.actor import Actor, ActorId, ActorProxy
from dapr.actor.runtime._method_context import ActorMethodContext
from dapr.actor.runtime.context import ActorRuntimeContext

from interfaces.accumulator_interface import AccumulatorInterface

warnings.simplefilter(action='ignore', category=FutureWarning)
from geopandas import GeoDataFrame
from pyproj import Transformer
from shapely import wkt
from shapely.geometry import LineString

from interfaces.distributed_index_interface import DistributedIndexInterface
from interfaces.index_meta_interface import IndexMetaInterface
from interfaces.types import TrajectorySegment, TrajectoryPoint

with open("tests/parameters.json") as f:
    para: dict = json.load(f)
    # buffer里最大存储数量
    MAX_BUFFER_SIZE = para.get("MAX_BUFFER_SIZE", 50)
    # buffer里数量:树里的数量
    TREE_INSERTION_THRESHOLD = para.get("TREE_INSERTION_THRESHOLD", 0.2)
    # 分裂所需的树索引阈值
    SPLIT_THRESHOLD = para.get("SPLIT_THRESHOLD", 2000)

print(f"{MAX_BUFFER_SIZE=}", flush=True)
print(f"{TREE_INSERTION_THRESHOLD=}", flush=True)
print(f"{SPLIT_THRESHOLD=}", flush=True)


class DistributedIndexActor(Actor, DistributedIndexInterface):
    """
    自动分裂的分布式轨迹段空间索引
    """

    def __init__(self, ctx: ActorRuntimeContext, actor_id: ActorId):
        super().__init__(ctx, actor_id)

        self.STATE_KEY = f"DistributedIndexActor_{self.id.id}"
        self.BUFFER_KEY = f"DistributedIndexActor_{self.id.id}_buffer"
        self.RETIRED_KRY = f"DistributedIndexActor_{self.id.id}_retired"

        self.retired: bool = False

        # h3 grid id
        self.h: str = self.id.id
        # 当前的分辨率
        self.resolution: int = h3.h3_get_resolution(self.h)

        # 球面坐标转墨卡托平面投影
        self.transformer = Transformer.from_crs(4326, 3857, always_xy=True)

        # str-tree索引
        self.segments: Optional[GeoDataFrame] = None
        # Buffer
        self.cache: Set[TrajectorySegment] = set()

        self.logger = Logger.with_default_handlers(name=f"DistributedIndex_{self.id.id}", level=LogLevel.INFO,
                                                   formatter=Formatter("%(name)s-%(asctime)s: %(message)s"))

        self.lock = aiorwlock.RWLock()
        self.full = False

    async def _on_activate(self) -> None:
        has_value, p = await self._state_manager.try_get_state(self.BUFFER_KEY)
        if has_value:
            val: Set[TrajectorySegment] = set(map(lambda x: from_dict(TrajectorySegment, x), p))
            self.cache.update(val)
        has_value, p = await self._state_manager.try_get_state(self.STATE_KEY)
        if has_value:
            val: Set[TrajectorySegment] = set(map(lambda x: from_dict(TrajectorySegment, x), p))
            self.cache.update(val)
            # self.logger.info("Got segments restored")
        # else:
        #     self.logger.info("No previous_segments available")
        has_value, p = await self._state_manager.try_get_state(self.RETIRED_KRY)
        if has_value:
            val: bool = p
            self.retired = val
            if self.retired:
                await self.logger.warn(f"Why wake {self.id.id}_region up?")
        await self.logger.info(f"{self.id}_region activated")

    async def _on_deactivate(self) -> None:
        await self.logger.info("Deactivated")
        await self.logger.shutdown()

    async def _on_post_actor_method(self, method_context: ActorMethodContext):
        if self.segments is not None:
            await self._state_manager.set_state(self.STATE_KEY, [asdict(i) for i in self.segments["obj"]])
        await self._state_manager.set_state(self.RETIRED_KRY, self.retired)
        await self._state_manager.set_state(self.BUFFER_KEY, [asdict(i) for i in self.cache])

    async def accept_new_segment(self, segment: dict) -> Tuple[bool, int]:
        """
        接受一个轨迹段并先插入buffer
        """
        try:
            if self.retired:
                return False, self.resolution + 1
            async with self.lock.writer_lock:
                s: TrajectorySegment = from_dict(TrajectorySegment, segment)
                if self.cache:
                    self.cache.add(s)
                else:
                    self.cache = {s}
                await self._check_insertion()
                # self.logger.info(
                #     f"Buffer: {len(self.cache) if self.cache else 0}, Tree: {len(self.segments) if self.segments is not None else 0}")
                return True, self.resolution
        except Exception as e:
            traceback.print_exc()
            print("!error:", str(e), e.__context__, flush=True)
            return False, self.resolution

    async def _check_insertion(self):
        """
        看看是不是要进行合并
        """
        if self._need_insertion():
            await self._do_insertion()
        await self._check_split()

    async def _do_insertion(self):
        """
        进行合并
        """
        gdf = self._cache_to_gdf()
        if self.segments is not None and gdf is not None:
            self.segments = self.segments.append(gdf, ignore_index=True)
            accumulator_proxy = ActorProxy.create('AccumulatorActor', ActorId("0"), AccumulatorInterface)
            await accumulator_proxy.Add()
        elif self.segments is not None:
            pass
        else:
            self.segments = gdf
            accumulator_proxy = ActorProxy.create('AccumulatorActor', ActorId("0"), AccumulatorInterface)
            await accumulator_proxy.Add()
        self.cache = set()

    def _cache_to_gdf(self) -> Optional[GeoDataFrame]:
        """
        buffer转成gdf
        """
        # TODO: 转换墨卡托投影
        if self.cache is not None:
            d = [{"id": i.id, "obj": i, "geometry": LineString([self.transformer.transform(i.start.lng, i.start.lat),
                                                                self.transformer.transform(i.end.lng, i.end.lat)])}
                 for i
                 in
                 self.cache]
            return GeoDataFrame(d, crs="EPSG:3857")
        else:
            return None

    def _need_insertion(self) -> bool:
        return len(self.cache) > MAX_BUFFER_SIZE or (
                self.segments is not None and (len(self.cache) / len(self.segments)) > TREE_INSERTION_THRESHOLD)

    async def initialize_as_a_new_child_region(self, segments: List[dict]) -> bool:
        """
        接受母亲那来的一堆轨迹段进行初始化
        """
        if self.cache:
            self.cache.update(list(map(lambda x: from_dict(TrajectorySegment, x), segments)))
        else:
            self.cache = set(list(map(lambda x: from_dict(TrajectorySegment, x), segments)))
        await self._check_insertion()
        return True

    async def query(self, wkt_string: str) -> Tuple[bool, List[int]]:
        """
        根据给定的mbr进行查询
        """
        # check retired may ended up in an endless chasing
        # if self.retired:
        #     return False, [self.resolution + 1]
        # TODO: use threshold
        # TODO: 转换墨卡托投影
        res = set()
        mbr_polygon = wkt.loads(wkt_string)
        async with self.lock.reader_lock:
            if self.segments is not None:
                ids = self.segments.sindex.query(mbr_polygon)
                if len(ids) > 0:
                    res.update(self.segments.iloc[list(ids)]["id"])
            if self.cache:
                for i in self.cache:
                    if mbr_polygon.intersects(
                            LineString([self.transformer.transform(i.start.lng, i.start.lat),
                                        self.transformer.transform(i.end.lng, i.end.lat)])):
                        res.add(i.id)
            return True, list(res)

    async def _check_split(self):
        """
        查看是否要分裂
        """
        if await self._need_split():
            await self._do_insertion()
            await self.logger.info(
                f"Buffer: {len(self.cache) if self.cache else 0}, Tree: {len(self.segments) if self.segments is not None else 0}")
            # 1. 切分数据到16个子区块
            children_resolution: int = self.resolution + 1
            children: set = self.split_h3_area(self.h, children_resolution)
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

            # 3. 更新meta服务
            data = {
                "mother": self.id.id,
                "children": list(children)
            }

            if "8031fffffffffff" in children:
                await self.logger.warn("SHITTTTTTTTTTTTT")
            meta_proxy = ActorProxy.create('IndexMetaActor', ActorId("0"), IndexMetaInterface)
            resp: str = await meta_proxy.RegionSplit(data)
            self.retired = True
            await self._state_manager.set_state(self.RETIRED_KRY, self.retired)
            await self._state_manager.save_state()

            # 2. 初始化16个子区块
            for k, v in buckets.items():
                await self._childbirth(k, v)
            await self.logger.info(f"{resp}, I'm retired")

    @staticmethod
    def split_h3_area(h: str, resolution: int) -> Set[str]:
        """
        因为扩展问题需要扩长度为2以完全包裹上一层
        """
        center: str = h3.h3_to_center_child(h, resolution)
        return h3.k_ring(center, 2)

    @staticmethod
    async def _childbirth(h: str, segments: List[TrajectorySegment]) -> bool:
        proxy = ActorProxy.create('DistributedIndexActor', ActorId(h), DistributedIndexInterface)
        return await proxy.InitializeAsANewChildRegion(list(map(asdict, segments)))

    async def _need_split(self) -> bool:
        ratio = len(self.segments) if self.segments is not None else 0
        if self.resolution == 15 and not self.full:
            await self.logger.critical(f"{self.id.id}_No more cells left")
            self.full = True
            return False
        return self.resolution < 15 and ratio > SPLIT_THRESHOLD
