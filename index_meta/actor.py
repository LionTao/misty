import json
import time
import traceback
import warnings
from typing import Optional, List

from aiologger.formatters.base import Formatter
from pyproj import Transformer
from shapely import wkt

warnings.simplefilter(action='ignore', category=FutureWarning)

import h3
import numpy as np
from aiologger import Logger
from aiologger.levels import LogLevel
from dacite import from_dict
from dapr.actor import Actor, ActorId
from dapr.actor.runtime.context import ActorRuntimeContext
from geopandas import GeoDataFrame
from shapely.geometry import box, Polygon

from interfaces.index_meta_interface import IndexMetaInterface
from interfaces.types import TrajectoryPoint

import aiorwlock

BOSS = h3.get_res0_indexes()

with open("tests/parameters.json") as f:
    INIT_RESOLUTION = json.load(f)["INIT_RESOLUTION"]

print(f"{INIT_RESOLUTION=}", flush=True)


class IndexMetaActor(Actor, IndexMetaInterface):
    def __init__(self, ctx: ActorRuntimeContext, actor_id: ActorId):
        super().__init__(ctx, actor_id)

        # self.gdf: Optional[GeoDataFrame] = GeoDataFrame(
        #     [{"h": h, "geometry": IndexMetaActor.h3_to_box(h)} for h in BOSS])
        self.gdf: Optional[GeoDataFrame] = None

        # 球面坐标转墨卡托平面投影
        self.transformer = Transformer.from_crs(4326, 3857, always_xy=True)

        self.logger = Logger.with_default_handlers(name="index_meta", level=LogLevel.INFO,
                                                   formatter=Formatter("%(name)s-%(asctime)s: %(message)s"))

        self.locker = aiorwlock.RWLock()

    async def _is_region_retired(self, h: str) -> bool:
        has_value, _ = await self._state_manager.try_get_state(f"DistributedIndexActor_{h}_retired")
        # await self.logger.info(f"{h} retired but queried")
        return has_value

    async def region_split(self, data: dict) -> str:
        # TODO: fix double splitting
        async with self.locker.writer_lock:
            try:
                start = time.perf_counter()
                children: List[str] = []
                for h in data["children"]:
                    is_retired = await self._is_region_retired(h)
                    if not is_retired:
                        children.append(h)
                new_data = GeoDataFrame([
                    {
                        "h": i,
                        "geometry": self.h3_to_box(i)
                    }
                    for i in children
                ], crs="EPSG:3857")
                self.gdf = self.gdf.loc[self.gdf["h"] != data["mother"]]
                self.gdf = self.gdf.append(new_data, ignore_index=True) if self.gdf is not None else GeoDataFrame(
                    new_data)
                self.gdf.drop_duplicates().reset_index(drop=True)
                # if "8031fffffffffff" in data['children'] or "8031fffffffffff" == data['mother']:
                await self.logger.info(
                    f"Split {data['mother']} to {children},len:{len(self.gdf)}, using: {time.perf_counter() - start}s")
                # self.logger.info(f"len:{len(self.gdf)}")

                return 'SPLIT_RECEIVED'
            except Exception as e:
                await self.logger.info("Split failed")
                traceback.print_tb(e.__traceback__)
                print("error:", str(e), flush=True)
                return ""

    async def query(self, data: dict) -> List[str]:
        try:
            start = from_dict(TrajectoryPoint, data["start"])
            end = from_dict(TrajectoryPoint, data["end"])
            start_lng, start_lat = start.lng, start.lat
            end_lng, end_lat = end.lng, end.lat
            async with self.locker.reader_lock:
                start_res = self.gdf.sindex.query(
                    box(start_lng, start_lat, start_lng, start_lat)) if self.gdf is not None else []

            res = set()
            if len(start_res) == 0:
                # await self.logger.info(f"start:{start} is orphan")
                h = h3.geo_to_h3(start_lat, start_lng, INIT_RESOLUTION)
                # if h == "8031fffffffffff":
                #     await self.logger.warn("Spotted in start")
                other = GeoDataFrame([{"h": h, "geometry": self.h3_to_box(h)}], crs="EPSG:3857")
                async with self.locker.writer_lock:
                    self.gdf = self.gdf.append(other, ignore_index=True) if self.gdf is not None else other
                res.add(h)
            else:
                async with self.locker.writer_lock:
                    res.update(list(self.gdf.iloc[list(start_res)]["h"]))
            async with self.locker.reader_lock:
                end_res = self.gdf.sindex.query(box(end_lng, end_lat, end_lng, end_lat)) if self.gdf is not None else []
            if len(end_res) == 0:
                # await self.logger.info(f"end:{start} is orphan")

                h = h3.geo_to_h3(end_lat, end_lng, INIT_RESOLUTION)
                if h == "8031fffffffffff":
                    await self.logger.warn("Spotted in end")
                other = GeoDataFrame([{"h": h, "geometry": self.h3_to_box(h)}], crs="EPSG:3857")
                async with self.locker.writer_lock:
                    self.gdf = self.gdf.append(other, ignore_index=True) if self.gdf is not None else other
                res.add(h)
            else:
                async with self.locker.writer_lock:
                    res.update(list(self.gdf.iloc[list(end_res)]["h"]))
            # self.logger.info(f"\nQuery found:{res},{start},{end}\n{self.gdf}\n")
            return list(res)
        except Exception as e:
            await self.logger.info("query failed")
            traceback.print_tb(e.__traceback__)
            print("error:", str(e), flush=True)
            return []

    async def agent_query(self, wkt_string: str) -> List[str]:
        async with self.locker.reader_lock:
            p: Polygon = wkt.loads(wkt_string)
            res = self.gdf.sindex.query(p) if self.gdf is not None else []
            return list(set(self.gdf.iloc[list(res)]["h"]))

    def h3_to_box(self, h: str) -> box:
        # TODO:墨卡托投影
        coordinates = np.array(h3.h3_set_to_multi_polygon([h], geo_json=False))
        # 注意lng lat顺序，h3是lat lng
        max_y, max_x = coordinates[0][0].max(axis=0)
        min_y, min_x = coordinates[0][0].min(axis=0)
        max_x, max_y = self.transformer.transform(max_x, max_y)
        min_x, min_y = self.transformer.transform(min_x, min_y)
        return box(min_x, min_y, max_x, max_y)
