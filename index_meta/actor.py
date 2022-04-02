import traceback
import warnings
from typing import Optional, List

from aiologger.formatters.base import Formatter

warnings.simplefilter(action='ignore', category=FutureWarning)

import h3
import numpy as np
from aiologger import Logger
from aiologger.levels import LogLevel
from dacite import from_dict
from dapr.actor import Actor, ActorId
from dapr.actor.runtime.context import ActorRuntimeContext
from geopandas import GeoDataFrame
from shapely.geometry import box

from interfaces.index_meta_interface import IndexMetaInterface
from interfaces.types import TrajectoryPoint

import aiorwlock

BOSS = "8031fffffffffff,8025fffffffffff,8015fffffffffff,800bfffffffffff,8011fffffffffff,8021fffffffffff,8009fffffffffff,801ffffffffffff,802dfffffffffff,803dfffffffffff,8043fffffffffff,8041fffffffffff,803ffffffffffff,8053fffffffffff,8039fffffffffff,8055fffffffffff,8059fffffffffff,806bfffffffffff,8075fffffffffff,807bfffffffffff,8097fffffffffff,8083fffffffffff,80adfffffffffff,80bdfffffffffff,8005fffffffffff,8017fffffffffff,800dfffffffffff,802ffffffffffff,8065fffffffffff,8061fffffffffff,8069fffffffffff,808dfffffffffff,8095fffffffffff,80a7fffffffffff,80b9fffffffffff,80bffffffffffff,809dfffffffffff,8073fffffffffff,80c9fffffffffff,8013fffffffffff,8007fffffffffff,8001fffffffffff,8003fffffffffff,800ffffffffffff,8027fffffffffff,8049fffffffffff,8029fffffffffff,806dfffffffffff,8045fffffffffff,802bfffffffffff,801bfffffffffff,8067fffffffffff,808bfffffffffff,808ffffffffffff,80b3fffffffffff,80a9fffffffffff,8081fffffffffff,805ffffffffffff,80c3fffffffffff,80cffffffffffff".split(
    ",")


class IndexMetaActor(Actor, IndexMetaInterface):
    def __init__(self, ctx: ActorRuntimeContext, actor_id: ActorId):
        super().__init__(ctx, actor_id)

        # self.gdf: Optional[GeoDataFrame] = GeoDataFrame(
        #     [{"h": h, "geometry": IndexMetaActor.h3_to_box(h)} for h in BOSS])
        self.gdf: Optional[GeoDataFrame] = None

        self.logger = Logger.with_default_handlers(name="index_meta", level=LogLevel.INFO,
                                                   formatter=Formatter("%(name)s-%(asctime)s: %(message)s"))

        self.locker = aiorwlock.RWLock()

    async def region_split(self, data: dict) -> str:
        # TODO: fix double splitting
        async with self.locker.writer_lock:
            try:
                children: List[str] = data["children"]
                new_data = GeoDataFrame([
                    {
                        "h": i,
                        "geometry": IndexMetaActor.h3_to_box(i)
                    }
                    for i in children
                ])
                self.gdf = self.gdf.loc[self.gdf["h"] != data["mother"]]
                self.gdf = self.gdf.append(new_data, ignore_index=True) if self.gdf is not None else GeoDataFrame(
                    new_data)
                self.gdf.drop_duplicates().reset_index(drop=True)
                # if "8031fffffffffff" in data['children'] or "8031fffffffffff" == data['mother']:
                self.logger.info(
                    f"Split {data['mother']} to {data['children']},len:{len(self.gdf)}")
                # self.logger.info(f"len:{len(self.gdf)}")

                return 'SPLIT_RECEIVED'
            except Exception as e:
                self.logger.info("Split failed")
                traceback.print_tb(e.__traceback__)
                print("error:", str(e), flush=True)
                return ""

    async def query(self, data: dict) -> List[str]:
        async with self.locker.writer_lock:
            try:
                start = from_dict(TrajectoryPoint, data["start"])
                end = from_dict(TrajectoryPoint, data["end"])
                start_lng, start_lat = start.lng, start.lat
                end_lng, end_lat = end.lng, end.lat
                start_res = self.gdf.sindex.query(
                    box(start_lng, start_lat, start_lng, start_lat)) if self.gdf is not None else []

                res = set()
                if len(start_res) == 0:
                    self.logger.info(f"start:{start} is orphan")
                    h = h3.geo_to_h3(start_lat, start_lng, 0)
                    if h == "8031fffffffffff":
                        self.logger.warn("Spotted in start")
                    other = GeoDataFrame([{"h": h, "geometry": IndexMetaActor.h3_to_box(h)}])
                    self.gdf = self.gdf.append(other, ignore_index=True) if self.gdf is not None else other
                    res.add(h)
                else:
                    res.update(list(self.gdf.iloc[list(start_res)]["h"]))

                end_res = self.gdf.sindex.query(box(end_lng, end_lat, end_lng, end_lat)) if self.gdf is not None else []
                if len(end_res) == 0:
                    self.logger.info(f"end:{start} is orphan")

                    h = h3.geo_to_h3(end_lat, end_lng, 0)
                    if h == "8031fffffffffff":
                        self.logger.warn("Spotted in end")
                    other = GeoDataFrame([{"h": h, "geometry": IndexMetaActor.h3_to_box(h)}])
                    self.gdf = self.gdf.append(other, ignore_index=True) if self.gdf is not None else other
                    res.add(h)
                else:
                    res.update(list(self.gdf.iloc[list(end_res)]["h"]))
                # self.logger.info(f"\nQuery found:{res},{start},{end}\n{self.gdf}\n")
                return list(res)
            except Exception as e:
                self.logger.info("query failed")
                traceback.print_tb(e.__traceback__)
                print("error:", str(e), flush=True)
                return []

    @staticmethod
    def h3_to_box(h: str) -> box:
        # TODO:墨卡托投影
        coordinates = np.array(h3.h3_set_to_multi_polygon([h], geo_json=False))
        # 注意lng lat顺序，h3是lat lng
        max_y, max_x = coordinates[0][0].max(axis=0)
        min_y, min_x = coordinates[0][0].min(axis=0)
        return box(min_x, min_y, max_x, max_y)
