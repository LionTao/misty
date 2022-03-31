import traceback
import warnings
from typing import Optional, List

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


class IndexMetaActor(Actor, IndexMetaInterface):
    def __init__(self, ctx: ActorRuntimeContext, actor_id: ActorId):
        super().__init__(ctx, actor_id)

        self.gdf: Optional[GeoDataFrame] = None

        self.logger = Logger.with_default_handlers(name="index_meta", level=LogLevel.INFO)

    async def region_split(self, data: dict) -> str:
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
            self.gdf = self.gdf.append(new_data, ignore_index=True) if self.gdf is not None else GeoDataFrame(new_data)

            self.logger.info(f"Split {data['mother']} to {data['children']}")

            return 'SPLIT_RECEIVED'
        except Exception as e:
            self.logger.info("Split failed")
            traceback.print_tb(e.__traceback__)
            print("error:", str(e), flush=True)
            return ""

    async def query(self, data: dict) -> List[str]:
        try:
            start = from_dict(TrajectoryPoint, data["start"])
            end = from_dict(TrajectoryPoint, data["end"])
            start_lng, start_lat = start.lng, start.lat
            end_lng, end_lat = end.lng, end.lat
            start_res = self.gdf.sindex.query(
                box(start_lng, start_lat, start_lng, start_lat)) if self.gdf is not None else []

            res = set()
            if len(start_res) == 0:
                h = h3.geo_to_h3(start_lat, start_lng, 0)
                other = GeoDataFrame([{"h": h, "geometry": IndexMetaActor.h3_to_box(h)}])
                self.gdf = self.gdf.append(other, ignore_index=True) if self.gdf is not None else other
                res.add(h)
            else:
                res.update(list(self.gdf.iloc[list(start_res)]["h"]))

            end_res = self.gdf.sindex.query(box(end_lng, end_lat, end_lng, end_lat)) if self.gdf is not None else []
            if len(end_res) == 0:
                h = h3.geo_to_h3(end_lat, end_lng, 0)
                other = GeoDataFrame([{"h": h, "geometry": IndexMetaActor.h3_to_box(h)}])
                self.gdf = self.gdf.append(other, ignore_index=True) if self.gdf is not None else other
                res.add(h)
            else:
                res.update(list(self.gdf.iloc[list(end_res)]["h"]))
            self.logger.info(f"Query found:{len(res)} targets,len:{len(self.gdf)}")
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
