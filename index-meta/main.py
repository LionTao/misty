import json
from typing import Optional, List

import h3
import numpy as np
from dapr.ext.grpc import App, InvokeMethodRequest, InvokeMethodResponse
from geopandas import GeoDataFrame
from shapely.geometry import box

app = App()
gdf: Optional[GeoDataFrame] = None


@app.method(name='region-split')
def region_split(request: InvokeMethodRequest) -> InvokeMethodResponse:
    global gdf
    data = json.loads(request.text())
    children: List[str] = data["children"]
    new_data = GeoDataFrame([
        {
            "h": i,
            "geometry": h3_to_box(i)
        }
        for i in children
    ])

    gdf: GeoDataFrame = gdf.loc["h" != data["mother"]]
    gdf: GeoDataFrame = gdf.append(new_data)

    return InvokeMethodResponse(b'SPLIT_RECEIVED', "text/plain; charset=UTF-8")


@app.method(name="query")
def query(request: InvokeMethodRequest) -> InvokeMethodResponse:
    global gdf
    data: List[float] = json.loads(request.text())
    lng, lat = data[0], data[1]
    res = gdf.sindex.query(box(lng, lat, lng, lat))
    if len(res) == 0:
        h = h3.geoToH3(lat, lng, 0)
        gdf = gdf.append(GeoDataFrame([{"h": h, "geometry": h3_to_box(h)}]))
        return h
    else:
        return gdf.iloc[res]["h"]


def h3_to_box(h: str) -> box:
    # TODO:墨卡托投影
    coordinates = np.array(h3.h3_set_to_multi_polygon([h], geo_json=False))
    max_x, max_y = coordinates[0][0].max(axis=0)
    min_x, min_y = coordinates[0][0].min(axis=0)
    return box(min_x, min_y, max_x, max_y)


app.run(50051)
