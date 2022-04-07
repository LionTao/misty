import asyncio
import datetime
import sys
import os
sys.path.append(os.curdir)
print(sys.path)
from dataclasses import asdict
from typing import List
from dapr.actor import ActorProxy
from dapr.actor.id import ActorId

from interfaces.index_meta_interface import IndexMetaInterface
from interfaces.types import TrajectoryPoint

prev = TrajectoryPoint(9, datetime.datetime(2008, 2, 4, 7, 39, 28), 116.4769, 39.9547)
p = TrajectoryPoint(9, datetime.datetime(2008, 2, 4, 7, 40, 30), 116.47802, 39.95492)


async def main():
    data = {
        "start": asdict(prev),
        "end": asdict(p)
    }

    meta_proxy = ActorProxy.create('IndexMetaActor', ActorId("0"), IndexMetaInterface)
    target_regions: List[str] = await meta_proxy.Query(data)
    print(target_regions)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
