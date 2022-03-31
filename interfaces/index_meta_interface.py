from typing import List

from dapr.actor import ActorInterface, actormethod


class IndexMetaInterface(ActorInterface):
    @actormethod(name="RegionSplit")
    async def region_split(self, data: dict) -> str:
        ...

    @actormethod(name="Query")
    async def query(self, data: dict) -> List[str]:
        ...
