from typing import List

from dapr.actor import ActorInterface, actormethod


class DistributedIndexInterface(ActorInterface):
    @actormethod(name="AcceptNewSegment")
    async def accept_new_segment(self, segment: dict) -> None:
        ...

    @actormethod(name="InitializeAsANewChildRegion")
    async def initialize_as_a_new_child_region(self, segments: List[dict]) -> bool:
        ...

    @actormethod(name="Query")
    async def query(self, mbr: dict, threshold: float) -> List[int]:
        ...

    async def split(self) -> None:
        ...
