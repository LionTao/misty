from typing import List, Tuple

from dapr.actor import ActorInterface, actormethod


class DistributedIndexInterface(ActorInterface):
    @actormethod(name="AcceptNewSegment")
    async def accept_new_segment(self, segment: dict) -> Tuple[bool, int]:
        ...

    @actormethod(name="InitializeAsANewChildRegion")
    async def initialize_as_a_new_child_region(self, segments: List[dict]) -> bool:
        ...

    @actormethod(name="Query")
    async def query(self, wkt_string: str) -> List[int]:
        ...

    async def split(self) -> None:
        ...
