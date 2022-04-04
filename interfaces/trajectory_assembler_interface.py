from typing import List

from dapr.actor import ActorInterface, actormethod


class TrajectoryAssemblerInterface(ActorInterface):
    @actormethod(name="AcceptNewPoint")
    async def accept_new_point(self, p: dict) -> bool:
        ...

    @actormethod(name="Query")
    async def query(self) -> List[dict]:
        ...
