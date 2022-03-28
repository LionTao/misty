from dapr.actor import ActorInterface, actormethod

from interfaces.my_types import TrajectoryPoint


class TrajectoryAssemblerInterface(ActorInterface):
    @actormethod(name="AcceptNewPoint")
    async def accept_new_point(self, p: TrajectoryPoint) -> bool:
        ...
