from typing import Dict

from dapr.actor import ActorInterface, actormethod


class AccumulatorInterface(ActorInterface):
    @actormethod(name="Add")
    async def add(self):
        ...

    @actormethod(name="MetaAdd")
    async def meta_add(self):
        ...

    @actormethod(name="Get")
    async def get(self) -> Dict[str, int]:
        ...
