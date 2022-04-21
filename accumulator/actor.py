from typing import Dict

import aiorwlock
from dapr.actor import Actor
from dapr.actor.id import ActorId
from dapr.actor.runtime.context import ActorRuntimeContext

from interfaces.accumulator_interface import AccumulatorInterface


class AccumulatorActor(Actor, AccumulatorInterface):
    def __init__(self, ctx: ActorRuntimeContext, actor_id: ActorId):
        super().__init__(ctx, actor_id)
        self.tree_counter = 0
        self.meta_counter = 0
        self.locker = aiorwlock.RWLock()

    async def add(self):
        async with self.locker.writer_lock:
            self.tree_counter += 1

    async def meta_add(self):
        async with self.locker.writer_lock:
            self.tree_counter += 1
            self.meta_counter += 1

    async def get(self) -> Dict[str, int]:
        async with self.locker.reader_lock:
            return {"tree": self.tree_counter, "meta": self.meta_counter}
