from dataclasses import asdict
from typing import List, Optional

from aiologger import Logger
from aiologger.levels import LogLevel
from dacite import from_dict
from dapr.actor import Actor
from dapr.actor.id import ActorId
from dapr.actor.runtime.context import ActorRuntimeContext

from interfaces.trajectory_assembler_interface import TrajectoryAssemblerInterface
from interfaces.types import TrajectoryPoint


class TrajectoryAssemblerActor(Actor, TrajectoryAssemblerInterface):
    def __init__(self, ctx: ActorRuntimeContext, actor_id: ActorId):
        super().__init__(ctx, actor_id)
        self.PREVIOUS_POINT_STATE_KEY = f"previous_point_{self.id}"
        self.TRAJECTORY_STATE_KEY = f"trajectory_{self.id}"

        self.previous_point: Optional[TrajectoryPoint] = None
        self.trajectory: Optional[List[TrajectoryPoint]] = None
        self.logger = Logger.with_default_handlers(name=f"TrajectoryAssembler_{self.id}", level=LogLevel.INFO)

    async def accept_new_point(self, p: dict) -> None:
        if self.previous_point:
            p: TrajectoryPoint = from_dict(TrajectoryPoint, p)
            prev = self.previous_point
            t = self.trajectory
            t.append(p)
            await self.__save_previous_point(p)
            await self.__save_trajectory(t)
            # TODO: 发送到index模块
            # self.logger.warn(f"Dummy sending: {[prev, p]}")
        else:
            p: TrajectoryPoint = from_dict(TrajectoryPoint, p)
            await self.__save_previous_point(p)
            await self.__save_trajectory([p])

    async def __retrieve_previous_point(self) -> Optional[TrajectoryPoint]:
        has_value, p = await self._state_manager.try_get_state(self.PREVIOUS_POINT_STATE_KEY)
        if has_value:
            val: TrajectoryPoint = from_dict(TrajectoryPoint, p)
            self.previous_point = val
            self.logger.info("Got previous_point restored")
            return self.previous_point
        else:
            self.logger.info("No previous_point available")
            return None

    async def __save_previous_point(self, p: TrajectoryPoint) -> None:
        self.previous_point = p
        await self._state_manager.set_state(self.PREVIOUS_POINT_STATE_KEY, asdict(self.previous_point))
        await self._state_manager.save_state()

    async def __save_trajectory(self, t: List[TrajectoryPoint]) -> None:
        self.trajectory = t
        await self._state_manager.set_state(self.TRAJECTORY_STATE_KEY, list(map(asdict, self.trajectory)))
        await self._state_manager.save_state()

    async def __retrieve_trajectory(self) -> Optional[List[TrajectoryPoint]]:
        has_value, val = await self._state_manager.try_get_state(self.TRAJECTORY_STATE_KEY)
        if has_value:
            self.trajectory = list(map(lambda x: from_dict(TrajectoryPoint, x), val))
            self.logger.info("Got trajectory restored")
            return self.trajectory
        else:
            self.logger.info("No trajectory available")
            return None

    async def _on_activate(self) -> None:
        await self.__retrieve_previous_point()
        await self.__retrieve_trajectory()
        self.logger.info(f"{self.id}_TrajectoryAssembler activated")

    async def _on_deactivate(self) -> None:
        await self._state_manager.set_state(self.PREVIOUS_POINT_STATE_KEY, self.previous_point)
        await self._state_manager.set_state(self.TRAJECTORY_STATE_KEY, self.trajectory)
        await self._state_manager.save_state()
        self.logger.info("State backuped")
        await self.logger.shutdown()
