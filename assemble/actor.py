import asyncio
import traceback
from asyncio import Queue
from dataclasses import asdict
from typing import List, Optional, Tuple, Set

import h3
from aiologger import Logger
from aiologger.formatters.base import Formatter
from aiologger.levels import LogLevel
from dacite import from_dict
from dapr.actor import Actor, ActorProxy
from dapr.actor.id import ActorId
from dapr.actor.runtime.context import ActorRuntimeContext

from interfaces.distributed_index_interface import DistributedIndexInterface
from interfaces.index_meta_interface import IndexMetaInterface
from interfaces.trajectory_assembler_interface import TrajectoryAssemblerInterface
from interfaces.types import TrajectoryPoint, TrajectorySegment


class TrajectoryAssemblerActor(Actor, TrajectoryAssemblerInterface):
    def __init__(self, ctx: ActorRuntimeContext, actor_id: ActorId):
        super().__init__(ctx, actor_id)
        self.PREVIOUS_POINT_STATE_KEY = f"previous_point_{self.id.id}"
        self.TRAJECTORY_STATE_KEY = f"trajectory_{self.id.id}"

        self.previous_point: Optional[TrajectoryPoint] = None
        self.trajectory: Optional[List[TrajectoryPoint]] = None
        self.logger = Logger.with_default_handlers(name=f"{self.__class__.__name__}_{self.id.id}", level=LogLevel.INFO,
                                                   formatter=Formatter("%(name)s-%(asctime)s: %(message)s"))

    async def query(self) -> List[dict]:
        return list(map(asdict, self.trajectory))

    async def accept_new_point(self, p: dict) -> bool:
        try:
            if self.previous_point:
                p: TrajectoryPoint = from_dict(TrajectoryPoint, p)
                prev = self.previous_point
                dis = h3.point_dist((p.lat, p.lng), (prev.lat, prev.lng), unit='km')
                if dis > 100:
                    # 怪异错误点，直接剔除
                    return True
                # 发送到index模块
                # try:
                data = {
                    "start": asdict(prev),
                    "end": asdict(p)
                }

                await self._send_to_regions(data, prev, p)
                # 发送可能会出错，因此本地状态更新最后做以保证重试正确性
                t = self.trajectory
                t.append(p)
                await self._save_previous_point(p)
                await self._save_trajectory(t)
                return True
            else:
                p: TrajectoryPoint = from_dict(TrajectoryPoint, p)
                await self._save_previous_point(p)
                await self._save_trajectory([p])
                return True
        except asyncio.TimeoutError:
            print({"results": f"timeout error"}, flush=True)
            return False
        except Exception as e:
            traceback.print_exc()
            print("error:", e, flush=True)
            return False

    async def _send_to_regions(self, data, prev: TrajectoryPoint, p: TrajectoryPoint) -> None:
        meta_proxy = ActorProxy.create('IndexMetaActor', ActorId("0"), IndexMetaInterface)
        init_regions: List[str] = await meta_proxy.Query(data)
        # coroutines = []
        q: Queue[int] = Queue()
        for r in init_regions:
            proxy = ActorProxy.create('DistributedIndexActor', ActorId(r), DistributedIndexInterface)
            # coroutines.append(proxy.AcceptNewSegment(asdict(TrajectorySegment(int(self.id.id), prev, p))))
            res: Tuple[bool, int] = await proxy.AcceptNewSegment(asdict(TrajectorySegment(self.id.id, prev, p)))
            if not res[0]:
                await q.put(res[1])
        while not q.empty():
            resolution = await q.get()
            target_regions: Set[str] = {
                h3.geo_to_h3(prev.lat, prev.lng, resolution),
                h3.geo_to_h3(p.lat, p.lng, resolution)
            }
            for r in target_regions:
                await self.logger.warning(f"Retrying: {r},queue size:{q.qsize()}")
                proxy = ActorProxy.create('DistributedIndexActor', ActorId(r), DistributedIndexInterface)
                # coroutines.append(proxy.AcceptNewSegment(asdict(TrajectorySegment(int(self.id.id), prev, p))))
                res: Tuple[bool, int] = await proxy.AcceptNewSegment(
                    asdict(TrajectorySegment(self.id.id, prev, p)))
                if not res[0]:
                    await q.put(res[1])

        # return await asyncio.gather(*coroutines)

    async def _retrieve_previous_point(self) -> Optional[TrajectoryPoint]:
        has_value, p = await self._state_manager.try_get_state(self.PREVIOUS_POINT_STATE_KEY)
        if has_value:
            val: TrajectoryPoint = from_dict(TrajectoryPoint, p)
            self.previous_point = val
            await self.logger.info("Got previous_point restored")
            return self.previous_point
        else:
            # self.logger.info("No previous_point available")
            return None

    async def _save_previous_point(self, p: TrajectoryPoint) -> None:
        self.previous_point = p
        await self._state_manager.set_state(self.PREVIOUS_POINT_STATE_KEY, asdict(self.previous_point))
        # await self._state_manager.save_state()

    async def _save_trajectory(self, t: List[TrajectoryPoint]) -> None:
        """
        更新全局状态的轨迹信息
        """
        self.trajectory = t
        await self._state_manager.set_state(self.TRAJECTORY_STATE_KEY, list(map(asdict, self.trajectory)))
        # await self._state_manager.save_state()

    async def _retrieve_trajectory(self) -> Optional[List[TrajectoryPoint]]:
        has_value, val = await self._state_manager.try_get_state(self.TRAJECTORY_STATE_KEY)
        if has_value:
            self.trajectory = list(map(lambda x: from_dict(TrajectoryPoint, x), val))
            await self.logger.info(f"{self.id.id}_Got trajectory restored:{self.trajectory}")
            return self.trajectory
        else:
            await self.logger.info("No trajectory available")
            return None

    async def _on_activate(self) -> None:
        await self._retrieve_previous_point()
        await self._retrieve_trajectory()
        await self.logger.info(f"{self.id}_TrajectoryAssembler activated")

    async def _on_deactivate(self) -> None:
        await self.logger.info("Deactivated")
        await self.logger.shutdown()
