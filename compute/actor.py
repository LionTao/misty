from typing import List, Dict

import numpy as np
import traj_dist.distance as tdist
from dacite import from_dict
from dapr.actor import Actor, ActorId, ActorProxy
from dapr.actor.runtime.context import ActorRuntimeContext

from interfaces.distance_compute_interface import DistanceComputeInterface
from interfaces.trajectory_assembler_interface import TrajectoryAssemblerInterface
from interfaces.types import TrajectoryPoint


class DistanceComputeActor(Actor, DistanceComputeInterface):
    def __init__(self, ctx: ActorRuntimeContext, actor_id: ActorId):
        super().__init__(ctx, actor_id)

    @staticmethod
    async def _get_trajectory_to_numpy(i: str) -> np.ndarray:
        proxy = ActorProxy.create('TrajectoryAssemblerActor', ActorId(i), TrajectoryAssemblerInterface)
        raw_data: List[TrajectoryPoint] = [from_dict(TrajectoryPoint, i) for i in
                                           await proxy.Query()]
        return np.array([[i.lng, i.lat] for i in raw_data])

    async def compute_hausdorff_with_id(self, data: dict) -> Dict[str, float]:
        target_trajectory_id: str = data["target_trajectory_id"]
        candidates: List[str] = data["candidates"]
        target = await self._get_trajectory_to_numpy(target_trajectory_id)
        return await self._calculate_result(target, candidates)

    async def compute_hausdorff_with_provided_track(self, data: dict) -> Dict[str, float]:
        target_trajectory: List[List[float]] = data["target_trajectory"]
        candidates: List[str] = data["candidates"]
        target = np.array(target_trajectory)
        return await self._calculate_result(target, candidates)

    async def _calculate_result(self, target_trajectory: np.ndarray, candidates: List[str]) -> Dict[str, float]:
        res = dict()
        for candidate_id in candidates:
            candidate: np.ndarray = await self._get_trajectory_to_numpy(candidate_id)
            res[candidate_id] = tdist.hausdorff(target_trajectory, candidate, "spherical")
        return res
