from typing import Dict, List

from dacite import from_dict
from dapr.actor import Actor, ActorId, ActorProxy
from dapr.actor.runtime.context import ActorRuntimeContext

from interfaces.query_agent_interface import QueryAgentInterface
from interfaces.trajectory_assembler_interface import TrajectoryAssemblerInterface
from interfaces.types import TrajectoryPoint


class TrajectoryAssemblerActor(Actor, QueryAgentInterface):
    def __init__(self, ctx: ActorRuntimeContext, actor_id: ActorId):
        super().__init__(ctx, actor_id)

    async def query_with_id(self, target_id: str, threshold: float) -> Dict[str, float]:
        # 1. 拿到轨迹所有的点
        proxy = ActorProxy.create('TrajectoryAssemblerActor', ActorId(target_id), TrajectoryAssemblerInterface)
        target: List[TrajectoryPoint] = [from_dict(TrajectoryPoint, i) for i in await proxy.Query()]
        # 2. 两两分组组成轨迹段
        # 3. 加上范围相乘矩形列表
        # 4. 加上每个点的圆形
        # 5. 组成复合polygon送meta查询rtree
        # 6. 根据meta返回的index id调用query取得候选轨迹id
        # 7. 发送compute计算距离
        # 8. 返回结果
        pass
