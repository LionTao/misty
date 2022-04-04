from typing import Dict

from dapr.actor import ActorInterface, actormethod


class QueryAgentInterface(ActorInterface):
    @actormethod(name="QueryWithId")
    async def query_with_id(self, target_id: str, threshold: float) -> Dict[str, float]:
        ...
