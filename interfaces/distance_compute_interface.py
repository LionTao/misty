from typing import Dict

from dapr.actor import ActorInterface, actormethod


class DistanceComputeInterface(ActorInterface):
    @actormethod(name="ComputeHausdorffWithID")
    async def compute_hausdorff_with_id(self, data: dict) -> Dict[str, float]:
        ...

    @actormethod(name="ComputeHausdorffWithProvidedTrack")
    async def compute_hausdorff_with_provided_track(self, data: dict) -> Dict[str, float]:
        ...
