from datetime import timedelta

from dapr.actor import ActorRuntime
from dapr.actor.runtime.config import ActorRuntimeConfig, ActorReentrancyConfig
from dapr.ext.fastapi import DaprActor  # type: ignore
from fastapi import FastAPI  # type: ignore

from compute.actor import DistanceComputeActor

ActorRuntime.set_actor_config(
    ActorRuntimeConfig(
        actor_idle_timeout=timedelta(hours=10),
        actor_scan_interval=timedelta(minutes=1),
        drain_ongoing_call_timeout=timedelta(minutes=2),
        drain_rebalanced_actors=True,
        reentrancy=ActorReentrancyConfig(enabled=False),
    )
)
app = FastAPI(title=f'{DistanceComputeActor.__name__}Service')
# Add Dapr Actor Extension
actor = DaprActor(app)


@app.on_event("startup")
async def startup_event():
    # Register
    await actor.register_actor(DistanceComputeActor)
