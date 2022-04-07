from datetime import timedelta

from dapr.actor import ActorRuntime
from dapr.actor.runtime.config import ActorRuntimeConfig, ActorReentrancyConfig
from dapr.ext.fastapi import DaprActor  # type: ignore
from fastapi import FastAPI  # type: ignore

from index.actor import DistributedIndexActor

ActorRuntime.set_actor_config(
    ActorRuntimeConfig(
        actor_idle_timeout=timedelta(hours=30),
        actor_scan_interval=timedelta(hours=1),
        drain_ongoing_call_timeout=timedelta(seconds=50),
        drain_rebalanced_actors=True,
        reentrancy=ActorReentrancyConfig(enabled=True),
    )
)
app = FastAPI(title=f'{DistributedIndexActor.__name__}Service')

# Add Dapr Actor Extension
actor = DaprActor(app)


@app.on_event("startup")
async def startup_event():
    # Register
    await actor.register_actor(DistributedIndexActor)
