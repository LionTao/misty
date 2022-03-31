from dapr.ext.fastapi import DaprActor  # type: ignore
from fastapi import FastAPI  # type: ignore

from index_meta.actor import IndexMetaActor

app = FastAPI(title=f'{IndexMetaActor.__name__}Service')

# Add Dapr Actor Extension
actor = DaprActor(app)


@app.on_event("startup")
async def startup_event():
    # Register
    await actor.register_actor(IndexMetaActor)
