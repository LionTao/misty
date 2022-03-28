from fastapi import FastAPI  # type: ignore
from dapr.ext.fastapi import DaprActor  # type: ignore
from assemble.actor import TrajectoryAssemblerActor


app = FastAPI(title=f'{TrajectoryAssemblerActor.__name__}Service')

# Add Dapr Actor Extension
actor = DaprActor(app)

@app.on_event("startup")
async def startup_event():
    # Register DemoActor
    await actor.register_actor(TrajectoryAssemblerActor)