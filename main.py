from fastapi import FastAPI
import asyncio
from contextlib import asynccontextmanager
from app.api.routes import router
from app.tasks.schedule_top100 import start_scheduler

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler_task = asyncio.create_task(start_scheduler())
    try:
        yield
    finally:
        scheduler_task.cancel()

app = FastAPI(lifespan=lifespan)
app.include_router(router)
