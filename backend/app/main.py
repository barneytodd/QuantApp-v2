from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.db.async_pool import init_db_pool, close_db_pool
from app.api import prices_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db_pool()
    yield
    # Shutdown
    await close_db_pool()

app = FastAPI(title="QuantApp", lifespan=lifespan)

# Include routers
app.include_router(prices_router)