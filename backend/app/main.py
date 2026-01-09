from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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

origins = [
    "http://localhost:5173",  # your Vite frontend
    "http://localhost:3000",  # optional, other dev URLs
    # "https://your-production-frontend.com",  # production
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # allow POST, GET, OPTIONS, etc.
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"status": "ok"}