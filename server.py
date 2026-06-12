from fastapi import FastAPI, APIRouter, HTTPException
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
import uuid
from datetime import datetime, timezone
import asyncio

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

app = FastAPI()
api_router = APIRouter(prefix="/api")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Ticket(BaseModel):
    model_config = ConfigDict(extra="ignore")
    
    id: str
    user_id: str
    username: str
    type: str
    status: str
    created_at: str
    closed_at: Optional[str] = None
    channel_id: Optional[str] = None


class Message(BaseModel):
    model_config = ConfigDict(extra="ignore")
    
    ticket_id: str
    role: str
    content: str
    timestamp: str


class Stats(BaseModel):
    total_tickets: int
    active_tickets: int
    completed_tickets: int
    pending_tickets: int


class BotStatus(BaseModel):
    is_running: bool
    uptime: Optional[str] = None


@api_router.get("/")
async def root():
    return {"message": "Discord Bot Generator API", "status": "running"}


@api_router.get("/stats", response_model=Stats)
async def get_stats():
    """Get dashboard statistics"""
    total = await db.tickets.count_documents({})
    active = await db.tickets.count_documents({"status": "active"})
    completed = await db.tickets.count_documents({"status": "completed"})
    pending = await db.tickets.count_documents({"status": "pending"})
    
    return Stats(
        total_tickets=total,
        active_tickets=active,
        completed_tickets=completed,
        pending_tickets=pending
    )


@api_router.get("/tickets", response_model=List[Ticket])
async def get_tickets(status: Optional[str] = None):
    """Get all tickets"""
    query = {}
    if status:
        query["status"] = status
    
    tickets = await db.tickets.find(query, {"_id": 0}).sort("created_at", -1).to_list(100)
    
    return tickets


@api_router.get("/tickets/{ticket_id}", response_model=dict)
async def get_ticket_detail(ticket_id: str):
    """Get ticket details with messages"""
    ticket = await db.tickets.find_one({"id": ticket_id}, {"_id": 0})
    
    if not ticket:
        raise HTTPException(status_code=404, detail="Ticket not found")
    
    messages = await db.messages.find(
        {"ticket_id": ticket_id},
        {"_id": 0}
    ).sort("timestamp", 1).to_list(1000)
    
    return {
        "ticket": ticket,
        "messages": messages
    }


@api_router.get("/bot/status", response_model=BotStatus)
async def get_bot_status():
    """Get Discord bot status"""
    from discord_bot import bot_instance
    
    is_running = bot_instance is not None and bot_instance.bot.is_ready()
    uptime = None
    
    if is_running and bot_instance.start_time:
        delta = datetime.now(timezone.utc) - bot_instance.start_time
        hours = int(delta.total_seconds() // 3600)
        minutes = int((delta.total_seconds() % 3600) // 60)
        uptime = f"{hours}h {minutes}m"
    
    return BotStatus(
        is_running=is_running,
        uptime=uptime
    )


@api_router.post("/bot/start")
async def start_bot():
    """Start Discord bot"""
    from discord_bot import start_bot, bot_instance
    
    if bot_instance and bot_instance.bot.is_ready():
        return {"message": "Bot is already running"}
    
    asyncio.create_task(start_bot())
    
    return {"message": "Bot starting..."}


@api_router.post("/bot/stop")
async def stop_bot():
    """Stop Discord bot"""
    from discord_bot import stop_bot
    
    await stop_bot()
    
    return {"message": "Bot stopped"}


app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Start Discord bot on startup"""
    logger.info("Starting Discord bot...")
    from discord_bot import start_bot
    asyncio.create_task(start_bot())


@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()