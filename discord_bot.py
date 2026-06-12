import discord
from discord.ext import commands, tasks
from discord import ui
import os
import asyncio
from datetime import datetime, timezone, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
import logging
from emergentintegrations.llm.chat import LlmChat, UserMessage, TextDelta, StreamDone
import json
import zipfile
import io
from pathlib import Path
import secrets
import hashlib
import subprocess
import sys
import tempfile
import uuid

CRASH_STORE_LOGO = "https://customer-assets.emergentagent.com/job_bot-genius/artifacts/9ie7efu2_image.png"

logger = logging.getLogger('discord_bot')

mongo_url = os.environ.get('MONGO_URL')
client_mongo = AsyncIOMotorClient(mongo_url)
db = client_mongo[os.environ.get('DB_NAME')]


class PremiumBotManager:
    """Manages Premium bots with process monitoring and auto-restart"""
    
    def __init__(self):
        self.running_bots = {}  # {bot_id: {'process': subprocess.Popen, 'info': dict}}
        self.bot_dir = Path("/app/premium_bots")
        self.bot_dir.mkdir(exist_ok=True)
    
    async def deploy_bot(self, ticket_id: str, user_id: str, bot_code: str, bot_token: str):
        """Deploy a premium bot and keep it running"""
        try:
            bot_id = f"bot_{ticket_id}"
            bot_path = self.bot_dir / bot_id
            bot_path.mkdir(exist_ok=True)
            
            # Clean and prepare bot code
            bot_code = bot_code.strip()
            
            # Remove any existing bot.run() calls
            import re
            bot_code = re.sub(r'bot\.run\([^)]*\)', '', bot_code)
            bot_code = re.sub(r'client\.run\([^)]*\)', '', bot_code)
            
            # Ensure proper bot initialization
            if 'discord.Client' not in bot_code and 'commands.Bot' not in bot_code:
                # AI generated incomplete code - add basic structure
                bot_code = f"""import discord
from discord.ext import commands

# Generated bot code
{bot_code}

# Bot setup
if 'bot' not in dir() and 'client' not in dir():
    bot = commands.Bot(command_prefix='!', intents=discord.Intents.all())
"""
            
            # Add token at the very end
            bot_code += f"""

# Start the bot
if __name__ == '__main__':
    import asyncio
    bot.run('{bot_token}')
"""
            
            # Create bot.py file
            bot_file = bot_path / "bot.py"
            with open(bot_file, 'w', encoding='utf-8') as f:
                f.write(bot_code)
            
            # Create log file
            log_file = bot_path / "bot.log"
            
            # Create requirements.txt
            requirements_file = bot_path / "requirements.txt"
            with open(requirements_file, 'w') as f:
                f.write("discord.py>=2.3.2\naiohttp>=3.8.0\n")
            
            # Install dependencies
            install_process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "pip", "install", "-q", "-r", str(requirements_file),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            install_stdout, install_stderr = await install_process.communicate()
            
            if install_process.returncode != 0:
                error_msg = install_stderr.decode() if install_stderr else "Failed to install dependencies"
                logger.error(f"Install error for {bot_id}: {error_msg}")
                return False, f"فشل تثبيت المكتبات: {error_msg[:200]}"
            
            # Start bot process with logging
            log_handle = open(log_file, 'w')
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-u", str(bot_file),
                stdout=log_handle,
                stderr=asyncio.subprocess.STDOUT,
                cwd=str(bot_path)
            )
            
            # Wait 3 seconds to check if bot starts successfully
            await asyncio.sleep(3)
            
            if process.returncode is not None:
                # Bot crashed immediately
                log_handle.close()
                with open(log_file, 'r') as f:
                    error_log = f.read()
                logger.error(f"Bot {bot_id} crashed on start: {error_log}")
                return False, f"البوت توقف فوراً. الخطأ:\n{error_log[-500:]}"
            
            # Store bot info
            self.running_bots[bot_id] = {
                'process': process,
                'ticket_id': ticket_id,
                'user_id': user_id,
                'bot_path': str(bot_path),
                'log_file': log_handle,
                'started_at': datetime.now(timezone.utc).isoformat(),
                'restart_count': 0
            }
            
            # Update database
            await db.premium_bots.update_one(
                {"ticket_id": ticket_id},
                {"$set": {
                    "bot_id": bot_id,
                    "status": "running",
                    "process_id": process.pid,
                    "started_at": datetime.now(timezone.utc).isoformat()
                }},
                upsert=True
            )
            
            # Start monitoring task
            asyncio.create_task(self.monitor_bot(bot_id))
            
            logger.info(f"Bot {bot_id} deployed successfully with PID {process.pid}")
            return True, bot_id
            
        except Exception as e:
            logger.error(f"Error deploying bot: {e}", exc_info=True)
            return False, f"خطأ في التشغيل: {str(e)}"
    
    async def monitor_bot(self, bot_id: str):
        """Monitor bot process and restart if it crashes"""
        while bot_id in self.running_bots:
            try:
                bot_info = self.running_bots[bot_id]
                process = bot_info['process']
                
                # Check if process is still running
                returncode = process.returncode
                
                if returncode is not None:
                    # Bot crashed - restart it
                    logger.warning(f"Bot {bot_id} crashed with code {returncode}. Restarting...")
                    
                    bot_info['restart_count'] += 1
                    
                    # Close old log file
                    if 'log_file' in bot_info and bot_info['log_file']:
                        try:
                            bot_info['log_file'].close()
                        except:
                            pass
                    
                    # Update database
                    await db.premium_bots.update_one(
                        {"bot_id": bot_id},
                        {"$set": {
                            "status": "restarting",
                            "restart_count": bot_info['restart_count'],
                            "last_restart": datetime.now(timezone.utc).isoformat()
                        }}
                    )
                    
                    # Restart bot
                    bot_file = Path(bot_info['bot_path']) / "bot.py"
                    log_file = Path(bot_info['bot_path']) / "bot.log"
                    
                    # Open log file in append mode
                    log_handle = open(log_file, 'a')
                    log_handle.write(f"\n\n=== RESTART #{bot_info['restart_count']} at {datetime.now(timezone.utc).isoformat()} ===\n\n")
                    
                    new_process = await asyncio.create_subprocess_exec(
                        sys.executable, "-u", str(bot_file),
                        stdout=log_handle,
                        stderr=asyncio.subprocess.STDOUT,
                        cwd=bot_info['bot_path']
                    )
                    
                    bot_info['process'] = new_process
                    bot_info['log_file'] = log_handle
                    
                    # Update database
                    await db.premium_bots.update_one(
                        {"bot_id": bot_id},
                        {"$set": {
                            "status": "running",
                            "process_id": new_process.pid,
                            "restarted_at": datetime.now(timezone.utc).isoformat()
                        }}
                    )
                    
                    logger.info(f"Bot {bot_id} restarted successfully (restart #{bot_info['restart_count']})")
                
                # Wait before next check
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error monitoring bot {bot_id}: {e}")
                await asyncio.sleep(30)
    
    async def stop_bot(self, bot_id: str):
        """Stop a running bot"""
        if bot_id in self.running_bots:
            bot_info = self.running_bots[bot_id]
            process = bot_info['process']
            
            try:
                process.terminate()
                await asyncio.sleep(2)
                
                if process.returncode is None:
                    process.kill()
                
                del self.running_bots[bot_id]
                
                # Update database
                await db.premium_bots.update_one(
                    {"bot_id": bot_id},
                    {"$set": {
                        "status": "stopped",
                        "stopped_at": datetime.now(timezone.utc).isoformat()
                    }}
                )
                
                return True
            except Exception as e:
                logger.error(f"Error stopping bot {bot_id}: {e}")
                return False
        
        return False
    
    def get_bot_status(self, bot_id: str):
        """Get status of a bot"""
        if bot_id in self.running_bots:
            bot_info = self.running_bots[bot_id]
            process = bot_info['process']
            
            return {
                'status': 'running' if process.returncode is None else 'stopped',
                'pid': process.pid,
                'restart_count': bot_info['restart_count'],
                'started_at': bot_info['started_at']
            }
        
        return {'status': 'not_found'}

class PremiumCodeModal(ui.Modal, title='🔑 أدخل كود Premium'):
    code_input = ui.TextInput(
        label='الكود',
        placeholder='أدخل كود Premium الخاص بك',
        required=True,
        max_length=50
    )

    async def on_submit(self, interaction: discord.Interaction):
        code = self.code_input.value.strip()
        
        # Verify code
        code_data = await db.premium_codes.find_one({"code": code})
        
        if not code_data:
            await interaction.response.send_message("❌ الكود غير صحيح!", ephemeral=True)
            return
        
        # Check if expired
        if datetime.now(timezone.utc) > datetime.fromisoformat(code_data['expires_at']):
            await interaction.response.send_message("❌ الكود منتهي الصلاحية!", ephemeral=True)
            return
        
        # Check if used by someone else (SINGLE USER LOCK)
        if code_data.get('used', False):
            locked_user_id = code_data.get('used_by')
            if locked_user_id != str(interaction.user.id):
                await interaction.response.send_message(
                    "❌ الكود مستخدم بالفعل من قبل مستخدم آخر!\n\n"
                    "كل كود مخصص لشخص واحد فقط.",
                    ephemeral=True
                )
                return
            # Same user using the code again - allow it
        
        # Mark as used and lock to this user
        await db.premium_codes.update_one(
            {"code": code},
            {"$set": {
                "used": True, 
                "used_by": str(interaction.user.id), 
                "used_at": datetime.now(timezone.utc).isoformat()
            }}
        )
        
        # Create premium ticket
        await create_ticket(interaction, "premium_development", code)


async def create_premium_ticket(interaction: discord.Interaction):
    """Create premium ticket with code verification"""
    modal = PremiumCodeModal()
    await interaction.response.send_modal(modal)


async def create_purchase_request(interaction: discord.Interaction):
    """Create a purchase request ticket for manual response"""
    guild = interaction.guild
    user = interaction.user
    
    # Get ticket category
    settings = await db.bot_settings.find_one({"guild_id": str(guild.id)})
    if not settings:
        await interaction.response.send_message(
            "❌ يجب تعيين الكاتجوري أولاً! استخدم !setcategory", 
            ephemeral=True
        )
        return
    
    category_id = settings.get('ticket_category_id')
    category = guild.get_channel(int(category_id))
    
    if not category:
        await interaction.response.send_message(
            "❌ الكاتجوري غير موجود!", 
            ephemeral=True
        )
        return
    
    # Check if user already has an open purchase request
    existing = await db.purchase_requests.find_one({
        "user_id": str(user.id),
        "status": "open"
    })
    
    if existing:
        await interaction.response.send_message(
            "⚠️ لديك طلب شراء مفتوح بالفعل! يرجى انتظار الرد.",
            ephemeral=True
        )
        return
    
    try:
        # Create ticket channel
        ticket_id = str(uuid.uuid4())[:8]
        channel = await category.create_text_channel(
            name=f"💳-purchase-{user.name}",
            topic=f"طلب شراء كود Premium | User: {user.id}"
        )
        
        # Set permissions
        await channel.set_permissions(guild.default_role, read_messages=False)
        await channel.set_permissions(user, read_messages=True, send_messages=True)
        
        # Create purchase request in database
        await db.purchase_requests.insert_one({
            "id": ticket_id,
            "user_id": str(user.id),
            "username": user.name,
            "channel_id": str(channel.id),
            "status": "open",
            "created_at": datetime.now(timezone.utc).isoformat()
        })
        
        # Send welcome message
        embed = discord.Embed(
            title="💳 طلب شراء كود Premium",
            description=f"مرحباً {user.mention}!\n\n"
                       "شكراً لاهتمامك بخدمة Premium! 🌟\n\n"
                       "**ماذا تحصل مع Premium:**\n"
                       "✅ بوت كامل جاهز 100%\n"
                       "✅ تشغيل مباشر على سيرفراتنا\n"
                       "✅ يعمل 24/7 بدون توقف\n"
                       "✅ جاهز للتحسينات والإضافات\n\n"
                       "⏳ **يرجى الانتظار...**\nسيتم الرد عليك قريباً من قبل الإدارة.",
            color=0xF59E0B
        )
        embed.set_thumbnail(url=CRASH_STORE_LOGO)
        embed.set_footer(text="Crash Store • Premium Bot Services", icon_url=CRASH_STORE_LOGO)
        
        await channel.send(embed=embed)
        
        # Notify admins (send to a specific admin channel if configured)
        admin_channel_setting = await db.bot_settings.find_one({"setting": "admin_notifications"})
        if admin_channel_setting:
            admin_channel = guild.get_channel(int(admin_channel_setting.get('channel_id')))
            if admin_channel:
                admin_embed = discord.Embed(
                    title="🔔 طلب شراء جديد!",
                    description=f"**المستخدم:** {user.mention}\n"
                               f"**القناة:** {channel.mention}\n"
                               f"**الوقت:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC",
                    color=0xF59E0B
                )
                await admin_channel.send(embed=admin_embed)
        
        await interaction.response.send_message(
            f"✅ تم إنشاء طلبك! توجه إلى {channel.mention}",
            ephemeral=True
        )
        
    except Exception as e:
        logger.error(f"Error creating purchase request: {e}")
        await interaction.response.send_message(
            "❌ حدث خطأ أثناء إنشاء الطلب!",
            ephemeral=True
        )


class TicketButtons(ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @ui.button(label="🤖 تطوير بوت", style=discord.ButtonStyle.primary, custom_id="bot_dev")
    async def bot_dev_button(self, interaction: discord.Interaction, button: ui.Button):
        await create_ticket(interaction, "bot_development")

    @ui.button(label="💻 دعم برمجي", style=discord.ButtonStyle.secondary, custom_id="tech_support")
    async def tech_support_button(self, interaction: discord.Interaction, button: ui.Button):
        await create_ticket(interaction, "technical_support")
    
    @ui.button(label="⭐ Premium", style=discord.ButtonStyle.success, custom_id="premium")
    async def premium_button(self, interaction: discord.Interaction, button: ui.Button):
        await create_premium_ticket(interaction)
    
    @ui.button(label="💳 شراء كود", style=discord.ButtonStyle.secondary, custom_id="buy_code")
    async def buy_code_button(self, interaction: discord.Interaction, button: ui.Button):
        await create_purchase_request(interaction)


class RatingView(ui.View):
    def __init__(self, ticket_id: str, username: str, ticket_channel, zip_message):
        super().__init__(timeout=300)  # 5 minutes timeout
        self.ticket_id = ticket_id
        self.username = username
        self.ticket_channel = ticket_channel
        self.zip_message = zip_message
    
    async def send_to_reviews_channel(self, interaction: discord.Interaction, rating: int):
        """إرسال التقييم إلى قناة التقييمات"""
        try:
            # Get reviews channel
            reviews_setting = await db.bot_settings.find_one({"setting": "reviews_channel"})
            if not reviews_setting:
                return
            
            reviews_channel = interaction.guild.get_channel(int(reviews_setting['channel_id']))
            if not reviews_channel:
                return
            
            # Create stars display
            stars = "⭐" * rating + "☆" * (5 - rating)
            
            # Create professional review embed
            review_embed = discord.Embed(
                title="🌟 تقييم جديد من عميل",
                description=f"تم تطوير بوت جديد بنجاح وحصل على تقييم ممتاز",
                color=0xFFD700 if rating >= 4 else 0x5865F2
            )
            review_embed.add_field(name="⭐ التقييم", value=f"**{stars}** ({rating}/5)", inline=False)
            review_embed.add_field(name="👤 العميل", value=f"`{self.username}`", inline=True)
            review_embed.add_field(name="📅 التاريخ", value=discord.utils.format_dt(datetime.now(timezone.utc), style='R'), inline=True)
            review_embed.set_footer(text="Crash Store • Premium Bot Development")
            
            # Create call-to-action embed (without image)
            cta_embed = discord.Embed(
                title="🚀 جاهز لإنشاء بوتك؟",
                description="**افتح تذكرة الآن وانشئ بوتك الخاص!**\n\n"
                           "✨ بوتات احترافية ومخصصة\n"
                           "⚡ تسليم سريع\n"
                           "💯 جودة عالية\n\n"
                           "اضغط على الزر 🤖 تطوير بوت لتبدأ!",
                color=0x10B981
            )
            cta_embed.set_footer(text="Crash Store • Premium Bot Services")
            
            # Send to reviews channel (only once)
            await reviews_channel.send(embeds=[review_embed, cta_embed])
            
        except Exception as e:
            logger.error(f"Error sending to reviews channel: {e}")
    
    @ui.button(label="1⭐", style=discord.ButtonStyle.secondary, custom_id="rate_1")
    async def rate_1(self, interaction: discord.Interaction, button: ui.Button):
        await self.handle_rating(interaction, 1)
    
    @ui.button(label="2⭐", style=discord.ButtonStyle.secondary, custom_id="rate_2")
    async def rate_2(self, interaction: discord.Interaction, button: ui.Button):
        await self.handle_rating(interaction, 2)
    
    @ui.button(label="3⭐", style=discord.ButtonStyle.secondary, custom_id="rate_3")
    async def rate_3(self, interaction: discord.Interaction, button: ui.Button):
        await self.handle_rating(interaction, 3)
    
    @ui.button(label="4⭐", style=discord.ButtonStyle.primary, custom_id="rate_4")
    async def rate_4(self, interaction: discord.Interaction, button: ui.Button):
        await self.handle_rating(interaction, 4)
    
    @ui.button(label="5⭐", style=discord.ButtonStyle.success, custom_id="rate_5")
    async def rate_5(self, interaction: discord.Interaction, button: ui.Button):
        await self.handle_rating(interaction, 5)
    
    async def handle_rating(self, interaction: discord.Interaction, rating: int):
        """معالجة التقييم"""
        # Check if already rated
        existing_ticket = await db.tickets.find_one({"id": self.ticket_id})
        if existing_ticket and existing_ticket.get('rating'):
            await interaction.response.send_message("✅ تم استلام تقييمك مسبقاً. شكراً لك!", ephemeral=True)
            return
        
        # Save rating to database
        await db.tickets.update_one(
            {"id": self.ticket_id},
            {"$set": {"rating": rating, "status": "completed"}}
        )
        
        # Send thank you message
        stars = "⭐" * rating
        thank_embed = discord.Embed(
            title="✅ شكراً لك!",
            description=f"تم استلام تقييمك: {stars}\n\nنقدر وقتك وثقتك في Crash Store! 💙",
            color=0x10B981
        )
        thank_embed.set_footer(text="Crash Store • Premium Bot Development")
        
        await interaction.response.send_message(embed=thank_embed)
        
        # Send to reviews channel (only once)
        await self.send_to_reviews_channel(interaction, rating)
        
        # Disable all buttons
        for item in self.children:
            item.disabled = True
        
        try:
            await interaction.message.edit(view=self)
        except:
            pass  # Ignore if message was deleted
        
        # Close ticket after 5 seconds
        await asyncio.sleep(5)
        try:
            await self.ticket_channel.delete(reason="Ticket completed and rated")
        except:
            pass


async def create_ticket(interaction: discord.Interaction, ticket_type: str, premium_code: str = None):
    """Create a new ticket channel"""
    guild = interaction.guild
    user = interaction.user
    
    # Generate unique ticket ID
    import uuid
    ticket_id = str(uuid.uuid4())
    
    # Create ticket in database
    ticket_data = {
        "id": ticket_id,
        "user_id": str(user.id),
        "username": user.name,
        "type": ticket_type,
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "closed_at": None,
        "channel_id": None,
        "premium_code": premium_code
    }
    
    result = await db.tickets.insert_one(ticket_data)
    
    # Get category from settings
    category_setting = await db.bot_settings.find_one({"guild_id": str(guild.id)})
    category = None
    if category_setting and category_setting.get('ticket_category_id'):
        category = guild.get_channel(int(category_setting['ticket_category_id']))
    
    # Create private channel
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(read_messages=False),
        user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True)
    }
    
    ticket_name = "bot-dev" if ticket_type == "bot_development" else "support"
    channel = await guild.create_text_channel(
        name=f"ticket-{ticket_name}-{user.name}",
        overwrites=overwrites,
        category=category,
        reason=f"Ticket created by {user.name}"
    )
    
    # Update ticket with channel ID
    await db.tickets.update_one(
        {"id": ticket_id},
        {"$set": {"channel_id": str(channel.id)}}
    )
    
    await interaction.response.send_message(
        f"✅ تم إنشاء تذكرتك! {channel.mention}",
        ephemeral=True
    )
    
    # Send welcome message with Crash Store branding
    embed = discord.Embed(
        title="🎫 مرحباً بك في Crash Store",
        description=f"مرحباً {user.mention}! شكراً لتواصلك معنا.",
        color=0x5865F2
    )
    embed.set_thumbnail(url="https://customer-assets.emergentagent.com/job_bot-genius/artifacts/9ie7efu2_image.png")
    embed.set_footer(text="Crash Store • Premium Bot Services", icon_url="https://customer-assets.emergentagent.com/job_bot-genius/artifacts/9ie7efu2_image.png")
    
    if ticket_type == "premium_development":
        embed.add_field(
            name="⭐ Premium - بوت كامل جاهز",
            value="مبروك! لديك وصول Premium.\nسنصنع لك بوت Discord **كامل وجاهز للعمل مباشرة**!",
            inline=False
        )
        embed.add_field(
            name="✨ مميزات Premium",
            value="• بوت كامل 100% جاهز للتشغيل\n"
                  "• كود محترف ومختبر\n"
                  "• تعليمات تفصيلية\n"
                  "• **لا يحتاج تعديلات**",
            inline=False
        )
        embed.add_field(
            name="📝 الخطوات التالية",
            value="سأطرح عليك بعض الأسئلة لفهم متطلباتك.",
            inline=False
        )
    elif ticket_type == "bot_development":
        embed.add_field(
            name="🤖 خدمة تطوير البوتات",
            value="سنساعدك في إنشاء بوت ديسكورد مخصص يناسب احتياجاتك.",
            inline=False
        )
        embed.add_field(
            name="📝 الخطوات التالية",
            value="سأطرح عليك بعض الأسئلة لفهم متطلباتك بشكل أفضل.",
            inline=False
        )
        embed.add_field(
            name="⚠️ ملاحظة مهمة",
            value="**نحن نعطيك الملفات والأكواد فقط**\n"
                  "• نساعدك في بناء بوتك بتوفير الكود الأساسي\n"
                  "• الملفات تحتاج تعديل وتخصيص حسب احتياجاتك\n"
                  "• **ليست بوتات جاهزة تعمل مباشرة**",
            inline=False
        )
    else:
        embed.add_field(
            name="💻 الدعم البرمجي",
            value="فريق الدعم هنا لمساعدتك في حل أي مشاكل برمجية.",
            inline=False
        )
        embed.add_field(
            name="📝 الخطوات التالية",
            value="يرجى وصف مشكلتك بالتفصيل، وسأبذل قصارى جهدي لمساعدتك.",
            inline=False
        )
    
    await channel.send(embed=embed)
    
    # Start AI conversation
    if ticket_type == "premium_development":
        await channel.send("**⭐ مرحباً بك في Premium!**\n\n"
                          "سأصنع لك بوت Discord **جاهز ومشغّل أونلاين** 🚀\n\n"
                          "**الخطوة 1:** أرسل **توكن البوت** الخاص بك\n"
                          "• انتقل إلى [Discord Developer Portal](https://discord.com/developers/applications)\n"
                          "• أنشئ بوت جديد أو استخدم موجود\n"
                          "• انسخ التوكن وأرسله هنا")
        
        # Save initial AI message
        await db.messages.insert_one({
            "ticket_id": ticket_id,
            "role": "assistant",
            "content": "أرسل توكن البوت",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    elif ticket_type == "bot_development":
        await channel.send("**💡 ما فكرة البوت؟**\n\nوصف بسيط للبوت المطلوب:")
        
        # Save initial AI message
        await db.messages.insert_one({
            "ticket_id": ticket_id,
            "role": "assistant",
            "content": "ما فكرة البوت؟ وصف بسيط للبوت المطلوب",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    else:
        await channel.send("**🔧 وش مشكلتك؟**\n\nاكتب المشكلة أو الخطأ اللي تواجهه:")
        
        # Save initial AI message
        await db.messages.insert_one({
            "ticket_id": ticket_id,
            "role": "assistant",
            "content": "وش مشكلتك؟ اكتب المشكلة أو الخطأ اللي تواجهه",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })


class DiscordBot:
    def __init__(self, token: str):
        intents = discord.Intents.default()
        intents.messages = True
        intents.message_content = True
        intents.guilds = True
        intents.members = True
        
        self.bot = commands.Bot(command_prefix="!", intents=intents)
        self.token = token
        self.ai_chat_sessions = {}
        self.start_time = None
        self.stats_channel_id = None
        self.premium_manager = PremiumBotManager()  # Initialize premium bot manager
        
        self.setup_events()
        
    @tasks.loop(minutes=3)
    async def send_auto_stats(self):
        """إرسال الإحصائيات تلقائياً كل 3 دقائق"""
        if not self.stats_channel_id:
            return
            
        try:
            channel = self.bot.get_channel(int(self.stats_channel_id))
            if not channel:
                return
            
            # Get statistics
            total_tickets = await db.tickets.count_documents({})
            active_tickets = await db.tickets.count_documents({"status": "active"})
            completed_tickets = await db.tickets.count_documents({"status": "completed"})
            
            # Calculate uptime
            uptime_str = "غير متاح"
            if self.start_time:
                delta = datetime.now(timezone.utc) - self.start_time
                hours = int(delta.total_seconds() // 3600)
                minutes = int((delta.total_seconds() % 3600) // 60)
                uptime_str = f"{hours}h {minutes}m"
            
            # Create bilingual embed
            embed = discord.Embed(
                title="📊 Crash Store Bot Statistics | إحصائيات البوت",
                color=0x5865F2,
                timestamp=datetime.now(timezone.utc)
            )
            
            embed.set_thumbnail(url=CRASH_STORE_LOGO)
            
            # Arabic section
            embed.add_field(
                name="🇸🇦 الإحصائيات بالعربية",
                value=f"```\n"
                      f"📋 إجمالي التذاكر: {total_tickets}\n"
                      f"🟢 تذاكر نشطة: {active_tickets}\n"
                      f"✅ تذاكر مكتملة: {completed_tickets}\n"
                      f"⏱️ وقت التشغيل: {uptime_str}\n"
                      f"```",
                inline=False
            )
            
            # English section
            embed.add_field(
                name="🇬🇧 Statistics in English",
                value=f"```\n"
                      f"📋 Total Tickets: {total_tickets}\n"
                      f"🟢 Active Tickets: {active_tickets}\n"
                      f"✅ Completed Tickets: {completed_tickets}\n"
                      f"⏱️ Uptime: {uptime_str}\n"
                      f"```",
                inline=False
            )
            
            embed.set_footer(text="Crash Store • Premium Bot Services", icon_url=CRASH_STORE_LOGO)
            
            await channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error sending auto stats: {e}")
    
    @send_auto_stats.before_loop
    async def before_auto_stats(self):
        """انتظر حتى يكون البوت جاهزاً"""
        await self.bot.wait_until_ready()

    def setup_events(self):
        # Commands for bot owner
        @self.bot.command(name='setup')
        @commands.is_owner()
        async def setup_panel(ctx, channel: discord.TextChannel = None):
            """أمر لصاحب البوت فقط: إرسال بانل التذاكر في قناة معينة"""
            target_channel = channel or ctx.channel
            
            embed = discord.Embed(
                title="🎫 Crash Store - نظام التذاكر",
                description="اضغط على الزر المناسب لبدء طلبك:",
                color=0x5865F2
            )
            embed.set_thumbnail(url="https://customer-assets.emergentagent.com/job_bot-genius/artifacts/9ie7efu2_image.png")
            embed.add_field(
                name="🤖 تطوير بوت",
                value="احصل على بوت ديسكورد مخصص مصمم خصيصاً لاحتياجاتك",
                inline=False
            )
            embed.add_field(
                name="💻 دعم برمجي",
                value="احصل على مساعدة في مشاكلك وأسئلتك البرمجية",
                inline=False
            )
            embed.add_field(
                name="⭐ Premium",
                value="**بوت كامل جاهز 100%!** (يتطلب كود Premium)",
                inline=False
            )
            embed.set_footer(text="Crash Store • Premium Bot Services", icon_url="https://customer-assets.emergentagent.com/job_bot-genius/artifacts/9ie7efu2_image.png")
            
            await target_channel.send(embed=embed, view=TicketButtons())
            await ctx.send(f"✅ تم إرسال البانل في {target_channel.mention}", delete_after=5)
        
        @self.bot.command(name='setcategory')
        @commands.is_owner()
        async def set_category(ctx, category_id: str):
            """أمر لصاحب البوت فقط: تحديد الكاتجوري للتذاكر بالـ ID"""
            try:
                category = ctx.guild.get_channel(int(category_id))
                if not category or not isinstance(category, discord.CategoryChannel):
                    await ctx.send("❌ الكاتجوري غير موجود! تأكد من الـ ID", delete_after=5)
                    return
                
                # Save category ID in database
                await db.bot_settings.update_one(
                    {"guild_id": str(ctx.guild.id)},
                    {"$set": {"ticket_category_id": category_id}},
                    upsert=True
                )
                await ctx.send(f"✅ تم تعيين كاتجوري التذاكر إلى: {category.name}", delete_after=5)
            except ValueError:
                await ctx.send("❌ الـ ID غير صحيح!", delete_after=5)
        
        @self.bot.command(name='stats')
        @commands.is_owner()
        async def send_stats(ctx, channel_id: str):
            """أمر لصاحب البوت فقط: إرسال إحصائيات البوت في قناة معينة"""
            try:
                channel = self.bot.get_channel(int(channel_id))
                if not channel:
                    await ctx.send("❌ القناة غير موجودة! تأكد من الـ ID", delete_after=5)
                    return
                
                # Save channel ID for auto stats
                self.stats_channel_id = channel_id
                
                # Save to database
                await db.bot_settings.update_one(
                    {"setting": "stats_channel"},
                    {"$set": {"channel_id": channel_id}},
                    upsert=True
                )
                
                # Start auto stats task if not running
                if not self.send_auto_stats.is_running():
                    self.send_auto_stats.start()
                
                # Get statistics
                total_tickets = await db.tickets.count_documents({})
                active_tickets = await db.tickets.count_documents({"status": "active"})
                completed_tickets = await db.tickets.count_documents({"status": "completed"})
                
                # Calculate uptime
                uptime_str = "غير متاح"
                if self.start_time:
                    delta = datetime.now(timezone.utc) - self.start_time
                    hours = int(delta.total_seconds() // 3600)
                    minutes = int((delta.total_seconds() % 3600) // 60)
                    uptime_str = f"{hours}h {minutes}m"
                
                # Create bilingual embed
                embed = discord.Embed(
                    title="📊 Crash Store Bot Statistics | إحصائيات البوت",
                    color=0x5865F2,
                    timestamp=datetime.now(timezone.utc)
                )
                
                # Add Crash Store logo
                embed.set_thumbnail(url=CRASH_STORE_LOGO)
                
                # Arabic section
                embed.add_field(
                    name="🇸🇦 الإحصائيات بالعربية",
                    value=f"```\n"
                          f"📋 إجمالي التذاكر: {total_tickets}\n"
                          f"🟢 تذاكر نشطة: {active_tickets}\n"
                          f"✅ تذاكر مكتملة: {completed_tickets}\n"
                          f"⏱️ وقت التشغيل: {uptime_str}\n"
                          f"```",
                    inline=False
                )
                
                # English section
                embed.add_field(
                    name="🇬🇧 Statistics in English",
                    value=f"```\n"
                          f"📋 Total Tickets: {total_tickets}\n"
                          f"🟢 Active Tickets: {active_tickets}\n"
                          f"✅ Completed Tickets: {completed_tickets}\n"
                          f"⏱️ Uptime: {uptime_str}\n"
                          f"```",
                    inline=False
                )
                
                embed.set_footer(text="Crash Store • Premium Bot Services", icon_url=CRASH_STORE_LOGO)
                
                await channel.send(embed=embed)
                await ctx.send(f"✅ تم إرسال الإحصائيات في {channel.mention}\n🔄 سيتم إرسال الإحصائيات تلقائياً كل 3 دقائق", delete_after=10)
                
            except ValueError:
                await ctx.send("❌ الـ ID غير صحيح!", delete_after=5)
        
        @self.bot.command(name='setreviews')
        @commands.is_owner()
        async def set_reviews_channel(ctx, channel_id: str):
            """أمر لصاحب البوت فقط: تحديد قناة التقييمات"""
            try:
                channel = self.bot.get_channel(int(channel_id))
                if not channel:
                    await ctx.send("❌ القناة غير موجودة! تأكد من الـ ID", delete_after=5)
                    return
                
                # Save channel ID in database
                await db.bot_settings.update_one(
                    {"setting": "reviews_channel"},
                    {"$set": {"channel_id": channel_id}},
                    upsert=True
                )
                await ctx.send(f"✅ تم تعيين قناة التقييمات إلى: {channel.mention}", delete_after=5)
            except ValueError:
                await ctx.send("❌ الـ ID غير صحيح!", delete_after=5)
        
        @self.bot.command(name='setadmin')
        @commands.is_owner()
        async def set_admin_channel(ctx, channel_id: str):
            """أمر لصاحب البوت فقط: تحديد قناة إشعارات الأدمن"""
            try:
                channel = self.bot.get_channel(int(channel_id))
                if not channel:
                    await ctx.send("❌ القناة غير موجودة! تأكد من الـ ID", delete_after=5)
                    return
                
                # Save channel ID in database
                await db.bot_settings.update_one(
                    {"setting": "admin_notifications"},
                    {"$set": {"channel_id": channel_id}},
                    upsert=True
                )
                await ctx.send(f"✅ تم تعيين قناة إشعارات الأدمن إلى: {channel.mention}", delete_after=5)
            except ValueError:
                await ctx.send("❌ الـ ID غير صحيح!", delete_after=5)
        
        @self.bot.command(name='createcode')
        @commands.is_owner()
        async def create_premium_code(ctx, duration: str, count: int = 1):
            """إنشاء أكواد Premium - الاستخدام: !createcode [day/month/year] [عدد]"""
            duration_map = {
                "day": ("يوم", 1),
                "month": ("شهر", 30),
                "year": ("سنة", 365)
            }
            
            if duration not in duration_map:
                await ctx.send("❌ استخدم: day, month, أو year", delete_after=5)
                return
            
            if count < 1 or count > 50:
                await ctx.send("❌ العدد يجب أن يكون بين 1 و 50", delete_after=5)
                return
            
            codes = []
            duration_name, days = duration_map[duration]
            expires_at = datetime.now(timezone.utc) + timedelta(days=days)
            
            for _ in range(count):
                # Generate secure code
                code = f"CRASH-{secrets.token_hex(8).upper()}"
                
                await db.premium_codes.insert_one({
                    "code": code,
                    "duration": duration,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "expires_at": expires_at.isoformat(),
                    "used": False,
                    "created_by": str(ctx.author.id)
                })
                
                codes.append(code)
            
            # Send codes in DM
            codes_text = "\n".join([f"• `{code}`" for code in codes])
            embed = discord.Embed(
                title=f"✅ تم إنشاء {count} كود Premium",
                description=f"**المدة:** {duration_name}\n**الصلاحية:** حتى {expires_at.strftime('%Y-%m-%d')}\n\n{codes_text}",
                color=0x10B981
            )
            embed.set_footer(text="احتفظ بهذه الأكواد في مكان آمن")
            
            try:
                await ctx.author.send(embed=embed)
                await ctx.send("✅ تم إرسال الأكواد في رسالة خاصة", delete_after=5)
            except:
                await ctx.send(embed=embed, delete_after=60)
        
        @self.bot.command(name='codes')
        @commands.is_owner()
        async def list_codes(ctx):
            """عرض جميع أكواد Premium"""
            codes = await db.premium_codes.find({}).sort("created_at", -1).to_list(50)
            
            if not codes:
                await ctx.send("❌ لا توجد أكواد", delete_after=5)
                return
            
            # Create embeds
            active_codes = [c for c in codes if not c.get('used', False) and datetime.now(timezone.utc) < datetime.fromisoformat(c['expires_at'])]
            used_codes = [c for c in codes if c.get('used', False)]
            expired_codes = [c for c in codes if not c.get('used', False) and datetime.now(timezone.utc) >= datetime.fromisoformat(c['expires_at'])]
            
            embed = discord.Embed(
                title="📊 أكواد Premium",
                color=0x5865F2
            )
            
            if active_codes:
                active_text = "\n".join([f"`{c['code']}` - {c['duration']}" for c in active_codes[:10]])
                embed.add_field(name=f"✅ نشطة ({len(active_codes)})", value=active_text or "لا يوجد", inline=False)
            
            if used_codes:
                embed.add_field(name=f"🔒 مستخدمة ({len(used_codes)})", value=f"{len(used_codes)} كود", inline=True)
            
            if expired_codes:
                embed.add_field(name=f"⏰ منتهية ({len(expired_codes)})", value=f"{len(expired_codes)} كود", inline=True)
            
            await ctx.send(embed=embed, delete_after=30)
        
        @self.bot.command(name='sendcode')
        @commands.is_owner()
        async def send_code_to_user(ctx, user: discord.Member, duration: str = "month"):
            """إرسال كود Premium لشخص - الاستخدام: !sendcode @user [day/month/year]"""
            duration_map = {
                "day": ("يوم", 1),
                "month": ("شهر", 30),
                "year": ("سنة", 365)
            }
            
            if duration not in duration_map:
                duration = "month"
            
            duration_name, days = duration_map[duration]
            expires_at = datetime.now(timezone.utc) + timedelta(days=days)
            
            # Generate code
            code = f"CRASH-{secrets.token_hex(8).upper()}"
            
            await db.premium_codes.insert_one({
                "code": code,
                "duration": duration,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": expires_at.isoformat(),
                "used": False,
                "created_by": str(ctx.author.id),
                "sent_to": str(user.id)
            })
            
            # Send to user
            embed = discord.Embed(
                title="🎁 كود Premium خاص بك!",
                description=f"تم منحك كود Premium من {ctx.author.mention}",
                color=0xFFD700
            )
            embed.add_field(name="🔑 الكود", value=f"`{code}`", inline=False)
            embed.add_field(name="⏰ المدة", value=duration_name, inline=True)
            embed.add_field(name="📅 ينتهي في", value=expires_at.strftime('%Y-%m-%d'), inline=True)
            embed.add_field(
                name="📝 كيفية الاستخدام",
                value="1. اذهب إلى السيرفر\n2. اضغط زر ⭐ Premium\n3. أدخل الكود",
                inline=False
            )
            embed.set_footer(text="Crash Store • Premium Access")
            
            try:
                await user.send(embed=embed)
                await ctx.send(f"✅ تم إرسال الكود لـ {user.mention}", delete_after=5)
            except:
                await ctx.send(f"❌ لا يمكن إرسال رسالة لـ {user.mention}", delete_after=5)
        
        @self.bot.command(name='deletecode')
        @commands.is_owner()
        async def delete_user_codes(ctx, user: discord.Member):
            """حذف جميع أكواد شخص - الاستخدام: !deletecode @user"""
            result = await db.premium_codes.delete_many({"used_by": str(user.id)})
            
            if result.deleted_count > 0:
                await ctx.send(f"✅ تم حذف {result.deleted_count} كود لـ {user.mention}", delete_after=5)
            else:
                await ctx.send(f"❌ لا توجد أكواد لـ {user.mention}", delete_after=5)
        
        @self.bot.command(name='premiumlist')
        @commands.is_owner()
        async def list_premium_users(ctx):
            """عرض قائمة المشتركين في Premium"""
            used_codes = await db.premium_codes.find({"used": True}).to_list(100)
            
            if not used_codes:
                await ctx.send("❌ لا يوجد مشتركين", delete_after=5)
                return
            
            # Group by user
            users_premium = {}
            for code in used_codes:
                user_id = code.get('used_by')
                if user_id:
                    if user_id not in users_premium:
                        users_premium[user_id] = []
                    users_premium[user_id].append(code)
            
            embed = discord.Embed(
                title="👑 قائمة مشتركي Premium",
                description=f"إجمالي: {len(users_premium)} مشترك",
                color=0xFFD700
            )
            
            for user_id, codes in list(users_premium.items())[:15]:
                try:
                    user = await self.bot.fetch_user(int(user_id))
                    user_name = f"{user.name}"
                except:
                    user_name = f"User {user_id}"
                
                # Get latest code
                latest_code = max(codes, key=lambda x: x.get('used_at', ''))
                expires = datetime.fromisoformat(latest_code['expires_at'])
                is_active = datetime.now(timezone.utc) < expires
                
                status = "🟢 نشط" if is_active else "🔴 منتهي"
                embed.add_field(
                    name=f"{status} {user_name}",
                    value=f"الأكواد: {len(codes)} • المدة: {latest_code['duration']}",
                    inline=True
                )
            
            if len(users_premium) > 15:
                embed.set_footer(text=f"عرض 15 من {len(users_premium)}")
            
            await ctx.send(embed=embed, delete_after=60)
        
        @self.bot.command(name='bots')
        @commands.is_owner()
        async def list_premium_bots(ctx):
            """عرض جميع البوتات Premium النشطة"""
            bots = await db.premium_bots.find({"status": "running"}, {"_id": 0}).to_list(50)
            
            if not bots:
                await ctx.send("❌ لا توجد بوتات نشطة", delete_after=5)
                return
            
            embed = discord.Embed(
                title="🤖 البوتات Premium النشطة",
                description=f"إجمالي: {len(bots)} بوت",
                color=0x10B981
            )
            
            for bot in bots[:10]:
                user_id = bot.get('user_id')
                try:
                    user = await self.bot.fetch_user(int(user_id))
                    user_name = user.name
                except:
                    user_name = f"User {user_id}"
                
                bot_id = bot.get('bot_id', 'N/A')
                status = self.premium_manager.get_bot_status(bot_id)
                restart_count = bot.get('restart_count', 0)
                
                embed.add_field(
                    name=f"🤖 {user_name}",
                    value=f"**Bot ID:** `{bot_id[:12]}...`\n"
                          f"**Status:** {status.get('status', 'unknown')}\n"
                          f"**Restarts:** {restart_count}",
                    inline=True
                )
            
            if len(bots) > 10:
                embed.set_footer(text=f"عرض 10 من {len(bots)}")
            
            await ctx.send(embed=embed, delete_after=60)
        
        @self.bot.command(name='stopbot')
        @commands.is_owner()
        async def stop_premium_bot(ctx, bot_id: str):
            """إيقاف بوت Premium - الاستخدام: !stopbot [bot_id]"""
            success = await self.premium_manager.stop_bot(bot_id)
            
            if success:
                await ctx.send(f"✅ تم إيقاف البوت `{bot_id}`", delete_after=5)
            else:
                await ctx.send(f"❌ لم يتم العثور على البوت أو حدث خطأ", delete_after=5)
        
        @self.bot.command(name='botlogs')
        @commands.is_owner()
        async def show_bot_logs(ctx, bot_id: str, lines: int = 30):
            """عرض آخر سطور من log البوت - الاستخدام: !botlogs [bot_id] [عدد_السطور]"""
            try:
                log_file = Path(f"/app/premium_bots/{bot_id}/bot.log")
                
                if not log_file.exists():
                    await ctx.send(f"❌ لا توجد logs للبوت `{bot_id}`", delete_after=5)
                    return
                
                with open(log_file, 'r') as f:
                    all_lines = f.readlines()
                    last_lines = all_lines[-lines:]
                    log_content = ''.join(last_lines)
                
                if not log_content.strip():
                    await ctx.send(f"📝 لا توجد logs حالياً للبوت `{bot_id}`", delete_after=5)
                    return
                
                # Split into chunks if too long
                chunks = [log_content[i:i+1900] for i in range(0, len(log_content), 1900)]
                
                for i, chunk in enumerate(chunks[:3]):  # Max 3 chunks
                    await ctx.send(f"```\n{chunk}\n```")
                
                if len(chunks) > 3:
                    await ctx.send(f"⚠️ Log طويل جداً، تم عرض أول 3 أجزاء فقط")
                    
            except Exception as e:
                await ctx.send(f"❌ خطأ: {str(e)}", delete_after=5)
            
            await ctx.send(embed=embed)
        
        @setup_panel.error
        @set_category.error
        @send_stats.error
        @set_reviews_channel.error
        @create_premium_code.error
        @list_codes.error
        @send_code_to_user.error
        @delete_user_codes.error
        @list_premium_users.error
        async def command_error(ctx, error):
            if isinstance(error, commands.NotOwner):
                await ctx.send("❌ هذا الأمر مخصص لصاحب البوت فقط!", delete_after=5)
        
        @self.bot.event
        async def on_ready():
            logger.info(f'Bot logged in as {self.bot.user}')
            self.start_time = datetime.now(timezone.utc)
            self.bot.add_view(TicketButtons())
            
            # Load stats channel from database
            stats_setting = await db.bot_settings.find_one({"setting": "stats_channel"})
            if stats_setting:
                self.stats_channel_id = stats_setting.get('channel_id')
                if self.stats_channel_id and not self.send_auto_stats.is_running():
                    self.send_auto_stats.start()
                    logger.info(f'Auto stats enabled for channel {self.stats_channel_id}')

        @self.bot.event
        async def on_message(message):
            if message.author.bot:
                return
            
            # Process commands first
            await self.bot.process_commands(message)
            
            # Check if message is in a ticket channel
            ticket = await db.tickets.find_one({
                "channel_id": str(message.channel.id),
                "status": "active"
            }, {"_id": 0})
            
            if ticket:
                if ticket['type'] == 'premium_development' or ticket['type'] == 'bot_development':
                    # Process with AI for bot development
                    await self.handle_ai_conversation(message, ticket)
                elif ticket['type'] == 'technical_support':
                    # Process with AI for technical support
                    await self.handle_technical_support(message, ticket)

    async def handle_ai_conversation(self, message, ticket):
        """Handle AI conversation for bot development"""
        ticket_id = ticket.get('id')
        is_premium = ticket.get('type') == 'premium_development'
        ticket_status = ticket.get('status', 'active')
        
        # Check if waiting for feedback on deployed bot
        if ticket_status == 'awaiting_feedback':
            # User is giving feedback on the bot
            feedback_keywords_positive = ['تمام', 'ممتاز', 'زين', 'حلو', 'شكراً', 'perfect', 'good', 'great', 'thanks']
            feedback_keywords_negative = ['مشكلة', 'خطأ', 'ما يشتغل', 'error', 'not working', 'doesn\'t work']
            feedback_keywords_improvement = ['أبي', 'أبغى', 'تحسين', 'إضافة', 'ضيف', 'عدل', 'want', 'add', 'change']
            
            content_lower = message.content.lower()
            
            if any(kw in content_lower for kw in feedback_keywords_positive):
                # Customer is happy!
                await message.channel.send("🎉 **ممتاز! يسعدني إنك راضي عن البوت!**\n\n"
                                         "البوت شغال الحين 24/7 وإذا احتجت أي شي بالمستقبل، خبرني!")
                
                # Mark ticket as completed
                await db.tickets.update_one(
                    {"id": ticket_id},
                    {"$set": {"status": "completed", "completed_at": datetime.now(timezone.utc).isoformat()}}
                )
                return
                
            elif any(kw in content_lower for kw in feedback_keywords_improvement):
                # Customer wants improvements
                await message.channel.send(f"👍 تمام! راح أحدث البوت الحين...\n\n"
                                         f"**التعديل المطلوب:** {message.content}\n\n"
                                         f"⏳ جاري التحديث...")
                
                # Get bot_id from database
                bot_data = await db.premium_bots.find_one({"ticket_id": ticket_id}, {"_id": 0})
                if bot_data:
                    bot_id = bot_data.get('bot_id')
                    
                    # Stop the old bot
                    await self.premium_manager.stop_bot(bot_id)
                    
                    # Regenerate with improvement
                    await db.tickets.update_one(
                        {"id": ticket_id},
                        {"$set": {"status": "active"}}
                    )
                    
                    # Add improvement message
                    await db.messages.insert_one({
                        "ticket_id": ticket_id,
                        "role": "user",
                        "content": f"[تحسين] {message.content}",
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
                    
                    # Regenerate bot
                    messages_history = await db.messages.find(
                        {"ticket_id": ticket_id},
                        {"_id": 0}
                    ).sort("timestamp", 1).to_list(100)
                    
                    await self.generate_bot_code(message.channel, ticket, messages_history)
                return
                
            else:
                # Customer reports a problem
                await message.channel.send(f"😔 آسف على المشكلة! راح أصلحها الحين...\n\n"
                                         f"**المشكلة:** {message.content}\n\n"
                                         f"⏳ جاري إعادة البناء والإصلاح...")
                
                # Get bot_id and stop it
                bot_data = await db.premium_bots.find_one({"ticket_id": ticket_id}, {"_id": 0})
                if bot_data:
                    bot_id = bot_data.get('bot_id')
                    await self.premium_manager.stop_bot(bot_id)
                
                # Regenerate with fix
                await db.tickets.update_one(
                    {"id": ticket_id},
                    {"$set": {"status": "active"}}
                )
                
                # Add problem message
                await db.messages.insert_one({
                    "ticket_id": ticket_id,
                    "role": "user",
                    "content": f"[مشكلة] {message.content}",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
                
                # Regenerate bot
                messages_history = await db.messages.find(
                    {"ticket_id": ticket_id},
                    {"_id": 0}
                ).sort("timestamp", 1).to_list(100)
                
                await self.generate_bot_code(message.channel, ticket, messages_history)
                return
        
        # For Premium: Check if we need token first
        if is_premium and not ticket.get('bot_token'):
            if 'توكن' in message.content.lower() or 'token' in message.content.lower() or len(message.content) > 50:
                # This might be the token
                potential_token = message.content.strip()
                if len(potential_token) > 50:  # Discord tokens are long
                    # Save token
                    await db.tickets.update_one(
                        {"id": ticket_id},
                        {"$set": {"bot_token": potential_token}}
                    )
                    await message.channel.send("✅ تم استلام التوكن!\n\n**وش تبي البوت يسوي؟ اشرح كل شي في رسالة وحدة:**")
                    return
            else:
                # Still waiting for token
                await message.channel.send("⚠️ يرجى إرسال **توكن البوت** أولاً:\n\n"
                                         "• انتقل إلى Discord Developer Portal\n"
                                         "• أنشئ بوت جديد\n"
                                         "• انسخ التوكن وأرسله هنا")
                return
        
        # Save user message
        await db.messages.insert_one({
            "ticket_id": ticket_id,
            "role": "user",
            "content": message.content,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        
        # Get conversation history
        messages = await db.messages.find(
            {"ticket_id": ticket_id},
            {"_id": 0}
        ).sort("timestamp", 1).to_list(100)
        
        # Check if enough information collected
        user_messages_count = len([m for m in messages if m['role'] == 'user'])
        
        try:
            # Create AI chat session
            chat = LlmChat(
                api_key=os.environ.get('EMERGENT_LLM_KEY'),
                session_id=ticket_id,
                system_message="""أنت خبير تطوير بوتات Discord محترف. مهمتك:

1. اسأل سؤال واحد فقط مباشر:
   "وش تبي البوت يسوي بالضبط؟ (اشرح كل شيء في رسالة وحدة)"

2. بعد ما العميل يرد، قل فوراً:
   "تمام فهمت! ⚡ جاري بناء بوتك الحين..."

3. **لا تسأل أسئلة إضافية** - خلي العميل يشرح كل شيء في رسالة وحدة
4. لا تطول في الكلام
5. استخدم لهجة سعودية بسيطة

مثال:
العميل: "أبي بوت ترحيب"
أنت: "وش تبي البوت يسوي بالضبط؟ (اشرح كل شيء في رسالة وحدة)"
العميل: "يرحب بالأعضاء الجدد مع صورة ويرد على !ping و !help"
أنت: "تمام فهمت! ⚡ جاري بناء بوتك الحين..."

**مهم جداً**: بعد رد واحد فقط من العميل، ابدأ البناء مباشرة!
"""
            ).with_model("openai", "gpt-5.2")
            
            # Build conversation history (last 4 messages only for speed)
            conversation = "\n".join([
                f"{m['role']}: {m['content']}" for m in messages[-4:]
            ])
            
            user_msg = UserMessage(text=f"المحادثة السابقة:\n{conversation}\n\nالرد الأخير من المستخدم: {message.content}")
            
            # Stream AI response
            response_text = ""
            async with message.channel.typing():
                async for event in chat.stream_message(user_msg):
                    if isinstance(event, TextDelta):
                        response_text += event.content
                    elif isinstance(event, StreamDone):
                        break
            
            # Send AI response
            if response_text:
                await message.channel.send(response_text)
                
                # Save AI message
                await db.messages.insert_one({
                    "ticket_id": ticket_id,
                    "role": "assistant",
                    "content": response_text,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
                
                # Check if ready to generate bot
                # بعد رسالة واحدة فقط من العميل + رد AI يقول "جاري بناء"
                ready_phrases = [
                    "جاري بناء",
                    "جاري بناء بوتك",
                    "تمام فهمت",
                    "حسناً، سأبدأ"
                ]
                
                is_ready = any(phrase in response_text for phrase in ready_phrases)
                
                # بعد رسالة واحدة فقط إذا AI جاهز
                if user_messages_count >= 1 and is_ready:
                    await asyncio.sleep(1)
                    await self.generate_bot_code(message.channel, ticket, messages)
        
        except Exception as e:
            logger.error(f"Error in AI conversation: {e}")
            await message.channel.send("❌ عذراً، حدث خطأ. يرجى المحاولة مرة أخرى.")

    async def handle_technical_support(self, message, ticket):
        """Handle AI conversation for technical support"""
        ticket_id = ticket.get('id')
        
        # Check if user is asking to build a bot
        build_keywords = ['اصنع بوت', 'اسوي بوت', 'ابني بوت', 'بوت جديد', 'انشئ بوت', 'طور بوت', 'build bot', 'create bot', 'make bot']
        if any(keyword in message.content.lower() for keyword in build_keywords):
            embed = discord.Embed(
                title="❌ عذراً",
                description="هذه التذكرة مخصصة للدعم البرمجي فقط.\n\nلبناء بوت جديد، يرجى إغلاق هذه التذكرة وفتح تذكرة جديدة من زر **🤖 تطوير بوت**",
                color=0xFF0000
            )
            embed.set_thumbnail(url=CRASH_STORE_LOGO)
            embed.set_footer(text="Crash Store • Premium Bot Services", icon_url=CRASH_STORE_LOGO)
            await message.channel.send(embed=embed)
            return
        
        # Save user message
        await db.messages.insert_one({
            "ticket_id": ticket_id,
            "role": "user",
            "content": message.content,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        
        # Get conversation history
        messages = await db.messages.find(
            {"ticket_id": ticket_id},
            {"_id": 0}
        ).sort("timestamp", 1).to_list(100)
        
        try:
            # Create AI chat session for technical support
            chat = LlmChat(
                api_key=os.environ.get('EMERGENT_LLM_KEY'),
                session_id=f"support_{ticket_id}",
                system_message="""أنت خبير برمجة محترف متخصص في Python وDiscord.py. مهمتك:

1. افهم المشكلة بسرعة وقدم حل مباشر
2. اعطِ أمثلة كود واضحة وجاهزة للاستخدام
3. اشرح الحل بطريقة بسيطة ومباشرة
4. استخدم code blocks مع التعليقات العربية
5. كن سريعاً ومختصراً - لا تطيل في الشرح
6. ركز على الحل البرمجي أولاً

أسلوب الرد:
```python
# شرح مختصر للحل
[الكود هنا]
```

شرح بسيط لما يفعله الكود

هذا دعم برمجي فقط - لا تقبل طلبات بناء بوتات كاملة."""
            ).with_model("openai", "gpt-5.2")
            
            # Build conversation history (last 4 messages for faster response)
            conversation = "\n".join([
                f"{m['role']}: {m['content']}" for m in messages[-4:]
            ])
            
            user_msg = UserMessage(text=f"المحادثة السابقة:\n{conversation}\n\nالرد الأخير من المستخدم: {message.content}")
            
            # Stream AI response
            response_text = ""
            async with message.channel.typing():
                async for event in chat.stream_message(user_msg):
                    if isinstance(event, TextDelta):
                        response_text += event.content
                    elif isinstance(event, StreamDone):
                        break
            
            # Send AI response
            if response_text:
                # Split into chunks if too long (Discord limit is 2000 chars)
                chunks = [response_text[i:i+1900] for i in range(0, len(response_text), 1900)]
                for chunk in chunks:
                    await message.channel.send(chunk)
                
                # Save AI message
                await db.messages.insert_one({
                    "ticket_id": ticket_id,
                    "role": "assistant",
                    "content": response_text,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
        
        except Exception as e:
            logger.error(f"Error in technical support: {e}")
            await message.channel.send("❌ عذراً، حدث خطأ. يرجى المحاولة مرة أخرى.")

    async def generate_bot_code(self, channel, ticket, messages):
        """Generate bot code based on conversation"""
        is_premium = ticket.get('type') == 'premium_development'
        bot_token = ticket.get('bot_token')
        
        if is_premium:
            # Premium: Deploy bot directly
            await channel.send("⚡ **جاري بناء وتشغيل بوتك...** ⚡\n\n"
                             "🔄 جاري البناء...\n"
                             "⏳ يرجى الانتظار...")
            
            try:
                # Collect requirements
                requirements = "\n".join([m['content'] for m in messages if m['role'] == 'user'])
                
                # Generate bot code with AI
                chat = LlmChat(
                    api_key=os.environ.get('EMERGENT_LLM_KEY'),
                    session_id=f"premium_{ticket.get('id')}",
                    system_message="""You are a world-class expert Python & discord.py developer.

Your mission: Generate a **COMPLETE, PRODUCTION-READY** Discord bot that works 100% on first run.

CRITICAL REQUIREMENTS:
1. Use commands.Bot with a clear prefix (!, $, or /)
2. Add discord.Intents.all()
3. Add @bot.event for on_ready with a clear success message
4. Implement **EXACTLY** what the client requested - don't miss anything
5. Every command MUST work perfectly
6. Use proper async/await syntax
7. Add error handling for each command
8. Support both Arabic and English commands where applicable
9. Clean, professional code
10. DO NOT include bot.run() - it will be added automatically

**CRITICAL**: 
- Read requirements CAREFULLY
- Implement EVERYTHING the client asked for
- Test logic in your head before generating
- Make sure commands actually do what they're supposed to do
- Code must work on FIRST RUN

Structure example:
```python
import discord
from discord.ext import commands
import asyncio

bot = commands.Bot(command_prefix='!', intents=discord.Intents.all())

@bot.event
async def on_ready():
    print(f'✅ Bot is online! Logged in as {bot.user}')
    print(f'Bot ID: {bot.user.id}')

@bot.event
async def on_member_join(member):
    # Welcome message
    channel = member.guild.system_channel
    if channel:
        await channel.send(f'Welcome {member.mention}! 👋')

@bot.command(name='ping')
async def ping(ctx):
    await ctx.send(f'🏓 Pong! Latency: {round(bot.latency * 1000)}ms')

@bot.command(name='help')
async def help_command(ctx):
    embed = discord.Embed(title="Bot Commands", color=0x5865F2)
    embed.add_field(name="!ping", value="Check bot latency", inline=False)
    await ctx.send(embed=embed)

# All other commands here...
```

**REMEMBER**: Client is paying for Premium - the bot MUST work perfectly!"""
                ).with_model("openai", "gpt-5.2")
                
                code_prompt = f"""Read the client requirements **VERY CAREFULLY** and create a complete Discord bot:

CLIENT REQUIREMENTS:
{requirements}

IMPLEMENTATION CHECKLIST:
1. ✅ commands.Bot with clear prefix
2. ✅ Intents.all()
3. ✅ @bot.event on_ready with success message
4. ✅ Implement **EVERY SINGLE FEATURE** the client requested
5. ✅ Every command works 100%
6. ✅ Proper error handling
7. ✅ Support Arabic + English where applicable
8. ✅ DO NOT include bot.run()

**CRITICAL**: Make sure the bot actually does what the client wants!

Give me ONLY the complete code without explanations:
```python
[CODE HERE]
```"""
                
                user_msg = UserMessage(text=code_prompt)
                
                code_response = ""
                async with channel.typing():
                    async for event in chat.stream_message(user_msg):
                        if isinstance(event, TextDelta):
                            code_response += event.content
                        elif isinstance(event, StreamDone):
                            break
                
                # Extract code
                import re
                code_match = re.search(r'```(?:python)?\n(.*?)```', code_response, re.DOTALL)
                if code_match:
                    bot_code = code_match.group(1).strip()
                else:
                    bot_code = code_response.strip()
                
                # Deploy bot using PremiumBotManager
                success, result = await self.premium_manager.deploy_bot(
                    ticket_id=ticket.get('id'),
                    user_id=ticket.get('user_id'),
                    bot_code=bot_code,
                    bot_token=bot_token
                )
                
                if not success:
                    # Deployment failed - send error and try to fix
                    await channel.send(f"⚠️ **Error during bot deployment:**\n```\n{result[:500]}\n```\n\n"
                                     "🔄 **Attempting auto-fix...**")
                    
                    # Try to fix the code using AI with better prompt
                    fix_chat = LlmChat(
                        api_key=os.environ.get('EMERGENT_LLM_KEY'),
                        session_id=f"fix_{ticket.get('id')}",
                        system_message="""You are an expert Python & discord.py debugger.

Fix the code error WITHOUT changing the functionality.
Return ONLY the fixed code, no explanations."""
                    ).with_model("openai", "gpt-5.2")
                    
                    fix_prompt = f"""The following Discord bot code has an error:

```python
{bot_code}
```

ERROR:
{result}

CLIENT REQUIREMENTS (DO NOT CHANGE FUNCTIONALITY):
{requirements}

Fix ONLY the error while keeping all the client's requested features.
Return the corrected code without any explanation:
```python
[FIXED CODE HERE]
```"""
                    
                    fixed_response = ""
                    async with channel.typing():
                        async for event in fix_chat.stream_message(UserMessage(text=fix_prompt)):
                            if isinstance(event, TextDelta):
                                fixed_response += event.content
                            elif isinstance(event, StreamDone):
                                break
                    
                    # Extract fixed code
                    fixed_match = re.search(r'```(?:python)?\n(.*?)```', fixed_response, re.DOTALL)
                    if fixed_match:
                        fixed_code = fixed_match.group(1).strip()
                        
                        # Try deploying again
                        success, result = await self.premium_manager.deploy_bot(
                            ticket_id=ticket.get('id'),
                            user_id=ticket.get('user_id'),
                            bot_code=fixed_code,
                            bot_token=bot_token
                        )
                        
                        if not success:
                            # Second attempt also failed
                            await channel.send(f"❌ **Unable to auto-fix the issue.**\n\n"
                                             f"Please contact support for manual assistance.\n\n"
                                             f"**Error:** ```{result[:300]}```")
                            
                            # Save error for admin review
                            await db.premium_bots.update_one(
                                {"ticket_id": ticket.get('id')},
                                {"$set": {
                                    "status": "failed",
                                    "error": result,
                                    "failed_at": datetime.now(timezone.utc).isoformat()
                                }},
                                upsert=True
                            )
                            return
                
                # Save bot code to database
                await db.premium_bots.update_one(
                    {"ticket_id": ticket.get('id')},
                    {"$set": {
                        "bot_code": bot_code,
                        "bot_token": bot_token,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "status": "running"
                    }},
                    upsert=True
                )
                
                # Success message
                bot_status = self.premium_manager.get_bot_status(result)  # result is bot_id
                bot_id = result
                
                success_embed = discord.Embed(
                    title="✅ بوتك شغال الآن!",
                    description="تم بناء وتشغيل بوتك بنجاح! 🎉\n\n"
                               "⏳ **جاري التحقق من الاتصال...**",
                    color=0x10B981
                )
                success_embed.add_field(
                    name="🟢 الحالة",
                    value="🟢 البوت يعمل الآن",
                    inline=True
                )
                success_embed.add_field(
                    name="⚙️ الخدمة",
                    value="⭐ Premium",
                    inline=True
                )
                success_embed.add_field(
                    name="🔄 Auto-Restart",
                    value="✅ مفعّل",
                    inline=True
                )
                success_embed.add_field(
                    name="💡 ملاحظة",
                    value="• البوت مشغّل على سيرفراتنا الخاصة\n"
                          "• يعمل 24/7 بدون توقف\n"
                          "• يُعاد تشغيله تلقائياً عند أي توقف\n"
                          "• جاهز للتحسينات في أي وقت",
                    inline=False
                )
                success_embed.set_footer(text=f"Crash Store • Premium Bot Hosting • Bot ID: {bot_id[:8]}")
                
                success_msg = await channel.send(embed=success_embed)
                
                # Wait 10 seconds and verify bot is still running
                await asyncio.sleep(10)
                
                bot_status = self.premium_manager.get_bot_status(bot_id)
                if bot_status.get('status') == 'running':
                    # Update embed with confirmation
                    success_embed.description = "تم بناء وتشغيل بوتك بنجاح! 🎉\n\n✅ **تم التحقق: البوت متصل ويعمل بنجاح!**"
                    success_embed.color = 0x10B981
                    try:
                        await success_msg.edit(embed=success_embed)
                    except:
                        pass
                    
                    # Send testing instructions
                    await asyncio.sleep(1)
                    test_instructions = discord.Embed(
                        title="📱 كيف تجرب بوتك؟",
                        description="**البوت جاهز! جربه الحين:**",
                        color=0x5865F2
                    )
                    test_instructions.add_field(
                        name="1️⃣ تأكد من دعوة البوت",
                        value="تأكد أن البوت موجود في سيرفرك (يظهر أونلاين 🟢)",
                        inline=False
                    )
                    test_instructions.add_field(
                        name="2️⃣ جرب الأوامر",
                        value="جرب جميع الأوامر اللي طلبتها\nمثلاً: `!ping` أو `!help` أو أي أمر طلبته",
                        inline=False
                    )
                    test_instructions.add_field(
                        name="3️⃣ هل البوت يشتغل؟",
                        value="✅ **إذا يشتغل زين** → اكتب 'تمام' أو 'ممتاز'\n"
                              "❌ **إذا في مشكلة** → اكتب وش المشكلة وراح أصلحها فوراً\n"
                              "🔧 **تبي تحسينات** → قل لي وش تبي تضيف",
                        inline=False
                    )
                    test_instructions.set_footer(text="⚡ رد عليني خلال دقيقتين عشان أعرف إذا كل شيء تمام")
                    
                    await channel.send(embed=test_instructions)
                else:
                    # Bot crashed within 10 seconds
                    success_embed.description = "⚠️ **البوت توقف بعد التشغيل!**\n\nاستخدم `!botlogs {bot_id}` لرؤية الأخطاء."
                    success_embed.color = 0xF59E0B
                    try:
                        await success_msg.edit(embed=success_embed)
                    except:
                        pass
                
                # Update ticket status
                await db.tickets.update_one(
                    {"channel_id": str(channel.id)},
                    {"$set": {"status": "awaiting_feedback"}}
                )
                
            except Exception as e:
                logger.error(f"Error deploying premium bot: {e}")
                await channel.send(f"❌ حدث خطأ: {str(e)}")
            
            # Premium stops here - DO NOT send ZIP!
            return
        
        # Regular users only: Send ZIP file
        await channel.send("⚡ **جاري توليد كود البوت...** ⚡")
        
        try:
            # Collect all requirements
            requirements = "\n".join([m['content'] for m in messages if m['role'] == 'user'])
            
            # Generate code using AI
            chat = LlmChat(
                api_key=os.environ.get('EMERGENT_LLM_KEY'),
                session_id=f"codegen_{ticket.get('id', 'temp')}",
                system_message="""أنت مبرمج Python وdiscord.py خبير محترف.

مهمتك: توليد كود بوت Discord كامل وجاهز للتشغيل مباشرة.

المتطلبات:
- كود نظيف ومنظم واحترافي
- تعليقات عربية واضحة ومختصرة
- جاهز للتشغيل فوراً بدون تعديل
- يتضمن error handling أساسي
- استخدم أفضل الممارسات (best practices)

الملفات المطلوبة:
1. bot.py - الكود الرئيسي (كامل وجاهز)
2. requirements.txt - المكتبات بالإصدارات
3. README.md - دليل سريع بالعربية

تأكد أن الكود يعمل مباشرة!"""
            ).with_model("openai", "gpt-5.2")
            
            code_prompt = f"""أنشئ بوت Discord كامل وجاهز للتشغيل بناءً على هذه المتطلبات:

{requirements}

الملفات المطلوبة (بالضبط بهذا التنسيق):

```python:bot.py
# كود البوت الكامل هنا
```

```txt:requirements.txt
discord.py>=2.3.2
# المكتبات الأخرى
```

```md:README.md
# اسم البوت

## التثبيت
1. ثبت المكتبات: `pip install -r requirements.txt`
2. أضف التوكن في الملف
3. شغل البوت: `python bot.py`

## المميزات
- [الميزات هنا]
```

**مهم:** الكود يجب أن يكون كاملاً وجاهزاً للتشغيل مباشرة!"""
            
            user_msg = UserMessage(text=code_prompt)
            
            code_response = ""
            async with channel.typing():
                async for event in chat.stream_message(user_msg):
                    if isinstance(event, TextDelta):
                        code_response += event.content
                    elif isinstance(event, StreamDone):
                        break
            
            # Parse files from response
            files = parse_code_files(code_response)
            
            if not files or len(files) == 0:
                await channel.send("❌ خطأ: لم أتمكن من توليد ملفات البوت. يرجى المحاولة مرة أخرى.")
                return
            
            # Create ZIP file
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for filename, content in files.items():
                    zip_file.writestr(filename, content)
            
            zip_buffer.seek(0)
            
            # Send success message
            embed = discord.Embed(
                title="✅ ملفات بوتك جاهزة!",
                description="تم إنشاء ملفات وأكواد البوت بنجاح.",
                color=0x10B981
            )
            embed.set_thumbnail(url="https://customer-assets.emergentagent.com/job_bot-genius/artifacts/9ie7efu2_image.png")
            embed.add_field(name="📁 الملفات", value=f"{len(files)} ملف متضمن", inline=True)
            embed.add_field(name="📝 التعليمات", value="اقرأ ملف README.md للإعداد", inline=True)
            embed.add_field(
                name="⚠️ تنبيه مهم",
                value="**هذه ملفات وأكواد فقط**\n"
                      "• الملفات تحتاج تعديل التوكن والإعدادات\n"
                      "• قد تحتاج تعديلات إضافية حسب احتياجك\n"
                      "• اقرأ README.md للتعليمات الكاملة",
                inline=False
            )
            embed.set_footer(text="Crash Store • Premium Bot Development")
            
            await channel.send(embed=embed)
            
            # Send ZIP file
            discord_file = discord.File(zip_buffer, filename=f"crash_store_bot_{ticket.get('id', 'custom')}.zip")
            zip_message = await channel.send("📦 **حمّل بوتك:**", file=discord_file)
            
            # Ask for rating with buttons
            await asyncio.sleep(1)
            rating_view = RatingView(ticket.get('id'), ticket.get('username'), channel, zip_message)
            rating_msg = await channel.send("**قيّم خدمتنا! ⭐**\nاضغط على عدد النجوم:", view=rating_view)
            
            # Save rating message ID
            await db.tickets.update_one(
                {"channel_id": str(channel.id)},
                {"$set": {
                    "status": "awaiting_rating",
                    "closed_at": datetime.now(timezone.utc).isoformat(),
                    "rating_message_id": str(rating_msg.id)
                }}
            )
            
        except Exception as e:
            logger.error(f"Error generating bot code: {e}")
            await channel.send(f"❌ عذراً، حدث خطأ أثناء توليد الكود: {str(e)}")

    async def start(self):
        """Start the bot"""
        await self.bot.start(self.token)

    async def stop(self):
        """Stop the bot"""
        await self.bot.close()


def parse_code_files(response: str) -> dict:
    """Parse code blocks from AI response"""
    files = {}
    
    # Simple parser for code blocks
    import re
    pattern = r'```(?:\w+):([\w.]+)\n([\s\S]*?)```'
    matches = re.findall(pattern, response)
    
    for filename, content in matches:
        files[filename] = content.strip()
    
    # Default files if parsing fails
    if not files:
        files['bot.py'] = response
        files['requirements.txt'] = 'discord.py>=2.3.2'
        files['README.md'] = '# بوت ديسكورد\n\nللتشغيل:\n```\npip install -r requirements.txt\npython bot.py\n```'
    
    return files


# Global bot instance
bot_instance = None


async def start_bot():
    """Start the Discord bot"""
    global bot_instance
    
    token = os.environ.get('DISCORD_BOT_TOKEN')
    if not token:
        raise ValueError("DISCORD_BOT_TOKEN not found")
    
    bot_instance = DiscordBot(token)
    await bot_instance.start()


async def stop_bot():
    """Stop the Discord bot"""
    global bot_instance
    if bot_instance:
        await bot_instance.stop()
        bot_instance = None