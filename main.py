
import os
import re
import asyncio
import sqlite3
import logging
from datetime import datetime, timezone

import discord
from discord.ext import commands, tasks
from discord import app_commands

# ==================================================
# CẤU HÌNH CHUNG
# ==================================================
TOKEN = os.getenv("DISCORD_TOKEN", "").strip()

OWNER_IDS = {
    1426024743282147358,
    1356498767788118099,
}
LOG_CHANNEL_ID = 1559916071601963028
VOICE_DB_FILE = "voice_lock.sqlite3"

SOURCE_GUILD_ID = 1495362331163754589
STORAGE_GUILD_IDS = [
    1562337763804844087,
    1562338023151108118,
    1562338303129424003,
    1562338556163129346,
    1562338815815712810,
    1562339088428965929,
    1562339341974380564,
    1562339598632489000,
    1562339890610569236,
    1562340146865508443,
]
CHANNEL_LIMIT = 450
MEMBER_DB_PATH = "members.sqlite3"
SCAN_DELAY = 0.15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("combined_bot")

# ==================================================
# KHỞI TẠO CƠ SỞ DỮ LIỆU (DATABASE)
# ==================================================
db_voice = sqlite3.connect(VOICE_DB_FILE)
db_voice.execute("""
CREATE TABLE IF NOT EXISTS voice_lock (
    guild_id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1
)
""")
db_voice.commit()

db_member = sqlite3.connect(MEMBER_DB_PATH)
db_member.row_factory = sqlite3.Row
db_member.execute("""
CREATE TABLE IF NOT EXISTS members (
    user_id INTEGER PRIMARY KEY,
    stt INTEGER NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    username TEXT NOT NULL,
    storage_guild_id INTEGER,
    channel_id INTEGER,
    is_member INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
""")
db_member.execute("""
CREATE TABLE IF NOT EXISTS message_logs (
    source_message_id INTEGER PRIMARY KEY,
    target_channel_id INTEGER NOT NULL,
    target_message_id INTEGER NOT NULL
)
""")
db_member.commit()

def save_lock(guild_id: int, channel_id: int):
    db_voice.execute(
        """
        INSERT INTO voice_lock (guild_id, channel_id, enabled)
        VALUES (?, ?, 1)
        ON CONFLICT(guild_id)
        DO UPDATE SET
            channel_id = excluded.channel_id,
            enabled = 1
        """,
        (guild_id, channel_id),
    )
    db_voice.commit()

def remove_lock(guild_id: int):
    db_voice.execute("DELETE FROM voice_lock WHERE guild_id = ?", (guild_id,))
    db_voice.commit()

def get_locks():
    rows = db_voice.execute("SELECT guild_id, channel_id FROM voice_lock WHERE enabled = 1").fetchall()
    return {g_id: c_id for g_id, c_id in rows}

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def get_member_row(user_id: int):
    return db_member.execute("SELECT * FROM members WHERE user_id = ?", (user_id,)).fetchone()

def get_next_stt():
    row = db_member.execute("SELECT COALESCE(MAX(stt), 0) + 1 AS next_stt FROM members").fetchone()
    return int(row["next_stt"])

def save_member(member: discord.Member):
    current = get_member_row(member.id)
    timestamp = now_iso()
    if current:
        db_member.execute("""
            UPDATE members SET display_name = ?, username = ?, is_member = 1, updated_at = ?
            WHERE user_id = ?
        """, (member.display_name[:100], member.name[:100], timestamp, member.id))
    else:
        stt = get_next_stt()
        db_member.execute("""
            INSERT INTO members (user_id, stt, display_name, username, storage_guild_id, channel_id, is_member, created_at, updated_at)
            VALUES (?, ?, ?, ?, NULL, NULL, 1, ?, ?)
        """, (member.id, stt, member.display_name[:100], member.name[:100], timestamp, timestamp))
    db_member.commit()
    return get_member_row(member.id)

def set_channel(user_id: int, guild_id: int, channel_id: int):
    db_member.execute("""
        UPDATE members SET storage_guild_id = ?, channel_id = ?, updated_at = ?
        WHERE user_id = ?
    """, (guild_id, channel_id, now_iso(), user_id))
    db_member.commit()

def set_member_status(user_id: int, is_member: bool):
    db_member.execute("""
        UPDATE members SET is_member = ?, updated_at = ?
        WHERE user_id = ?
    """, (1 if is_member else 0, now_iso(), user_id))
    db_member.commit()

def remember_message(source_id: int, channel_id: int, target_id: int):
    db_member.execute("""
        INSERT OR REPLACE INTO message_logs (source_message_id, target_channel_id, target_message_id)
        VALUES (?, ?, ?)
    """, (source_id, channel_id, target_id))
    db_member.commit()

def get_message_log(source_id: int):
    return db_member.execute("SELECT * FROM message_logs WHERE source_message_id = ?", (source_id,)).fetchone()

# ==================================================
# INTENTS
# ==================================================
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.voice_states = True
intents.message_content = True

# ==================================================
# BOT CLASS CHÍNH
# ==================================================
class CombinedBot(commands.Bot):
    def __init__(self):
        super().__init__(
            command_prefix="!",
            intents=intents,
        )
        self.voice_targets = get_locks()
        self.embed_running = False
        self.embed_task = None
        self.embed_number = 2
        self.cleaned_guild_commands = False

        self.db_lock = asyncio.Lock()
        self.channel_lock = asyncio.Lock()
        self.initialized = False
        self.scan_completed = False  # <-- Cờ quản lý trạng thái quét kênh ban đầu

    async def setup_hook(self):
        if not self.voice_monitor.is_running():
            self.voice_monitor.start()

    async def on_ready(self):
        logger.info("Bot online: %s | ID: %s", self.user, self.user.id if self.user else "unknown")
        
        if not self.initialized:
            try:
                synced = await self.tree.sync()
                logger.info("Đã đồng bộ %s lệnh toàn cục.", len(synced))
            except Exception:
                logger.exception("Không thể đồng bộ lệnh.")
            self.initialized = True

        if not self.cleaned_guild_commands:
            self.cleaned_guild_commands = True
            for guild in self.guilds:
                try:
                    self.tree.clear_commands(guild=guild)
                    await self.tree.sync(guild=guild)
                except Exception:
                    pass

            # Tiến hành quét và tạo kênh toàn bộ thành viên trước
            await initial_scan()

    async def try_reconnect(self, guild_id: int):
        channel_id = self.voice_targets.get(guild_id)
        if channel_id is None:
            return
        guild = self.get_guild(guild_id)
        if guild is None:
            return
        channel = guild.get_channel(channel_id)
        if not isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
            return
        member = guild.me
        if member is None:
            return
        permissions = channel.permissions_for(member)
        if not permissions.view_channel or not permissions.connect:
            return
        try:
            voice_client = guild.voice_client
            if voice_client and voice_client.is_connected():
                if voice_client.channel.id != channel_id:
                    await voice_client.move_to(channel)
            else:
                await channel.connect(timeout=20, reconnect=True)
        except Exception:
            pass

    @tasks.loop(seconds=10)
    async def voice_monitor(self):
        for guild_id in list(self.voice_targets.keys()):
            await self.try_reconnect(guild_id)

    @voice_monitor.before_loop
    async def before_voice_monitor(self):
        await self.wait_until_ready()

    async def on_voice_state_update(self, member, before, after):
        if self.user is None or member.id != self.user.id:
            return
        guild_id = member.guild.id
        if guild_id not in self.voice_targets:
            return
        target_id = self.voice_targets[guild_id]
        if after.channel is None or after.channel.id != target_id:
            await asyncio.sleep(2)
            await self.try_reconnect(guild_id)

bot = CombinedBot()

# ==================================================
# HỖ TRỢ MEMBER MANAGER
# ==================================================
def safe_channel_part(text: str, limit: int = 35):
    text = re.sub(r"[/\\#:@`*<>|]", "-", text or "")
    text = re.sub(r"\s+", "-", text.strip())
    text = re.sub(r"-+", "-", text)
    return (text or "unknown")[:limit].strip("-") or "unknown"

def make_channel_name(row):
    status = "✅" if row["is_member"] else "🛑"
    name = safe_channel_part(row["display_name"], 35)
    return f'{row["stt"]}-{name}-{row["user_id"]}-{status}'[:100]

def get_storage_guilds():
    result = []
    for guild_id in STORAGE_GUILD_IDS:
        guild = bot.get_guild(guild_id)
        if guild is not None:
            result.append(guild)
    return result

async def choose_storage_guild():
    for guild_id in STORAGE_GUILD_IDS:
        guild = bot.get_guild(guild_id)
        if guild is None:
            continue
        if len(guild.channels) < CHANNEL_LIMIT:
            return guild
    return None

async def find_existing_channel(user_id: int):
    row = get_member_row(user_id)
    if row and row["channel_id"]:
        channel = bot.get_channel(int(row["channel_id"]))
        if isinstance(channel, discord.TextChannel):
            return channel

    marker = f"-{user_id}-"
    for guild in get_storage_guilds():
        for channel in guild.text_channels:
            if marker in f"-{channel.name}-":
                return channel
    return None

async def create_member_channel(row):
    guild = await choose_storage_guild()
    if guild is None:
        return None
    bot_member = guild.me
    if bot_member is None:
        return None

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        bot_member: discord.PermissionOverwrite(
            view_channel=True, send_messages=True, read_message_history=True,
            embed_links=True, attach_files=True, manage_channels=True,
        ),
    }

    owner_id_val = next(iter(OWNER_IDS))
    owner = guild.get_member(owner_id_val)
    if owner is not None:
        overwrites[owner] = discord.PermissionOverwrite(
            view_channel=True, send_messages=True, read_message_history=True, embed_links=True, attach_files=True,
        )

    channel = await guild.create_text_channel(
        name=make_channel_name(row),
        overwrites=overwrites,
        reason=f"Member tracking: Discord ID {row['user_id']}",
        topic=f"member_id={row['user_id']} | stt={row['stt']}",
    )
    set_channel(row["user_id"], guild.id, channel.id)
    return channel

async def ensure_member_channel(member: discord.Member):
    async with bot.channel_lock:
        row = save_member(member)
        channel = await find_existing_channel(member.id)
        if channel is None:
            channel = await create_member_channel(row)
        else:
            set_channel(member.id, channel.guild.id, channel.id)

        if channel is not None:
            latest = get_member_row(member.id)
            desired_name = make_channel_name(latest)
            if channel.name != desired_name:
                try:
                    await channel.edit(name=desired_name, reason="Update member name/status")
                except discord.HTTPException:
                    pass
        return channel

async def update_channel_status(user_id: int, is_member: bool):
    row = get_member_row(user_id)
    if row is None:
        return
    set_member_status(user_id, is_member)
    row = get_member_row(user_id)
    channel = await find_existing_channel(user_id)
    if channel is None:
        return
    set_channel(user_id, channel.guild.id, channel.id)
    try:
        await channel.edit(name=make_channel_name(row), reason="Update member status")
    except discord.HTTPException:
        pass

def message_embed(message: discord.Message, edited=False):
    description = message.content.strip() if message.content else "*Không có nội dung văn bản.*"
    if edited:
        description += "\n\n✏️ Tin nhắn đã được chỉnh sửa."
    if len(description) > 4000:
        description = description[:3990] + "\n…"

    embed = discord.Embed(description=description, color=discord.Color.blurple(), timestamp=message.created_at)
    embed.set_author(name=f"{message.author.display_name} | ID {message.author.id}", icon_url=message.author.display_avatar.url)
    embed.add_field(name="Kênh nguồn", value=f"#{message.channel.name}", inline=True)
    embed.add_field(name="Tin nhắn", value=f"[Mở tin nhắn gốc]({message.jump_url})", inline=True)
    embed.set_footer(text=f"Message ID: {message.id}")
    return embed

async def copy_source_message(message: discord.Message, edited=False):
    if message.guild is None or message.guild.id != SOURCE_GUILD_ID or message.author.bot:
        return
    if not isinstance(message.author, discord.Member):
        return
    target = await ensure_member_channel(message.author)
    if target is None:
        return
    embed = message_embed(message, edited=edited)
    try:
        sent_msg = await target.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        remember_message(message.id, target.id, sent_msg.id)
    except discord.HTTPException:
        pass

async def initial_scan():
    source = bot.get_guild(SOURCE_GUILD_ID)
    if source is None:
        logger.error("Không tìm thấy server nguồn để quét.")
        return
    try:
        members = [m async for m in source.fetch_members(limit=None)]
    except Exception:
        logger.exception("Không thể lấy danh sách thành viên từ server nguồn.")
        return
    
    members.sort(key=lambda m: (m.id, m.name.lower()))
    total = len(members)
    logger.info("Bắt đầu quét và tạo kênh cho %s thành viên...", total)

    for index, member in enumerate(members, start=1):
        try:
            await ensure_member_channel(member)
        except Exception:
            pass
        if index == 1 or index % 25 == 0 or index == total:
            logger.info("Tiến độ quét: %s/%s", index, total)
        await asyncio.sleep(SCAN_DELAY)

    # Đánh dấu những người đã rời đi trước đó nếu cần
    scanned_ids = {m.id for m in members}
    rows = db_member.execute("SELECT user_id FROM members").fetchall()
    for row in rows:
        uid = int(row["user_id"])
        if uid not in scanned_ids and get_member_row(uid)["is_member"]:
            await update_channel_status(uid, False)

    # ==========================================
    # ĐÃ QUÉT XONG HẾT KÊNH -> BẬT CỜ CHO PHÉP THU TIN NHẮN
    # ==========================================
    bot.scan_completed = True
    logger.info("Hoàn tất tạo kênh! Bot chính thức bắt đầu lắng nghe và thu tin nhắn.")

# ==================================================
# KIỂM TRA QUYỀN OWNER
# ==================================================
def owner_only():
    async def predicate(interaction: discord.Interaction):
        if interaction.user.id not in OWNER_IDS:
            raise app_commands.CheckFailure("Bạn không có quyền sử dụng lệnh này.")
        return True
    return app_commands.check(predicate)

# ==================================================
# CÁC LỆNH SLASH
# ==================================================
@bot.tree.command(name="run", description="Bắt đầu gửi embed số vào kênh log.")
@owner_only()
async def run_embed(interaction: discord.Interaction):
    if bot.embed_running:
        await interaction.response.send_message("⚠️ Bot đang gửi embed rồi!", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    channel = bot.get_channel(LOG_CHANNEL_ID)
    if channel is None:
        try:
            channel = await bot.fetch_channel(LOG_CHANNEL_ID)
        except Exception:
            await interaction.followup.send("❌ Không tìm thấy kênh log.", ephemeral=True)
            return

    bot.embed_running = True
    async def send_embeds():
        try:
            while bot.embed_running:
                embed = discord.Embed(description=str(bot.embed_number), color=discord.Color.blue())
                await channel.send(embed=embed)
                bot.embed_number += 1
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            raise
        finally:
            bot.embed_running = False
            bot.embed_task = None
    bot.embed_task = asyncio.create_task(send_embeds())
    await interaction.followup.send(f"✅ Đã bắt đầu gửi embed vào <#{LOG_CHANNEL_ID}>.", ephemeral=True)

@bot.tree.command(name="stop", description="Dừng gửi embed tự động.")
@owner_only()
async def stop_embed(interaction: discord.Interaction):
    if not bot.embed_running:
        await interaction.response.send_message("⚠️ Bot hiện không gửi embed.", ephemeral=True)
        return
    bot.embed_running = False
    if bot.embed_task is not None:
        bot.embed_task.cancel()
        bot.embed_task = None
    await interaction.response.send_message("🛑 Đã dừng gửi embed.", ephemeral=True)

@bot.tree.command(name="join", description="Khóa vị trí phòng voice của bot.")
@owner_only()
async def join(interaction: discord.Interaction):
    if interaction.guild is None or not interaction.user.voice:
        await interaction.response.send_message("Bạn cần vào phòng voice trước khi dùng /join.", ephemeral=True)
        return
    channel = interaction.user.voice.channel
    bot.voice_targets[interaction.guild.id] = channel.id
    save_lock(interaction.guild.id, channel.id)
    try:
        voice_client = interaction.guild.voice_client
        if voice_client and voice_client.is_connected():
            await voice_client.move_to(channel)
        else:
            await channel.connect(timeout=20, reconnect=True)
        await interaction.response.send_message(f"🔒 Đã khóa vị trí phòng voice tại {channel.mention}.", ephemeral=True)
    except Exception:
        await interaction.response.send_message("Đã lưu phòng cần khóa nhưng có lỗi khi kết nối.", ephemeral=True)

@bot.tree.command(name="leave", description="Tắt khóa vị trí và cho bot rời voice.")
@owner_only()
async def leave(interaction: discord.Interaction):
    if interaction.guild is None:
        return
    guild_id = interaction.guild.id
    bot.voice_targets.pop(guild_id, None)
    remove_lock(guild_id)
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.disconnect(force=True)
    await interaction.response.send_message("🔓 Đã tắt khóa vị trí phòng voice.", ephemeral=True)

@bot.tree.command(name="sen", description="Gửi tin nhắn vào kênh hiện tại.")
@app_commands.describe(noi_dung="Nội dung tin nhắn muốn gửi")
@owner_only()
async def sen(interaction: discord.Interaction, noi_dung: str):
    if not isinstance(interaction.channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message("Không thể gửi tin nhắn ở kênh này.", ephemeral=True)
        return
    await interaction.channel.send(noi_dung, allowed_mentions=discord.AllowedMentions.none())
    await interaction.response.send_message("✅ Đã gửi tin nhắn.", ephemeral=True)

@bot.tree.command(name="timnguoi", description="Tìm thành viên theo Discord User ID")
@app_commands.describe(user_id="Discord User ID cần tìm")
@owner_only()
async def timnguoi(interaction: discord.Interaction, user_id: str):
    try:
        uid = int(user_id.strip())
    except ValueError:
        await interaction.response.send_message("ID không hợp lệ.", ephemeral=True)
        return
    row = get_member_row(uid)
    if row is None:
        await interaction.response.send_message("Không tìm thấy ID này trong cơ sở dữ liệu.", ephemeral=True)
        return
    
    guild = bot.get_guild(row["storage_guild_id"]) if row["storage_guild_id"] else None
    channel = bot.get_channel(row["channel_id"]) if row["channel_id"] else None
    
    embed = discord.Embed(title="Kết quả tìm thành viên", color=discord.Color.green() if row["is_member"] else discord.Color.red())
    embed.add_field(name="STT", value=str(row["stt"]), inline=True)
    embed.add_field(name="Tên", value=row["display_name"], inline=True)
    embed.add_field(name="Discord ID", value=str(row["user_id"]), inline=False)
    embed.add_field(name="Trạng thái", value="✅ Đang ở server" if row["is_member"] else "🛑 Đã rời server", inline=False)
    embed.add_field(name="Kênh lưu trữ", value=channel.mention if channel else "Chưa có", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ==================================================
# CÁC SỰ KIỆN (EVENTS)
# ==================================================
@bot.event
async def on_member_join(member: discord.Member):
    if member.guild.id == SOURCE_GUILD_ID:
        await ensure_member_channel(member)
        await update_channel_status(member.id, True)

@bot.event
async def on_member_remove(member: discord.Member):
    if member.guild.id == SOURCE_GUILD_ID:
        await update_channel_status(member.id, False)

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    if after.guild.id == SOURCE_GUILD_ID and before.display_name != after.display_name:
        await ensure_member_channel(after)

@bot.event
async def on_message(message: discord.Message):
    # CHẶN THU TIN NHẮN KHI CHƯA QUÉT XONG KÊNH
    if not bot.scan_completed:
        await bot.process_commands(message)
        return

    if message.guild and message.guild.id == SOURCE_GUILD_ID:
        await copy_source_message(message)
    await bot.process_commands(message)

@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if not bot.scan_completed:
        return
    if after.guild is None or after.guild.id != SOURCE_GUILD_ID or after.author.bot:
        return
    log_row = get_message_log(after.id)
    if log_row is None:
        return
    channel = bot.get_channel(log_row["target_channel_id"])
    if isinstance(channel, discord.TextChannel):
        try:
            target_message = await channel.fetch_message(log_row["target_message_id"])
            await target_message.edit(embed=message_embed(after, edited=True))
        except Exception:
            pass

@bot.event
async def on_message_delete(message: discord.Message):
    if not bot.scan_completed:
        return
    if message.guild is None or message.guild.id != SOURCE_GUILD_ID:
        return
    log_row = get_message_log(message.id)
    if log_row is None:
        return
    channel = bot.get_channel(log_row["target_channel_id"])
    if isinstance(channel, discord.TextChannel):
        try:
            target_message = await channel.fetch_message(log_row["target_message_id"])
            embed = target_message.embeds[0] if target_message.embeds else discord.Embed()
            embed.add_field(name="Trạng thái", value="🗑️ Tin nhắn gốc đã bị xóa.", inline=False)
            await target_message.edit(embed=embed)
        except Exception:
            pass

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        message = "⛔ Bạn không có quyền sử dụng lệnh này."
    else:
        message = "❌ Lệnh gặp lỗi khi thực thi."
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except Exception:
        pass

# ==================================================
# CHẠY BOT
# ==================================================
if not TOKEN:
    raise RuntimeError("Thiếu biến môi trường DISCORD_TOKEN.")

bot.run(TOKEN)
