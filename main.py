import asyncio
import hashlib
import os
import json
import random
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import asyncpg
import discord
from discord.ext import commands

TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise RuntimeError("Missing TOKEN environment variable")

GENERAL_CHAT_CHANNEL_ID = 1449829926990512189
STAFF_LOG_CHANNEL_ID = 1440730206187950122
INVITES_PANEL_CHANNEL_ID = 1442936279644897381
STAFF_TICKET_ROLE_ID = 1431610644721041587

EMOJI = {
    "blur_shock": "<:meru_blur_shock:1461406604619419894>",
    "bongo_hype": "<a:meru_bongo_hype:1461406795808510153>",
    "cheek_pull": "<a:meru_cheek_pull:1461402888378122365>",
    "clap": "<a:meru_clap:1461406550286405642>",
    "clown": "<:meru_clown:1461406927404667053>",
    "cozy": "<:meru_cozy:1461402695641600072>",
    "crying": "<:meru_crying:1461406843938148467>",
    "drool": "<a:meru_drool:1461406497249562847>",
    "finger_point": "<:meru_finger_point:1461402946498461810>",
    "gun": "<:meru_gun:1461403205656248370>",
    "happy_pat": "<:meru_happy_pat:1461407271895433349>",
    "heart": "<:meru_heart:1461406961160421569>",
    "jump_hype": "<a:meru_jump_hype:1461407222931128597>",
    "no_bully": "<:meru_no_bully:1461403110936154402>",
    "panic": "<:meru_panic:1461406647066038628>",
    "panties": "<:meru_panties:1461407146829811959>",
    "pout": "<:meru_pout:1461402833239806096>",
    "sad_cry": "<:meru_sad_cry:1461406746781286440>",
    "shy_blush": "<:meru_shy_blush:1461402784992591924>",
    "sips_tea": "<:meru_sips_tea:1461403002727432383>",
    "sleeping": "<:meru_sleeping:1461402603194814736>",
    "sparkle_eyes": "<:meru_sparkle_eyes:1461403164950401271>",
    "stare": "<a:meru_stare:1461406688761610497>",
    "teef": "<:meru_teef:1461407089544007680>",
    "bonk": "<:meru_the_succubus_bonk:1461407580307063065>",
    "derp": "<a:meru_the_succubus_derp:1461407409510551552>",
    "nervous_stare": "<:meru_the_succubus_nervous_stare:1461407346403184641>",
    "nomnom": "<a:meru_the_succubus_nomnom:1461407480562323713>",
    "peace": "<:meru_the_succubus_peace:1461407692693442774>",
    "police": "<:meru_the_succubus_police:1461407047940833464>",
    "tongue_lick": "<a:meru_tongue_lick:1461406893732790272>",
    "yell": "<:meru_yell:1461407532579946690>",
}

MIN_MSG_LEN = 10
RNG_COOLDOWN = 30

timezone_berlin = ZoneInfo("Europe/Berlin")


def safe_button_emoji(custom_emoji: str | None, fallback_unicode: str):
    """
    Returns a discord.PartialEmoji if the custom emoji is STATIC.
    Returns fallback Unicode emoji if animated, invalid, or missing.
    """
    if custom_emoji is None:
        return fallback_unicode
    if custom_emoji.startswith("<a:"):
        return fallback_unicode
    if custom_emoji.startswith("<:"):
        match = re.match(r"^<:([^:]+):(\d+)>$", custom_emoji)
        if not match:
            return fallback_unicode
        name, emoji_id = match.groups()
        return discord.PartialEmoji(name=name, id=int(emoji_id))
    return fallback_unicode

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True

bot = commands.Bot(command_prefix="!", intents=intents)

TIME_MULTIPLIERS = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
}

BLACKLIST = [
    "slur", "badword", "nastyword", "hateword"
]

LINK_REGEX = re.compile(
    r"(https?://|discord\.gg|discord\.com/invite|\b[a-z0-9-]+\.[a-z]{2,})",
    re.IGNORECASE,
)

INVITE_STOCK_DEFAULTS = {
    "stock_3": 10,
    "stock_5": 5,
    "stock_10": 2,
}

invite_cache: Dict[int, int] = {}
background_tasks: List[asyncio.Task] = []


db_pool = None


@dataclass
class EventState:
    ends_at: int
    last_reset: int
    stock_3: int
    stock_5: int
    stock_10: int
    panel_message_id: Optional[int]


def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def berlin_now() -> datetime:
    return datetime.now(timezone_berlin)


def normalize_message(content: str) -> str:
    lowered = content.lower().strip()
    collapsed = re.sub(r"\s+", " ", lowered)
    return collapsed


def hash_message(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def parse_duration(value: str) -> int:
    if len(value) < 2:
        raise ValueError("Time format must include a number and a unit (s, m, h, d).")
    unit = value[-1].lower()
    if unit not in TIME_MULTIPLIERS:
        raise ValueError("Time unit must be one of: s, m, h, d.")
    number = value[:-1]
    if not number.isdigit():
        raise ValueError("Time amount must be a positive integer.")
    seconds = int(number) * TIME_MULTIPLIERS[unit]
    if seconds <= 0:
        raise ValueError("Time amount must be at least 1 second.")
    return seconds


def build_embed(kind: str, title: str, description: str, fields: List[Tuple[str, str, bool]]) -> discord.Embed:
    color_map = {
        "bank": discord.Color.teal(),
        "giveaway_normal": discord.Color.purple(),
        "giveaway_big": discord.Color.gold(),
        "invite": discord.Color.green(),
        "support": discord.Color.blue(),
        "logs": discord.Color.dark_red(),
        "rng": discord.Color.gold(),
    }
    embed = discord.Embed(title=title, description=description, color=color_map.get(kind, discord.Color.blurple()))
    for name, value, inline in fields:
        embed.add_field(name=name, value=value, inline=inline)
    footer = "ZyraBot • Economy/Invites/Support/Giveaways"
    embed.set_footer(text=footer)
    return embed


async def run_migrations():
    async with db_pool.acquire() as conn:
        invite_tables = {
            "invite_codes": "invite_id",
            "invite_joins": "invite_id",
        }
        for table, column in invite_tables.items():
            data_type = await conn.fetchval(
                """
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=$1 AND column_name=$2
                """,
                table,
                column,
            )
            if data_type and data_type != "text":
                await conn.execute(
                    f"ALTER TABLE {table} ALTER COLUMN {column} TYPE TEXT USING {column}::text"
                )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                entries BIGINT NOT NULL DEFAULT 0,
                last_interest_ts BIGINT NOT NULL DEFAULT 0
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NULL,
                action TEXT NOT NULL,
                details TEXT NULL,
                created_at BIGINT NOT NULL
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS automod_state (
                user_id BIGINT PRIMARY KEY,
                last_msg_hash TEXT NULL,
                last_msg_ts BIGINT NOT NULL DEFAULT 0,
                streak_count INT NOT NULL DEFAULT 0,
                last_streak_ts BIGINT NOT NULL DEFAULT 0,
                no_entry_until BIGINT NOT NULL DEFAULT 0,
                rng_cooldown_until BIGINT NOT NULL DEFAULT 0
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS giveaways (
                id SERIAL PRIMARY KEY,
                message_id BIGINT NULL,
                channel_id BIGINT NOT NULL,
                prize TEXT NOT NULL,
                winner_count INT NOT NULL,
                is_big BOOLEAN NOT NULL DEFAULT FALSE,
                ends_at BIGINT NOT NULL,
                ended BOOLEAN NOT NULL DEFAULT FALSE,
                created_by BIGINT NOT NULL,
                created_at BIGINT NOT NULL
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS giveaway_entries (
                giveaway_id INT REFERENCES giveaways(id) ON DELETE CASCADE,
                user_id BIGINT,
                entries_spent BIGINT NOT NULL,
                entered_at BIGINT NOT NULL,
                PRIMARY KEY (giveaway_id, user_id)
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS invite_event_state (
                key TEXT PRIMARY KEY,
                ends_at BIGINT NOT NULL,
                last_reset BIGINT NOT NULL,
                stock_3 INT NOT NULL,
                stock_5 INT NOT NULL,
                stock_10 INT NOT NULL,
                panel_message_id BIGINT NULL
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS invite_codes (
                code TEXT PRIMARY KEY,
                creator_id BIGINT NOT NULL,
                invite_url TEXT NOT NULL,
                invite_id TEXT NOT NULL,
                created_at BIGINT NOT NULL,
                expires_at BIGINT NOT NULL,
                uses_total INT NOT NULL DEFAULT 0,
                valid_uses INT NOT NULL DEFAULT 0,
                invalid_uses INT NOT NULL DEFAULT 0
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS invite_joins (
                invited_id BIGINT PRIMARY KEY,
                invite_id TEXT NOT NULL,
                code TEXT REFERENCES invite_codes(code),
                inviter_id BIGINT NOT NULL,
                joined_at BIGINT NOT NULL,
                valid BOOLEAN NOT NULL,
                invalid_reason TEXT NULL
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                channel_id BIGINT PRIMARY KEY,
                opener_id BIGINT NOT NULL,
                open BOOLEAN NOT NULL DEFAULT TRUE,
                created_at BIGINT NOT NULL,
                closed_at BIGINT NULL
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS panels (
                key TEXT PRIMARY KEY,
                channel_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                updated_at BIGINT NOT NULL
            );
            """
        )


async def get_or_create_user(user_id: int) -> Dict[str, int]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT user_id, entries, last_interest_ts FROM users WHERE user_id=$1", user_id)
        if row:
            return dict(row)
        await conn.execute(
            "INSERT INTO users (user_id, entries, last_interest_ts) VALUES ($1, $2, $3)",
            user_id,
            0,
            0,
        )
        return {"user_id": user_id, "entries": 0, "last_interest_ts": 0}


async def apply_interest(user_id: int, conn: Optional[asyncpg.Connection] = None) -> Dict[str, int]:
    if conn is None:
        async with db_pool.acquire() as pool_conn:
            async with pool_conn.transaction():
                return await apply_interest(user_id, pool_conn)
    row = await conn.fetchrow(
        "SELECT user_id, entries, last_interest_ts FROM users WHERE user_id=$1 FOR UPDATE",
        user_id,
    )
    if not row:
        await conn.execute(
            "INSERT INTO users (user_id, entries, last_interest_ts) VALUES ($1, $2, $3)",
            user_id,
            0,
            0,
        )
        row = await conn.fetchrow(
            "SELECT user_id, entries, last_interest_ts FROM users WHERE user_id=$1 FOR UPDATE",
            user_id,
        )
    entries = int(row["entries"])
    last_ts = int(row["last_interest_ts"])
    current_ts = now_ts()
    if last_ts == 0:
        await conn.execute(
            "UPDATE users SET last_interest_ts=$1 WHERE user_id=$2",
            current_ts,
            user_id,
        )
        return {"user_id": user_id, "entries": entries, "last_interest_ts": current_ts}
    days = (current_ts - last_ts) // 86400
    if days > 0:
        factor = 1.0025 ** days
        updated_entries = max(0, int(entries * factor))
        new_ts = last_ts + days * 86400
        await conn.execute(
            "UPDATE users SET entries=$1, last_interest_ts=$2 WHERE user_id=$3",
            updated_entries,
            new_ts,
            user_id,
        )
        return {"user_id": user_id, "entries": updated_entries, "last_interest_ts": new_ts}
    return {"user_id": user_id, "entries": entries, "last_interest_ts": last_ts}


async def log_event(action: str, user_id: Optional[int], details: str):
    created_ts = now_ts()
    detail_text = (details or "")[:500]
    try:
        await db_pool.execute(
            "INSERT INTO logs (user_id, action, details, created_at) VALUES ($1, $2, $3, $4)",
            user_id,
            action,
            detail_text,
            created_ts,
        )
    except Exception:
        pass

    channel = bot.get_channel(STAFF_LOG_CHANNEL_ID)
    if not channel:
        return

    parsed_details = {}
    if detail_text:
        try:
            parsed_details = json.loads(detail_text)
        except json.JSONDecodeError:
            parsed_details = {}

    user_label = f"<@{user_id}>" if user_id else "Unknown"
    embed = None

    if action == "automod_punishment":
        title = f"{EMOJI['police']} {EMOJI['no_bully']} Automod Punishment"
        embed = discord.Embed(title=title, color=discord.Color.dark_red())
        embed.add_field(name="User", value=f"{user_label} ({user_id})" if user_id else "Unknown", inline=False)
        embed.add_field(name="Reason", value=parsed_details.get("reason", "Unknown"), inline=True)
        embed.add_field(name="Channel", value=str(parsed_details.get("channel_id", "Unknown")), inline=True)
        embed.add_field(name="Message Content", value=parsed_details.get("content", "Unknown"), inline=False)
        cooldown_ts = parsed_details.get("no_entry_until", created_ts)
        embed.add_field(name="No-Entry Cooldown Ends", value=f"<t:{cooldown_ts}:R>", inline=False)
    elif action == "entries_gain":
        title = f"{EMOJI['sparkle_eyes']} {EMOJI['heart']} Entries Gained"
        embed = discord.Embed(title=title, color=discord.Color.teal())
        embed.add_field(name="User", value=f"{user_label} ({user_id})" if user_id else "Unknown", inline=False)
        embed.add_field(name="Source", value=str(parsed_details.get("source", "Unknown")), inline=True)
        embed.add_field(name="Amount Gained", value=str(parsed_details.get("amount", "0")), inline=True)
        embed.add_field(name="Balance After", value=str(parsed_details.get("new_balance", "Unknown")), inline=True)
        embed.add_field(name="Time", value=f"<t:{created_ts}:R>", inline=False)
    elif action == "invite_entries_granted":
        title = f"{EMOJI['finger_point']} {EMOJI['nomnom']} Invite Entries Granted"
        embed = discord.Embed(title=title, color=discord.Color.green())
        embed.add_field(name="User", value=f"{user_label} ({user_id})" if user_id else "Unknown", inline=False)
        embed.add_field(name="Tier", value=str(parsed_details.get("tier", "Unknown")), inline=True)
        embed.add_field(name="Entries Granted", value=str(parsed_details.get("entries_granted", "0")), inline=True)
        embed.add_field(
            name="Remaining Stock",
            value=str(parsed_details.get("remaining_stock", "Unknown")),
            inline=True,
        )
        embed.add_field(
            name="Valid Invites",
            value=str(parsed_details.get("valid_invites", "Unknown")),
            inline=True,
        )
        embed.add_field(name="Time", value=f"<t:{created_ts}:R>", inline=False)
    elif action == "admin_bypass":
        title = f"{EMOJI['sleeping']} Admin Bypass Notice"
        embed = discord.Embed(title=title, color=discord.Color.dark_grey())
        embed.add_field(name="User", value=f"{user_label} ({user_id})" if user_id else "Unknown", inline=False)
        embed.add_field(name="Reason", value=parsed_details.get("reason", "Unknown"), inline=True)
        embed.add_field(name="Channel", value=str(parsed_details.get("channel_id", "Unknown")), inline=True)
        embed.add_field(name="Message Content", value=parsed_details.get("content", "Unknown"), inline=False)
        embed.add_field(name="Time", value=f"<t:{created_ts}:R>", inline=False)
    else:
        title = "Staff Log"
        embed = discord.Embed(title=title, color=discord.Color.dark_grey())
        embed.add_field(name="User", value=f"{user_label} ({user_id})" if user_id else "Unknown", inline=False)
        embed.add_field(name="Action", value=action, inline=True)
        embed.add_field(name="Details", value=detail_text or "None", inline=False)
        embed.add_field(name="Time", value=f"<t:{created_ts}:R>", inline=False)

    embed.set_footer(text=f"Timestamp: <t:{created_ts}:F>")
    try:
        await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    except Exception:
        return


async def get_event_state() -> EventState:
    row = await db_pool.fetchrow("SELECT * FROM invite_event_state WHERE key='global'")
    if row:
        return EventState(
            ends_at=row["ends_at"],
            last_reset=row["last_reset"],
            stock_3=row["stock_3"],
            stock_5=row["stock_5"],
            stock_10=row["stock_10"],
            panel_message_id=row["panel_message_id"],
        )
    current = now_ts()
    next_reset = int(next_daily_time(21, 0).timestamp())
    await db_pool.execute(
        """
        INSERT INTO invite_event_state (key, ends_at, last_reset, stock_3, stock_5, stock_10, panel_message_id)
        VALUES ('global', $1, $2, $3, $4, $5, NULL)
        """,
        next_reset,
        current,
        INVITE_STOCK_DEFAULTS["stock_3"],
        INVITE_STOCK_DEFAULTS["stock_5"],
        INVITE_STOCK_DEFAULTS["stock_10"],
    )
    return EventState(
        ends_at=next_reset,
        last_reset=current,
        stock_3=INVITE_STOCK_DEFAULTS["stock_3"],
        stock_5=INVITE_STOCK_DEFAULTS["stock_5"],
        stock_10=INVITE_STOCK_DEFAULTS["stock_10"],
        panel_message_id=None,
    )


def next_daily_time(hour: int, minute: int) -> datetime:
    now_local = berlin_now()
    target = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now_local:
        target = target + timedelta(days=1)
    return target


async def ensure_panel_message(key: str, channel_id: int, embed: discord.Embed, view: discord.ui.View):
    channel = bot.get_channel(channel_id)
    if not channel:
        return
    row = await db_pool.fetchrow("SELECT message_id FROM panels WHERE key=$1", key)
    if row:
        try:
            message = await channel.fetch_message(row["message_id"])
            await message.edit(embed=embed, view=view)
            await db_pool.execute(
                "UPDATE panels SET updated_at=$1 WHERE key=$2",
                now_ts(),
                key,
            )
            return message
        except (discord.NotFound, discord.Forbidden):
            pass
    try:
        message = await channel.send(embed=embed, view=view)
    except (discord.Forbidden, discord.NotFound):
        return None
    await db_pool.execute(
        """
        INSERT INTO panels (key, channel_id, message_id, updated_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (key) DO UPDATE SET channel_id=$2, message_id=$3, updated_at=$4
        """,
        key,
        channel_id,
        message.id,
        now_ts(),
    )
    return message


async def load_panel_message(key: str, channel_id: int) -> Optional[discord.Message]:
    row = await db_pool.fetchrow("SELECT message_id FROM panels WHERE key=$1", key)
    if not row:
        return None
    channel = bot.get_channel(channel_id)
    if not channel:
        return None
    try:
        return await channel.fetch_message(row["message_id"])
    except (discord.Forbidden, discord.NotFound):
        return None


async def update_invites_cache(guild: discord.Guild):
    invite_cache.clear()
    try:
        invites = await guild.invites()
    except (discord.Forbidden, discord.HTTPException):
        return
    for invite in invites:
        invite_cache[invite.code] = invite.uses or 0


class BankView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="View My Entries",
        style=discord.ButtonStyle.primary,
        emoji=safe_button_emoji(EMOJI["sparkle_eyes"], "✨"),
        custom_id="bank_view_entries",
    )
    async def view_entries(self, interaction: discord.Interaction, button: discord.ui.Button):
        user = interaction.user
        data = await apply_interest(user.id)
        description = (
            f"Balance: **{data['entries']:,}** entries {EMOJI['happy_pat']} {EMOJI['teef']}\n"
            f"Next interest applies automatically on your next interaction."
        )
        embed = build_embed(
            "bank",
            f"{EMOJI['sparkle_eyes']} {EMOJI['heart']} Bank Entries",
            description,
            [("Last Interest", f"<t:{data['last_interest_ts']}:R>", True)],
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(
        label="Interest Info",
        style=discord.ButtonStyle.secondary,
        emoji=safe_button_emoji(EMOJI["sips_tea"], "🍵"),
        custom_id="bank_interest_info",
    )
    async def interest_info(self, interaction: discord.Interaction, button: discord.ui.Button):
        user = interaction.user
        data = await apply_interest(user.id)
        description = (
            f"Daily interest is **0.25%** compounded. {EMOJI['sips_tea']}\n"
            "It applies lazily when you use bank/giveaway/invite actions."
        )
        embed = build_embed(
            "bank",
            f"{EMOJI['sparkle_eyes']} {EMOJI['heart']} Interest Info",
            description,
            [("Current Balance", f"{data['entries']:,} entries", True)],
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


class GiveawayEntryModal(discord.ui.Modal):
    def __init__(self, giveaway_id: int):
        super().__init__(title=f"{EMOJI['tongue_lick']} Spend entries")
        self.giveaway_id = giveaway_id
        self.entries_amount = discord.ui.TextInput(
            label="Entries to spend",
            style=discord.TextStyle.short,
            placeholder="Enter a whole number",
            required=True,
            max_length=10,
        )
        self.add_item(self.entries_amount)

    async def on_submit(self, interaction: discord.Interaction):
        user = interaction.user
        try:
            amount = int(self.entries_amount.value.strip())
        except ValueError:
            await interaction.response.send_message(
                f"{EMOJI['cheek_pull']} {EMOJI['pout']} Invalid entry amount.",
                ephemeral=True,
            )
            await log_event("economy_abuse", user.id, "Non-integer giveaway entry attempt.")
            return
        if amount <= 0 or amount > 10_000_000:
            await interaction.response.send_message(
                f"{EMOJI['cheek_pull']} {EMOJI['pout']} Invalid entry amount.",
                ephemeral=True,
            )
            await log_event("economy_abuse", user.id, f"Invalid giveaway entry amount: {amount}.")
            return
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await apply_interest(user.id, conn)
                row = await conn.fetchrow(
                    "SELECT entries FROM users WHERE user_id=$1 FOR UPDATE",
                    user.id,
                )
                balance = int(row["entries"])
                if balance < amount:
                    await interaction.response.send_message(
                        f"{EMOJI['cheek_pull']} {EMOJI['pout']} You don't have enough entries.",
                        ephemeral=True,
                    )
                    return
                existing = await conn.fetchrow(
                    "SELECT 1 FROM giveaway_entries WHERE giveaway_id=$1 AND user_id=$2",
                    self.giveaway_id,
                    user.id,
                )
                if existing:
                    await interaction.response.send_message(
                        f"{EMOJI['cheek_pull']} {EMOJI['pout']} You already entered.",
                        ephemeral=True,
                    )
                    await log_event("economy_abuse", user.id, "Duplicate giveaway entry attempt.")
                    return
                await conn.execute(
                    "UPDATE users SET entries=entries-$1 WHERE user_id=$2",
                    amount,
                    user.id,
                )
                await conn.execute(
                    """
                    INSERT INTO giveaway_entries (giveaway_id, user_id, entries_spent, entered_at)
                    VALUES ($1, $2, $3, $4)
                    """,
                    self.giveaway_id,
                    user.id,
                    amount,
                    now_ts(),
                )
        await interaction.response.send_message(
            f"{EMOJI['nomnom']} {EMOJI['finger_point']} Entry recorded! Good luck.",
            ephemeral=True,
        )


class GiveawayView(discord.ui.View):
    def __init__(self, giveaway_id: int):
        super().__init__(timeout=None)
        self.giveaway_id = giveaway_id
        button = discord.ui.Button(
            label="Enter Giveaway",
            style=discord.ButtonStyle.success,
            emoji=safe_button_emoji(EMOJI["finger_point"], "👉"),
            custom_id=f"giveaway_enter_{giveaway_id}",
        )
        button.callback = self.enter_giveaway
        self.add_item(button)

    async def enter_giveaway(self, interaction: discord.Interaction):
        await interaction.response.send_modal(GiveawayEntryModal(self.giveaway_id))


class InvitesPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Code",
        style=discord.ButtonStyle.primary,
        emoji=safe_button_emoji(EMOJI["tongue_lick"], "🏷️"),
        custom_id="invites_code",
    )
    async def invite_code(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if not guild:
            return
        code = await generate_invite_code()
        expires_at = now_ts() + 7 * 86400
        channel = interaction.channel if isinstance(interaction.channel, discord.TextChannel) else guild.system_channel
        if not channel:
            await interaction.response.send_message(
                f"{EMOJI['cheek_pull']} {EMOJI['pout']} I couldn't find a channel for invites.",
                ephemeral=True,
            )
            return
        try:
            invite = await channel.create_invite(
                max_age=7 * 86400,
                max_uses=0,
                unique=True,
                reason=f"Invite code generated by {interaction.user.id}",
            )
        except (discord.Forbidden, discord.HTTPException):
            await interaction.response.send_message(
                f"{EMOJI['cheek_pull']} {EMOJI['pout']} I couldn't create an invite.",
                ephemeral=True,
            )
            return
        await db_pool.execute(
            """
            INSERT INTO invite_codes (code, creator_id, invite_url, invite_id, created_at, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            code,
            interaction.user.id,
            invite.url,
            invite.code,
            now_ts(),
            expires_at,
        )
        event_state = await get_event_state()
        description = f"Invite URL: {invite.url}\nExpires: <t:{expires_at}:R>"
        embed = build_embed(
            "invite",
            f"{EMOJI['nomnom']} Your Invite Code",
            description,
            [
                ("Uses", "0", True),
                ("Event Ends", f"<t:{event_state.ends_at}:R>", True),
            ],
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(
        label="Stats",
        style=discord.ButtonStyle.secondary,
        emoji=safe_button_emoji(EMOJI["stare"], "📊"),
        custom_id="invites_stats",
    )
    async def invite_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        event_state = await get_event_state()
        row = await db_pool.fetchrow(
            """
            SELECT
                SUM(valid_uses) AS valid_uses,
                SUM(invalid_uses) AS invalid_uses
            FROM invite_codes
            WHERE created_at >= $1
            """,
            event_state.last_reset,
        )
        valid_uses = int(row["valid_uses"] or 0)
        invalid_uses = int(row["invalid_uses"] or 0)
        embed = build_embed(
            "invite",
            f"{EMOJI['stare']} Invite Stats",
            "Current event stats.",
            [
                ("Valid", str(valid_uses), True),
                ("Invalid", str(invalid_uses), True),
                ("Ends", f"<t:{event_state.ends_at}:R>", True),
            ],
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(
        label="Buy",
        style=discord.ButtonStyle.success,
        emoji=safe_button_emoji(EMOJI["drool"], "🛒"),
        custom_id="invites_buy",
    )
    async def invite_buy(self, interaction: discord.Interaction, button: discord.ui.Button):
        event_state = await get_event_state()
        stock_info = (
            f"3 Invites → 10 Entries: {'✅' if event_state.stock_3 > 0 else '❌'}\n"
            f"5 Invites → 25 Entries: {'✅' if event_state.stock_5 > 0 else '❌'}\n"
            f"10 Invites → 75 Entries: {'✅' if event_state.stock_10 > 0 else '❌'}"
        )
        embed = build_embed(
            "invite",
            f"{EMOJI['drool']} Buy Rewards",
            "Check current stock before buying.",
            [("Stock", stock_info, False)],
        )
        await interaction.response.send_message(
            embed=embed,
            view=InviteBuyView(interaction.user.id),
            ephemeral=True,
        )


class InviteBuyView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=60)
        self.user_id = user_id

    @discord.ui.button(
        label="3 Invites → 10",
        style=discord.ButtonStyle.primary,
        emoji=safe_button_emoji(None, "3️⃣"),
    )
    async def buy_3(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_invite_purchase(interaction, self.user_id, 3, 10)

    @discord.ui.button(
        label="5 Invites → 25",
        style=discord.ButtonStyle.primary,
        emoji=safe_button_emoji(None, "5️⃣"),
    )
    async def buy_5(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_invite_purchase(interaction, self.user_id, 5, 25)

    @discord.ui.button(
        label="10 Invites → 75",
        style=discord.ButtonStyle.primary,
        emoji=safe_button_emoji(None, "🔟"),
    )
    async def buy_10(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_invite_purchase(interaction, self.user_id, 10, 75)


class SupportPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Support",
        style=discord.ButtonStyle.primary,
        emoji=safe_button_emoji(EMOJI["heart"], "🆘"),
        custom_id="support_create",
    )
    async def support_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if not guild:
            return
        existing = await db_pool.fetchrow(
            "SELECT channel_id FROM tickets WHERE opener_id=$1 AND open=true",
            interaction.user.id,
        )
        if existing:
            await interaction.response.send_message(
                f"{EMOJI['nervous_stare']} You already have an open ticket.",
                ephemeral=True,
            )
            return
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.get_role(STAFF_TICKET_ROLE_ID): discord.PermissionOverwrite(view_channel=True, send_messages=True),
            interaction.user: discord.PermissionOverwrite(view_channel=True, send_messages=True),
        }
        channel = await guild.create_text_channel(
            name=f"ticket-{interaction.user.display_name}",
            overwrites=overwrites,
            reason="Support ticket created",
        )
        await db_pool.execute(
            "INSERT INTO tickets (channel_id, opener_id, open, created_at) VALUES ($1, $2, true, $3)",
            channel.id,
            interaction.user.id,
            now_ts(),
        )
        await channel.send(
            f"{EMOJI['heart']} Thanks for reaching out! A staff member will be with you soon.",
            view=TicketCloseView(channel.id),
        )
        await interaction.response.send_message(
            f"{EMOJI['nervous_stare']} Ticket created: {channel.mention}",
            ephemeral=True,
        )
        await log_event("ticket_created", interaction.user.id, f"Ticket channel {channel.id}")

    @discord.ui.button(
        label="Trade",
        style=discord.ButtonStyle.secondary,
        emoji=safe_button_emoji(None, "💱"),
        custom_id="support_trade",
    )
    async def trade(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            f"{EMOJI['derp']} {EMOJI['panties']} Trading is coming soon!",
            ephemeral=True,
        )

    @discord.ui.button(
        label="Report",
        style=discord.ButtonStyle.danger,
        emoji=safe_button_emoji(None, "🚨"),
        custom_id="support_report",
    )
    async def report(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ReportModal())


class ReportModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Report a User")
        self.reported_user = discord.ui.TextInput(
            label="Reported User ID",
            placeholder="1234567890",
            required=True,
            max_length=20,
        )
        self.reason = discord.ui.TextInput(
            label="Reason",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=500,
        )
        self.add_item(self.reported_user)
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        reported_id = self.reported_user.value.strip()
        reason = self.reason.value.strip()
        await log_event(
            "report_submitted",
            interaction.user.id,
            f"Reported ID: {reported_id} | Reason: {reason}",
        )
        await interaction.response.send_message(
            f"{EMOJI['gun']} {EMOJI['no_bully']} Your report was submitted.",
            ephemeral=True,
        )


class TicketCloseView(discord.ui.View):
    def __init__(self, channel_id: int):
        super().__init__(timeout=None)
        self.channel_id = channel_id
        button = discord.ui.Button(
            label="Close Ticket",
            style=discord.ButtonStyle.danger,
            emoji=safe_button_emoji(EMOJI["bonk"], "🔒"),
            custom_id=f"ticket_close_{channel_id}",
        )
        button.callback = self.close_ticket
        self.add_item(button)

    async def close_ticket(self, interaction: discord.Interaction):
        if interaction.channel_id != self.channel_id:
            await interaction.response.send_message(
                f"{EMOJI['cheek_pull']} {EMOJI['pout']} This close button isn't for this channel.",
                ephemeral=True,
            )
            return
        await db_pool.execute(
            "UPDATE tickets SET open=false, closed_at=$1 WHERE channel_id=$2",
            now_ts(),
            self.channel_id,
        )
        await interaction.response.send_message("Ticket will close in 10 seconds.")
        await log_event("ticket_closed", interaction.user.id, f"Ticket channel {self.channel_id}")
        await asyncio.sleep(10)
        try:
            await interaction.channel.delete(reason="Ticket closed")
        except (discord.Forbidden, discord.NotFound):
            return


async def generate_invite_code() -> str:
    while True:
        code = secrets.token_hex(3)
        exists = await db_pool.fetchrow("SELECT 1 FROM invite_codes WHERE code=$1", code)
        if not exists:
            return code


async def handle_invite_purchase(interaction: discord.Interaction, user_id: int, needed_invites: int, reward: int):
    if interaction.user.id != user_id:
        await interaction.response.send_message(
            f"{EMOJI['cheek_pull']} {EMOJI['pout']} This menu isn't for you.",
            ephemeral=True,
        )
        return
    event_state = await get_event_state()
    stock_key = f"stock_{needed_invites}"
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            state_row = await conn.fetchrow(
                "SELECT ends_at, stock_3, stock_5, stock_10 FROM invite_event_state WHERE key='global' FOR UPDATE"
            )
            stock_value = int(state_row[stock_key])
            if stock_value <= 0:
                embed = build_embed(
                    "invite",
                    f"{EMOJI['panic']} {EMOJI['pout']} Out of Stock",
                    "Try again after the next reset.",
                    [("Resets", f"<t:{state_row['ends_at']}:R>", True)],
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return
            row = await conn.fetchrow(
                """
                SELECT SUM(valid) AS valid_count
                FROM invite_joins
                WHERE inviter_id=$1 AND joined_at >= $2 AND valid=true
                """,
                user_id,
                event_state.last_reset,
            )
            valid_invites = int(row["valid_count"] or 0)
            if valid_invites < needed_invites:
                await interaction.response.send_message(
                    f"{EMOJI['cheek_pull']} {EMOJI['pout']} Not enough valid invites.",
                    ephemeral=True,
                )
                return
            await apply_interest(user_id, conn)
            row = await conn.fetchrow(
                "SELECT entries FROM users WHERE user_id=$1 FOR UPDATE",
                user_id,
            )
            old_balance = int(row["entries"])
            new_balance = old_balance + reward
            await conn.execute(
                "UPDATE invite_event_state SET {0}={0}-1 WHERE key='global'".format(stock_key)
            )
            await conn.execute(
                "UPDATE users SET entries=$1 WHERE user_id=$2",
                new_balance,
                user_id,
            )
    embed = build_embed(
        "invite",
        f"{EMOJI['happy_pat']} {EMOJI['clap']} Purchase Complete",
        f"You gained **{reward}** entries.",
        [("New Balance", "Updated", True)],
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)
    await log_event(
        "entries_gain",
        user_id,
        json.dumps(
            {
                "source": "invite_reward",
                "amount": reward,
                "old_balance": old_balance,
                "new_balance": new_balance,
                "channel_id": interaction.channel_id,
                "tier": needed_invites,
            }
        ),
    )
    await log_event(
        "invite_entries_granted",
        user_id,
        json.dumps(
            {
                "tier": needed_invites,
                "entries_granted": reward,
                "remaining_stock": max(0, stock_value - 1),
                "valid_invites": valid_invites,
            }
        ),
    )


async def create_bank_panel(channel: discord.TextChannel):
    description = f"Entries persist forever and earn 0.25% daily interest. {EMOJI['cozy']} {EMOJI['shy_blush']}"
    fields = [
        ("Daily Interest", "0.25% compounded daily", True),
        ("Uses", "Giveaways + events", True),
    ]
    embed = build_embed(
        "bank",
        f"{EMOJI['sparkle_eyes']} {EMOJI['heart']} Bank Panel",
        description,
        fields,
    )
    await ensure_panel_message("bank_panel", channel.id, embed, BankView())


async def create_support_panel(channel: discord.TextChannel):
    description = "Trading is 100% Secured. If you have serious issues create a ticket."
    embed = build_embed(
        "support",
        f"{EMOJI['police']} {EMOJI['no_bully']} Support Panel",
        description,
        [("Need help?", "Open a ticket below.", False)],
    )
    await ensure_panel_message("support_panel", channel.id, embed, SupportPanelView())


async def create_invites_panel(channel: discord.TextChannel, event_state: EventState):
    description = "Invite friends to earn entries rewards."
    fields = [
        (
            "Tiers",
            "3 invites → 10 entries\n5 invites → 25 entries\n10 invites → 75 entries",
            False,
        ),
        ("Cooldown resets", f"<t:{event_state.ends_at}:R>", True),
    ]
    embed = build_embed(
        "invite",
        f"{EMOJI['finger_point']} {EMOJI['peace']} Invites Panel",
        description,
        fields,
    )
    message = await ensure_panel_message("invites_panel", channel.id, embed, InvitesPanelView())
    if message:
        await db_pool.execute(
            "UPDATE invite_event_state SET panel_message_id=$1 WHERE key='global'",
            message.id,
        )


async def refresh_invites_panel():
    event_state = await get_event_state()
    channel = bot.get_channel(INVITES_PANEL_CHANNEL_ID)
    if not channel:
        return
    await create_invites_panel(channel, event_state)


async def scheduled_tasks():
    while True:
        try:
            now_local = berlin_now()
            next_panel = next_daily_time(9, 0)
            next_reset = next_daily_time(21, 0)
            next_event = min(next_panel, next_reset)
            sleep_for = max(5, int((next_event - now_local).total_seconds()))
            await asyncio.sleep(sleep_for)
            now_local = berlin_now()
            if next_panel <= now_local <= next_panel + timedelta(minutes=5):
                await refresh_invites_panel()
            if next_reset <= now_local <= next_reset + timedelta(minutes=5):
                new_reset = now_ts()
                next_reset_ts = int(next_daily_time(21, 0).timestamp())
                await db_pool.execute(
                    """
                    UPDATE invite_event_state
                    SET ends_at=$1, last_reset=$2, stock_3=$3, stock_5=$4, stock_10=$5
                    WHERE key='global'
                    """,
                    next_reset_ts,
                    new_reset,
                    INVITE_STOCK_DEFAULTS["stock_3"],
                    INVITE_STOCK_DEFAULTS["stock_5"],
                    INVITE_STOCK_DEFAULTS["stock_10"],
                )
                await refresh_invites_panel()
        except Exception:
            await asyncio.sleep(10)


async def giveaway_ender():
    while True:
        try:
            rows = await db_pool.fetch(
                "SELECT * FROM giveaways WHERE ended=false AND ends_at <= $1",
                now_ts(),
            )
            for row in rows:
                await end_giveaway(row)
        except Exception:
            await asyncio.sleep(5)
        await asyncio.sleep(15)


async def end_giveaway(row: asyncpg.Record):
    channel = bot.get_channel(GENERAL_CHAT_CHANNEL_ID)
    if not channel:
        return
    giveaway_id = row["id"]
    entries = await db_pool.fetch(
        "SELECT user_id, entries_spent FROM giveaway_entries WHERE giveaway_id=$1",
        giveaway_id,
    )
    if not entries:
        embed = build_embed(
            "giveaway_normal",
            f"{EMOJI['sad_cry']} {EMOJI['crying']} Giveaway Ended",
            "No participants joined this time.",
            [("Prize", row["prize"], False)],
        )
        await channel.send(embed=embed)
        await db_pool.execute("UPDATE giveaways SET ended=true WHERE id=$1", giveaway_id)
        await log_event("giveaway_no_participants", row["created_by"], f"Giveaway {giveaway_id} ended empty")
        return
    weights = []
    for entry in entries:
        weights.append((entry["user_id"], entry["entries_spent"]))
    winners = pick_weighted_winners(weights, row["winner_count"])
    winner_mentions = "\n".join(f"<@{user_id}>" for user_id in winners)
    embed = build_embed(
        "giveaway_normal",
        f"{EMOJI['clap']} {EMOJI['peace']} {EMOJI['sparkle_eyes']} Winners!",
        "Congrats to the winners!",
        [
            ("Prize", row["prize"], False),
            ("Winners", winner_mentions, False),
        ],
    )
    await channel.send(embed=embed)
    await db_pool.execute("UPDATE giveaways SET ended=true WHERE id=$1", giveaway_id)
    await log_event("giveaway_ended", row["created_by"], f"Giveaway {giveaway_id} ended")


def pick_weighted_winners(entries: List[Tuple[int, int]], count: int) -> List[int]:
    winners = []
    pool = entries.copy()
    while pool and len(winners) < count:
        total = sum(weight for _, weight in pool)
        roll = random.uniform(0, total)
        upto = 0
        for user_id, weight in pool:
            upto += weight
            if roll <= upto:
                winners.append(user_id)
                pool = [(uid, w) for uid, w in pool if uid != user_id]
                break
    return winners


async def process_rng(message: discord.Message):
    user_id = message.author.id
    state = await db_pool.fetchrow("SELECT * FROM automod_state WHERE user_id=$1", user_id)
    if not state:
        await db_pool.execute(
            "INSERT INTO automod_state (user_id) VALUES ($1)",
            user_id,
        )
        state = await db_pool.fetchrow("SELECT * FROM automod_state WHERE user_id=$1", user_id)
    if state["no_entry_until"] > now_ts():
        return
    if state["rng_cooldown_until"] > now_ts():
        return
    if len(message.content) < MIN_MSG_LEN:
        return
    roll = random.random()
    award = None
    public = False
    if roll < 0.000001:
        award = 250
        public = True
        title = f"{EMOJI['yell']} {EMOJI['jump_hype']} {EMOJI['bongo_hype']} Huge Drop!"
    elif roll < 0.000011:
        award = 100
        public = True
        title = f"{EMOJI['yell']} {EMOJI['jump_hype']} {EMOJI['bongo_hype']} Jackpot!"
    elif roll < 0.000111:
        award = 50
        public = True
        title = f"{EMOJI['blur_shock']} {EMOJI['stare']} Big Bonus!"
    elif roll < 0.00111:
        award = 25
        public = True
        title = f"{EMOJI['sparkle_eyes']} Lucky +25"
    elif roll < 0.00361:
        award = 10
        public = True
        title = f"{EMOJI['sparkle_eyes']} Lucky +10"
    elif roll < 0.00861:
        award = 8
    elif roll < 0.01861:
        award = 5
    elif roll < 0.04361:
        award = 4
    elif roll < 0.09361:
        award = 2
    elif roll < 0.19361:
        award = 1
    if not award:
        await db_pool.execute(
            "UPDATE automod_state SET rng_cooldown_until=$1 WHERE user_id=$2",
            now_ts() + RNG_COOLDOWN,
            user_id,
        )
        return
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await apply_interest(user_id, conn)
            row = await conn.fetchrow(
                "SELECT entries FROM users WHERE user_id=$1 FOR UPDATE",
                user_id,
            )
            old_balance = int(row["entries"])
            new_balance = old_balance + award
            await conn.execute(
                "UPDATE users SET entries=$1 WHERE user_id=$2",
                new_balance,
                user_id,
            )
            await conn.execute(
                "UPDATE automod_state SET rng_cooldown_until=$1 WHERE user_id=$2",
                now_ts() + RNG_COOLDOWN,
                user_id,
            )
    await log_event(
        "entries_gain",
        user_id,
        json.dumps(
            {
                "source": "chat_rng",
                "amount": award,
                "old_balance": old_balance,
                "new_balance": new_balance,
                "channel_id": message.channel.id,
            }
        ),
    )
    if public:
        description = f"{message.author.mention} gained **+{award}** entries!"
        embed = build_embed(
            "rng",
            title,
            description,
            [("Hype", "Keep chatting for more drops!", False)],
        )
        try:
            await message.channel.send(embed=embed)
        except (discord.Forbidden, discord.NotFound):
            return


async def apply_automod(message: discord.Message) -> bool:
    if message.author.bot:
        return False
    if message.author.guild_permissions.manage_guild:
        normalized = normalize_message(message.content)
        if normalized and (LINK_REGEX.search(normalized) or any(word in normalized for word in BLACKLIST)):
            await log_event(
                "admin_bypass",
                message.author.id,
                json.dumps(
                    {
                        "reason": "flagged_content_bypassed",
                        "channel_id": message.channel.id,
                        "message_id": message.id,
                        "content": message.content[:300],
                    }
                ),
            )
        return True
    user_id = message.author.id
    state = await db_pool.fetchrow("SELECT * FROM automod_state WHERE user_id=$1", user_id)
    if not state:
        await db_pool.execute("INSERT INTO automod_state (user_id) VALUES ($1)", user_id)
        state = await db_pool.fetchrow("SELECT * FROM automod_state WHERE user_id=$1", user_id)
    current = now_ts()
    normalized = normalize_message(message.content)
    msg_hash = hash_message(normalized)
    streak_count = state["streak_count"]
    if current - state["last_streak_ts"] <= 12:
        streak_count += 1
    else:
        streak_count = 1
    punish_reason = None
    if streak_count >= 3:
        punish_reason = "severe_spam_streak"
    if state["last_msg_hash"] == msg_hash and current - state["last_msg_ts"] <= 120:
        punish_reason = punish_reason or "repeat_message"
    if LINK_REGEX.search(normalized):
        punish_reason = punish_reason or "link_detected"
    if any(word in normalized for word in BLACKLIST):
        punish_reason = punish_reason or "blacklist_detected"
    await db_pool.execute(
        """
        UPDATE automod_state
        SET last_msg_hash=$1, last_msg_ts=$2, streak_count=$3, last_streak_ts=$4
        WHERE user_id=$5
        """,
        msg_hash,
        current,
        streak_count,
        current,
        user_id,
    )
    if punish_reason:
        try:
            await message.delete()
        except (discord.Forbidden, discord.NotFound):
            pass
        await db_pool.execute(
            "UPDATE automod_state SET no_entry_until=$1 WHERE user_id=$2",
            current + 120,
            user_id,
        )
        try:
            await message.author.send("Please avoid spam or prohibited content. Further issues may result in action.")
        except (discord.Forbidden, discord.HTTPException):
            pass
        reason_map = {
            "severe_spam_streak": "spam",
            "repeat_message": "duplicate",
            "link_detected": "link",
            "blacklist_detected": "language",
        }
        await log_event(
            "automod_punishment",
            user_id,
            json.dumps(
                {
                    "reason": reason_map.get(punish_reason, punish_reason),
                    "channel_id": message.channel.id,
                    "message_id": message.id,
                    "content": message.content[:300],
                    "duration_seconds": 120,
                    "no_entry_until": current + 120,
                }
            ),
        )
        return False
    return True


@bot.event
async def on_ready():
    global db_pool
    db_pool = await asyncpg.create_pool(
        dsn=os.getenv("DATABASE_URL"),
        min_size=1,
        max_size=5
    )
    print("✅ Database connected")
    await run_migrations()
    await get_event_state()
    bot.add_view(BankView())
    bot.add_view(InvitesPanelView())
    bot.add_view(SupportPanelView())
    panels = await db_pool.fetch("SELECT key, channel_id FROM panels")
    for panel in panels:
        channel = bot.get_channel(panel["channel_id"])
        if not channel:
            continue
        if panel["key"] == "bank_panel":
            await create_bank_panel(channel)
        elif panel["key"] == "support_panel":
            await create_support_panel(channel)
        elif panel["key"] == "invites_panel":
            await refresh_invites_panel()
    rows = await db_pool.fetch("SELECT channel_id FROM tickets WHERE open=true")
    for row in rows:
        bot.add_view(TicketCloseView(row["channel_id"]))
    active_giveaways = await db_pool.fetch("SELECT id FROM giveaways WHERE ended=false")
    for row in active_giveaways:
        bot.add_view(GiveawayView(row["id"]))
    guilds = bot.guilds
    if guilds:
        await update_invites_cache(guilds[0])
    if not background_tasks:
        background_tasks.append(asyncio.create_task(giveaway_ender()))
        background_tasks.append(asyncio.create_task(scheduled_tasks()))


@bot.event
async def on_invite_create(invite: discord.Invite):
    invite_cache[invite.code] = invite.uses or 0


@bot.event
async def on_invite_delete(invite: discord.Invite):
    invite_cache.pop(invite.code, None)


@bot.event
async def on_member_join(member: discord.Member):
    guild = member.guild
    try:
        invites = await guild.invites()
    except (discord.Forbidden, discord.HTTPException):
        return
    used_invite = None
    for invite in invites:
        previous_uses = invite_cache.get(invite.code, 0)
        if invite.uses and invite.uses > previous_uses:
            used_invite = invite
            break
    await update_invites_cache(guild)
    if not used_invite:
        return
    code_row = await db_pool.fetchrow("SELECT * FROM invite_codes WHERE invite_id=$1", used_invite.code)
    valid = True
    invalid_reason = None
    if not code_row:
        valid = False
        invalid_reason = "not_bot_invite"
    account_age = (datetime.now(timezone.utc) - member.created_at).days
    if account_age < 30:
        valid = False
        invalid_reason = "account_too_new"
    existing = await db_pool.fetchrow("SELECT 1 FROM invite_joins WHERE invited_id=$1", member.id)
    if existing:
        valid = False
        invalid_reason = "rejoin"
    await db_pool.execute(
        """
        INSERT INTO invite_joins (invited_id, invite_id, code, inviter_id, joined_at, valid, invalid_reason)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        member.id,
        used_invite.code,
        code_row["code"] if code_row else None,
        used_invite.inviter.id if used_invite.inviter else 0,
        now_ts(),
        valid,
        invalid_reason,
    )
    if code_row:
        column = "valid_uses" if valid else "invalid_uses"
        await db_pool.execute(
            f"UPDATE invite_codes SET {column}={column}+1, uses_total=uses_total+1 WHERE code=$1",
            code_row["code"],
        )
    if not valid:
        await log_event("invite_invalid", member.id, f"Invite invalid: {invalid_reason}")


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    approved = await apply_automod(message)
    if approved:
        await process_rng(message)
    await bot.process_commands(message)


@bot.command()
@commands.has_guild_permissions(manage_guild=True)
async def bank(ctx: commands.Context):
    await create_bank_panel(ctx.channel)
    await log_event("admin_command", ctx.author.id, "!bank")


@bot.command()
@commands.has_guild_permissions(manage_guild=True)
async def support(ctx: commands.Context):
    await create_support_panel(ctx.channel)
    await log_event("admin_command", ctx.author.id, "!support")


@bot.command()
@commands.has_guild_permissions(manage_guild=True)
async def invites(ctx: commands.Context):
    event_state = await get_event_state()
    channel = bot.get_channel(INVITES_PANEL_CHANNEL_ID)
    if not channel:
        await ctx.send("Invites panel channel not found.")
        return
    await create_invites_panel(channel, event_state)
    await log_event("admin_command", ctx.author.id, "!invites")


@bot.command()
@commands.has_guild_permissions(manage_guild=True)
async def host(ctx: commands.Context, *args: str):
    if len(args) < 3:
        await ctx.send(
            "Use: `!host <prize text> <winner_amount> <time>`"
        )
        return
    time_str = args[-1]
    winners_str = args[-2]
    prize = " ".join(args[:-2]).strip()
    if not winners_str.isdigit():
        await ctx.send("Winner amount must be a whole number (1 or more).")
        return
    winner_amount = int(winners_str)
    try:
        duration = parse_duration(time_str)
    except ValueError as exc:
        await ctx.send(f"Invalid time format: {exc}")
        return
    end_timestamp = now_ts() + duration
    embed = build_embed(
        "giveaway_normal",
        f"{EMOJI['heart']} {EMOJI['clap']} Giveaway",
        "Spend entries to enter. Each entry increases your weight.",
        [
            ("Prize", prize, False),
            ("Winners", str(winner_amount), True),
            ("Ends", f"<t:{end_timestamp}:R>", True),
        ],
    )
    embed.set_footer(text=f"Hosted by {ctx.author.display_name}")
    channel = ctx.channel
    row = await db_pool.fetchrow(
        """
        INSERT INTO giveaways (channel_id, prize, winner_count, is_big, ends_at, created_by, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        """,
        channel.id,
        prize,
        winner_amount,
        False,
        end_timestamp,
        ctx.author.id,
        now_ts(),
    )
    giveaway_id = row["id"]
    message = await channel.send(embed=embed, view=GiveawayView(giveaway_id))
    await db_pool.execute(
        "UPDATE giveaways SET message_id=$1 WHERE id=$2",
        message.id,
        giveaway_id,
    )
    await log_event("admin_command", ctx.author.id, f"!host {prize} {winner_amount} {time_str}")


@bot.command()
@commands.has_guild_permissions(manage_guild=True)
async def bighost(ctx: commands.Context, *args: str):
    if len(args) < 3:
        await ctx.send("Use: `!bighost <prize text> <winner_amount> <time>`")
        return
    time_str = args[-1]
    winners_str = args[-2]
    prize = " ".join(args[:-2]).strip()
    if not winners_str.isdigit():
        await ctx.send("Winner amount must be a whole number (1 or more).")
        return
    winner_amount = int(winners_str)
    try:
        duration = parse_duration(time_str)
    except ValueError as exc:
        await ctx.send(f"Invalid time format: {exc}")
        return
    end_timestamp = now_ts() + duration
    embed = build_embed(
        "giveaway_big",
        f"{EMOJI['bongo_hype']} {EMOJI['jump_hype']} {EMOJI['sparkle_eyes']} {EMOJI['blur_shock']} BIG Giveaway",
        "Spend entries to enter. Each entry increases your weight.",
        [
            ("Prize", prize, False),
            ("Winners", str(winner_amount), True),
            ("Ends", f"<t:{end_timestamp}:R>", True),
        ],
    )
    embed.set_footer(text=f"Hosted by {ctx.author.display_name}")
    channel = ctx.channel
    row = await db_pool.fetchrow(
        """
        INSERT INTO giveaways (channel_id, prize, winner_count, is_big, ends_at, created_by, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        """,
        channel.id,
        prize,
        winner_amount,
        True,
        end_timestamp,
        ctx.author.id,
        now_ts(),
    )
    giveaway_id = row["id"]
    message = await channel.send(embed=embed, view=GiveawayView(giveaway_id))
    await db_pool.execute(
        "UPDATE giveaways SET message_id=$1 WHERE id=$2",
        message.id,
        giveaway_id,
    )
    await log_event("admin_command", ctx.author.id, f"!bighost {prize} {winner_amount} {time_str}")


@host.error
async def host_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("You need the Manage Server permission to host a giveaway.")
        return
    await ctx.send("Something went wrong while starting the giveaway.")


@bighost.error
async def bighost_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("You need the Manage Server permission to host a giveaway.")
        return
    await ctx.send("Something went wrong while starting the giveaway.")


@bank.error
async def bank_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("You need the Manage Server permission to post the bank panel.")
        return
    await ctx.send("Something went wrong while posting the bank panel.")


@support.error
async def support_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("You need the Manage Server permission to post the support panel.")
        return
    await ctx.send("Something went wrong while posting the support panel.")


@invites.error
async def invites_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("You need the Manage Server permission to post the invites panel.")
        return
    await ctx.send("Something went wrong while posting the invites panel.")


bot.run(TOKEN)
