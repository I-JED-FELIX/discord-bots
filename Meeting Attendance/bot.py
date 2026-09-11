import os
import csv
import asyncio
import threading
import wave
import json
import hmac
import hashlib
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError, available_timezones
from pathlib import Path

import asyncpg
import aiohttp
from aiohttp import web
import discord
from discord import app_commands
from discord.ext import commands, tasks, voice_recv
from dotenv import load_dotenv
from openai import OpenAI


# Load Opus for Discord voice decoding
if not discord.opus.is_loaded():
    try:
        discord.opus.load_opus("libopus.so.0")
        print("Opus loaded successfully: libopus.so.0")
    except Exception as e:
        print(f"Failed to load libopus.so.0: {e}")

print(f"Discord Opus loaded: {discord.opus.is_loaded()}")

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = os.getenv("GUILD_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET")
RAZORPAY_MONTHLY_PLAN_ID = os.getenv("RAZORPAY_MONTHLY_PLAN_ID")
RAZORPAY_ANNUAL_PLAN_ID = os.getenv("RAZORPAY_ANNUAL_PLAN_ID")
RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET")
PORT = int(os.getenv("PORT", "8080"))
SUPPORT_SERVER_URL = os.getenv("SUPPORT_SERVER_URL")
PUBLIC_BOT_INVITE_URL = os.getenv("PUBLIC_BOT_INVITE_URL")
DEV_GUILD_ID = os.getenv("DEV_GUILD_ID") or GUILD_ID

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

RECORDINGS_DIR = DATA_DIR / "recordings"
RECORDINGS_DIR.mkdir(exist_ok=True)

ATTENDANCE_DIR = DATA_DIR / "attendance"
ATTENDANCE_DIR.mkdir(exist_ok=True)

intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

# One active recorded meeting per Discord server.
active_meetings = {}

# One active attendance-only session per Discord server.
active_attendance = {}

# PostgreSQL connection pool. Railway supplies DATABASE_URL.
db_pool = None

# Embedded HTTP server used for Razorpay webhooks/health checks.
web_runner = None


async def init_database():
    """Connect to PostgreSQL and create Frost Scribe's core tables."""
    global db_pool

    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is missing. Add the Railway Postgres reference "
            "to the Frost Scribe service."
        )

    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=30,
    )

    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS guilds (
                guild_id BIGINT PRIMARY KEY,
                plan TEXT NOT NULL DEFAULT 'FREE'
                    CHECK (plan IN ('FREE', 'PRO')),
                subscription_status TEXT NOT NULL DEFAULT 'inactive',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_meetings (
                id BIGSERIAL PRIMARY KEY,
                guild_id BIGINT NOT NULL,
                name TEXT NOT NULL,
                scheduled_at TIMESTAMPTZ NOT NULL,
                reminder_channel_id BIGINT NOT NULL,
                tag_text TEXT,
                created_by BIGINT NOT NULL,
                reminder_1h_sent BOOLEAN NOT NULL DEFAULT FALSE,
                reminder_30m_sent BOOLEAN NOT NULL DEFAULT FALSE,
                cancelled BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_scheduled_meetings_due
            ON scheduled_meetings (cancelled, scheduled_at)
        """)

        await conn.execute("""
            ALTER TABLE guilds
            ADD COLUMN IF NOT EXISTS billing_cycle TEXT
        """)
        await conn.execute("""
            ALTER TABLE guilds
            ADD COLUMN IF NOT EXISTS subscription_expires_at TIMESTAMPTZ
        """)
        await conn.execute("""
            ALTER TABLE guilds
            ADD COLUMN IF NOT EXISTS entitlement_source TEXT
        """)
        await conn.execute("""
            ALTER TABLE guilds
            ADD COLUMN IF NOT EXISTS lifetime_code TEXT
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS promo_codes (
                code TEXT PRIMARY KEY,
                entitlement_type TEXT NOT NULL,
                max_redemptions INTEGER NOT NULL DEFAULT 1,
                redemptions INTEGER NOT NULL DEFAULT 0,
                redeemed_guild_id BIGINT,
                redeemed_by BIGINT,
                redeemed_at TIMESTAMPTZ,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            INSERT INTO promo_codes (code, entitlement_type, max_redemptions)
            VALUES
                ('CLOVER', 'LIFETIME_PRO', 1),
                ('FROST', 'LIFETIME_PRO', 1)
            ON CONFLICT (code) DO NOTHING
        """)


        await conn.execute("""
            ALTER TABLE guilds
            ADD COLUMN IF NOT EXISTS razorpay_subscription_id TEXT
        """)
        await conn.execute("""
            ALTER TABLE guilds
            ADD COLUMN IF NOT EXISTS cancel_at_cycle_end BOOLEAN NOT NULL DEFAULT FALSE
        """)

        await conn.execute("""
            ALTER TABLE guilds
            ADD COLUMN IF NOT EXISTS setup_complete BOOLEAN NOT NULL DEFAULT FALSE
        """)
        await conn.execute("""
            ALTER TABLE guilds
            ADD COLUMN IF NOT EXISTS default_voice_channel_id BIGINT
        """)
        await conn.execute("""
            ALTER TABLE guilds
            ADD COLUMN IF NOT EXISTS default_report_channel_id BIGINT
        """)
        await conn.execute("""
            ALTER TABLE guilds
            ADD COLUMN IF NOT EXISTS timezone_name TEXT NOT NULL DEFAULT 'UTC'
        """)
        await conn.execute("""
            ALTER TABLE guilds
            ADD COLUMN IF NOT EXISTS attendance_enabled BOOLEAN NOT NULL DEFAULT TRUE
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS guild_subscriptions (
                subscription_id TEXT PRIMARY KEY,
                guild_id BIGINT NOT NULL,
                created_by BIGINT NOT NULL,
                billing_cycle TEXT NOT NULL,
                plan_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'created',
                short_url TEXT,
                current_period_end TIMESTAMPTZ,
                cancel_at_cycle_end BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_guild_subscriptions_guild
            ON guild_subscriptions (guild_id, created_at DESC)
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS donations (
                id BIGSERIAL PRIMARY KEY,
                payment_link_id TEXT UNIQUE,
                payment_id TEXT,
                guild_id BIGINT,
                user_id BIGINT,
                amount_paise BIGINT NOT NULL,
                currency TEXT NOT NULL DEFAULT 'INR',
                status TEXT NOT NULL DEFAULT 'paid',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS razorpay_webhook_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

    print("PostgreSQL connected.")
    print("Database tables ready: guilds, scheduled_meetings, promo_codes, guild_subscriptions, donations")


async def ensure_guild(guild_id: int):
    """Ensure a Discord server has a FREE plan row."""
    if db_pool is None:
        return

    await db_pool.execute("""
        INSERT INTO guilds (guild_id)
        VALUES ($1)
        ON CONFLICT (guild_id) DO NOTHING
    """, guild_id)


def utcnow():
    return datetime.now(timezone.utc)


async def get_report_channel(guild: discord.Guild, fallback_channel):
    """Return configured output channel, or the command channel as fallback."""
    try:
        config = await get_guild_config(guild.id)
        channel_id = config["default_report_channel_id"] if config else None
        if channel_id:
            channel = guild.get_channel(channel_id)
            if channel is not None:
                return channel
    except Exception:
        pass
    return fallback_channel


def format_duration(seconds: float) -> str:
    total_minutes = int(round(seconds / 60))
    hours, minutes = divmod(total_minutes, 60)
    return f"{hours}h {minutes}m" if hours else f"{minutes}m"


def safe_filename(value: str) -> str:
    value = value or "meeting"
    return "".join(
        c if c.isalnum() or c in ("-", "_") else "_"
        for c in value
    ).strip("_") or "meeting"


def ensure_participant(meeting, member: discord.Member):
    uid = member.id
    if uid not in meeting["participants"]:
        meeting["participants"][uid] = {
            "display_name": member.display_name,
            "username": str(member),
            "joined_at": None,
            "seconds": 0.0,
        }
    else:
        meeting["participants"][uid]["display_name"] = member.display_name
        meeting["participants"][uid]["username"] = str(member)
    return meeting["participants"][uid]


def start_session(meeting, member: discord.Member):
    participant = ensure_participant(meeting, member)
    if participant["joined_at"] is None:
        participant["joined_at"] = utcnow()


def end_session(meeting, member: discord.Member):
    participant = ensure_participant(meeting, member)
    if participant["joined_at"] is not None:
        participant["seconds"] += (
            utcnow() - participant["joined_at"]
        ).total_seconds()
        participant["joined_at"] = None


def current_seconds(participant):
    seconds = participant["seconds"]
    if participant["joined_at"] is not None:
        seconds += (
            utcnow() - participant["joined_at"]
        ).total_seconds()
    return seconds


class PerSpeakerWaveSink(voice_recv.AudioSink):
    """
    Records decoded Discord PCM into one WAV file per speaker.

    discord-ext-voice-recv currently does not reliably mix/fill silence for
    multiple speakers, so separate files are safer and also preserve speaker
    identity for transcription.
    """

    CHANNELS = 2
    SAMPLE_WIDTH = 2
    SAMPLE_RATE = 48000

    def __init__(self, folder: Path):
        super().__init__()
        self.folder = folder
        self.folder.mkdir(parents=True, exist_ok=True)
        self._writers = {}
        self.paths = {}
        self.speaker_names = {}
        self._lock = threading.Lock()
        self.closed = False

    def wants_opus(self) -> bool:
        return False

    def write(self, user, data):
        if self.closed:
            return
        if user is None or getattr(user, "bot", False):
            return

        pcm = getattr(data, "pcm", None)
        if not pcm:
            return

        uid = int(user.id)
        speaker = getattr(user, "display_name", str(user))

        with self._lock:
            if uid not in self._writers:
                path = self.folder / (
                    f"{uid}_{safe_filename(speaker)}.wav"
                )
                writer = wave.open(str(path), "wb")
                writer.setnchannels(self.CHANNELS)
                writer.setsampwidth(self.SAMPLE_WIDTH)
                writer.setframerate(self.SAMPLE_RATE)

                self._writers[uid] = writer
                self.paths[uid] = path
                self.speaker_names[uid] = speaker

            self._writers[uid].writeframes(pcm)

    def cleanup(self):
        with self._lock:
            if self.closed:
                return
            self.closed = True
            for writer in self._writers.values():
                try:
                    writer.close()
                except Exception:
                    pass
            self._writers.clear()


def split_wav_if_needed(path: Path, max_bytes: int = 20 * 1024 * 1024):
    """
    Split large WAVs before upload. Uses ~20 MB chunks to stay comfortably
    below common multipart audio-upload limits.
    """
    if path.stat().st_size <= max_bytes:
        return [path]

    parts = []
    with wave.open(str(path), "rb") as src:
        channels = src.getnchannels()
        width = src.getsampwidth()
        rate = src.getframerate()
        bytes_per_frame = channels * width

        # Keep room for WAV headers.
        frames_per_part = max(
            1, (max_bytes - 4096) // bytes_per_frame
        )

        part_num = 1
        while True:
            frames = src.readframes(frames_per_part)
            if not frames:
                break

            part_path = path.with_name(
                f"{path.stem}_part{part_num:03d}.wav"
            )
            with wave.open(str(part_path), "wb") as out:
                out.setnchannels(channels)
                out.setsampwidth(width)
                out.setframerate(rate)
                out.writeframes(frames)

            parts.append(part_path)
            part_num += 1

    return parts


def transcribe_meeting(meeting, ended_at):
    if not OPENAI_API_KEY:
        raise RuntimeError(
            "OPENAI_API_KEY is missing from Railway Variables."
        )

    client = OpenAI(api_key=OPENAI_API_KEY)
    sink = meeting["audio_sink"]

    speaker_sections = []

    for uid, path in sink.paths.items():
        if not path.exists() or path.stat().st_size <= 44:
            continue

        speaker = sink.speaker_names.get(uid, str(uid))
        speaker_parts = []

        for part_path in split_wav_if_needed(path):
            with part_path.open("rb") as audio_file:
                result = client.audio.transcriptions.create(
                    model="gpt-4o-transcribe",
                    file=audio_file,
                )

            text = (getattr(result, "text", "") or "").strip()
            if text:
                speaker_parts.append(text)

        if speaker_parts:
            speaker_sections.append(
                f"### {speaker}\n" + "\n".join(speaker_parts)
            )

    transcript = "\n\n".join(speaker_sections).strip()

    transcript_path = meeting["recording_dir"] / "transcript.txt"
    if transcript:
        transcript_path.write_text(transcript, encoding="utf-8")
    else:
        transcript_path.write_text(
            "No intelligible speech was captured.",
            encoding="utf-8",
        )

    return transcript, transcript_path


def summarize_transcript(meeting, ended_at, transcript):
    if not transcript:
        return "No intelligible speech was captured.", None

    client = OpenAI(api_key=OPENAI_API_KEY)

    prompt = f"""
Create accurate meeting minutes from the transcript below.

Meeting: {meeting['name']}
Voice channel: {meeting['channel_name']}
Started UTC: {meeting['started_at'].isoformat()}
Ended UTC: {ended_at.isoformat()}

The recording was captured as separate Discord speaker tracks. The text under
each speaker heading belongs to that Discord member. Do not invent chronology
when it is not clear.

Return exactly these sections:

## Executive Summary
A concise overview of what the meeting covered.

## Key Discussion Points
Important topics and viewpoints.

## Decisions Made
Only decisions actually supported by the transcript.

## Action Items
Use bullets in this format where possible:
- Owner — task — deadline
Only include an owner or deadline when explicitly stated.

## Open Questions / Risks
Anything unresolved, blocked, uncertain, or requiring follow-up.

Rules:
- Never invent facts, decisions, owners, deadlines, or commitments.
- Preserve names from the speaker headings.
- If a section has nothing supported by the transcript, write "None identified."

TRANSCRIPT:
{transcript}
""".strip()

    response = client.responses.create(
        model="gpt-5",
        input=prompt,
        store=False,
    )

    summary = response.output_text.strip()

    summary_path = meeting["recording_dir"] / "meeting_summary.txt"
    summary_path.write_text(summary, encoding="utf-8")

    return summary, summary_path


@bot.event
async def on_ready():
    global db_pool

    try:
        if db_pool is None:
            await init_database()

        for connected_guild in bot.guilds:
            await ensure_guild(connected_guild.id)

        if not scheduled_reminder_worker.is_running():
            scheduled_reminder_worker.start()

        await start_web_server()

    except Exception as e:
        print(f"Database initialization failed: {type(e).__name__}: {e}")
        return

    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} global commands")

        if DEV_GUILD_ID:
            dev_guild = discord.Object(id=int(DEV_GUILD_ID))
            bot.tree.copy_global_to(guild=dev_guild)
            dev_synced = await bot.tree.sync(guild=dev_guild)
            print(
                f"Synced {len(dev_synced)} commands to dev guild "
                f"{DEV_GUILD_ID}"
            )
    except Exception as e:
        print(f"Command sync failed: {e}")

    print(f"Logged in as {bot.user} ({bot.user.id})")


@bot.event
async def on_guild_join(guild: discord.Guild):
    """Initialize Frost Scribe data immediately when added to a new server."""
    try:
        await ensure_guild(guild.id)
        print(f"Initialized Frost Scribe for guild {guild.id} ({guild.name})")
    except Exception as e:
        print(
            f"Guild initialization failed for {guild.id}: "
            f"{type(e).__name__}: {e}"
        )



def razorpay_configured() -> bool:
    return bool(
        RAZORPAY_KEY_ID
        and RAZORPAY_KEY_SECRET
        and RAZORPAY_MONTHLY_PLAN_ID
        and RAZORPAY_ANNUAL_PLAN_ID
    )


async def razorpay_request(method: str, path: str, payload=None):
    if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        raise RuntimeError("Razorpay API credentials are not configured.")

    url = f"https://api.razorpay.com/v1{path}"
    auth = aiohttp.BasicAuth(
        RAZORPAY_KEY_ID,
        RAZORPAY_KEY_SECRET,
    )

    async with aiohttp.ClientSession(auth=auth) as session:
        async with session.request(
            method,
            url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            data = await response.json(content_type=None)

            if response.status >= 400:
                description = (
                    data.get("error", {}).get("description")
                    if isinstance(data, dict)
                    else None
                )
                raise RuntimeError(
                    description or f"Razorpay API error {response.status}"
                )

            return data


async def create_razorpay_subscription(
    guild_id: int,
    user_id: int,
    billing_cycle: str,
):
    cycle = billing_cycle.lower()

    if cycle == "monthly":
        plan_id = RAZORPAY_MONTHLY_PLAN_ID
        total_count = 120
    elif cycle == "annual":
        plan_id = RAZORPAY_ANNUAL_PLAN_ID
        total_count = 10
    else:
        raise ValueError("Unsupported billing cycle.")

    if not plan_id:
        raise RuntimeError(
            f"Razorpay {cycle} Plan ID is missing."
        )

    payload = {
        "plan_id": plan_id,
        "total_count": total_count,
        "customer_notify": 1,
        "notes": {
            "product": "Frost Scribe Pro",
            "guild_id": str(guild_id),
            "created_by": str(user_id),
            "billing_cycle": cycle,
        },
    }

    result = await razorpay_request(
        "POST",
        "/subscriptions",
        payload,
    )

    await db_pool.execute(
        """
        INSERT INTO guild_subscriptions (
            subscription_id,
            guild_id,
            created_by,
            billing_cycle,
            plan_id,
            status,
            short_url,
            updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
        ON CONFLICT (subscription_id)
        DO UPDATE SET
            status = EXCLUDED.status,
            short_url = EXCLUDED.short_url,
            updated_at = NOW()
        """,
        result["id"],
        guild_id,
        user_id,
        cycle,
        plan_id,
        result.get("status", "created"),
        result.get("short_url"),
    )

    return result


async def create_donation_link(
    guild_id: int | None,
    user_id: int,
    amount_inr: int,
):
    if amount_inr < 10 or amount_inr > 100000:
        raise ValueError(
            "Donation must be between ₹10 and ₹1,00,000."
        )

    amount_paise = amount_inr * 100

    payload = {
        "amount": amount_paise,
        "currency": "INR",
        "accept_partial": False,
        "description": "Support Frost Scribe development",
        "reminder_enable": False,
        "notes": {
            "kind": "donation",
            "product": "Frost Scribe",
            "guild_id": str(guild_id or 0),
            "user_id": str(user_id),
            "amount_inr": str(amount_inr),
        },
    }

    return await razorpay_request(
        "POST",
        "/payment_links",
        payload,
    )


async def activate_paid_subscription(
    subscription_id: str,
    status: str,
    current_period_end=None,
):
    row = await db_pool.fetchrow(
        """
        SELECT guild_id, billing_cycle
        FROM guild_subscriptions
        WHERE subscription_id = $1
        """,
        subscription_id,
    )

    if not row:
        return

    guild_id = row["guild_id"]

    await db_pool.execute(
        """
        UPDATE guild_subscriptions
        SET status = $2,
            current_period_end = COALESCE($3, current_period_end),
            updated_at = NOW()
        WHERE subscription_id = $1
        """,
        subscription_id,
        status,
        current_period_end,
    )

    await db_pool.execute(
        """
        UPDATE guilds
        SET plan = 'PRO',
            subscription_status = 'active',
            billing_cycle = $2,
            subscription_expires_at = COALESCE($3, subscription_expires_at),
            entitlement_source = 'RAZORPAY',
            lifetime_code = NULL,
            razorpay_subscription_id = $4,
            cancel_at_cycle_end = FALSE,
            updated_at = NOW()
        WHERE guild_id = $1
          AND COALESCE(entitlement_source, '') <> 'LIFETIME_PROMO'
        """,
        guild_id,
        row["billing_cycle"],
        current_period_end,
        subscription_id,
    )


async def mark_subscription_inactive(
    subscription_id: str,
    status: str,
):
    row = await db_pool.fetchrow(
        """
        SELECT guild_id
        FROM guild_subscriptions
        WHERE subscription_id = $1
        """,
        subscription_id,
    )

    if not row:
        return

    await db_pool.execute(
        """
        UPDATE guild_subscriptions
        SET status = $2,
            updated_at = NOW()
        WHERE subscription_id = $1
        """,
        subscription_id,
        status,
    )

    await db_pool.execute(
        """
        UPDATE guilds
        SET plan = 'FREE',
            subscription_status = $2,
            subscription_expires_at = NULL,
            entitlement_source = NULL,
            razorpay_subscription_id = NULL,
            cancel_at_cycle_end = FALSE,
            updated_at = NOW()
        WHERE guild_id = $1
          AND COALESCE(entitlement_source, '') <> 'LIFETIME_PROMO'
        """,
        row["guild_id"],
        status,
    )


def _unix_to_datetime(value):
    if not value:
        return None
    return datetime.fromtimestamp(
        int(value),
        tz=timezone.utc,
    )


async def razorpay_webhook(request: web.Request):
    if not RAZORPAY_WEBHOOK_SECRET:
        return web.Response(
            status=503,
            text="Webhook secret not configured",
        )

    raw_body = await request.read()
    signature = request.headers.get(
        "X-Razorpay-Signature",
        "",
    )

    expected = hmac.new(
        RAZORPAY_WEBHOOK_SECRET.encode("utf-8"),
        raw_body,
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(signature, expected):
        return web.Response(
            status=401,
            text="Invalid signature",
        )

    payload = json.loads(raw_body.decode("utf-8"))
    event_type = payload.get("event", "unknown")
    event_id = (
        request.headers.get("X-Razorpay-Event-Id")
        or hashlib.sha256(raw_body).hexdigest()
    )

    inserted = await db_pool.fetchval(
        """
        INSERT INTO razorpay_webhook_events (
            event_id,
            event_type
        )
        VALUES ($1,$2)
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id
        """,
        event_id,
        event_type,
    )

    if not inserted:
        return web.Response(
            status=200,
            text="Duplicate ignored",
        )

    try:
        if event_type.startswith("subscription."):
            entity = (
                payload.get("payload", {})
                .get("subscription", {})
                .get("entity", {})
            )

            subscription_id = entity.get("id")
            status = entity.get("status") or event_type.split(".", 1)[1]
            period_end = _unix_to_datetime(
                entity.get("current_end")
                or entity.get("end_at")
            )

            if subscription_id:
                if event_type in {
                    "subscription.activated",
                    "subscription.charged",
                    "subscription.resumed",
                }:
                    await activate_paid_subscription(
                        subscription_id,
                        "active",
                        period_end,
                    )
                elif event_type in {
                    "subscription.cancelled",
                    "subscription.completed",
                    "subscription.expired",
                }:
                    await mark_subscription_inactive(
                        subscription_id,
                        status,
                    )
                else:
                    await db_pool.execute(
                        """
                        UPDATE guild_subscriptions
                        SET status = $2,
                            current_period_end = COALESCE($3, current_period_end),
                            updated_at = NOW()
                        WHERE subscription_id = $1
                        """,
                        subscription_id,
                        status,
                        period_end,
                    )

        elif event_type == "payment_link.paid":
            payment_link = (
                payload.get("payload", {})
                .get("payment_link", {})
                .get("entity", {})
            )
            payment = (
                payload.get("payload", {})
                .get("payment", {})
                .get("entity", {})
            )

            notes = payment_link.get("notes") or {}
            if notes.get("kind") == "donation":
                await db_pool.execute(
                    """
                    INSERT INTO donations (
                        payment_link_id,
                        payment_id,
                        guild_id,
                        user_id,
                        amount_paise,
                        currency,
                        status
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,'paid')
                    ON CONFLICT (payment_link_id) DO NOTHING
                    """,
                    payment_link.get("id"),
                    payment.get("id"),
                    int(notes.get("guild_id", "0")) or None,
                    int(notes.get("user_id", "0")) or None,
                    int(payment_link.get("amount") or payment.get("amount") or 0),
                    payment_link.get("currency") or payment.get("currency") or "INR",
                )

    except Exception as e:
        print(
            f"Razorpay webhook processing failed: "
            f"{type(e).__name__}: {e}"
        )
        return web.Response(
            status=500,
            text="Processing failed",
        )

    return web.Response(
        status=200,
        text="OK",
    )


async def health_check(request: web.Request):
    return web.json_response(
        {
            "ok": True,
            "bot": str(bot.user) if bot.user else None,
            "database": db_pool is not None,
        }
    )


async def start_web_server():
    global web_runner

    if web_runner is not None:
        return

    app = web.Application()
    app.router.add_get("/health", health_check)
    app.router.add_post(
        "/razorpay/webhook",
        razorpay_webhook,
    )

    web_runner = web.AppRunner(app)
    await web_runner.setup()

    site = web.TCPSite(
        web_runner,
        "0.0.0.0",
        PORT,
    )
    await site.start()

    print(f"HTTP server listening on port {PORT}")


async def get_guild_plan(guild_id: int) -> str:
    """Return the effective FREE/PRO plan for a Discord server."""
    if db_pool is None:
        return "FREE"

    await ensure_guild(guild_id)

    row = await db_pool.fetchrow(
        """
        SELECT plan, subscription_status,
               subscription_expires_at, entitlement_source
        FROM guilds
        WHERE guild_id = $1
        """,
        guild_id,
    )

    if not row or (row["plan"] or "FREE").upper() != "PRO":
        return "FREE"

    if (row["entitlement_source"] or "").upper() == "LIFETIME_PROMO":
        return "PRO"

    status = (row["subscription_status"] or "").lower()
    expires_at = row["subscription_expires_at"]

    if status == "active" and (expires_at is None or expires_at > utcnow()):
        return "PRO"

    return "FREE"


async def is_pro_guild(guild_id: int) -> bool:
    return await get_guild_plan(guild_id) == "PRO"


async def require_pro(interaction: discord.Interaction) -> bool:
    """Return True for PRO guilds; otherwise show the upgrade message."""
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command must be used inside a server.",
            ephemeral=True,
        )
        return False

    if await is_pro_guild(interaction.guild.id):
        return True

    await interaction.response.send_message(
        "❄️ **Frost Scribe Pro required**\n\n"
        "Recording, transcription, and AI meeting summaries are Pro features "
        "because they use paid processing credits.\n\n"
        "Free features remain available: scheduling, reminders, and attendance tracking.",
        ephemeral=True,
    )
    return False


async def get_guild_config(guild_id: int):
    await ensure_guild(guild_id)
    return await db_pool.fetchrow(
        """
        SELECT setup_complete,
               default_voice_channel_id,
               default_report_channel_id,
               timezone_name,
               attendance_enabled,
               plan,
               subscription_status
        FROM guilds
        WHERE guild_id = $1
        """,
        guild_id,
    )


def setup_embed(guild: discord.Guild, config):
    voice_id = config["default_voice_channel_id"] if config else None
    report_id = config["default_report_channel_id"] if config else None
    timezone_name = config["timezone_name"] if config else "UTC"
    attendance_enabled = (
        config["attendance_enabled"] if config else True
    )
    setup_complete = config["setup_complete"] if config else False
    plan = (config["plan"] if config else "FREE") or "FREE"

    embed = discord.Embed(
        title="❄️ Frost Scribe — Server Setup",
        description=(
            f"Configure Frost Scribe for **{guild.name}**.\n"
            "Changes are saved immediately. Press **Finish Setup** when ready."
        ),
    )
    embed.add_field(
        name="Plan",
        value=f"**{plan.title()}**",
        inline=True,
    )
    embed.add_field(
        name="Setup Status",
        value="✅ Complete" if setup_complete else "🛠️ In progress",
        inline=True,
    )
    embed.add_field(
        name="Default Voice Channel",
        value=f"<#{voice_id}>" if voice_id else "Not set",
        inline=False,
    )
    embed.add_field(
        name="Reports / Output Channel",
        value=f"<#{report_id}>" if report_id else "Not set",
        inline=False,
    )
    embed.add_field(
        name="Timezone",
        value=f"`{timezone_name or 'UTC'}`",
        inline=True,
    )
    embed.add_field(
        name="Attendance",
        value="Enabled" if attendance_enabled else "Disabled",
        inline=True,
    )
    embed.set_footer(
        text="You can run /setup again at any time to change these settings."
    )
    return embed


class SetupVoiceChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, owner_id: int, guild_id: int):
        self.owner_id = owner_id
        self.guild_id = guild_id
        super().__init__(
            placeholder="Select default voice channel",
            channel_types=[
                discord.ChannelType.voice,
                discord.ChannelType.stage_voice,
            ],
            min_values=1,
            max_values=1,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "This setup panel belongs to another administrator.",
                ephemeral=True,
            )
            return

        channel = self.values[0]
        await db_pool.execute(
            """
            UPDATE guilds
            SET default_voice_channel_id = $2,
                updated_at = NOW()
            WHERE guild_id = $1
            """,
            self.guild_id,
            channel.id,
        )
        config = await get_guild_config(self.guild_id)
        await interaction.response.edit_message(
            embed=setup_embed(interaction.guild, config),
            view=SetupView(self.owner_id, self.guild_id),
        )


class SetupReportChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, owner_id: int, guild_id: int):
        self.owner_id = owner_id
        self.guild_id = guild_id
        super().__init__(
            placeholder="Select reports/output channel",
            channel_types=[
                discord.ChannelType.text,
                discord.ChannelType.news,
            ],
            min_values=1,
            max_values=1,
            row=1,
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "This setup panel belongs to another administrator.",
                ephemeral=True,
            )
            return

        channel = self.values[0]
        await db_pool.execute(
            """
            UPDATE guilds
            SET default_report_channel_id = $2,
                updated_at = NOW()
            WHERE guild_id = $1
            """,
            self.guild_id,
            channel.id,
        )
        config = await get_guild_config(self.guild_id)
        await interaction.response.edit_message(
            embed=setup_embed(interaction.guild, config),
            view=SetupView(self.owner_id, self.guild_id),
        )


class SetupTimezoneModal(
    discord.ui.Modal,
    title="Frost Scribe Timezone",
):
    timezone_name = discord.ui.TextInput(
        label="IANA timezone",
        placeholder="Example: Asia/Kolkata, Asia/Manila, Europe/London",
        max_length=64,
    )

    def __init__(self, owner_id: int, guild_id: int, current: str):
        super().__init__()
        self.owner_id = owner_id
        self.guild_id = guild_id
        self.timezone_name.default = current or "UTC"

    async def on_submit(self, interaction: discord.Interaction):
        value = str(self.timezone_name).strip()
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError:
            await interaction.response.send_message(
                "❌ Unknown timezone. Use an IANA timezone such as "
                "`Asia/Kolkata`, `Asia/Manila`, `Europe/London`, or "
                "`America/New_York`.",
                ephemeral=True,
            )
            return

        await db_pool.execute(
            """
            UPDATE guilds
            SET timezone_name = $2,
                updated_at = NOW()
            WHERE guild_id = $1
            """,
            self.guild_id,
            value,
        )

        config = await get_guild_config(self.guild_id)
        await interaction.response.edit_message(
            embed=setup_embed(interaction.guild, config),
            view=SetupView(self.owner_id, self.guild_id),
        )


class SetupView(discord.ui.View):
    def __init__(self, owner_id: int, guild_id: int):
        super().__init__(timeout=900)
        self.owner_id = owner_id
        self.guild_id = guild_id
        self.add_item(SetupVoiceChannelSelect(owner_id, guild_id))
        self.add_item(SetupReportChannelSelect(owner_id, guild_id))

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "This setup panel belongs to another administrator.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(
        label="Timezone",
        emoji="🌍",
        style=discord.ButtonStyle.secondary,
        row=2,
    )
    async def timezone_button(self, interaction, button):
        config = await get_guild_config(self.guild_id)
        await interaction.response.send_modal(
            SetupTimezoneModal(
                self.owner_id,
                self.guild_id,
                config["timezone_name"] or "UTC",
            )
        )

    @discord.ui.button(
        label="Toggle Attendance",
        emoji="📋",
        style=discord.ButtonStyle.secondary,
        row=2,
    )
    async def attendance_button(self, interaction, button):
        config = await get_guild_config(self.guild_id)
        new_value = not bool(config["attendance_enabled"])
        await db_pool.execute(
            """
            UPDATE guilds
            SET attendance_enabled = $2,
                updated_at = NOW()
            WHERE guild_id = $1
            """,
            self.guild_id,
            new_value,
        )
        config = await get_guild_config(self.guild_id)
        await interaction.response.edit_message(
            embed=setup_embed(interaction.guild, config),
            view=SetupView(self.owner_id, self.guild_id),
        )

    @discord.ui.button(
        label="Finish Setup",
        emoji="✅",
        style=discord.ButtonStyle.success,
        row=3,
    )
    async def finish_button(self, interaction, button):
        config = await get_guild_config(self.guild_id)
        missing = []
        if not config["default_voice_channel_id"]:
            missing.append("default voice channel")
        if not config["default_report_channel_id"]:
            missing.append("reports/output channel")

        if missing:
            await interaction.response.send_message(
                "❌ Before finishing, set: " + ", ".join(missing) + ".",
                ephemeral=True,
            )
            return

        await db_pool.execute(
            """
            UPDATE guilds
            SET setup_complete = TRUE,
                updated_at = NOW()
            WHERE guild_id = $1
            """,
            self.guild_id,
        )
        config = await get_guild_config(self.guild_id)

        await interaction.response.edit_message(
            embed=discord.Embed(
                title="✅ Frost Scribe is ready",
                description=(
                    f"**{interaction.guild.name}** has been configured.\n\n"
                    f"🎙️ Default voice: <#{config['default_voice_channel_id']}>\n"
                    f"📝 Reports: <#{config['default_report_channel_id']}>\n"
                    f"🌍 Timezone: `{config['timezone_name']}`\n"
                    f"📋 Attendance: "
                    f"**{'Enabled' if config['attendance_enabled'] else 'Disabled'}**\n\n"
                    "Use `/help` to see Frost Scribe commands."
                ),
            ),
            view=None,
        )

    @discord.ui.button(
        label="Reset Setup",
        emoji="♻️",
        style=discord.ButtonStyle.danger,
        row=3,
    )
    async def reset_button(self, interaction, button):
        await db_pool.execute(
            """
            UPDATE guilds
            SET setup_complete = FALSE,
                default_voice_channel_id = NULL,
                default_report_channel_id = NULL,
                timezone_name = 'UTC',
                attendance_enabled = TRUE,
                updated_at = NOW()
            WHERE guild_id = $1
            """,
            self.guild_id,
        )
        config = await get_guild_config(self.guild_id)
        await interaction.response.edit_message(
            embed=setup_embed(interaction.guild, config),
            view=SetupView(self.owner_id, self.guild_id),
        )


@bot.tree.command(
    name="setup",
    description="Configure Frost Scribe for this server",
)
@app_commands.checks.has_permissions(manage_guild=True)
async def setup_command(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            "Use this command inside a server.",
            ephemeral=True,
        )
        return

    config = await get_guild_config(interaction.guild.id)
    await interaction.response.send_message(
        embed=setup_embed(interaction.guild, config),
        view=SetupView(
            interaction.user.id,
            interaction.guild.id,
        ),
        ephemeral=True,
    )


@bot.tree.command(
    name="help",
    description="Show Frost Scribe commands and features",
)
async def frost_help(interaction: discord.Interaction):
    embed = discord.Embed(
        title="❄️ Frost Scribe — Help",
        description=(
            "Meeting scheduling, attendance, recording, and AI-powered "
            "meeting notes for Discord."
        ),
    )
    embed.add_field(
        name="🛠️ Setup",
        value=(
            "`/setup` — Configure this server *(Manage Server)*\n"
            "`/plan status` — Check Free/Pro status"
        ),
        inline=False,
    )
    embed.add_field(
        name="📅 Scheduling",
        value=(
            "`/schedule create` — Interactive meeting scheduler\n"
            "`/schedule list` — Upcoming meetings\n"
            "`/schedule cancel` — Cancel a scheduled meeting"
        ),
        inline=False,
    )
    embed.add_field(
        name="📋 Attendance",
        value=(
            "`/attendance start` — Start attendance-only tracking\n"
            "`/attendance status` — View attendance session\n"
            "`/attendance stop` — Export attendance report"
        ),
        inline=False,
    )
    embed.add_field(
        name="🎙️ Recording",
        value=(
            "`/record start` — Record a meeting + attendance\n"
            "`/record status` — View active recording\n"
            "`/record stop` — Stop and export results\n"
            "Free includes recording; Pro adds AI transcription and summaries."
        ),
        inline=False,
    )
    embed.add_field(
        name="💎 Pro & Billing",
        value=(
            "`/plan upgrade` — Monthly ₹199 / Annual ₹1,999\n"
            "`/plan billing` — Billing details\n"
            "`/plan cancel` — Cancel at billing-cycle end\n"
            "`/redeem` — Redeem a promo code"
        ),
        inline=False,
    )
    embed.add_field(
        name="❤️ Support",
        value=(
            "`/donate` — Optional one-time donation\n"
            "`/invite` — Add Frost Scribe to another server\n"
            "`/support` — Support/community link"
        ),
        inline=False,
    )

    await interaction.response.send_message(
        embed=embed,
        ephemeral=True,
    )


def frost_invite_url():
    if PUBLIC_BOT_INVITE_URL:
        return PUBLIC_BOT_INVITE_URL

    if bot.user is None:
        return None

    permissions = discord.Permissions(2184301568)
    return discord.utils.oauth_url(
        bot.user.id,
        permissions=permissions,
        scopes=("bot", "applications.commands"),
    )


@bot.tree.command(
    name="invite",
    description="Get the Frost Scribe server install link",
)
async def invite_command(interaction: discord.Interaction):
    url = frost_invite_url()
    if not url:
        await interaction.response.send_message(
            "The invite link is not available yet.",
            ephemeral=True,
        )
        return

    view = discord.ui.View(timeout=None)
    view.add_item(
        discord.ui.Button(
            label="Add Frost Scribe",
            emoji="❄️",
            style=discord.ButtonStyle.link,
            url=url,
        )
    )
    await interaction.response.send_message(
        "❄️ **Install Frost Scribe on another Discord server:**",
        view=view,
        ephemeral=True,
    )


@bot.tree.command(
    name="support",
    description="Get Frost Scribe support information",
)
async def support_command(interaction: discord.Interaction):
    if SUPPORT_SERVER_URL:
        view = discord.ui.View(timeout=None)
        view.add_item(
            discord.ui.Button(
                label="Frost Scribe Support",
                emoji="🛟",
                style=discord.ButtonStyle.link,
                url=SUPPORT_SERVER_URL,
            )
        )
        await interaction.response.send_message(
            "Need help with Frost Scribe?",
            view=view,
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        "🛟 **Frost Scribe Support**\n"
        "A public support-server link has not been configured yet.\n"
        "Server administrators can still use `/help` for command guidance.",
        ephemeral=True,
    )


plan_group = app_commands.Group(
    name="plan",
    description="View Frost Scribe plan information",
)


@plan_group.command(
    name="status",
    description="Show this server's Frost Scribe plan",
)
async def plan_status(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command must be used inside a server.",
            ephemeral=True,
        )
        return

    await ensure_guild(interaction.guild.id)

    row = await db_pool.fetchrow(
        """
        SELECT plan, subscription_status, billing_cycle,
               subscription_expires_at, entitlement_source
        FROM guilds
        WHERE guild_id = $1
        """,
        interaction.guild.id,
    )

    plan = await get_guild_plan(interaction.guild.id)

    if plan == "PRO":
        if (row["entitlement_source"] or "").upper() == "LIFETIME_PROMO":
            await interaction.response.send_message(
                "👑 **Frost Scribe Lifetime Pro**\n"
                "This server has permanent Pro access."
            )
            return

        expires_at = row["subscription_expires_at"]
        expiry_text = discord_time(expires_at, "F") if expires_at else "No expiry recorded"

        await interaction.response.send_message(
            "💎 **Frost Scribe Pro**\n"
            f"Billing: **{row['billing_cycle'] or 'subscription'}**\n"
            f"Status: **{row['subscription_status'] or 'active'}**\n"
            f"Expires/Renews: {expiry_text}"
        )
        return

    await interaction.response.send_message(
        "❄️ **Frost Scribe Free**\n"
        "Scheduling, reminders, attendance tracking, and recording are enabled.\n\n"
        "💎 **Pro** adds AI transcription and AI meeting summaries."
    )


@bot.tree.command(
    name="redeem",
    description="Redeem a Frost Scribe promo code for this server",
)
@app_commands.describe(code="Promo code")
@app_commands.checks.has_permissions(manage_guild=True)
async def redeem_code(interaction: discord.Interaction, code: str):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command must be used inside a server.",
            ephemeral=True,
        )
        return

    normalized = code.strip().upper()
    await interaction.response.defer(ephemeral=True)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            promo = await conn.fetchrow(
                "SELECT * FROM promo_codes WHERE code = $1 FOR UPDATE",
                normalized,
            )

            if promo is None or not promo["active"]:
                await interaction.followup.send(
                    "❌ Invalid or inactive promo code.",
                    ephemeral=True,
                )
                return

            if promo["redemptions"] >= promo["max_redemptions"]:
                if promo["redeemed_guild_id"] == interaction.guild.id:
                    await interaction.followup.send(
                        f"👑 This server already has Lifetime Pro from **{normalized}**.",
                        ephemeral=True,
                    )
                else:
                    await interaction.followup.send(
                        "❌ This promo code has already been redeemed.",
                        ephemeral=True,
                    )
                return

            current = await conn.fetchrow(
                """
                SELECT entitlement_source
                FROM guilds
                WHERE guild_id = $1
                FOR UPDATE
                """,
                interaction.guild.id,
            )

            if current and (current["entitlement_source"] or "").upper() == "LIFETIME_PROMO":
                await interaction.followup.send(
                    "👑 This server already has Lifetime Pro.",
                    ephemeral=True,
                )
                return

            await conn.execute(
                """
                INSERT INTO guilds (
                    guild_id, plan, subscription_status, billing_cycle,
                    subscription_expires_at, entitlement_source,
                    lifetime_code, updated_at
                )
                VALUES (
                    $1, 'PRO', 'active', 'lifetime',
                    NULL, 'LIFETIME_PROMO', $2, NOW()
                )
                ON CONFLICT (guild_id)
                DO UPDATE SET
                    plan = 'PRO',
                    subscription_status = 'active',
                    billing_cycle = 'lifetime',
                    subscription_expires_at = NULL,
                    entitlement_source = 'LIFETIME_PROMO',
                    lifetime_code = EXCLUDED.lifetime_code,
                    updated_at = NOW()
                """,
                interaction.guild.id,
                normalized,
            )

            await conn.execute(
                """
                UPDATE promo_codes
                SET redemptions = redemptions + 1,
                    redeemed_guild_id = $2,
                    redeemed_by = $3,
                    redeemed_at = NOW()
                WHERE code = $1
                """,
                normalized,
                interaction.guild.id,
                interaction.user.id,
            )

    await interaction.followup.send(
        "👑 **Lifetime Pro activated!**\n"
        "This server now has permanent Frost Scribe Pro access.\n"
        "Use `/plan status` to confirm.",
        ephemeral=True,
    )


class CheckoutLinkView(discord.ui.View):
    def __init__(self, url: str):
        super().__init__(timeout=None)
        self.add_item(
            discord.ui.Button(
                label="Pay securely with Razorpay",
                emoji="💳",
                style=discord.ButtonStyle.link,
                url=url,
            )
        )


class UpgradeView(discord.ui.View):
    def __init__(self, owner_id: int, guild_id: int):
        super().__init__(timeout=300)
        self.owner_id = owner_id
        self.guild_id = guild_id

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the administrator who opened this panel can use it.",
                ephemeral=True,
            )
            return False
        return True

    async def create_checkout(
        self,
        interaction: discord.Interaction,
        cycle: str,
    ):
        await interaction.response.defer(ephemeral=True)

        try:
            result = await create_razorpay_subscription(
                self.guild_id,
                interaction.user.id,
                cycle,
            )
        except Exception as e:
            await interaction.followup.send(
                "❌ I couldn't create the checkout link.\n"
                f"`{type(e).__name__}: {e}`",
                ephemeral=True,
            )
            return

        short_url = result.get("short_url")
        if not short_url:
            await interaction.followup.send(
                "❌ Razorpay did not return a payment link.",
                ephemeral=True,
            )
            return

        price_text = (
            "₹199/month"
            if cycle == "monthly"
            else "₹1,999/year"
        )

        await interaction.followup.send(
            f"💎 **Frost Scribe Pro — {cycle.title()}**\n"
            f"Price: **{price_text}**\n\n"
            "Complete the secure Razorpay checkout below. "
            "Pro activates automatically after Razorpay confirms payment.",
            view=CheckoutLinkView(short_url),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Monthly ₹199",
        emoji="📅",
        style=discord.ButtonStyle.primary,
    )
    async def monthly_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        await self.create_checkout(
            interaction,
            "monthly",
        )

    @discord.ui.button(
        label="Annual ₹1,999",
        emoji="💎",
        style=discord.ButtonStyle.success,
    )
    async def annual_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        await self.create_checkout(
            interaction,
            "annual",
        )


@plan_group.command(
    name="upgrade",
    description="Upgrade this server to Frost Scribe Pro",
)
@app_commands.checks.has_permissions(manage_guild=True)
async def plan_upgrade(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            "Use this command inside a server.",
            ephemeral=True,
        )
        return

    if not razorpay_configured():
        await interaction.response.send_message(
            "⚠️ Billing is not fully configured yet.",
            ephemeral=True,
        )
        return

    row = await db_pool.fetchrow(
        """
        SELECT plan, entitlement_source, razorpay_subscription_id
        FROM guilds
        WHERE guild_id = $1
        """,
        interaction.guild.id,
    )

    if row and (row["entitlement_source"] or "").upper() == "LIFETIME_PROMO":
        await interaction.response.send_message(
            "👑 This server already has **Lifetime Pro**. No payment is required.",
            ephemeral=True,
        )
        return

    if await is_pro_guild(interaction.guild.id):
        await interaction.response.send_message(
            "💎 This server already has an active Pro plan. "
            "Use `/plan billing` for billing details.",
            ephemeral=True,
        )
        return

    annual_saving = 199 * 12 - 1999

    embed = discord.Embed(
        title="💎 Frost Scribe Pro",
        description=(
            "**Monthly — ₹199/month**\n"
            "Flexible monthly subscription.\n\n"
            "**Annual — ₹1,999/year**\n"
            f"Save **₹{annual_saving}/year** compared with monthly billing.\n\n"
            "**Pro includes**\n"
            "• AI transcription\n"
            "• AI meeting summaries\n"
            "• Key discussion points\n"
            "• Decisions and action items"
        ),
    )

    await interaction.response.send_message(
        embed=embed,
        view=UpgradeView(
            interaction.user.id,
            interaction.guild.id,
        ),
        ephemeral=True,
    )


@plan_group.command(
    name="billing",
    description="Show this server's subscription and renewal details",
)
async def plan_billing(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            "Use this command inside a server.",
            ephemeral=True,
        )
        return

    await ensure_guild(interaction.guild.id)

    guild_row = await db_pool.fetchrow(
        """
        SELECT plan, subscription_status, billing_cycle,
               subscription_expires_at, entitlement_source,
               razorpay_subscription_id, cancel_at_cycle_end
        FROM guilds
        WHERE guild_id = $1
        """,
        interaction.guild.id,
    )

    if (
        guild_row
        and (guild_row["entitlement_source"] or "").upper()
        == "LIFETIME_PROMO"
    ):
        await interaction.response.send_message(
            "👑 **Lifetime Pro**\n"
            "No billing, renewal, or expiry applies to this server.",
            ephemeral=True,
        )
        return

    subscription_id = (
        guild_row["razorpay_subscription_id"]
        if guild_row
        else None
    )

    if not subscription_id:
        latest = await db_pool.fetchrow(
            """
            SELECT *
            FROM guild_subscriptions
            WHERE guild_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            interaction.guild.id,
        )

        if not latest:
            await interaction.response.send_message(
                "❄️ This server has no paid subscription yet. "
                "Use `/plan upgrade`.",
                ephemeral=True,
            )
            return

        status = latest["status"]
        cycle = latest["billing_cycle"]
        period_end = latest["current_period_end"]
        cancel_pending = latest["cancel_at_cycle_end"]
        subscription_id = latest["subscription_id"]
    else:
        latest = await db_pool.fetchrow(
            """
            SELECT *
            FROM guild_subscriptions
            WHERE subscription_id = $1
            """,
            subscription_id,
        )
        status = (
            latest["status"]
            if latest
            else guild_row["subscription_status"]
        )
        cycle = (
            latest["billing_cycle"]
            if latest
            else guild_row["billing_cycle"]
        )
        period_end = (
            latest["current_period_end"]
            if latest
            else guild_row["subscription_expires_at"]
        )
        cancel_pending = (
            latest["cancel_at_cycle_end"]
            if latest
            else guild_row["cancel_at_cycle_end"]
        )

    renewal = (
        discord_time(period_end, "F")
        if period_end
        else "Pending Razorpay confirmation"
    )

    await interaction.response.send_message(
        "💳 **Frost Scribe Billing**\n"
        f"Plan: **Pro {str(cycle).title()}**\n"
        f"Status: **{status}**\n"
        f"Next renewal/end: {renewal}\n"
        f"Cancel at cycle end: **{'Yes' if cancel_pending else 'No'}**\n"
        f"Subscription: `{subscription_id}`",
        ephemeral=True,
    )


@plan_group.command(
    name="cancel",
    description="Cancel Pro renewal at the end of the current billing cycle",
)
@app_commands.checks.has_permissions(manage_guild=True)
async def plan_cancel(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            "Use this command inside a server.",
            ephemeral=True,
        )
        return

    row = await db_pool.fetchrow(
        """
        SELECT entitlement_source, razorpay_subscription_id
        FROM guilds
        WHERE guild_id = $1
        """,
        interaction.guild.id,
    )

    if row and (row["entitlement_source"] or "").upper() == "LIFETIME_PROMO":
        await interaction.response.send_message(
            "👑 Lifetime Pro has no recurring billing to cancel.",
            ephemeral=True,
        )
        return

    subscription_id = row["razorpay_subscription_id"] if row else None

    if not subscription_id:
        latest = await db_pool.fetchrow(
            """
            SELECT subscription_id
            FROM guild_subscriptions
            WHERE guild_id = $1
              AND status IN ('created','authenticated','active','pending')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            interaction.guild.id,
        )
        subscription_id = latest["subscription_id"] if latest else None

    if not subscription_id:
        await interaction.response.send_message(
            "There is no active paid subscription to cancel.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)

    try:
        result = await razorpay_request(
            "POST",
            f"/subscriptions/{subscription_id}/cancel",
            {"cancel_at_cycle_end": 1},
        )
    except Exception as e:
        await interaction.followup.send(
            "❌ I couldn't schedule the cancellation.\n"
            f"`{type(e).__name__}: {e}`",
            ephemeral=True,
        )
        return

    await db_pool.execute(
        """
        UPDATE guild_subscriptions
        SET cancel_at_cycle_end = TRUE,
            status = $2,
            updated_at = NOW()
        WHERE subscription_id = $1
        """,
        subscription_id,
        result.get("status", "active"),
    )

    await db_pool.execute(
        """
        UPDATE guilds
        SET cancel_at_cycle_end = TRUE,
            updated_at = NOW()
        WHERE guild_id = $1
        """,
        interaction.guild.id,
    )

    await interaction.followup.send(
        "✅ **Cancellation scheduled.**\n"
        "Pro remains available through the current paid period. "
        "It will not renew afterward.",
        ephemeral=True,
    )


class DonationCustomModal(
    discord.ui.Modal,
    title="Support Frost Scribe",
):
    amount = discord.ui.TextInput(
        label="Donation amount in INR",
        placeholder="Example: 250",
        max_length=6,
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            amount_inr = int(str(self.amount).strip())
            result = await create_donation_link(
                interaction.guild_id,
                interaction.user.id,
                amount_inr,
            )
        except ValueError as e:
            await interaction.response.send_message(
                f"❌ {e}",
                ephemeral=True,
            )
            return
        except Exception as e:
            await interaction.response.send_message(
                "❌ I couldn't create the donation link.\n"
                f"`{type(e).__name__}: {e}`",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            f"❤️ **Thank you for supporting Frost Scribe.**\n"
            f"Donation: **₹{amount_inr}**",
            view=CheckoutLinkView(result["short_url"]),
            ephemeral=True,
        )


class DonationView(discord.ui.View):
    def __init__(self, owner_id: int):
        super().__init__(timeout=300)
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Open your own `/donate` panel to donate.",
                ephemeral=True,
            )
            return False
        return True

    async def fixed_amount(
        self,
        interaction: discord.Interaction,
        amount_inr: int,
    ):
        await interaction.response.defer(ephemeral=True)

        try:
            result = await create_donation_link(
                interaction.guild_id,
                interaction.user.id,
                amount_inr,
            )
        except Exception as e:
            await interaction.followup.send(
                "❌ I couldn't create the donation link.\n"
                f"`{type(e).__name__}: {e}`",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            f"❤️ **Thank you for supporting Frost Scribe.**\n"
            f"Donation: **₹{amount_inr}**\n\n"
            "Donations are optional and do not change your Free/Pro entitlement.",
            view=CheckoutLinkView(result["short_url"]),
            ephemeral=True,
        )

    @discord.ui.button(
        label="₹99",
        style=discord.ButtonStyle.secondary,
    )
    async def donate_99(self, interaction, button):
        await self.fixed_amount(interaction, 99)

    @discord.ui.button(
        label="₹199",
        style=discord.ButtonStyle.primary,
    )
    async def donate_199(self, interaction, button):
        await self.fixed_amount(interaction, 199)

    @discord.ui.button(
        label="₹499",
        style=discord.ButtonStyle.success,
    )
    async def donate_499(self, interaction, button):
        await self.fixed_amount(interaction, 499)

    @discord.ui.button(
        label="Custom",
        emoji="✍️",
        style=discord.ButtonStyle.secondary,
    )
    async def donate_custom(self, interaction, button):
        await interaction.response.send_modal(
            DonationCustomModal()
        )


@bot.tree.command(
    name="donate",
    description="Support Frost Scribe with an optional one-time donation",
)
async def donate(interaction: discord.Interaction):
    if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        await interaction.response.send_message(
            "⚠️ Donations are not configured yet.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        "❤️ **Support Frost Scribe**\n\n"
        "Donations help with hosting, development, and AI infrastructure.\n"
        "**Donations do not unlock Pro features.**\n\n"
        "Choose an amount:",
        view=DonationView(interaction.user.id),
        ephemeral=True,
    )


attendance_group = app_commands.Group(
    name="attendance",
    description="Free attendance tracking without recording",
)


def build_attendance_rows(session, ended_at):
    session_seconds = max(
        1,
        (ended_at - session["started_at"]).total_seconds(),
    )

    rows = []

    for uid, participant in session["participants"].items():
        seconds = participant["seconds"]

        if participant["joined_at"] is not None:
            seconds += (
                ended_at - participant["joined_at"]
            ).total_seconds()

        attendance_pct = min(
            100.0,
            (seconds / session_seconds) * 100,
        )

        rows.append(
            {
                "discord_user_id": uid,
                "display_name": participant["display_name"],
                "username": participant["username"],
                "meeting_name": session["name"],
                "voice_channel": session["channel_name"],
                "meeting_started_utc": session["started_at"].isoformat(),
                "meeting_ended_utc": ended_at.isoformat(),
                "minutes_attended": round(seconds / 60, 2),
                "attendance_percent": round(attendance_pct, 2),
            }
        )

    rows.sort(
        key=lambda row: row["minutes_attended"],
        reverse=True,
    )

    return rows, session_seconds


def write_attendance_csv(session, rows, ended_at):
    stamp = ended_at.strftime("%Y-%m-%d_%H-%M-%S")
    filename = (
        f"{safe_filename(session['name'])}_{stamp}.csv"
    )
    path = ATTENDANCE_DIR / filename

    with path.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "discord_user_id",
                "display_name",
                "username",
                "meeting_name",
                "voice_channel",
                "meeting_started_utc",
                "meeting_ended_utc",
                "minutes_attended",
                "attendance_percent",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    return path


@attendance_group.command(
    name="start",
    description="Start attendance tracking without recording audio",
)
@app_commands.describe(
    name="Attendance session name",
    channel="Voice channel to track; leave blank to use your current channel",
)
@app_commands.checks.has_permissions(manage_guild=True)
async def attendance_start(
    interaction: discord.Interaction,
    name: str,
    channel: discord.VoiceChannel | None = None,
):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command must be used inside a server.",
            ephemeral=True,
        )
        return

    guild_id = interaction.guild.id

    if guild_id in active_attendance:
        current = active_attendance[guild_id]
        await interaction.response.send_message(
            f"Attendance is already active: **{current['name']}** "
            f"in <#{current['channel_id']}>.\n"
            "Stop it first with `/attendance stop`.",
            ephemeral=True,
        )
        return

    if guild_id in active_meetings:
        current = active_meetings[guild_id]
        await interaction.response.send_message(
            f"🔴 A recorded meeting is already active: "
            f"**{current['name']}** in <#{current['channel_id']}>.\n"
            "Recording already includes attendance tracking.",
            ephemeral=True,
        )
        return

    if channel is None:
        member = interaction.guild.get_member(
            interaction.user.id
        )

        if (
            member is not None
            and member.voice is not None
            and isinstance(member.voice.channel, discord.VoiceChannel)
        ):
            channel = member.voice.channel
        else:
            config = await get_guild_config(guild_id)
            default_voice_id = (
                config["default_voice_channel_id"] if config else None
            )
            default_voice = (
                interaction.guild.get_channel(default_voice_id)
                if default_voice_id else None
            )
            if isinstance(default_voice, discord.VoiceChannel):
                channel = default_voice
            else:
                await interaction.response.send_message(
                    "Join a voice channel, specify the `channel` option, "
                    "or configure a default voice channel with `/setup`.",
                    ephemeral=True,
                )
                return

    started_at = utcnow()

    session = {
        "name": name.strip(),
        "channel_id": channel.id,
        "channel_name": channel.name,
        "started_at": started_at,
        "participants": {},
    }

    for member in channel.members:
        if not member.bot:
            start_session(session, member)

    active_attendance[guild_id] = session

    await interaction.response.send_message(
        f"📋 **Attendance started: {session['name']}**\n"
        f"🎙️ Channel: {channel.mention}\n"
        f"👥 Already present: "
        f"{len([m for m in channel.members if not m.bot])}\n"
        f"🔒 **No audio is being recorded.**\n\n"
        "Use `/attendance status` to check progress and "
        "`/attendance stop` to finish."
    )


@attendance_group.command(
    name="status",
    description="Show the current attendance-only session",
)
async def attendance_status(
    interaction: discord.Interaction,
):
    if (
        interaction.guild is None
        or interaction.guild.id not in active_attendance
    ):
        await interaction.response.send_message(
            "There is no active attendance-only session.",
            ephemeral=True,
        )
        return

    session = active_attendance[
        interaction.guild.id
    ]

    rows = []

    for uid, participant in session[
        "participants"
    ].items():
        member = interaction.guild.get_member(uid)

        in_channel = (
            member is not None
            and member.voice is not None
            and member.voice.channel is not None
            and member.voice.channel.id
            == session["channel_id"]
        )

        rows.append(
            (
                participant["display_name"],
                current_seconds(participant),
                "🟢 Present" if in_channel else "⚪ Left",
            )
        )

    rows.sort(
        key=lambda item: item[1],
        reverse=True,
    )

    body = (
        "\n".join(
            f"• **{name}** — "
            f"{format_duration(seconds)} — {state}"
            for name, seconds, state in rows[:40]
        )
        if rows
        else "No attendees recorded yet."
    )

    await interaction.response.send_message(
        f"📋 **{session['name']}**\n"
        f"🎙️ Channel: <#{session['channel_id']}>\n"
        f"🔒 Recording: **NO**\n"
        f"⏱️ Running: "
        f"{format_duration((utcnow() - session['started_at']).total_seconds())}"
        f"\n\n{body}"
    )


@attendance_group.command(
    name="stop",
    description="Stop attendance tracking and export the report",
)
@app_commands.checks.has_permissions(manage_guild=True)
async def attendance_stop(
    interaction: discord.Interaction,
):
    if (
        interaction.guild is None
        or interaction.guild.id not in active_attendance
    ):
        await interaction.response.send_message(
            "There is no active attendance-only session.",
            ephemeral=True,
        )
        return

    guild_id = interaction.guild.id
    session = active_attendance[guild_id]
    ended_at = utcnow()

    # Finalize currently-present attendees.
    for participant in session["participants"].values():
        if participant["joined_at"] is not None:
            participant["seconds"] += (
                ended_at - participant["joined_at"]
            ).total_seconds()
            participant["joined_at"] = None

    rows, session_seconds = build_attendance_rows(
        session,
        ended_at,
    )

    attendance_path = write_attendance_csv(
        session,
        rows,
        ended_at,
    )

    active_attendance.pop(guild_id, None)

    if rows:
        attendance_summary = "\n".join(
            f"• **{row['display_name']}** — "
            f"{row['minutes_attended']:.1f} min "
            f"({row['attendance_percent']:.1f}%)"
            for row in rows[:30]
        )
    else:
        attendance_summary = "No attendees recorded."

    await interaction.response.send_message(
        f"🏁 **Attendance ended: {session['name']}**\n"
        f"🎙️ Channel: <#{session['channel_id']}>\n"
        f"⏱️ Session length: "
        f"{format_duration(session_seconds)}\n"
        f"🔒 No audio was recorded.\n\n"
        f"{attendance_summary}",
        file=discord.File(attendance_path),
    )


schedule_group = app_commands.Group(name="schedule", description="Schedule meetings and reminders")

def parse_scheduled_time(date_text, time_text, timezone_name):
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        raise ValueError("Unknown timezone. Example: `Asia/Kolkata` or `Asia/Manila`.")
    try:
        local_dt = datetime.strptime(f"{date_text.strip()} {time_text.strip()}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    except ValueError:
        raise ValueError("Use date `YYYY-MM-DD` and 24-hour time `HH:MM`.")
    return local_dt.astimezone(timezone.utc)

def discord_time(dt, style="F"):
    return f"<t:{int(dt.timestamp())}:{style}>"

class ScheduleDraft:
    def __init__(self, user_id: int, guild_id: int):
        self.user_id = user_id
        self.guild_id = guild_id
        self.name = None
        self.date = None
        self.time = None
        self.timezone_name = "Asia/Kolkata"
        self.reminder_channel_id = None
        self.role_ids = []


schedule_drafts = {}


def schedule_panel_embed(draft: ScheduleDraft):
    channel_text = f"<#{draft.reminder_channel_id}>" if draft.reminder_channel_id else "Not set"
    role_text = " ".join(f"<@&{rid}>" for rid in draft.role_ids) if draft.role_ids else "None"

    embed = discord.Embed(
        title="❄️ Frost Scribe — Schedule Meeting",
        description="Configure the meeting, then press **Schedule Meeting**.",
    )
    embed.add_field(name="Meeting Name", value=draft.name or "Not set", inline=False)
    embed.add_field(name="Date", value=draft.date or "Not set", inline=True)
    embed.add_field(name="Time", value=draft.time or "Not set", inline=True)
    embed.add_field(name="Timezone", value=draft.timezone_name, inline=False)
    embed.add_field(name="Reminder Channel", value=channel_text, inline=False)
    embed.add_field(name="Notify Roles", value=role_text, inline=False)
    return embed


class NameModal(discord.ui.Modal, title="Meeting Name"):
    meeting_name = discord.ui.TextInput(label="Meeting name", placeholder="Example: AOO Strategy Meeting", max_length=100)

    def __init__(self, draft):
        super().__init__()
        self.draft = draft
        if draft.name:
            self.meeting_name.default = draft.name

    async def on_submit(self, interaction):
        self.draft.name = str(self.meeting_name).strip()
        await interaction.response.edit_message(embed=schedule_panel_embed(self.draft), view=SchedulePanelView(self.draft))


class DateModal(discord.ui.Modal, title="Meeting Date"):
    meeting_date = discord.ui.TextInput(label="Date", placeholder="YYYY-MM-DD", max_length=10)

    def __init__(self, draft):
        super().__init__()
        self.draft = draft
        if draft.date:
            self.meeting_date.default = draft.date

    async def on_submit(self, interaction):
        value = str(self.meeting_date).strip()
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            await interaction.response.send_message("❌ Use date format `YYYY-MM-DD`.", ephemeral=True)
            return
        self.draft.date = value
        await interaction.response.edit_message(embed=schedule_panel_embed(self.draft), view=SchedulePanelView(self.draft))


class TimeModal(discord.ui.Modal, title="Meeting Time"):
    meeting_time = discord.ui.TextInput(label="Time", placeholder="24-hour HH:MM", max_length=5)

    def __init__(self, draft):
        super().__init__()
        self.draft = draft
        if draft.time:
            self.meeting_time.default = draft.time

    async def on_submit(self, interaction):
        value = str(self.meeting_time).strip()
        try:
            datetime.strptime(value, "%H:%M")
        except ValueError:
            await interaction.response.send_message("❌ Use 24-hour time format `HH:MM`.", ephemeral=True)
            return
        self.draft.time = value
        await interaction.response.edit_message(embed=schedule_panel_embed(self.draft), view=SchedulePanelView(self.draft))


class TimezoneModal(discord.ui.Modal, title="Meeting Timezone"):
    timezone_name = discord.ui.TextInput(label="Timezone", placeholder="Example: Asia/Kolkata", max_length=64)

    def __init__(self, draft):
        super().__init__()
        self.draft = draft
        self.timezone_name.default = draft.timezone_name

    async def on_submit(self, interaction):
        value = str(self.timezone_name).strip()
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError:
            await interaction.response.send_message(
                "❌ Unknown timezone. Example: `Asia/Kolkata`, `Asia/Manila`, or `America/New_York`.",
                ephemeral=True,
            )
            return
        self.draft.timezone_name = value
        await interaction.response.edit_message(embed=schedule_panel_embed(self.draft), view=SchedulePanelView(self.draft))


class ReminderChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, draft):
        self.draft = draft
        super().__init__(
            placeholder="Select reminder channel",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1,
        )

    async def callback(self, interaction):
        self.draft.reminder_channel_id = self.values[0].id
        await interaction.response.edit_message(embed=schedule_panel_embed(self.draft), view=SchedulePanelView(self.draft))


class NotifyRoleSelect(discord.ui.RoleSelect):
    def __init__(self, draft):
        self.draft = draft
        super().__init__(placeholder="Select role(s) to notify", min_values=0, max_values=5)

    async def callback(self, interaction):
        self.draft.role_ids = [role.id for role in self.values]
        await interaction.response.edit_message(embed=schedule_panel_embed(self.draft), view=SchedulePanelView(self.draft))


class SchedulePanelView(discord.ui.View):
    def __init__(self, draft):
        super().__init__(timeout=900)
        self.draft = draft
        self.add_item(ReminderChannelSelect(draft))
        self.add_item(NotifyRoleSelect(draft))

    async def interaction_check(self, interaction):
        if interaction.user.id != self.draft.user_id:
            await interaction.response.send_message("This scheduling panel belongs to another user.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Name", emoji="📝", style=discord.ButtonStyle.secondary, row=2)
    async def name_button(self, interaction, button):
        await interaction.response.send_modal(NameModal(self.draft))

    @discord.ui.button(label="Date", emoji="📅", style=discord.ButtonStyle.secondary, row=2)
    async def date_button(self, interaction, button):
        await interaction.response.send_modal(DateModal(self.draft))

    @discord.ui.button(label="Time", emoji="🕐", style=discord.ButtonStyle.secondary, row=2)
    async def time_button(self, interaction, button):
        await interaction.response.send_modal(TimeModal(self.draft))

    @discord.ui.button(label="Timezone", emoji="🌍", style=discord.ButtonStyle.secondary, row=2)
    async def timezone_button(self, interaction, button):
        await interaction.response.send_modal(TimezoneModal(self.draft))

    @discord.ui.button(label="Schedule Meeting", emoji="✅", style=discord.ButtonStyle.success, row=3)
    async def schedule_button(self, interaction, button):
        missing = []
        if not self.draft.name: missing.append("meeting name")
        if not self.draft.date: missing.append("date")
        if not self.draft.time: missing.append("time")
        if not self.draft.reminder_channel_id: missing.append("reminder channel")

        if missing:
            await interaction.response.send_message("❌ Missing: " + ", ".join(missing), ephemeral=True)
            return

        try:
            scheduled_at = parse_scheduled_time(
                self.draft.date,
                self.draft.time,
                self.draft.timezone_name,
            )
        except ValueError as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)
            return

        if scheduled_at <= utcnow():
            await interaction.response.send_message("❌ Meeting time must be in the future.", ephemeral=True)
            return

        tag_text = " ".join(f"<@&{rid}>" for rid in self.draft.role_ids)

        meeting_id = await db_pool.fetchval(
            """
            INSERT INTO scheduled_meetings
            (guild_id,name,scheduled_at,reminder_channel_id,tag_text,created_by)
            VALUES($1,$2,$3,$4,$5,$6)
            RETURNING id
            """,
            interaction.guild_id,
            self.draft.name,
            scheduled_at,
            self.draft.reminder_channel_id,
            tag_text,
            interaction.user.id,
        )

        schedule_drafts.pop((interaction.guild_id, interaction.user.id), None)

        await interaction.response.edit_message(
            embed=discord.Embed(
                title=f"📅 Meeting Scheduled — #{meeting_id}",
                description=(
                    f"**{self.draft.name}**\n"
                    f"🕒 {discord_time(scheduled_at)} ({discord_time(scheduled_at, 'R')})\n"
                    f"🌍 `{self.draft.timezone_name}`\n"
                    f"📣 Reminders: **1 hour** and **30 minutes** before\n"
                    f"💬 <#{self.draft.reminder_channel_id}>\n"
                    f"🏷️ {tag_text or 'No roles selected'}"
                ),
            ),
            view=None,
        )

    @discord.ui.button(label="Cancel", emoji="❌", style=discord.ButtonStyle.danger, row=3)
    async def cancel_button(self, interaction, button):
        schedule_drafts.pop((interaction.guild_id, interaction.user.id), None)
        await interaction.response.edit_message(content="❌ Scheduling cancelled.", embed=None, view=None)


@schedule_group.command(name="create", description="Open the interactive meeting scheduler")
@app_commands.checks.has_permissions(manage_guild=True)
async def schedule_create(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command inside a server.", ephemeral=True)
        return
    if db_pool is None:
        await interaction.response.send_message("The scheduling database is not available right now.", ephemeral=True)
        return

    draft = ScheduleDraft(interaction.user.id, interaction.guild.id)
    config = await get_guild_config(interaction.guild.id)
    if config:
        draft.timezone_name = config["timezone_name"] or "UTC"
        draft.reminder_channel_id = config["default_report_channel_id"]

    schedule_drafts[(interaction.guild.id, interaction.user.id)] = draft

    await interaction.response.send_message(
        embed=schedule_panel_embed(draft),
        view=SchedulePanelView(draft),
        ephemeral=True,
    )


@schedule_group.command(name="list", description="List upcoming meetings")
async def schedule_list(interaction: discord.Interaction):
    rows = await db_pool.fetch("""
        SELECT id,name,scheduled_at,reminder_channel_id,tag_text FROM scheduled_meetings
        WHERE guild_id=$1 AND cancelled=FALSE AND scheduled_at>NOW()
        ORDER BY scheduled_at LIMIT 25
    """, interaction.guild.id)
    if not rows:
        await interaction.response.send_message("📅 No upcoming scheduled meetings."); return
    body="\n\n".join(
        f"**#{r['id']} — {r['name']}**\n🕒 {discord_time(r['scheduled_at'])} ({discord_time(r['scheduled_at'],'R')})\n💬 <#{r['reminder_channel_id']}>\n🏷️ {r['tag_text'] or 'No tags'}"
        for r in rows)
    await interaction.response.send_message(f"📅 **Upcoming Meetings**\n\n{body}")

@schedule_group.command(name="cancel", description="Cancel an upcoming meeting")
@app_commands.checks.has_permissions(manage_guild=True)
async def schedule_cancel(interaction: discord.Interaction, meeting_id: int):
    row=await db_pool.fetchrow("""
        UPDATE scheduled_meetings SET cancelled=TRUE
        WHERE id=$1 AND guild_id=$2 AND cancelled=FALSE AND scheduled_at>NOW()
        RETURNING name
    """, meeting_id, interaction.guild.id)
    if not row:
        await interaction.response.send_message("❌ Active upcoming meeting not found.", ephemeral=True); return
    await interaction.response.send_message(f"🗑️ Cancelled **#{meeting_id} — {row['name']}**.")

async def send_schedule_reminder(row, label):
    channel=bot.get_channel(row["reminder_channel_id"])
    if channel is None:
        try: channel=await bot.fetch_channel(row["reminder_channel_id"])
        except Exception as e:
            print(f"Reminder channel error for {row['id']}: {e}"); return False
    tags=(row["tag_text"] or "").strip()
    try:
        await channel.send(
            f"🔔 **MEETING REMINDER**\n\n**{row['name']}** starts in **{label}**.\n"
            f"🕒 {discord_time(row['scheduled_at'])} ({discord_time(row['scheduled_at'],'R')})"
            + (f"\n\n{tags}" if tags else ""),
            allowed_mentions=discord.AllowedMentions(users=True,roles=True,everyone=False))
        return True
    except Exception as e:
        print(f"Reminder send error for {row['id']}: {e}"); return False

@tasks.loop(seconds=30)
async def scheduled_reminder_worker():
    if db_pool is None: return
    now=utcnow()
    rows=await db_pool.fetch("""
        SELECT * FROM scheduled_meetings
        WHERE cancelled=FALSE AND scheduled_at>$1
          AND ((reminder_1h_sent=FALSE AND scheduled_at<=$1+INTERVAL '1 hour')
            OR (reminder_30m_sent=FALSE AND scheduled_at<=$1+INTERVAL '30 minutes'))
        ORDER BY scheduled_at
    """, now)
    for row in rows:
        due30=(not row["reminder_30m_sent"] and row["scheduled_at"]<=now+timedelta(minutes=30))
        due60=(not row["reminder_1h_sent"] and row["scheduled_at"]<=now+timedelta(hours=1))
        if due30:
            remaining_seconds = max(
                0,
                (row["scheduled_at"] - now).total_seconds()
            )
            remaining_minutes = max(
                1,
                int(round(remaining_seconds / 60))
            )
            label = (
                "30 minutes"
                if 28 <= remaining_minutes <= 32
                else f"{remaining_minutes} minute"
                + ("" if remaining_minutes == 1 else "s")
            )

            if await send_schedule_reminder(row, label):
                await db_pool.execute(
                    "UPDATE scheduled_meetings "
                    "SET reminder_1h_sent=TRUE, reminder_30m_sent=TRUE "
                    "WHERE id=$1",
                    row["id"],
                )
        elif due60:
            if await send_schedule_reminder(row,"1 hour"):
                await db_pool.execute(
                    "UPDATE scheduled_meetings "
                    "SET reminder_1h_sent=TRUE WHERE id=$1",
                    row["id"],
                )

@scheduled_reminder_worker.before_loop
async def before_scheduled_reminder_worker():
    await bot.wait_until_ready()



async def send_recording_files(channel, meeting):
    """Send captured per-speaker WAV files in manageable chunks."""
    sink = meeting.get("audio_sink")
    if sink is None:
        return 0

    paths = []
    for uid, path in sink.paths.items():
        if not path.exists() or path.stat().st_size <= 44:
            continue

        try:
            parts = split_wav_if_needed(path)
        except Exception as e:
            print(
                f"Could not prepare recording for {uid}: "
                f"{type(e).__name__}: {e}"
            )
            continue

        paths.extend(parts)

    if not paths:
        await channel.send(
            "🎙️ No usable audio files were captured for this recording."
        )
        return 0

    sent = 0
    for start in range(0, len(paths), 8):
        batch_paths = paths[start:start + 8]
        files = [
            discord.File(p, filename=p.name)
            for p in batch_paths
        ]
        await channel.send(
            "🎙️ **Recording file(s)**",
            files=files,
        )
        sent += len(files)

    return sent


record_group = app_commands.Group(
    name="record",
    description=(
        "Voice recording with attendance; Pro adds transcription and AI summaries"
    ),
)


@record_group.command(
    name="start",
    description="Start voice recording with attendance tracking",
)
@app_commands.describe(
    name="Meeting name",
    channel=(
        "Voice channel to record; leave blank to use your current channel"
    ),
)
async def meeting_start(
    interaction: discord.Interaction,
    name: str,
    channel: discord.VoiceChannel | None = None,
):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command must be used inside a server.",
            ephemeral=True,
        )
        return

    guild_id = interaction.guild.id

    if guild_id in active_attendance:
        current = active_attendance[guild_id]
        await interaction.response.send_message(
            f"📋 Attendance-only tracking is already active: "
            f"**{current['name']}** in <#{current['channel_id']}>.\n"
            "Stop it with `/attendance stop` before starting a recorded meeting.",
            ephemeral=True,
        )
        return

    if guild_id in active_meetings:
        current = active_meetings[guild_id]
        await interaction.response.send_message(
            f"A meeting is already active: **{current['name']}** "
            f"in <#{current['channel_id']}>.\n"
            "End it first with `/record stop`.",
            ephemeral=True,
        )
        return

    if channel is None:
        member = interaction.guild.get_member(interaction.user.id)

        if (
            member is not None
            and member.voice is not None
            and isinstance(member.voice.channel, discord.VoiceChannel)
        ):
            channel = member.voice.channel
        else:
            config = await get_guild_config(guild_id)
            default_voice_id = (
                config["default_voice_channel_id"] if config else None
            )
            default_voice = (
                interaction.guild.get_channel(default_voice_id)
                if default_voice_id else None
            )
            if isinstance(default_voice, discord.VoiceChannel):
                channel = default_voice
            else:
                await interaction.response.send_message(
                    "Join a voice channel, specify the `channel` option, "
                    "or configure a default voice channel with `/setup`.",
                    ephemeral=True,
                )
                return

    await interaction.response.defer()

    started_at = utcnow()
    stamp = started_at.strftime("%Y-%m-%d_%H-%M-%S")
    recording_dir = RECORDINGS_DIR / (
        f"{safe_filename(name)}_{stamp}"
    )
    sink = PerSpeakerWaveSink(recording_dir)

    try:
        existing_voice = interaction.guild.voice_client
        if existing_voice is not None:
            await existing_voice.disconnect(force=True)

        voice_client = await channel.connect(
            cls=voice_recv.VoiceRecvClient
        )
        voice_client.listen(sink)

    except Exception as e:
        sink.cleanup()
        await interaction.followup.send(
            f"❌ I could not join/record {channel.mention}.\n"
            f"`{type(e).__name__}: {e}`"
        )
        return

    meeting = {
        "name": name,
        "channel_id": channel.id,
        "channel_name": channel.name,
        "started_at": started_at,
        "participants": {},
        "voice_client": voice_client,
        "audio_sink": sink,
        "recording_dir": recording_dir,
    }

    for member in channel.members:
        if not member.bot:
            start_session(meeting, member)

    active_meetings[guild_id] = meeting

    plan = await get_guild_plan(guild_id)

    ai_note = (
        "💎 **Pro AI enabled:** This recording will also be transcribed "
        "and summarized after `/record stop`."
        if plan == "PRO"
        else
        "❄️ **Free recording:** Audio and attendance are being captured. "
        "AI transcription and summaries require Frost Scribe Pro."
    )

    await interaction.followup.send(
        f"🔴 **Recording started: {name}**\n"
        f"🎙️ Channel: {channel.mention}\n"
        f"👥 Already present: "
        f"{len([m for m in channel.members if not m.bot])}\n\n"
        f"{ai_note}\n\n"
        "⚠️ **Recording notice:** Audio in this voice channel is being "
        "recorded. Everyone present should be informed and consent "
        "before continuing."
    )


@record_group.command(
    name="status",
    description="Show the active recording and attendance status",
)
async def meeting_status(interaction: discord.Interaction):
    if (
        interaction.guild is None
        or interaction.guild.id not in active_meetings
    ):
        await interaction.response.send_message(
            "There is no active meeting.",
            ephemeral=True,
        )
        return

    meeting = active_meetings[interaction.guild.id]
    rows = []

    for uid, participant in meeting["participants"].items():
        member = interaction.guild.get_member(uid)
        in_channel = (
            member is not None
            and member.voice is not None
            and member.voice.channel is not None
            and member.voice.channel.id == meeting["channel_id"]
        )

        rows.append(
            (
                participant["display_name"],
                current_seconds(participant),
                "🟢 In meeting" if in_channel else "⚪ Left",
            )
        )

    rows.sort(key=lambda x: x[1], reverse=True)

    if not rows:
        body = "No attendees recorded yet."
    else:
        body = "\n".join(
            f"• **{name}** — {format_duration(seconds)} — {state}"
            for name, seconds, state in rows[:40]
        )

    voice_client = meeting.get("voice_client")
    recording = bool(
        voice_client
        and voice_client.is_connected()
        and voice_client.is_listening()
    )

    await interaction.response.send_message(
        f"📋 **{meeting['name']}**\n"
        f"🎙️ Channel: <#{meeting['channel_id']}>\n"
        f"🔴 Recording: {'YES' if recording else 'NO'}\n"
        f"⏱️ Running: "
        f"{format_duration((utcnow() - meeting['started_at']).total_seconds())}"
        f"\n\n{body}"
    )


@record_group.command(
    name="stop",
    description=(
        "Stop recording and export attendance"
    ),
)
async def record_stop(interaction: discord.Interaction):
    if (
        interaction.guild is None
        or interaction.guild.id not in active_meetings
    ):
        await interaction.response.send_message(
            "There is no active meeting.",
            ephemeral=True,
        )
        return

    await interaction.response.defer()

    guild_id = interaction.guild.id
    meeting = active_meetings[guild_id]

    for participant in meeting["participants"].values():
        if participant["joined_at"] is not None:
            participant["seconds"] += (
                utcnow() - participant["joined_at"]
            ).total_seconds()
            participant["joined_at"] = None

    ended_at = utcnow()
    meeting_seconds = max(
        1,
        (ended_at - meeting["started_at"]).total_seconds(),
    )

    voice_client = meeting.get("voice_client")
    sink = meeting["audio_sink"]

    try:
        if voice_client and voice_client.is_listening():
            voice_client.stop_listening()
    except Exception as e:
        print(f"Could not stop voice listener cleanly: {e}")

    sink.cleanup()

    try:
        if voice_client and voice_client.is_connected():
            await voice_client.disconnect(force=True)
    except Exception as e:
        print(f"Could not disconnect voice client cleanly: {e}")

    date_str = ended_at.strftime("%Y-%m-%d_%H-%M-%S")
    attendance_name = (
        f"{safe_filename(meeting['name'])}_{date_str}.csv"
    )
    attendance_path = (
        meeting["recording_dir"] / attendance_name
    )

    rows = []

    for uid, participant in meeting["participants"].items():
        seconds = participant["seconds"]
        attendance_pct = min(
            100.0,
            (seconds / meeting_seconds) * 100,
        )

        rows.append(
            {
                "discord_user_id": uid,
                "display_name": participant["display_name"],
                "username": participant["username"],
                "meeting_name": meeting["name"],
                "voice_channel": meeting["channel_name"],
                "meeting_started_utc": (
                    meeting["started_at"].isoformat()
                ),
                "meeting_ended_utc": ended_at.isoformat(),
                "minutes_attended": round(seconds / 60, 2),
                "attendance_percent": round(
                    attendance_pct, 2
                ),
            }
        )

    rows.sort(
        key=lambda row: row["minutes_attended"],
        reverse=True,
    )

    with attendance_path.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "discord_user_id",
                "display_name",
                "username",
                "meeting_name",
                "voice_channel",
                "meeting_started_utc",
                "meeting_ended_utc",
                "minutes_attended",
                "attendance_percent",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    # Remove from active meetings before long AI processing.
    active_meetings.pop(guild_id, None)

    if rows:
        attendance_summary = "\n".join(
            f"• **{row['display_name']}** — "
            f"{row['minutes_attended']:.1f} min "
            f"({row['attendance_percent']:.1f}%)"
            for row in rows[:30]
        )
    else:
        attendance_summary = "No attendees recorded."

    plan = await get_guild_plan(guild_id)

    if plan == "PRO":
        next_step_text = (
            "⏳ Audio recording stopped. I am now transcribing "
            "the meeting and generating the AI summary."
        )
    else:
        next_step_text = (
            "🎙️ Audio recording stopped. Recording files will be attached below.\n"
            "💎 Upgrade to **Frost Scribe Pro** for transcription and AI summaries."
        )

    await interaction.followup.send(
        f"🏁 **Recording ended: {meeting['name']}**\n"
        f"🎙️ Channel: <#{meeting['channel_id']}>\n"
        f"⏱️ Recording length: "
        f"{format_duration(meeting_seconds)}\n\n"
        f"{attendance_summary}\n\n"
        f"{next_step_text}",
        file=discord.File(attendance_path),
    )

    try:
        await send_recording_files(
            interaction.channel,
            meeting,
        )
    except Exception as e:
        await (await get_report_channel(interaction.guild, interaction.channel)).send(
            "⚠️ I could not upload one or more recording files.\n"
            f"`{type(e).__name__}: {e}`"
        )

    if plan != "PRO":
        return

    try:
        transcript, transcript_path = await asyncio.to_thread(
            transcribe_meeting,
            meeting,
            ended_at,
        )

        summary, summary_path = await asyncio.to_thread(
            summarize_transcript,
            meeting,
            ended_at,
            transcript,
        )

        preview = summary[:1700]
        if len(summary) > 1700:
            preview += "\n\n…full summary attached."

        attachments = [discord.File(transcript_path)]

        if summary_path is not None and summary_path.exists():
            attachments.append(discord.File(summary_path))

        await (await get_report_channel(interaction.guild, interaction.channel)).send(
            f"📝 **AI Meeting Summary — {meeting['name']}**\n\n"
            f"{preview}",
            files=attachments,
        )

    except Exception as e:
        await (await get_report_channel(interaction.guild, interaction.channel)).send(
            "⚠️ The meeting attendance and WAV recording completed, "
            "but transcription or summarization failed.\n"
            f"`{type(e).__name__}: {e}`\n\n"
            "Check the Railway deploy logs. The recording files are "
            "stored in the service's temporary `data/recordings` "
            "directory for this deployment."
        )


@bot.event
async def on_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
):
    if member.bot:
        return

    sessions = []

    recorded_meeting = active_meetings.get(
        member.guild.id
    )
    if recorded_meeting:
        sessions.append(recorded_meeting)

    attendance_only = active_attendance.get(
        member.guild.id
    )
    if attendance_only:
        sessions.append(attendance_only)

    if not sessions:
        return

    before_id = (
        before.channel.id if before.channel else None
    )
    after_id = (
        after.channel.id if after.channel else None
    )

    for session in sessions:
        tracked_channel_id = session["channel_id"]

        if (
            before_id != tracked_channel_id
            and after_id == tracked_channel_id
        ):
            start_session(session, member)
            continue

        if (
            before_id == tracked_channel_id
            and after_id != tracked_channel_id
        ):
            end_session(session, member)


bot.tree.add_command(plan_group)
bot.tree.add_command(attendance_group)
bot.tree.add_command(schedule_group)
bot.tree.add_command(record_group)

if not TOKEN:
    raise RuntimeError(
        "DISCORD_TOKEN is missing. Add it to Railway Variables."
    )

bot.run(TOKEN)
