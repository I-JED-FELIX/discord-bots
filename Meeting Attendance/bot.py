# Frost Scribe V18 — public poll reports
# Frost Scribe V16 — AI Excel Dashboard + Standalone Pro Polls + Stage Audio
import os
import csv
import asyncio
import threading
import time
import wave
from array import array
import json
import hmac
import hashlib
import shutil
import subprocess
import traceback
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError, available_timezones
from pathlib import Path

import asyncpg
import aiohttp
from aiohttp import web
import discord
from discord import app_commands
from discord.ext import commands, tasks, voice_recv

try:
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    from openpyxl.utils import get_column_letter
    from openpyxl.worksheet.table import Table, TableStyleInfo
    from openpyxl.chart import BarChart, PieChart, Reference
    OPENPYXL_AVAILABLE = True
except ImportError:
    Workbook = None
    load_workbook = None
    Font = None
    Alignment = None
    PatternFill = None
    Border = None
    Side = None
    Table = None
    TableStyleInfo = None
    BarChart = None
    PieChart = None
    Reference = None
    get_column_letter = None
    OPENPYXL_AVAILABLE = False

# ---------------------------------------------------------------------------
# Frost Scribe Stage-channel compatibility patch
#
# discord-ext-voice-recv currently assumes every Discord VIDEO voice-gateway
# stream has a non-null ``max_resolution`` object. Stage channels can send
# stream entries where ``max_resolution`` is null. During the voice handshake
# that makes the extension raise ``TypeError: 'NoneType' object is not
# subscriptable`` inside voice_recv.video.VideoStreamResolution, which aborts
# the connection before audio receiving starts.
#
# Frost Scribe does not consume webcam/video stream metadata; it only needs
# the audio SSRC, which the extension registers before parsing the video
# metadata. Therefore malformed/non-video stream descriptors can safely be
# ignored. This patch is deliberately narrow and leaves normal voice/audio
# packet handling unchanged.
# ---------------------------------------------------------------------------
try:
    from discord.ext.voice_recv import video as _voice_recv_video

    def _frost_safe_video_streams(self, streams):
        parsed = []
        for stream in streams or []:
            if not isinstance(stream, dict):
                continue

            resolution = stream.get("max_resolution")
            if not isinstance(resolution, dict):
                # Discord Stage channels may emit a stream with
                # max_resolution=null. We do not need video metadata.
                continue

            try:
                parsed.append(_voice_recv_video.VideoStreamInfo(data=stream))
            except (TypeError, KeyError, ValueError):
                # A malformed video descriptor must never prevent audio
                # recording from connecting.
                continue

        return parsed

    _voice_recv_video.VoiceVideoStreams._get_streams = _frost_safe_video_streams
    print("Frost Scribe Stage video-metadata compatibility patch loaded.")
except Exception as _stage_patch_error:
    print(
        "Warning: Stage video-metadata compatibility patch could not load: "
        f"{type(_stage_patch_error).__name__}: {_stage_patch_error}"
    )

# ---------------------------------------------------------------------------
# Discord DAVE receive compatibility patch
#
# Some current discord-ext-voice-recv DAVE builds call
# ``dave_session.set_passthrough_mode(...)`` unconditionally when the first
# RTP decoder is created. On Stage channels Discord can begin delivering RTP
# before discord.py has created a DAVE session, so dave_session is None and
# the receive thread crashes. The rest of the library already checks whether
# a DAVE session exists and is ready before decrypting packets.
#
# Preserve the upstream initializer and only recover from this exact
# early-session None case. At the point of the upstream failure all decoder
# fields have already been initialized except _last_seq/_last_ts, so we finish
# those two fields and allow the receive thread to continue.
# ---------------------------------------------------------------------------
try:
    from discord.ext.voice_recv import opus as _voice_recv_opus

    _frost_original_packetdecoder_init = _voice_recv_opus.PacketDecoder.__init__

    def _frost_packetdecoder_init(self, router, ssrc):
        try:
            return _frost_original_packetdecoder_init(self, router, ssrc)
        except AttributeError as exc:
            vc = getattr(self, "vc", None)
            connection = getattr(vc, "_connection", None)
            dave_session = getattr(connection, "dave_session", None)

            if dave_session is None and "set_passthrough_mode" in str(exc):
                # These are the only fields the affected upstream initializer
                # has not assigned yet when it hits the bad DAVE call.
                self._last_seq = -1
                self._last_ts = -1
                print(
                    "Frost Scribe DAVE guard: RTP arrived before the DAVE "
                    f"session was ready (ssrc={ssrc}); continuing safely."
                )
                return None

            raise

    _voice_recv_opus.PacketDecoder.__init__ = _frost_packetdecoder_init
    print("Frost Scribe DAVE early-session compatibility patch loaded.")
except Exception as _dave_patch_error:
    print(
        "Warning: DAVE early-session compatibility patch could not load: "
        f"{type(_dave_patch_error).__name__}: {_dave_patch_error}"
    )
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

# Recording limits protect Railway disk/CPU from abandoned or excessively long sessions.
FREE_RECORDING_LIMIT_MINUTES = int(os.getenv("FREE_RECORDING_LIMIT_MINUTES", "45"))
PRO_RECORDING_LIMIT_MINUTES = int(os.getenv("PRO_RECORDING_LIMIT_MINUTES", "180"))

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

RECORDINGS_DIR = DATA_DIR / "recordings"
RECORDINGS_DIR.mkdir(exist_ok=True)

ATTENDANCE_DIR = DATA_DIR / "attendance"
ATTENDANCE_DIR.mkdir(exist_ok=True)

POLLS_DIR = DATA_DIR / "polls"
POLLS_DIR.mkdir(exist_ok=True)

intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

# One active recorded meeting per Discord server.
active_meetings = {}

# One active attendance-only session per Discord server.
active_attendance = {}

# Pro polls are independent of voice/attendance sessions. Each guild keeps its
# own poll registry so /poll can be used in any text channel, with or without
# an active meeting. Polls created while a meeting is active are also linked
# to that meeting so they appear in the final meeting workbook.
guild_poll_states = {}

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
    Capture decoded Discord PCM in two forms:

    1. Temporary per-speaker WAV tracks, retained only for Pro transcription
       so Frost Scribe can still produce higher-quality speaker-aware notes.
    2. One compact mixed meeting WAV for the Discord user-facing recording.

    The mixed track uses 20 ms time buckets. Packets that overlap in time are
    mixed together with 16-bit saturation. The full meeting timeline is
    preserved, including silence between speakers; the final user-facing copy
    is compressed to one Ogg/Opus file for Discord delivery.
    """

    CHANNELS = 2
    SAMPLE_WIDTH = 2
    SAMPLE_RATE = 48000
    MIX_SLOT_SECONDS = 0.020
    MIX_SLOT_FRAMES = int(SAMPLE_RATE * MIX_SLOT_SECONDS)  # 960 frames
    MIX_SLOT_BYTES = MIX_SLOT_FRAMES * CHANNELS * SAMPLE_WIDTH
    MIX_JITTER_SLOTS = 10  # ~200 ms reorder/jitter allowance

    def __init__(self, folder: Path):
        super().__init__()
        self.folder = folder
        self.folder.mkdir(parents=True, exist_ok=True)

        # Internal speaker tracks. These are not uploaded to Discord in V12;
        # they are used only for Pro speaker-aware transcription/summaries.
        self._writers = {}
        self.paths = {}
        self.speaker_names = {}

        # User-facing single meeting recording.
        self.combined_path = self.folder / "meeting_audio.wav"
        self._mix_writer = wave.open(str(self.combined_path), "wb")
        self._mix_writer.setnchannels(self.CHANNELS)
        self._mix_writer.setsampwidth(self.SAMPLE_WIDTH)
        self._mix_writer.setframerate(self.SAMPLE_RATE)
        self._mix_started = time.monotonic()
        self._mix_slots = {}
        self._mix_last_written_slot = None
        self._user_rtp_bases = {}

        self._lock = threading.Lock()
        self.closed = False

    def wants_opus(self) -> bool:
        return False

    @staticmethod
    def _mix_pcm(existing: bytes | None, incoming: bytes) -> bytes:
        """Mix two little-endian signed 16-bit PCM buffers with saturation."""
        if not existing:
            return bytes(incoming)

        left = array("h")
        left.frombytes(existing)
        right = array("h")
        right.frombytes(incoming)

        if len(left) < len(right):
            left.extend([0] * (len(right) - len(left)))
        elif len(right) < len(left):
            right.extend([0] * (len(left) - len(right)))

        for i in range(len(left)):
            sample = left[i] + right[i]
            if sample > 32767:
                sample = 32767
            elif sample < -32768:
                sample = -32768
            left[i] = sample

        return left.tobytes()

    def _wall_slot(self) -> int:
        elapsed = max(0.0, time.monotonic() - self._mix_started)
        return int(elapsed / self.MIX_SLOT_SECONDS)

    def _packet_slot(self, uid: int, data, pcm: bytes) -> int:
        """
        Place one user's packet on the shared meeting timeline.

        RTP timestamps preserve gaps within that user's speech. The first RTP
        packet is anchored to the meeting wall clock so separate speakers line
        up on one shared timeline. If packet metadata is unavailable, fall back
        to the current wall-clock slot.
        """
        now_slot = self._wall_slot()
        packet = getattr(data, "packet", None)
        timestamp = getattr(packet, "timestamp", None)

        if timestamp is None:
            return now_slot

        timestamp = int(timestamp) & 0xFFFFFFFF
        base = self._user_rtp_bases.get(uid)
        if base is None:
            self._user_rtp_bases[uid] = (timestamp, now_slot)
            return now_slot

        base_timestamp, base_slot = base
        delta_frames = (timestamp - base_timestamp) & 0xFFFFFFFF

        # RTP audio timestamps are expressed in 48 kHz sample frames.
        delta_slots = int(round(delta_frames / self.MIX_SLOT_FRAMES))
        candidate = max(0, base_slot + delta_slots)

        # Stage/voice RTP timestamps can occasionally reset after mute/unmute or
        # a stream transition. Do not let a timestamp discontinuity create an
        # enormous artificial silence gap in the master recording.
        if abs(candidate - now_slot) > 250:  # > ~5 seconds away from wall time
            self._user_rtp_bases[uid] = (timestamp, now_slot)
            return now_slot

        return candidate

    def _write_silence_slots_locked(self, slot_count: int):
        """Write exact timeline silence without allocating one huge buffer."""
        remaining = max(0, int(slot_count))
        chunk_slots = 250  # ~5 seconds per write
        while remaining > 0:
            count = min(remaining, chunk_slots)
            self._mix_writer.writeframes(
                b"\x00" * (count * self.MIX_SLOT_BYTES)
            )
            remaining -= count

    def _flush_mix_slots_locked(self, cutoff_slot: int | None = None):
        if cutoff_slot is None:
            ready = sorted(self._mix_slots)
        else:
            ready = sorted(
                slot for slot in self._mix_slots if slot <= cutoff_slot
            )

        for slot in ready:
            pcm = self._mix_slots.pop(slot)

            if self._mix_last_written_slot is not None:
                missing = slot - self._mix_last_written_slot - 1
                if missing > 0:
                    # V17 preserves the real meeting timeline rather than
                    # shortening long periods of silence.
                    self._write_silence_slots_locked(missing)

            self._mix_writer.writeframes(pcm)
            self._mix_last_written_slot = slot

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
            # Keep temporary per-speaker tracks for Pro AI processing.
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

            # Build one compact chronological meeting track.
            slot = self._packet_slot(uid, data, pcm)
            self._mix_slots[slot] = self._mix_pcm(
                self._mix_slots.get(slot), pcm
            )

            # Keep a small jitter window before committing audio to disk.
            cutoff = self._wall_slot() - self.MIX_JITTER_SLOTS
            if cutoff >= 0:
                self._flush_mix_slots_locked(cutoff)

    def cleanup(self):
        with self._lock:
            if self.closed:
                return
            self.closed = True

            # Flush the final mixed packets and close the combined recording.
            try:
                self._flush_mix_slots_locked()
            except Exception as e:
                print(
                    "Combined recording flush failed: "
                    f"{type(e).__name__}: {e}"
                )

            try:
                self._mix_writer.close()
            except Exception:
                pass

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

## Poll Results
Summarize the exact poll outcomes supplied below. Do not infer votes that are not present.

Rules:
- Never invent facts, decisions, owners, deadlines, or commitments.
- Preserve names from the speaker headings.
- If a section has nothing supported by the transcript, write "None identified."

POLL RESULTS:
{format_poll_results_for_ai(meeting)}

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


async def mark_subscription_pending(
    subscription_id: str,
    status: str,
    current_period_end=None,
):
    """Keep paid entitlement during Razorpay retry/pending states."""
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
            subscription_status = $2,
            billing_cycle = COALESCE($3, billing_cycle),
            subscription_expires_at = COALESCE($4, subscription_expires_at),
            entitlement_source = 'RAZORPAY',
            razorpay_subscription_id = $5,
            updated_at = NOW()
        WHERE guild_id = $1
          AND COALESCE(entitlement_source, '') <> 'LIFETIME_PROMO'
        """,
        row["guild_id"],
        status,
        row["billing_cycle"],
        current_period_end,
        subscription_id,
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
                elif event_type == "subscription.pending":
                    # Razorpay is retrying a failed recurring charge.
                    # Keep Pro during the retry window, but expose the
                    # pending billing state in /plan billing.
                    await mark_subscription_pending(
                        subscription_id,
                        "pending",
                        period_end,
                    )
                elif event_type == "subscription.halted":
                    # Razorpay exhausted payment retries. Remove paid
                    # entitlement unless a Lifetime Promo protects it.
                    await mark_subscription_inactive(
                        subscription_id,
                        "halted",
                    )
                elif event_type in {
                    "subscription.cancelled",
                    "subscription.completed",
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
            "`/attendance start` — Start attendance-only tracking (Voice/Stage)\n"
            "`/attendance status` — View attendance session\n"
            "`/attendance stop` — Export attendance report"
        ),
        inline=False,
    )
    embed.add_field(
        name="🎙️ Recording",
        value=(
            "`/record start` — Record a meeting + attendance (Voice/Stage)\n"
            "`/record status` — View active recording\n"
            "`/record stop` — Stop and export results\n"
            f"Free: up to {FREE_RECORDING_LIMIT_MINUTES} min/recording. "
            f"Pro: up to {PRO_RECORDING_LIMIT_MINUTES} min/recording + AI."
        ),
        inline=False,
    )
    embed.add_field(
        name="🗳️ Pro Polls",
        value=(
            "`/poll create` — Create a named poll anywhere in the server\n"
            "`/poll status` — View live poll results\n"
            "`/poll close` — Close a poll and export its Excel report\n"
            "`/poll report` — Re-download a poll Excel report\n"
            "No voice or attendance session is required. If a meeting is active, "
            "the poll is also included in that meeting's Pro Excel workbook."
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
        "Scheduling, reminders, attendance tracking, and recording are enabled.\n"
        f"Recording limit: **{FREE_RECORDING_LIMIT_MINUTES} minutes per meeting**.\n\n"
        f"💎 **Pro** raises recording to **{PRO_RECORDING_LIMIT_MINUTES} minutes** "
        "and adds AI transcription and AI meeting summaries."
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
    channel="Voice or Stage channel to track; leave blank to use your current channel",
)
@app_commands.checks.has_permissions(manage_guild=True)
async def attendance_start(
    interaction: discord.Interaction,
    name: str,
    channel: discord.VoiceChannel | discord.StageChannel | None = None,
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
            and isinstance(member.voice.channel, (discord.VoiceChannel, discord.StageChannel))
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
            if isinstance(default_voice, (discord.VoiceChannel, discord.StageChannel)):
                channel = default_voice
            else:
                await interaction.response.send_message(
                    "Join a voice or Stage channel, specify the `channel` option, "
                    "or configure a default voice/Stage channel with `/setup`.",
                    ephemeral=True,
                )
                return

    started_at = utcnow()

    plan = await get_guild_plan(guild_id)

    session = {
        "name": name.strip(),
        "channel_id": channel.id,
        "channel_name": channel.name,
        "started_at": started_at,
        "participants": {},
        "plan": plan,
        "session_mode": "attendance",
        "polls": {},
        "next_poll_id": 1,
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
        f"🔒 **No audio is being recorded.**\n"
        + (
            "🗳️ **Pro Polls enabled:** `/poll create` works anywhere in the server. "
            "Polls created while this session is active are also included in its Excel report.\n\n"
            if plan == "PRO"
            else "\n"
        )
        + "Use `/attendance status` to check progress and "
        "`/attendance stop` to finish."
    )


@attendance_group.command(
    name="status",
    description="Show live attendance for the current session or recording",
)
async def attendance_status(
    interaction: discord.Interaction,
):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command must be used inside a server.",
            ephemeral=True,
        )
        return

    guild_id = interaction.guild.id
    session = active_attendance.get(guild_id)
    recording = False

    # A recorded meeting already includes attendance tracking. If there is no
    # attendance-only session, surface the recording's live attendance here too.
    # Report the *actual* audio receiver state rather than merely the presence
    # of a recording session so /attendance status and /record status agree.
    if session is None:
        session = active_meetings.get(guild_id)
        if session is not None:
            voice_client = session.get("voice_client")
            recording = bool(
                voice_client
                and voice_client.is_connected()
                and voice_client.is_listening()
            )

    if session is None:
        await interaction.response.send_message(
            "There is no active attendance or recording session.",
            ephemeral=True,
        )
        return

    rows = []

    for uid, participant in session["participants"].items():
        member = interaction.guild.get_member(uid)
        in_channel = (
            member is not None
            and member.voice is not None
            and member.voice.channel is not None
            and member.voice.channel.id == session["channel_id"]
        )

        rows.append(
            (
                participant["display_name"],
                current_seconds(participant),
                "🟢 Present" if in_channel else "⚪ Left",
            )
        )

    rows.sort(key=lambda item: item[1], reverse=True)

    body = (
        "\n".join(
            f"• **{name}** — {format_duration(seconds)} — {state}"
            for name, seconds, state in rows[:40]
        )
        if rows
        else "No attendees recorded yet."
    )

    recording_text = "**YES**" if recording else "**NO**"
    await interaction.response.send_message(
        f"📋 **{session['name']}**\n"
        f"🎙️ Channel: <#{session['channel_id']}>\n"
        f"🔴 Recording: {recording_text}\n"
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

    # Polls work independently of audio. Attendance-only Pro sessions can
    # contain the same named polls as recorded meetings. Close any remaining
    # open polls before creating the final report.
    await close_all_meeting_polls(interaction.guild, session)

    attendance_path = write_attendance_csv(
        session,
        rows,
        ended_at,
    )

    plan = session.get("plan") or await get_guild_plan(guild_id)
    report_path = attendance_path
    report_label = "attendance CSV"
    if plan == "PRO":
        try:
            report_path = build_pro_excel_report(session, rows, ended_at)
            report_label = "Pro Excel meeting report"
        except Exception as e:
            print(
                "Pro Excel report generation failed for attendance-only session; "
                "using CSV fallback: "
                f"{type(e).__name__}: {e}"
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

    poll_count = len(session.get("polls", {}))
    poll_text = (
        f"🗳️ Polls included in report: **{poll_count}**\n"
        if plan == "PRO"
        else ""
    )

    await interaction.response.send_message(
        f"🏁 **Attendance ended: {session['name']}**\n"
        f"🎙️ Channel: <#{session['channel_id']}>\n"
        f"⏱️ Session length: "
        f"{format_duration(session_seconds)}\n"
        f"🔒 No audio was recorded.\n"
        f"{poll_text}"
        f"📊 Attached: **{report_label}**\n\n"
        f"{attendance_summary}",
        file=discord.File(report_path),
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




def format_poll_results_for_ai(meeting):
    polls = sorted(meeting.get("polls", {}).values(), key=lambda p: int(p["id"]))
    if not polls:
        return "No polls were conducted during this meeting."

    blocks = []
    for poll in polls:
        counts = _poll_counts(poll)
        total = sum(counts)
        lines = [f"Poll #{poll['id']}: {poll['question']}"]
        for index, option in enumerate(poll["options"]):
            count = counts[index]
            pct = (count / total * 100.0) if total else 0.0
            lines.append(f"- {option}: {count} vote(s), {pct:.1f}%")
        lines.append(f"Total respondents: {total}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def _excel_autofit(ws, min_width=10, max_width=50):
    if not OPENPYXL_AVAILABLE:
        return
    for column_cells in ws.columns:
        length = 0
        column_letter = get_column_letter(column_cells[0].column)
        for cell in column_cells:
            try:
                value = "" if cell.value is None else str(cell.value)
                length = max(length, max((len(line) for line in value.splitlines()), default=0))
            except Exception:
                pass
        ws.column_dimensions[column_letter].width = max(min_width, min(max_width, length + 2))


def _style_excel_sheet(ws, freeze="A2"):
    if not OPENPYXL_AVAILABLE:
        return
    ws.freeze_panes = freeze
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(vertical="top", wrap_text=True)
    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=True)
    _excel_autofit(ws)


def build_pro_excel_report(meeting, rows, ended_at):
    """Create the Pro meeting workbook with attendance and named poll results."""
    if not OPENPYXL_AVAILABLE:
        raise RuntimeError(
            "openpyxl is not installed. Add `openpyxl>=3.1.5` to requirements.txt."
        )

    workbook = Workbook()
    overview = workbook.active
    overview.title = "Meeting Overview"

    polls = sorted(meeting.get("polls", {}).values(), key=lambda p: int(p["id"]))
    total_votes = sum(len(poll.get("votes", {})) for poll in polls)
    overview_rows = [
        ("Field", "Value"),
        ("Meeting", meeting["name"]),
        ("Voice / Stage Channel", meeting["channel_name"]),
        ("Started UTC", meeting["started_at"].isoformat()),
        ("Ended UTC", ended_at.isoformat()),
        ("Duration (minutes)", round((ended_at - meeting["started_at"]).total_seconds() / 60, 2)),
        ("Plan", "PRO"),
        ("Attendees tracked", len(rows)),
        ("Polls conducted", len(polls)),
        ("Total poll responses", total_votes),
        ("Voting mode", "Named; one active vote per attendee per poll"),
    ]
    for row in overview_rows:
        overview.append(row)
    _style_excel_sheet(overview)

    # Attendance sheet with poll participation stats.
    attendance = workbook.create_sheet("Attendance")
    attendance_headers = [
        "Discord User ID",
        "Display Name",
        "Username",
        "Minutes Attended",
        "Attendance %",
        "Polls Answered",
        "Poll Participation %",
    ]
    attendance.append(attendance_headers)
    for row in rows:
        uid = int(row["discord_user_id"])
        answered = sum(1 for poll in polls if uid in poll.get("votes", {}))
        participation = (answered / len(polls) * 100.0) if polls else 0.0
        attendance.append([
            str(uid),
            row["display_name"],
            row["username"],
            row["minutes_attended"],
            row["attendance_percent"],
            answered,
            round(participation, 2),
        ])
    _style_excel_sheet(attendance)

    # One row per answer option so totals remain easy to filter/pivot.
    summary = workbook.create_sheet("Poll Summary")
    summary.append([
        "Poll ID",
        "Question",
        "Status",
        "Option",
        "Votes",
        "Percent",
        "Respondents",
        "Created UTC",
        "Closed UTC",
    ])
    for poll in polls:
        counts = _poll_counts(poll)
        total = sum(counts)
        for index, option in enumerate(poll["options"]):
            count = counts[index]
            pct = (count / total * 100.0) if total else 0.0
            summary.append([
                poll["id"],
                poll["question"],
                "Open" if poll.get("open") else "Closed",
                option,
                count,
                round(pct, 2),
                total,
                poll["created_at"].isoformat() if poll.get("created_at") else "",
                poll["closed_at"].isoformat() if poll.get("closed_at") else "",
            ])
    _style_excel_sheet(summary)

    # Exact voter-level data requested by the user.
    voter_detail = workbook.create_sheet("Voter Detail")
    voter_detail.append([
        "Poll ID",
        "Question",
        "Discord User ID",
        "Display Name",
        "Username",
        "Selected Option",
        "Voted UTC",
    ])
    for poll in polls:
        votes = sorted(
            poll.get("votes", {}).values(),
            key=lambda vote: (
                vote.get("display_name", "").casefold(),
                int(vote.get("discord_user_id", 0)),
            ),
        )
        for vote in votes:
            voter_detail.append([
                poll["id"],
                poll["question"],
                str(vote["discord_user_id"]),
                vote["display_name"],
                vote["username"],
                vote["option"],
                vote["voted_at"].isoformat() if vote.get("voted_at") else "",
            ])
    _style_excel_sheet(voter_detail)

    # Matrix-style sheet makes non-voters visible without losing voter detail.
    participation = workbook.create_sheet("Poll Participation")
    participation.append([
        "Poll ID",
        "Question",
        "Discord User ID",
        "Display Name",
        "Username",
        "Vote Status",
        "Selected Option",
    ])
    for poll in polls:
        for row in rows:
            uid = int(row["discord_user_id"])
            vote = poll.get("votes", {}).get(uid)
            participation.append([
                poll["id"],
                poll["question"],
                str(uid),
                row["display_name"],
                row["username"],
                "Voted" if vote else "Did not vote",
                vote["option"] if vote else "",
            ])
    _style_excel_sheet(participation)

    report_dir = meeting.get("recording_dir") or ATTENDANCE_DIR
    report_path = report_dir / (
        f"{safe_filename(meeting['name'])}_meeting_report.xlsx"
    )
    workbook.save(report_path)
    return report_path


# ---------------------------------------------------------------------------
# Pro AI Excel dashboard
# ---------------------------------------------------------------------------
_AI_STOPWORDS = {
    "the", "and", "for", "that", "this", "with", "from", "have", "has", "had",
    "was", "were", "are", "but", "not", "you", "your", "they", "their", "them",
    "our", "out", "all", "can", "could", "would", "should", "will", "just", "about",
    "into", "over", "under", "than", "then", "there", "here", "what", "when", "where",
    "who", "why", "how", "which", "while", "also", "been", "being", "because", "very",
    "some", "more", "most", "much", "many", "any", "each", "only", "other", "another",
    "such", "same", "between", "through", "during", "before", "after", "above", "below",
    "again", "further", "once", "does", "did", "doing", "done", "make", "made", "get",
    "got", "need", "needs", "needed", "want", "wants", "wanted", "say", "says", "said",
    "use", "used", "using", "one", "two", "three", "yes", "yeah", "okay", "ok", "like",
    "really", "think", "know", "going", "thing", "things", "meeting", "discussion", "speaker",
    "none", "identified", "vote", "votes", "poll", "results"
}


def _split_transcript_by_speaker(transcript: str):
    """Return [(speaker, text), ...] from Frost Scribe's speaker-headed transcript."""
    sections = []
    current_speaker = None
    current_lines = []

    for raw_line in (transcript or "").splitlines():
        line = raw_line.rstrip()
        if line.startswith("### "):
            if current_speaker is not None:
                sections.append((current_speaker, "\n".join(current_lines).strip()))
            current_speaker = line[4:].strip() or "Unknown"
            current_lines = []
        elif current_speaker is not None:
            current_lines.append(line)

    if current_speaker is not None:
        sections.append((current_speaker, "\n".join(current_lines).strip()))

    if not sections and transcript:
        sections.append(("Meeting", transcript.strip()))
    return sections


def _keyword_tokens(text: str):
    # Unicode letters, 3+ chars. Keeps non-English words where possible.
    words = re.findall(r"[^\W\d_]{3,}", (text or "").casefold(), flags=re.UNICODE)
    return [w for w in words if w not in _AI_STOPWORDS]


def _top_keywords(text: str, limit=60):
    return Counter(_keyword_tokens(text)).most_common(limit)


def _parse_ai_summary_sections(summary: str):
    """Parse the known Frost Scribe markdown headings into filterable rows."""
    sections = defaultdict(list)
    current = "Executive Summary"
    buffer = []

    def flush():
        nonlocal buffer
        if not buffer:
            return
        text = "\n".join(buffer).strip()
        if text:
            # Prefer bullet granularity for filtering; preserve paragraphs otherwise.
            bullets = []
            for line in text.splitlines():
                stripped = line.strip()
                if stripped.startswith(("- ", "• ", "* ")):
                    bullets.append(stripped[2:].strip())
                elif stripped:
                    bullets.append(stripped)
            for item in bullets:
                if item and item.casefold() not in {"none identified.", "none identified", "none."}:
                    sections[current].append(item)
        buffer = []

    for raw_line in (summary or "").splitlines():
        line = raw_line.strip()
        if line.startswith("## "):
            flush()
            current = line[3:].strip()
        else:
            buffer.append(raw_line)
    flush()
    return dict(sections)


def _summary_rows(summary: str):
    sections = _parse_ai_summary_sections(summary)
    rows = []
    for category, items in sections.items():
        for item in items:
            kws = [kw for kw, _ in _top_keywords(item, limit=6)]
            rows.append({
                "category": category,
                "item": item,
                "keywords": ", ".join(kws),
            })
    return rows, sections



def _parse_action_item(text: str):
    """Best-effort parse of the summary format: Owner — task — deadline."""
    parts = [p.strip() for p in re.split(r"\s+[—–-]\s+", text or "", maxsplit=2) if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return parts[0], parts[1], ""
    return "", (text or "").strip(), ""


def _safe_table(ws, ref: str, name: str):
    if Table is None or TableStyleInfo is None:
        return
    try:
        tab = Table(displayName=name, ref=ref)
        tab.tableStyleInfo = TableStyleInfo(
            name="TableStyleMedium2",
            showFirstColumn=False,
            showLastColumn=False,
            showRowStripes=True,
            showColumnStripes=False,
        )
        ws.add_table(tab)
    except Exception as exc:
        print(f"Could not add Excel table {name}: {type(exc).__name__}: {exc}")


def _dashboard_header(ws, title: str, subtitle: str = ""):
    ws.merge_cells("A1:H1")
    ws["A1"] = title
    ws["A1"].font = Font(size=20, bold=True, color="FFFFFF")
    ws["A1"].fill = PatternFill("solid", fgColor="0B1F33")
    ws["A1"].alignment = Alignment(vertical="center")
    ws.row_dimensions[1].height = 30
    if subtitle:
        ws.merge_cells("A2:H2")
        ws["A2"] = subtitle
        ws["A2"].font = Font(italic=True, color="5B6573")
        ws["A2"].alignment = Alignment(wrap_text=True)


def _kpi_cell(ws, cell: str, label: str, value):
    ws[cell] = label
    ws[cell].font = Font(bold=True, color="FFFFFF")
    ws[cell].fill = PatternFill("solid", fgColor="1877A8")
    below = ws.cell(row=ws[cell].row + 1, column=ws[cell].column)
    below.value = value
    below.font = Font(size=16, bold=True, color="0B1F33")
    below.alignment = Alignment(horizontal="center")
    below.fill = PatternFill("solid", fgColor="EAF5FB")
    thin = Side(style="thin", color="B8D7E8")
    ws[cell].border = Border(left=thin, right=thin, top=thin, bottom=thin)
    below.border = Border(left=thin, right=thin, top=thin, bottom=thin)
    ws[cell].alignment = Alignment(horizontal="center")


def enhance_pro_excel_with_ai(report_path: Path, meeting, rows, ended_at, transcript: str, summary: str):
    """
    Add an AI analysis dashboard to the existing Pro workbook.

    The dashboard is deliberately based on the already-generated AI summary plus
    deterministic keyword counts from the full transcript. This avoids a second
    expensive LLM pass for a long (for example 3-hour) meeting while still making
    the workbook easy to filter and analyze.
    """
    if not OPENPYXL_AVAILABLE or load_workbook is None:
        raise RuntimeError("openpyxl is required for the AI Excel dashboard.")
    report_path = Path(report_path)
    if report_path.suffix.casefold() != ".xlsx" or not report_path.exists():
        raise RuntimeError("AI dashboard requires the Pro .xlsx meeting report.")

    workbook = load_workbook(report_path)
    for sheet_name in [
        "AI Dashboard", "AI Summary Detail", "Topics", "Decisions",
        "Action Items", "Risks & Questions", "Keyword Index",
        "Speaker Insights", "Speaker Transcript"
    ]:
        if sheet_name in workbook.sheetnames:
            del workbook[sheet_name]

    summary_rows, parsed_sections = _summary_rows(summary)
    speaker_sections = _split_transcript_by_speaker(transcript)
    keyword_counts = _top_keywords(transcript, limit=100)

    decisions = parsed_sections.get("Decisions Made", [])
    actions = parsed_sections.get("Action Items", [])
    risks = parsed_sections.get("Open Questions / Risks", [])
    topics = parsed_sections.get("Key Discussion Points", [])

    # ---------------- Dashboard ----------------
    dash = workbook.create_sheet("AI Dashboard", 0)
    _dashboard_header(
        dash,
        f"Frost Scribe AI Dashboard — {meeting['name']}",
        "Use the filter dropdowns in AI Summary Detail, Keyword Index, Speaker Insights, "
        "Attendance, and Poll sheets to drill into a long meeting quickly.",
    )

    duration_min = round((ended_at - meeting["started_at"]).total_seconds() / 60.0, 1)
    _kpi_cell(dash, "A4", "Duration (min)", duration_min)
    _kpi_cell(dash, "C4", "Attendees", len(rows))
    _kpi_cell(dash, "E4", "Speakers captured", len(speaker_sections))
    _kpi_cell(dash, "G4", "Polls", len(meeting.get("polls", {})))
    _kpi_cell(dash, "A7", "Key topics", len(topics))
    _kpi_cell(dash, "C7", "Decisions", len(decisions))
    _kpi_cell(dash, "E7", "Action items", len(actions))
    _kpi_cell(dash, "G7", "Open risks/questions", len(risks))

    dash["A10"] = "Executive Summary"
    dash["A10"].font = Font(size=14, bold=True, color="0B1F33")
    dash.merge_cells("A11:H15")
    executive = parsed_sections.get("Executive Summary", [])
    dash["A11"] = "\n".join(executive) if executive else (summary[:2500] if summary else "No AI summary available.")
    dash["A11"].alignment = Alignment(vertical="top", wrap_text=True)
    dash["A11"].fill = PatternFill("solid", fgColor="F4F8FB")

    dash["A17"] = "Top Keywords"
    dash["A17"].font = Font(size=14, bold=True, color="0B1F33")
    dash.append([])  # harmless; ensures dimensions remain normal
    dash["A18"] = "Keyword"
    dash["B18"] = "Occurrences"
    for c in dash[18][:2]:
        c.font = Font(bold=True, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor="1877A8")
    top20 = keyword_counts[:20]
    for idx, (kw, count) in enumerate(top20, start=19):
        dash.cell(idx, 1, kw)
        dash.cell(idx, 2, count)

    dash["D17"] = "How to analyze"
    dash["D17"].font = Font(size=14, bold=True, color="0B1F33")
    dash.merge_cells("D18:H24")
    dash["D18"] = (
        "1. Open Keyword Index and use the Excel filter search box to find any term.\n"
        "2. Open AI Summary Detail and filter Category to Decisions, Action Items, Risks, or Topics.\n"
        "3. Filter Speaker Insights to see each speaker's main themes.\n"
        "4. Speaker Transcript keeps the full speaker-level text searchable with Excel's filter/search tools.\n"
        "5. Attendance and Poll sheets remain linked in the same workbook."
    )
    dash["D18"].alignment = Alignment(vertical="top", wrap_text=True)
    dash["D18"].fill = PatternFill("solid", fgColor="F4F8FB")

    for col, width in {"A": 22, "B": 14, "C": 4, "D": 18, "E": 18, "F": 18, "G": 18, "H": 18}.items():
        dash.column_dimensions[col].width = width
    dash.freeze_panes = "A4"

    if BarChart is not None and Reference is not None and top20:
        try:
            chart = BarChart()
            chart.type = "bar"
            chart.style = 10
            chart.title = "Most Mentioned Keywords"
            chart.y_axis.title = "Keyword"
            chart.x_axis.title = "Occurrences"
            data = Reference(dash, min_col=2, min_row=18, max_row=18 + min(10, len(top20)))
            cats = Reference(dash, min_col=1, min_row=19, max_row=18 + min(10, len(top20)))
            chart.add_data(data, titles_from_data=True)
            chart.set_categories(cats)
            chart.height = 7.2
            chart.width = 11
            dash.add_chart(chart, "D26")
        except Exception as exc:
            print(f"Could not add dashboard keyword chart: {type(exc).__name__}: {exc}")

    # ---------------- AI Summary Detail ----------------
    detail = workbook.create_sheet("AI Summary Detail")
    detail.append(["Category", "AI Finding", "Keywords"])
    if summary_rows:
        for item in summary_rows:
            detail.append([item["category"], item["item"], item["keywords"]])
    else:
        detail.append(["Summary", "No structured AI findings were available.", ""])
    _style_excel_sheet(detail)
    detail.column_dimensions["A"].width = 28
    detail.column_dimensions["B"].width = 90
    detail.column_dimensions["C"].width = 38
    _safe_table(detail, f"A1:C{detail.max_row}", "AISummaryDetailTable")


    # ---------------- Dedicated analysis sheets ----------------
    topics_ws = workbook.create_sheet("Topics")
    topics_ws.append(["Topic / Discussion Point", "Keywords"])
    for item in topics:
        topics_ws.append([item, ", ".join(kw for kw, _ in _top_keywords(item, limit=8))])
    if topics_ws.max_row == 1:
        topics_ws.append(["No key discussion points identified.", ""])
    _style_excel_sheet(topics_ws)
    topics_ws.column_dimensions["A"].width = 100
    topics_ws.column_dimensions["B"].width = 45
    _safe_table(topics_ws, f"A1:B{topics_ws.max_row}", "TopicsTable")

    decisions_ws = workbook.create_sheet("Decisions")
    decisions_ws.append(["Decision", "Keywords"])
    for item in decisions:
        decisions_ws.append([item, ", ".join(kw for kw, _ in _top_keywords(item, limit=8))])
    if decisions_ws.max_row == 1:
        decisions_ws.append(["No decisions identified.", ""])
    _style_excel_sheet(decisions_ws)
    decisions_ws.column_dimensions["A"].width = 100
    decisions_ws.column_dimensions["B"].width = 45
    _safe_table(decisions_ws, f"A1:B{decisions_ws.max_row}", "DecisionsTable")

    actions_ws = workbook.create_sheet("Action Items")
    actions_ws.append(["Owner", "Action", "Deadline", "Keywords", "Status"])
    for item in actions:
        owner, task, deadline = _parse_action_item(item)
        actions_ws.append([
            owner, task, deadline,
            ", ".join(kw for kw, _ in _top_keywords(item, limit=8)),
            "Open",
        ])
    if actions_ws.max_row == 1:
        actions_ws.append(["", "No action items identified.", "", "", ""])
    _style_excel_sheet(actions_ws)
    for col, width in {"A": 28, "B": 80, "C": 26, "D": 45, "E": 14}.items():
        actions_ws.column_dimensions[col].width = width
    _safe_table(actions_ws, f"A1:E{actions_ws.max_row}", "ActionItemsTable")

    risks_ws = workbook.create_sheet("Risks & Questions")
    risks_ws.append(["Open Question / Risk", "Keywords"])
    for item in risks:
        risks_ws.append([item, ", ".join(kw for kw, _ in _top_keywords(item, limit=8))])
    if risks_ws.max_row == 1:
        risks_ws.append(["No open questions or risks identified.", ""])
    _style_excel_sheet(risks_ws)
    risks_ws.column_dimensions["A"].width = 100
    risks_ws.column_dimensions["B"].width = 45
    _safe_table(risks_ws, f"A1:B{risks_ws.max_row}", "RisksQuestionsTable")

    # ---------------- Keyword Index ----------------
    keyword_ws = workbook.create_sheet("Keyword Index")
    keyword_ws.append(["Keyword", "Occurrences", "Speaker Count", "Speakers", "Related AI Categories"])
    speaker_token_sets = {
        speaker: Counter(_keyword_tokens(text))
        for speaker, text in speaker_sections
    }
    for kw, count in keyword_counts:
        speakers = [speaker for speaker, counts in speaker_token_sets.items() if counts.get(kw, 0) > 0]
        related_categories = sorted({
            item["category"] for item in summary_rows
            if re.search(rf"(?<!\w){re.escape(kw)}(?!\w)", item["item"].casefold())
            or kw in [x.strip() for x in item["keywords"].split(",") if x.strip()]
        })
        keyword_ws.append([
            kw, count, len(speakers), ", ".join(speakers), ", ".join(related_categories)
        ])
    _style_excel_sheet(keyword_ws)
    keyword_ws.column_dimensions["A"].width = 24
    keyword_ws.column_dimensions["B"].width = 14
    keyword_ws.column_dimensions["C"].width = 14
    keyword_ws.column_dimensions["D"].width = 50
    keyword_ws.column_dimensions["E"].width = 45
    _safe_table(keyword_ws, f"A1:E{max(2, keyword_ws.max_row)}", "KeywordIndexTable")
    if keyword_ws.max_row >= 2:
        try:
            keyword_ws.conditional_formatting.add(
                f"B2:B{keyword_ws.max_row}",
                # openpyxl data bars are supported without another dependency
                __import__("openpyxl.formatting.rule", fromlist=["DataBarRule"]).DataBarRule(
                    start_type="min", end_type="max", color="63C5DA", showValue=True
                )
            )
        except Exception:
            pass

    # ---------------- Speaker Insights ----------------
    speaker_ws = workbook.create_sheet("Speaker Insights")
    speaker_ws.append(["Speaker", "Word Count", "Top Keywords", "Transcript Preview"])
    for speaker, text in speaker_sections:
        kws = ", ".join(kw for kw, _ in _top_keywords(text, limit=10))
        preview = re.sub(r"\s+", " ", text).strip()[:700]
        speaker_ws.append([speaker, len(text.split()), kws, preview])
    _style_excel_sheet(speaker_ws)
    speaker_ws.column_dimensions["A"].width = 28
    speaker_ws.column_dimensions["B"].width = 14
    speaker_ws.column_dimensions["C"].width = 60
    speaker_ws.column_dimensions["D"].width = 95
    _safe_table(speaker_ws, f"A1:D{max(2, speaker_ws.max_row)}", "SpeakerInsightsTable")

    # ---------------- Searchable transcript ----------------
    transcript_ws = workbook.create_sheet("Speaker Transcript")
    transcript_ws.append(["Speaker", "Transcript Text"])
    for speaker, text in speaker_sections:
        # Excel cell limit is 32,767 chars. Split a very long speaker transcript
        # into continuation rows so a 3-hour meeting remains valid/searchable.
        if not text:
            transcript_ws.append([speaker, ""])
            continue
        chunk_size = 30000
        for index in range(0, len(text), chunk_size):
            label = speaker if index == 0 else f"{speaker} (cont.)"
            transcript_ws.append([label, text[index:index + chunk_size]])
    _style_excel_sheet(transcript_ws)
    transcript_ws.column_dimensions["A"].width = 30
    transcript_ws.column_dimensions["B"].width = 120
    _safe_table(transcript_ws, f"A1:B{max(2, transcript_ws.max_row)}", "SpeakerTranscriptTable")


    # Add filterable tables to the core Pro sheets too, where possible.
    for sheet_name, table_name in [
        ("Attendance", "AttendanceTable"),
        ("Poll Summary", "MeetingPollSummaryTable"),
        ("Voter Detail", "MeetingVoterDetailTable"),
        ("Poll Participation", "MeetingPollParticipationTable"),
    ]:
        if sheet_name in workbook.sheetnames:
            ws = workbook[sheet_name]
            if ws.max_row >= 2 and ws.max_column >= 1 and not ws.tables:
                ref = f"A1:{get_column_letter(ws.max_column)}{ws.max_row}"
                _safe_table(ws, ref, table_name)

    # Put workbook sheets into a logical analyst-friendly order.
    preferred_order = [
        "AI Dashboard", "AI Summary Detail", "Topics", "Decisions", "Action Items",
        "Risks & Questions", "Keyword Index", "Speaker Insights",
        "Meeting Overview", "Attendance", "Poll Summary", "Voter Detail",
        "Poll Participation", "Speaker Transcript"
    ]
    ordered = [workbook[name] for name in preferred_order if name in workbook.sheetnames]
    ordered += [ws for ws in workbook.worksheets if ws.title not in preferred_order]
    workbook._sheets = ordered

    workbook.save(report_path)
    return report_path


def encode_master_recording_for_discord(
    wav_path: Path,
    meeting_seconds: float,
    *,
    target_bytes: int = 18 * 1024 * 1024,
) -> Path:
    """
    Encode the chronological master WAV to one Ogg/Opus speech recording.

    The bitrate is selected from meeting duration so even a 180-minute Pro
    meeting targets one Discord-friendly attachment instead of dozens of WAV
    chunks. Per-speaker WAVs remain untouched for speaker-aware transcription.
    """
    if not wav_path.exists() or wav_path.stat().st_size <= 44:
        raise RuntimeError("The combined WAV contains no usable audio.")

    try:
        import imageio_ffmpeg
    except ImportError as exc:
        raise RuntimeError(
            "imageio-ffmpeg is not installed. Add "
            "imageio-ffmpeg>=0.6.0 to requirements.txt."
        ) from exc

    duration = max(1.0, float(meeting_seconds or 1.0))

    # Leave container/metadata headroom. Clamp for speech quality and ensure
    # the 180-minute Pro limit can still fit in a single ~18 MiB target file.
    budget_bits_per_second = int((target_bytes * 8 * 0.92) / duration)
    bitrate_kbps = max(8, min(64, budget_bits_per_second // 1000))

    output_path = wav_path.with_name("meeting_audio.ogg")
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()

    def run_encode(kbps: int):
        command = [
            ffmpeg_exe,
            "-y",
            "-loglevel", "error",
            "-i", str(wav_path),
            "-vn",
            "-ac", "1",
            "-ar", "24000",
            "-c:a", "libopus",
            "-application", "voip",
            "-b:a", f"{kbps}k",
            "-vbr", "on",
            "-compression_level", "10",
            str(output_path),
        ]
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=max(120, int(duration / 4)),
        )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "ffmpeg failed").strip()
            raise RuntimeError(detail[-1500:])

    run_encode(bitrate_kbps)

    # If variable bitrate overshoots the target, retry at the minimum speech
    # bitrate. 8 kbps Opus is intentionally reserved as the safety fallback.
    if output_path.stat().st_size > target_bytes and bitrate_kbps > 8:
        run_encode(8)

    if output_path.stat().st_size > target_bytes:
        raise RuntimeError(
            f"Compressed master is still {output_path.stat().st_size / 1024 / 1024:.1f} MiB. "
            "A storage/link delivery backend is required for this recording."
        )

    print(
        "Encoded single master recording: "
        f"{output_path.name} at ~{bitrate_kbps} kbps, "
        f"{output_path.stat().st_size / 1024 / 1024:.1f} MiB"
    )
    return output_path


async def send_recording_files(channel, meeting):
    """
    Send exactly one combined full-meeting audio file.

    V17 no longer splits the user-facing recording into WAV chunks. The raw
    combined WAV preserves the full meeting timeline and is compressed to one
    Ogg/Opus speech file before upload. Temporary per-speaker WAV tracks remain
    internal for speaker-aware transcription only.
    """
    sink = meeting.get("audio_sink")
    if sink is None:
        return 0

    wav_path = getattr(sink, "combined_path", None)
    if wav_path is None or not wav_path.exists() or wav_path.stat().st_size <= 44:
        await channel.send(
            "🎙️ No usable audio was captured for this recording."
        )
        return 0

    meeting_seconds = max(
        1.0,
        (utcnow() - meeting["started_at"]).total_seconds(),
    )

    try:
        master_path = await asyncio.to_thread(
            encode_master_recording_for_discord,
            wav_path,
            meeting_seconds,
        )
    except Exception as e:
        print(
            "Could not encode single master recording: "
            f"{type(e).__name__}: {e}"
        )
        await channel.send(
            "⚠️ The full meeting audio was captured, but I could not compress "
            "it into one Discord attachment.\n"
            f"`{type(e).__name__}: {e}`"
        )
        return 0

    await channel.send(
        "🎙️ **Full meeting recording**",
        file=discord.File(
            master_path,
            filename=f"{safe_filename(meeting['name'])}_recording.ogg",
        ),
    )
    return 1


def cleanup_recording_directory(path: Path):
    """Delete all temporary audio/transcript artifacts for one meeting."""
    try:
        if path and path.exists():
            shutil.rmtree(path)
            print(f"Cleaned recording directory: {path}")
    except Exception as e:
        print(
            f"Recording cleanup failed for {path}: "
            f"{type(e).__name__}: {e}"
        )


async def finalize_recording(
    guild: discord.Guild,
    fallback_channel,
    guild_id: int,
    *,
    stop_reason: str = "manual",
):
    """Stop one recording, export attendance/audio, run Pro AI, then clean disk."""
    meeting = active_meetings.pop(guild_id, None)
    if meeting is None:
        return None

    limit_task = meeting.get("limit_task")
    current_task = asyncio.current_task()
    if (
        limit_task
        and limit_task is not current_task
        and not limit_task.done()
    ):
        limit_task.cancel()

    ended_at = utcnow()

    # Polls are part of the recorded meeting lifecycle. Closing the meeting
    # closes every still-open poll and disables its voting buttons.
    await close_all_meeting_polls(guild, meeting)

    for participant in meeting["participants"].values():
        if participant["joined_at"] is not None:
            participant["seconds"] += (
                ended_at - participant["joined_at"]
            ).total_seconds()
            participant["joined_at"] = None

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

    output_channel = await get_report_channel(guild, fallback_channel)
    recording_dir = meeting["recording_dir"]

    try:
        date_str = ended_at.strftime("%Y-%m-%d_%H-%M-%S")
        attendance_name = (
            f"{safe_filename(meeting['name'])}_{date_str}.csv"
        )
        attendance_path = recording_dir / attendance_name

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
                    "meeting_started_utc": meeting["started_at"].isoformat(),
                    "meeting_ended_utc": ended_at.isoformat(),
                    "minutes_attended": round(seconds / 60, 2),
                    "attendance_percent": round(attendance_pct, 2),
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

        plan = meeting.get("plan") or await get_guild_plan(guild_id)
        report_path = attendance_path
        if plan == "PRO":
            try:
                report_path = build_pro_excel_report(meeting, rows, ended_at)
            except Exception as e:
                print(
                    "Pro Excel report generation failed; using CSV fallback: "
                    f"{type(e).__name__}: {e}"
                )
                report_path = attendance_path

        if rows:
            attendance_summary = "\n".join(
                f"• **{row['display_name']}** — "
                f"{row['minutes_attended']:.1f} min "
                f"({row['attendance_percent']:.1f}%)"
                for row in rows[:30]
            )
        else:
            attendance_summary = "No attendees recorded."

        if stop_reason == "limit":
            reason_text = (
                f"⏱️ Recording automatically stopped at the "
                f"{meeting['limit_minutes']}-minute {plan.title()} limit.\n"
            )
            if plan != "PRO":
                reason_text += (
                    f"💎 Upgrade to **Frost Scribe Pro** for up to "
                    f"**{PRO_RECORDING_LIMIT_MINUTES} minutes** per recording.\n"
                )
        else:
            reason_text = ""

        if plan == "PRO":
            next_step_text = (
                "📊 The Pro Excel meeting report is attached with attendance and poll results.\n"
                "⏳ Audio recording stopped. I am now transcribing "
                "the meeting and generating the AI summary."
            )
        else:
            next_step_text = (
                "🎙️ Audio recording stopped. The combined meeting recording is attached below.\n"
                "💎 Upgrade to **Frost Scribe Pro** for transcription and AI summaries."
            )

        await output_channel.send(
            f"🏁 **Recording ended: {meeting['name']}**\n"
            f"🎙️ Channel: <#{meeting['channel_id']}>\n"
            f"⏱️ Recording length: {format_duration(meeting_seconds)}\n"
            f"{reason_text}\n"
            f"{attendance_summary}\n\n"
            f"{next_step_text}",
            file=discord.File(report_path),
        )

        try:
            await send_recording_files(output_channel, meeting)
        except Exception as e:
            await output_channel.send(
                "⚠️ I could not upload the combined meeting recording.\n"
                f"`{type(e).__name__}: {e}`"
            )

        if plan == "PRO":
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

                enhanced_report_path = None
                try:
                    enhanced_report_path = await asyncio.to_thread(
                        enhance_pro_excel_with_ai,
                        report_path,
                        meeting,
                        rows,
                        ended_at,
                        transcript,
                        summary,
                    )
                except Exception as dashboard_error:
                    print(
                        "AI Excel dashboard generation failed: "
                        f"{type(dashboard_error).__name__}: {dashboard_error}"
                    )

                attachments = [discord.File(transcript_path)]
                if summary_path is not None and summary_path.exists():
                    attachments.append(discord.File(summary_path))
                if enhanced_report_path is not None and Path(enhanced_report_path).exists():
                    attachments.append(
                        discord.File(
                            enhanced_report_path,
                            filename=f"{safe_filename(meeting['name'])}_AI_dashboard.xlsx",
                        )
                    )

                dashboard_note = (
                    "\n\n📊 **AI Excel Dashboard attached** — filter keywords, topics, "
                    "decisions, actions, speakers, attendance, and polls."
                    if enhanced_report_path is not None else ""
                )
                await output_channel.send(
                    f"📝 **AI Meeting Summary — {meeting['name']}**\n\n"
                    f"{preview}{dashboard_note}",
                    files=attachments,
                )

            except Exception as e:
                await output_channel.send(
                    "⚠️ Attendance and audio recording completed, but "
                    "transcription or summarization failed.\n"
                    f"`{type(e).__name__}: {e}`\n\n"
                    "The temporary recording will still be deleted to "
                    "protect server storage."
                )

        return {
            "meeting": meeting,
            "seconds": meeting_seconds,
            "rows": rows,
            "output_channel": output_channel,
        }

    finally:
        # Recording files are temporary working files, not permanent storage.
        cleanup_recording_directory(recording_dir)


async def recording_limit_worker(guild_id: int):
    """Warn before the plan limit, then automatically finalize the recording."""
    meeting = active_meetings.get(guild_id)
    if meeting is None:
        return

    limit_seconds = meeting["limit_minutes"] * 60
    started_at = meeting["started_at"]
    guild = bot.get_guild(guild_id)
    if guild is None:
        return

    fallback_channel = guild.get_channel(meeting["command_channel_id"])

    async def still_current():
        return active_meetings.get(guild_id) is meeting

    async def sleep_until(offset_seconds: int):
        target = started_at + timedelta(seconds=offset_seconds)
        delay = max(0.0, (target - utcnow()).total_seconds())
        await asyncio.sleep(delay)

    try:
        if limit_seconds > 600:
            await sleep_until(limit_seconds - 600)
            if not await still_current():
                return
            output = await get_report_channel(guild, fallback_channel)
            warning_text = (
                f"⚠️ **Recording limit:** `{meeting['name']}` will "
                "automatically stop in **10 minutes**."
            )
            if meeting.get("plan") != "PRO":
                warning_text += (
                    f"\n💎 Frost Scribe Pro allows up to "
                    f"**{PRO_RECORDING_LIMIT_MINUTES} minutes** per recording."
                )
            await output.send(warning_text)

        if limit_seconds > 60:
            await sleep_until(limit_seconds - 60)
            if not await still_current():
                return
            output = await get_report_channel(guild, fallback_channel)
            warning_text = (
                f"⚠️ **Recording limit:** `{meeting['name']}` will "
                "automatically stop in **1 minute**."
            )
            if meeting.get("plan") != "PRO":
                warning_text += (
                    f"\n💎 Upgrade to Frost Scribe Pro for up to "
                    f"**{PRO_RECORDING_LIMIT_MINUTES} minutes** per recording."
                )
            await output.send(warning_text)

        await sleep_until(limit_seconds)
        if not await still_current():
            return

        await finalize_recording(
            guild,
            fallback_channel,
            guild_id,
            stop_reason="limit",
        )

    except asyncio.CancelledError:
        return
    except Exception as e:
        print(
            f"Recording limit worker failed for guild {guild_id}: "
            f"{type(e).__name__}: {e}"
        )



# ---------------------------------------------------------------------------
# Pro polls — usable anywhere, independent of voice/audio
# ---------------------------------------------------------------------------

def get_active_poll_session(guild_id: int):
    """Return an active recorded or attendance-only meeting, if one exists."""
    return active_meetings.get(guild_id) or active_attendance.get(guild_id)


def get_guild_poll_state(guild_id: int):
    """Return/create the guild-wide poll registry."""
    state = guild_poll_states.get(guild_id)
    if state is None:
        state = {"next_poll_id": 1, "polls": {}}
        guild_poll_states[guild_id] = state
    return state


poll_group = app_commands.Group(
    name="poll",
    description="Pro named polls with Excel voter reports",
)


def _poll_counts(poll):
    counts = [0] * len(poll["options"])
    for vote in poll["votes"].values():
        index = int(vote["option_index"])
        if 0 <= index < len(counts):
            counts[index] += 1
    return counts


def _latest_guild_poll(guild_id: int, *, open_only=False):
    state = get_guild_poll_state(guild_id)
    polls = list(state.get("polls", {}).values())
    if open_only:
        polls = [poll for poll in polls if poll.get("open")]
    if not polls:
        return None
    return max(polls, key=lambda poll: int(poll["id"]))


def _latest_poll(meeting, *, open_only=False):
    """Compatibility helper used by meeting-report code."""
    polls = list(meeting.get("polls", {}).values())
    if open_only:
        polls = [poll for poll in polls if poll.get("open")]
    if not polls:
        return None
    return max(polls, key=lambda poll: int(poll["id"]))


def build_poll_embed(poll):
    counts = _poll_counts(poll)
    total = sum(counts)
    state = "OPEN" if poll.get("open") else "CLOSED"
    linked_meeting = poll.get("linked_meeting_name")
    context = (
        f"Linked meeting: **{linked_meeting}**\n"
        if linked_meeting
        else "Standalone poll — no voice or attendance session required.\n"
    )
    embed = discord.Embed(
        title=f"🗳️ Poll #{poll['id']} — {state}",
        description=(
            f"**{poll['question']}**\n\n"
            f"{context}"
            "Votes are **named** and are included in the Pro Excel poll report. "
            "You can change your vote while the poll is open."
        ),
    )
    for index, option in enumerate(poll["options"]):
        count = counts[index]
        pct = (count / total * 100.0) if total else 0.0
        embed.add_field(
            name=f"{index + 1}. {option}",
            value=f"**{count}** vote{'s' if count != 1 else ''} · {pct:.1f}%",
            inline=False,
        )
    embed.set_footer(
        text=f"{total} respondent{'s' if total != 1 else ''} · Poll #{poll['id']}"
    )
    return embed


def build_poll_excel_report(guild: discord.Guild, poll):
    """Create a standalone Excel report for one poll, including named voters."""
    if not OPENPYXL_AVAILABLE:
        raise RuntimeError(
            "openpyxl is not installed. Add `openpyxl>=3.1.5` to requirements.txt."
        )

    workbook = Workbook()
    overview = workbook.active
    overview.title = "Poll Overview"
    counts = _poll_counts(poll)
    total = sum(counts)
    channel = guild.get_channel(poll.get("channel_id"))
    overview_rows = [
        ("Field", "Value"),
        ("Server", guild.name),
        ("Poll ID", poll["id"]),
        ("Question", poll["question"]),
        ("Status", "Open" if poll.get("open") else "Closed"),
        ("Channel", getattr(channel, "name", str(poll.get("channel_id") or ""))),
        ("Created by", poll.get("created_by_name") or str(poll.get("created_by") or "")),
        ("Created UTC", poll["created_at"].isoformat() if poll.get("created_at") else ""),
        ("Closed UTC", poll["closed_at"].isoformat() if poll.get("closed_at") else ""),
        ("Linked meeting", poll.get("linked_meeting_name") or "None"),
        ("Total respondents", total),
        ("Voting mode", "Named; one active vote per Discord member"),
    ]
    for row in overview_rows:
        overview.append(row)
    _style_excel_sheet(overview)

    summary = workbook.create_sheet("Poll Summary")
    summary.append(["Option", "Votes", "Percent"])
    for index, option in enumerate(poll["options"]):
        count = counts[index]
        pct = (count / total * 100.0) if total else 0.0
        summary.append([option, count, round(pct, 2)])
    _style_excel_sheet(summary)

    voters = workbook.create_sheet("Voter Detail")
    voters.append([
        "Discord User ID",
        "Display Name",
        "Username",
        "Selected Option",
        "Voted UTC",
    ])
    for vote in sorted(
        poll.get("votes", {}).values(),
        key=lambda item: (
            item.get("display_name", "").casefold(),
            int(item.get("discord_user_id", 0)),
        ),
    ):
        voters.append([
            str(vote["discord_user_id"]),
            vote["display_name"],
            vote["username"],
            vote["option"],
            vote["voted_at"].isoformat() if vote.get("voted_at") else "",
        ])
    _style_excel_sheet(voters)

    POLLS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = POLLS_DIR / (
        f"poll_{poll['id']}_{safe_filename(poll['question'])[:60]}_report.xlsx"
    )
    workbook.save(report_path)
    return report_path


class MeetingPollButton(discord.ui.Button):
    def __init__(self, guild_id: int, poll_id: int, option_index: int, label: str, *, disabled=False):
        style_cycle = [
            discord.ButtonStyle.primary,
            discord.ButtonStyle.secondary,
            discord.ButtonStyle.success,
            discord.ButtonStyle.secondary,
            discord.ButtonStyle.primary,
        ]
        super().__init__(
            label=f"{option_index + 1}. {label}"[:80],
            style=style_cycle[option_index % len(style_cycle)],
            custom_id=f"frost_poll:{guild_id}:{poll_id}:{option_index}",
            disabled=disabled,
        )
        self.guild_id = guild_id
        self.poll_id = poll_id
        self.option_index = option_index

    async def callback(self, interaction: discord.Interaction):
        state = get_guild_poll_state(self.guild_id)
        poll = state.get("polls", {}).get(self.poll_id)
        if poll is None or not poll.get("open"):
            await interaction.response.send_message(
                "This poll is closed or is no longer available.",
                ephemeral=True,
            )
            return

        # Polls intentionally do not require voice-channel participation.
        # Any human member who can see/interact with the poll can vote.
        if getattr(interaction.user, "bot", False):
            await interaction.response.send_message(
                "Bots cannot vote in Frost Scribe polls.",
                ephemeral=True,
            )
            return

        option = poll["options"][self.option_index]
        previous = poll["votes"].get(interaction.user.id)
        poll["votes"][interaction.user.id] = {
            "discord_user_id": interaction.user.id,
            "display_name": getattr(interaction.user, "display_name", interaction.user.name),
            "username": interaction.user.name,
            "option_index": self.option_index,
            "option": option,
            "voted_at": utcnow(),
        }

        try:
            await interaction.message.edit(
                embed=build_poll_embed(poll),
                view=MeetingPollView(
                    self.guild_id,
                    self.poll_id,
                    poll["options"],
                    disabled=False,
                ),
            )
        except Exception as e:
            print(f"Could not refresh poll message: {type(e).__name__}: {e}")

        changed = previous is not None and previous.get("option_index") != self.option_index
        await interaction.response.send_message(
            (
                f"✅ Vote {'updated' if changed else 'recorded'}: **{option}**\n"
                "Your name and vote will appear in the Pro Excel poll report."
            ),
            ephemeral=True,
        )


class MeetingPollView(discord.ui.View):
    def __init__(self, guild_id: int, poll_id: int, options, *, disabled=False):
        super().__init__(timeout=None)
        for index, option in enumerate(options):
            self.add_item(
                MeetingPollButton(
                    guild_id,
                    poll_id,
                    index,
                    option,
                    disabled=disabled,
                )
            )


async def refresh_poll_message(guild: discord.Guild, poll, *, disabled=False):
    channel = guild.get_channel(poll.get("channel_id"))
    if channel is None or poll.get("message_id") is None:
        return
    try:
        message = await channel.fetch_message(poll["message_id"])
        await message.edit(
            embed=build_poll_embed(poll),
            view=MeetingPollView(
                guild.id,
                poll["id"],
                poll["options"],
                disabled=disabled,
            ),
        )
    except Exception as e:
        print(
            f"Could not update poll #{poll.get('id')}: "
            f"{type(e).__name__}: {e}"
        )


async def close_meeting_poll(guild: discord.Guild, poll):
    if not poll.get("open"):
        return
    poll["open"] = False
    poll["closed_at"] = utcnow()
    await refresh_poll_message(guild, poll, disabled=True)


async def close_all_meeting_polls(guild: discord.Guild, meeting):
    # Only polls linked to this meeting are closed. Standalone guild polls stay open.
    for poll in meeting.get("polls", {}).values():
        if poll.get("open"):
            await close_meeting_poll(guild, poll)


class PollCreateModal(discord.ui.Modal, title="Create Pro Poll"):
    question = discord.ui.TextInput(
        label="Question",
        placeholder="Should we proceed with the migration?",
        max_length=200,
    )
    options_text = discord.ui.TextInput(
        label="Options — one per line (2–10)",
        placeholder="Yes\nNo\nAbstain",
        style=discord.TextStyle.paragraph,
        min_length=3,
        max_length=800,
    )

    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        if not await is_pro_guild(self.guild_id):
            await interaction.response.send_message(
                "💎 **Polls are a Frost Scribe Pro feature.**",
                ephemeral=True,
            )
            return

        options = []
        for raw in str(self.options_text.value).splitlines():
            option = raw.strip().lstrip("•-").strip()
            if option and option.casefold() not in {item.casefold() for item in options}:
                options.append(option)

        if not 2 <= len(options) <= 10:
            await interaction.response.send_message(
                "Enter between **2 and 10 unique options**, one option per line.",
                ephemeral=True,
            )
            return

        if any(len(option) > 75 for option in options):
            await interaction.response.send_message(
                "Keep each poll option to **75 characters or fewer**.",
                ephemeral=True,
            )
            return

        poll_state = get_guild_poll_state(self.guild_id)
        poll_id = int(poll_state.get("next_poll_id", 1))
        poll_state["next_poll_id"] = poll_id + 1

        meeting = get_active_poll_session(self.guild_id)
        poll = {
            "id": poll_id,
            "question": str(self.question.value).strip(),
            "options": options,
            "votes": {},
            "open": True,
            "created_at": utcnow(),
            "closed_at": None,
            "created_by": interaction.user.id,
            "created_by_name": getattr(interaction.user, "display_name", interaction.user.name),
            "channel_id": interaction.channel.id,
            "message_id": None,
            "linked_meeting_name": meeting.get("name") if meeting else None,
            "linked_meeting_started_at": meeting.get("started_at") if meeting else None,
        }
        poll_state.setdefault("polls", {})[poll_id] = poll

        # If a meeting/attendance session is active, the same poll object is linked
        # into it so /record stop or /attendance stop includes it in the meeting XLSX.
        if meeting is not None:
            meeting.setdefault("polls", {})[poll_id] = poll

        # Avoid unbounded growth in the in-memory registry: retain all open polls and
        # the 100 most recent closed polls for this guild.
        all_polls = poll_state.get("polls", {})
        closed_ids = sorted(
            [pid for pid, item in all_polls.items() if not item.get("open")],
            reverse=True,
        )
        for stale_id in closed_ids[100:]:
            all_polls.pop(stale_id, None)

        await interaction.response.send_message(
            embed=build_poll_embed(poll),
            view=MeetingPollView(self.guild_id, poll_id, options),
        )
        try:
            message = await interaction.original_response()
            poll["message_id"] = message.id
        except Exception as e:
            print(f"Could not store poll message id: {type(e).__name__}: {e}")


async def _get_poll_for_command(interaction: discord.Interaction, poll_id: int | None, *, open_only=False):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command must be used inside a server.",
            ephemeral=True,
        )
        return None

    if not await is_pro_guild(interaction.guild.id):
        await interaction.response.send_message(
            "💎 **Polls are a Frost Scribe Pro feature.**",
            ephemeral=True,
        )
        return None

    state = get_guild_poll_state(interaction.guild.id)
    if poll_id is None:
        poll = _latest_guild_poll(interaction.guild.id, open_only=open_only)
    else:
        poll = state.get("polls", {}).get(poll_id)
        if open_only and poll is not None and not poll.get("open"):
            poll = None

    if poll is None:
        await interaction.response.send_message(
            "No matching poll was found in this server.",
            ephemeral=True,
        )
        return None

    return poll


@poll_group.command(name="create", description="Create a Pro named poll anywhere in this server")
@app_commands.checks.has_permissions(manage_guild=True)
async def poll_create(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command must be used inside a server.",
            ephemeral=True,
        )
        return

    if not await is_pro_guild(interaction.guild.id):
        await interaction.response.send_message(
            "💎 **Polls are a Frost Scribe Pro feature.**",
            ephemeral=True,
        )
        return

    await interaction.response.send_modal(PollCreateModal(interaction.guild.id))


@poll_group.command(name="status", description="View results for a Pro poll in this server")
@app_commands.describe(poll_id="Poll number; leave blank for the latest poll")
async def poll_status(interaction: discord.Interaction, poll_id: int | None = None):
    poll = await _get_poll_for_command(interaction, poll_id)
    if poll is None:
        return
    await interaction.response.send_message(
        embed=build_poll_embed(poll),
        ephemeral=True,
    )


@poll_group.command(name="close", description="Close a Pro poll and export its Excel voter report")
@app_commands.describe(poll_id="Poll number; leave blank for the latest open poll")
@app_commands.checks.has_permissions(manage_guild=True)
async def poll_close(interaction: discord.Interaction, poll_id: int | None = None):
    poll = await _get_poll_for_command(interaction, poll_id, open_only=True)
    if poll is None:
        return
    await close_meeting_poll(interaction.guild, poll)

    try:
        report_path = build_poll_excel_report(interaction.guild, poll)
        await interaction.response.send_message(
            f"✅ Poll #{poll['id']} closed with **{len(poll['votes'])}** respondent(s).\n"
            "📊 Named-voter Excel report attached for everyone in this channel.",
            file=discord.File(str(report_path), filename=report_path.name),
        )
    except Exception as e:
        await interaction.response.send_message(
            f"✅ Poll #{poll['id']} closed with **{len(poll['votes'])}** respondent(s), "
            "but the Excel report could not be generated.\n"
            f"`{type(e).__name__}: {e}`",
            ephemeral=True,
        )


@poll_group.command(name="report", description="Export or re-download a Pro poll Excel voter report")
@app_commands.describe(poll_id="Poll number; leave blank for the latest poll")
@app_commands.checks.has_permissions(manage_guild=True)
async def poll_report(interaction: discord.Interaction, poll_id: int | None = None):
    poll = await _get_poll_for_command(interaction, poll_id)
    if poll is None:
        return
    try:
        report_path = build_poll_excel_report(interaction.guild, poll)
        await interaction.response.send_message(
            f"📊 **Poll #{poll['id']} Excel report** — visible to everyone in this channel",
            file=discord.File(str(report_path), filename=report_path.name),
        )
    except Exception as e:
        await interaction.response.send_message(
            "❌ The poll report could not be generated.\n"
            f"`{type(e).__name__}: {e}`",
            ephemeral=True,
        )


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
        "Voice or Stage channel to record; leave blank to use your current channel"
    ),
)
async def meeting_start(
    interaction: discord.Interaction,
    name: str,
    channel: discord.VoiceChannel | discord.StageChannel | None = None,
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
            and isinstance(member.voice.channel, (discord.VoiceChannel, discord.StageChannel))
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
            if isinstance(default_voice, (discord.VoiceChannel, discord.StageChannel)):
                channel = default_voice
            else:
                await interaction.response.send_message(
                    "Join a voice or Stage channel, specify the `channel` option, "
                    "or configure a default voice/Stage channel with `/setup`.",
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

        def _recording_receive_finished(error):
            if error is not None:
                print(
                    f"Voice receive stopped with error for guild={guild_id} "
                    f"channel={channel.id}: {type(error).__name__}: {error}"
                )
                print(
                    "".join(
                        traceback.format_exception(
                            type(error), error, error.__traceback__
                        )
                    )
                )
            else:
                print(
                    f"Voice receive stopped normally for guild={guild_id} "
                    f"channel={channel.id}."
                )

        voice_client.listen(sink, after=_recording_receive_finished)

    except Exception as e:
        # Keep the full traceback in Railway logs. Stage-channel failures can
        # originate inside discord.py / discord-ext-voice-recv, and the short
        # Discord error alone is not enough to identify the failing layer.
        print(
            f"Recording voice connection failed for guild={guild_id} "
            f"channel={getattr(channel, 'id', None)} "
            f"type={type(channel).__name__}: {type(e).__name__}: {e}"
        )
        print(traceback.format_exc())
        sink.cleanup()
        await interaction.followup.send(
            f"❌ I could not join/record {channel.mention}.\n"
            f"`{type(e).__name__}: {e}`\n"
            "The full traceback has been written to the Railway logs."
        )
        return

    plan = await get_guild_plan(guild_id)
    limit_minutes = (
        PRO_RECORDING_LIMIT_MINUTES
        if plan == "PRO"
        else FREE_RECORDING_LIMIT_MINUTES
    )

    meeting = {
        "name": name,
        "channel_id": channel.id,
        "channel_name": channel.name,
        "started_at": started_at,
        "participants": {},
        "voice_client": voice_client,
        "audio_sink": sink,
        "recording_dir": recording_dir,
        "plan": plan,
        "session_mode": "recording",
        "limit_minutes": limit_minutes,
        "command_channel_id": interaction.channel.id,
        "limit_task": None,
        "polls": {},
        "next_poll_id": 1,
    }

    for member in channel.members:
        if not member.bot:
            start_session(meeting, member)

    active_meetings[guild_id] = meeting
    meeting["limit_task"] = asyncio.create_task(
        recording_limit_worker(guild_id)
    )

    ai_note = (
        "💎 **Pro AI enabled:** This recording will also be transcribed "
        "and summarized after `/record stop`.\n"
        "🗳️ **Pro Polls enabled:** `/poll create` works anywhere in the server; "
        "polls created while this meeting is active are also included in the final Excel report."
        if plan == "PRO"
        else
        f"❄️ **Free recording:** Audio and attendance are being captured for up to "
        f"**{FREE_RECORDING_LIMIT_MINUTES} minutes**.\n"
        f"💎 Upgrade to **Frost Scribe Pro** for up to "
        f"**{PRO_RECORDING_LIMIT_MINUTES} minutes** per recording, plus AI transcription "
        f"and meeting summaries."
    )

    await interaction.followup.send(
        f"🔴 **Recording started: {name}**\n"
        f"🎙️ Channel: {channel.mention}\n"
        f"👥 Already present: "
        f"{len([m for m in channel.members if not m.bot])}\n"
        f"⏱️ Recording limit: **{limit_minutes} minutes**\n\n"
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

    elapsed_seconds = (utcnow() - meeting["started_at"]).total_seconds()
    limit_seconds = meeting.get("limit_minutes", 0) * 60
    remaining_seconds = max(0, limit_seconds - elapsed_seconds) if limit_seconds else 0

    upgrade_note = (
        f"\n\n💎 **Free plan:** {FREE_RECORDING_LIMIT_MINUTES}-minute recording limit. "
        f"Upgrade to **Frost Scribe Pro** for up to "
        f"**{PRO_RECORDING_LIMIT_MINUTES} minutes** per recording + AI."
        if meeting.get("plan") != "PRO"
        else ""
    )

    await interaction.response.send_message(
        f"📋 **{meeting['name']}**\n"
        f"🎙️ Channel: <#{meeting['channel_id']}>\n"
        f"🔴 Recording: {'YES' if recording else 'NO'}\n"
        f"⏱️ Running: {format_duration(elapsed_seconds)}\n"
        f"⌛ Limit: **{meeting.get('limit_minutes', '?')} min** "
        f"({format_duration(remaining_seconds)} remaining)"
        f"\n\n{body}"
        f"{upgrade_note}"
    )


@record_group.command(
    name="stop",
    description="Stop recording and export attendance",
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

    await interaction.response.defer(ephemeral=True)

    result = await finalize_recording(
        interaction.guild,
        interaction.channel,
        interaction.guild.id,
        stop_reason="manual",
    )

    if result is None:
        await interaction.followup.send(
            "The recording had already ended.",
            ephemeral=True,
        )
        return

    output_channel = result["output_channel"]
    await interaction.followup.send(
        f"✅ Recording stopped. Results were posted in {output_channel.mention}.\n"
        "Temporary recording files have been deleted from Frost Scribe's "
        "Railway storage after processing.",
        ephemeral=True,
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
bot.tree.add_command(poll_group)

if not TOKEN:
    raise RuntimeError(
        "DISCORD_TOKEN is missing. Add it to Railway Variables."
    )

bot.run(TOKEN)
