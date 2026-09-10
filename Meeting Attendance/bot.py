import os
import csv
import asyncio
import threading
import wave
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from pathlib import Path

import asyncpg
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

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

RECORDINGS_DIR = DATA_DIR / "recordings"
RECORDINGS_DIR.mkdir(exist_ok=True)

intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

# One active meeting per Discord server.
active_meetings = {}

# PostgreSQL connection pool. Railway supplies DATABASE_URL.
db_pool = None


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

    print("PostgreSQL connected.")
    print("Database tables ready: guilds, scheduled_meetings")


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

    except Exception as e:
        print(f"Database initialization failed: {type(e).__name__}: {e}")
        return

    try:
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            print(
                f"Synced {len(synced)} commands to guild {GUILD_ID}"
            )
        else:
            synced = await bot.tree.sync()
            print(f"Synced {len(synced)} global commands")
    except Exception as e:
        print(f"Command sync failed: {e}")

    print(f"Logged in as {bot.user} ({bot.user.id})")


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

@schedule_group.command(name="create", description="Schedule a meeting with 1-hour and 30-minute reminders")
@app_commands.describe(name="Meeting name", date="YYYY-MM-DD", time="24-hour HH:MM", timezone_name="Example: Asia/Kolkata", reminder_channel="Channel for reminders", tags="Role/user mentions")
@app_commands.checks.has_permissions(manage_guild=True)
async def schedule_create(interaction: discord.Interaction, name: str, date: str, time: str, timezone_name: str, reminder_channel: discord.TextChannel, tags: str = ""):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command inside a server.", ephemeral=True); return
    try:
        scheduled_at = parse_scheduled_time(date, time, timezone_name)
    except ValueError as e:
        await interaction.response.send_message(f"❌ {e}", ephemeral=True); return
    if scheduled_at <= utcnow():
        await interaction.response.send_message("❌ Meeting time must be in the future.", ephemeral=True); return
    meeting_id = await db_pool.fetchval("""
        INSERT INTO scheduled_meetings
        (guild_id,name,scheduled_at,reminder_channel_id,tag_text,created_by)
        VALUES($1,$2,$3,$4,$5,$6) RETURNING id
    """, interaction.guild.id, name.strip(), scheduled_at, reminder_channel.id, tags.strip(), interaction.user.id)
    await interaction.response.send_message(
        f"📅 **Meeting scheduled — #{meeting_id}**\n**{name.strip()}**\n"
        f"🕒 {discord_time(scheduled_at)} ({discord_time(scheduled_at,'R')})\n"
        f"📣 Reminders: **1 hour** and **30 minutes** before\n"
        f"💬 {reminder_channel.mention}\n🏷️ {tags.strip() or 'None'}"
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
            if await send_schedule_reminder(row,"30 minutes"):
                await db_pool.execute("UPDATE scheduled_meetings SET reminder_1h_sent=TRUE, reminder_30m_sent=TRUE WHERE id=$1",row["id"])
        elif due60:
            if await send_schedule_reminder(row,"1 hour"):
                await db_pool.execute("UPDATE scheduled_meetings SET reminder_1h_sent=TRUE WHERE id=$1",row["id"])

@scheduled_reminder_worker.before_loop
async def before_scheduled_reminder_worker():
    await bot.wait_until_ready()


meeting_group = app_commands.Group(
    name="meeting",
    description=(
        "Voice meeting attendance, recording, transcription and summaries"
    ),
)


@meeting_group.command(
    name="start",
    description="Start attendance tracking and voice recording",
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

    if guild_id in active_meetings:
        current = active_meetings[guild_id]
        await interaction.response.send_message(
            f"A meeting is already active: **{current['name']}** "
            f"in <#{current['channel_id']}>.\n"
            "End it first with `/meeting end`.",
            ephemeral=True,
        )
        return

    if channel is None:
        member = interaction.guild.get_member(interaction.user.id)

        if (
            member is None
            or member.voice is None
            or member.voice.channel is None
        ):
            await interaction.response.send_message(
                "Join the voice channel first, or specify the "
                "`channel` option.",
                ephemeral=True,
            )
            return

        if not isinstance(member.voice.channel, discord.VoiceChannel):
            await interaction.response.send_message(
                "Please use a standard Discord voice channel.",
                ephemeral=True,
            )
            return

        channel = member.voice.channel

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

    await interaction.followup.send(
        f"🔴 **Meeting recording started: {name}**\n"
        f"🎙️ Channel: {channel.mention}\n"
        f"👥 Already present: "
        f"{len([m for m in channel.members if not m.bot])}\n\n"
        "⚠️ **Recording notice:** Audio in this voice channel is "
        "being recorded, transcribed, and summarized. Everyone "
        "present should be informed and consent before continuing."
    )


@meeting_group.command(
    name="status",
    description="Show attendance and recording status",
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


@meeting_group.command(
    name="end",
    description=(
        "End meeting, export attendance, transcribe and summarize"
    ),
)
async def meeting_end(interaction: discord.Interaction):
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

    await interaction.followup.send(
        f"🏁 **Meeting ended: {meeting['name']}**\n"
        f"🎙️ Channel: <#{meeting['channel_id']}>\n"
        f"⏱️ Meeting length: "
        f"{format_duration(meeting_seconds)}\n\n"
        f"{attendance_summary}\n\n"
        "⏳ Audio recording stopped. I am now transcribing "
        "the meeting and generating the AI summary.",
        file=discord.File(attendance_path),
    )

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

        await interaction.channel.send(
            f"📝 **AI Meeting Summary — {meeting['name']}**\n\n"
            f"{preview}",
            files=attachments,
        )

    except Exception as e:
        await interaction.channel.send(
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

    meeting = active_meetings.get(member.guild.id)
    if not meeting:
        return

    tracked_channel_id = meeting["channel_id"]

    before_id = (
        before.channel.id if before.channel else None
    )
    after_id = (
        after.channel.id if after.channel else None
    )

    if (
        before_id != tracked_channel_id
        and after_id == tracked_channel_id
    ):
        start_session(meeting, member)
        return

    if (
        before_id == tracked_channel_id
        and after_id != tracked_channel_id
    ):
        end_session(meeting, member)
        return


bot.tree.add_command(schedule_group)
bot.tree.add_command(meeting_group)

if not TOKEN:
    raise RuntimeError(
        "DISCORD_TOKEN is missing. Add it to Railway Variables."
    )

bot.run(TOKEN)
