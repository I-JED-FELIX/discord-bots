import os
import csv
import asyncio
import threading
import wave
from datetime import datetime, timezone
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands, voice_recv
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = os.getenv("GUILD_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

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


bot.tree.add_command(meeting_group)

if not TOKEN:
    raise RuntimeError(
        "DISCORD_TOKEN is missing. Add it to Railway Variables."
    )

bot.run(TOKEN)
