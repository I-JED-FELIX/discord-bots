import os
import asyncio

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
import yt_dlp

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is missing.")

intents = discord.Intents.default()

bot = commands.Bot(
    command_prefix="!",
    intents=intents
)

# ============================================================
# FROST BEATS MUSIC STATE
# ============================================================

queues = {}
current_tracks = {}

YTDL_OPTIONS = {
    "format": "bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "default_search": "ytsearch1",
    "source_address": "0.0.0.0",
}

FFMPEG_OPTIONS = {
    "before_options": "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5",
    "options": "-vn",
}

ytdl = yt_dlp.YoutubeDL(YTDL_OPTIONS)


# ============================================================
# HELPERS
# ============================================================

async def get_track(query: str):
    loop = asyncio.get_running_loop()

    def extract():
        return ytdl.extract_info(query, download=False)

    data = await loop.run_in_executor(None, extract)

    if "entries" in data:
        entries = [entry for entry in data["entries"] if entry]
        if not entries:
            raise RuntimeError("No tracks found.")
        data = entries[0]

    return {
        "title": data.get("title", "Unknown Track"),
        "url": data.get("url"),
        "webpage_url": data.get("webpage_url", query),
        "thumbnail": data.get("thumbnail"),
        "duration": data.get("duration"),
    }


async def connect_to_user(interaction: discord.Interaction):
    if not isinstance(interaction.user, discord.Member):
        return None

    if not interaction.user.voice or not interaction.user.voice.channel:
        return None

    channel = interaction.user.voice.channel

    voice_client = interaction.guild.voice_client

    if voice_client:
        if voice_client.channel != channel:
            await voice_client.move_to(channel)
    else:
        voice_client = await channel.connect()

    return voice_client


def format_duration(seconds):
    if not seconds:
        return "Unknown"

    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours}:{minutes:02}:{seconds:02}"

    return f"{minutes}:{seconds:02}"


async def play_next(guild: discord.Guild):

    guild_id = guild.id

    queue = queues.get(guild_id, [])

    if not queue:
        current_tracks.pop(guild_id, None)
        return

    voice_client = guild.voice_client

    if not voice_client or not voice_client.is_connected():
        current_tracks.pop(guild_id, None)
        return

    track = queue.pop(0)
    current_tracks[guild_id] = track

    try:
        source = discord.FFmpegPCMAudio(
            track["url"],
            **FFMPEG_OPTIONS
        )

        def after_playing(error):
            if error:
                print(f"Playback error: {error}")

            asyncio.run_coroutine_threadsafe(
                play_next(guild),
                bot.loop
            )

        voice_client.play(
            source,
            after=after_playing
        )

    except Exception as e:
        print(f"Failed to play track: {e}")

        asyncio.run_coroutine_threadsafe(
            play_next(guild),
            bot.loop
        )


# ============================================================
# BOT EVENTS
# ============================================================

@bot.event
async def on_ready():
    print(f"Frost Beats logged in as {bot.user}")
    print(f"Connected to {len(bot.guilds)} server(s)")


@bot.event
async def setup_hook():
    synced = await bot.tree.sync()
    print(f"Synced {len(synced)} global command(s)")


# ============================================================
# BASIC COMMANDS
# ============================================================

@bot.tree.command(
    name="ping",
    description="Check whether Frost Beats is online."
)
async def ping(interaction: discord.Interaction):

    latency = round(bot.latency * 1000)

    embed = discord.Embed(
        title="❄️ Frost Beats",
        description=(
            "Online and ready! 🎵\n"
            f"Latency: **{latency} ms**"
        )
    )

    await interaction.response.send_message(embed=embed)


@bot.tree.command(
    name="join",
    description="Ask Frost Beats to join your voice channel."
)
async def join(interaction: discord.Interaction):

    voice_client = await connect_to_user(interaction)

    if not voice_client:
        await interaction.response.send_message(
            "❄️ Join a voice channel first.",
            ephemeral=True
        )
        return

    await interaction.response.send_message(
        f"🎧 Joined **{voice_client.channel.name}**."
    )


@bot.tree.command(
    name="leave",
    description="Disconnect Frost Beats from voice."
)
async def leave(interaction: discord.Interaction):

    voice_client = (
        interaction.guild.voice_client
        if interaction.guild
        else None
    )

    if not voice_client:
        await interaction.response.send_message(
            "I'm not connected to a voice channel.",
            ephemeral=True
        )
        return

    if voice_client.is_playing():
        voice_client.stop()

    queues.pop(interaction.guild.id, None)
    current_tracks.pop(interaction.guild.id, None)

    await voice_client.disconnect()

    await interaction.response.send_message(
        "❄️ Frost Beats disconnected."
    )


# ============================================================
# PLAY
# ============================================================

@bot.tree.command(
    name="play",
    description="Play a song or add it to the queue."
)
@app_commands.describe(
    query="Song name or URL"
)
async def play(
    interaction: discord.Interaction,
    query: str
):

    if not interaction.guild:
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True
        )
        return

    if (
        not isinstance(interaction.user, discord.Member)
        or not interaction.user.voice
        or not interaction.user.voice.channel
    ):
        await interaction.response.send_message(
            "❄️ Join a voice channel first.",
            ephemeral=True
        )
        return

    # Extraction can take several seconds.
    await interaction.response.defer()

    try:
        voice_client = await connect_to_user(interaction)

        track = await get_track(query)

        if not track["url"]:
            raise RuntimeError("No playable audio stream found.")

        guild_id = interaction.guild.id

        queues.setdefault(guild_id, [])

        track["requested_by"] = interaction.user.display_name

        voice_client = interaction.guild.voice_client

        # Nothing playing -> start immediately
        if (
            not voice_client.is_playing()
            and not voice_client.is_paused()
            and guild_id not in current_tracks
        ):
            queues[guild_id].append(track)

            await play_next(interaction.guild)

            embed = discord.Embed(
                title="🎵 Now Playing",
                description=f"**{track['title']}**"
            )

        else:
            queues[guild_id].append(track)

            embed = discord.Embed(
                title="❄️ Added to Queue",
                description=f"**{track['title']}**"
            )

            embed.add_field(
                name="Position",
                value=str(len(queues[guild_id]))
            )

        embed.add_field(
            name="Duration",
            value=format_duration(track["duration"])
        )

        embed.add_field(
            name="Requested by",
            value=interaction.user.mention
        )

        if track["thumbnail"]:
            embed.set_thumbnail(url=track["thumbnail"])

        await interaction.followup.send(embed=embed)

    except Exception as e:

        print(f"/play error: {type(e).__name__}: {e}")

        await interaction.followup.send(
            "❌ I couldn't load that track. Check the Railway logs for the exact error.",
            ephemeral=True
        )


# ============================================================
# PAUSE / RESUME
# ============================================================

@bot.tree.command(
    name="pause",
    description="Pause the current song."
)
async def pause(interaction: discord.Interaction):

    voice_client = interaction.guild.voice_client if interaction.guild else None

    if not voice_client or not voice_client.is_playing():
        await interaction.response.send_message(
            "Nothing is currently playing.",
            ephemeral=True
        )
        return

    voice_client.pause()

    await interaction.response.send_message(
        "⏸️ Music paused."
    )


@bot.tree.command(
    name="resume",
    description="Resume the paused song."
)
async def resume(interaction: discord.Interaction):

    voice_client = interaction.guild.voice_client if interaction.guild else None

    if not voice_client or not voice_client.is_paused():
        await interaction.response.send_message(
            "Nothing is currently paused.",
            ephemeral=True
        )
        return

    voice_client.resume()

    await interaction.response.send_message(
        "▶️ Music resumed."
    )


# ============================================================
# SKIP
# ============================================================

@bot.tree.command(
    name="skip",
    description="Skip the current song."
)
async def skip(interaction: discord.Interaction):

    voice_client = interaction.guild.voice_client if interaction.guild else None

    if not voice_client or not (
        voice_client.is_playing()
        or voice_client.is_paused()
    ):
        await interaction.response.send_message(
            "Nothing is currently playing.",
            ephemeral=True
        )
        return

    voice_client.stop()

    await interaction.response.send_message(
        "⏭️ Skipped."
    )


# ============================================================
# STOP
# ============================================================

@bot.tree.command(
    name="stop",
    description="Stop playback and clear the queue."
)
async def stop(interaction: discord.Interaction):

    if not interaction.guild:
        return

    voice_client = interaction.guild.voice_client

    queues[interaction.guild.id] = []
    current_tracks.pop(interaction.guild.id, None)

    if voice_client and (
        voice_client.is_playing()
        or voice_client.is_paused()
    ):
        voice_client.stop()

    await interaction.response.send_message(
        "⏹️ Playback stopped and queue cleared."
    )


# ============================================================
# NOW PLAYING
# ============================================================

@bot.tree.command(
    name="nowplaying",
    description="Show the currently playing track."
)
async def nowplaying(interaction: discord.Interaction):

    if not interaction.guild:
        return

    track = current_tracks.get(interaction.guild.id)

    if not track:
        await interaction.response.send_message(
            "Nothing is currently playing.",
            ephemeral=True
        )
        return

    embed = discord.Embed(
        title="🎧 Now Playing",
        description=f"**{track['title']}**"
    )

    embed.add_field(
        name="Duration",
        value=format_duration(track["duration"])
    )

    embed.add_field(
        name="Requested by",
        value=track.get("requested_by", "Unknown")
    )

    if track["thumbnail"]:
        embed.set_thumbnail(url=track["thumbnail"])

    await interaction.response.send_message(embed=embed)


# ============================================================
# QUEUE
# ============================================================

@bot.tree.command(
    name="queue",
    description="Show the current music queue."
)
async def show_queue(interaction: discord.Interaction):

    if not interaction.guild:
        return

    guild_id = interaction.guild.id

    current = current_tracks.get(guild_id)
    queue = queues.get(guild_id, [])

    if not current and not queue:
        await interaction.response.send_message(
            "🎵 The queue is empty."
        )
        return

    lines = []

    if current:
        lines.append(
            f"**Now Playing:**\n{current['title']}\n"
        )

    if queue:
        lines.append("**Up Next:**")

        for index, track in enumerate(queue[:10], start=1):
            lines.append(
                f"`{index}.` {track['title']}"
            )

        if len(queue) > 10:
            lines.append(
                f"\n…and **{len(queue) - 10}** more."
            )

    embed = discord.Embed(
        title="❄️ Frost Beats Queue",
        description="\n".join(lines)
    )

    await interaction.response.send_message(embed=embed)


bot.run(TOKEN)
