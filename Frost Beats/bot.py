import os
import re

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
import lavalink
from lavalink.events import TrackStartEvent, QueueEndEvent
from lavalink.server import LoadType


load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
LAVALINK_URI = os.getenv("LAVALINK_URI")
LAVALINK_PASSWORD = os.getenv("LAVALINK_PASSWORD")

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is missing.")

if not LAVALINK_URI:
    raise RuntimeError("LAVALINK_URI is missing.")

if not LAVALINK_PASSWORD:
    raise RuntimeError("LAVALINK_PASSWORD is missing.")


# ---------------------------------------------------------
# Lavalink connection details
# ---------------------------------------------------------

uri = LAVALINK_URI.replace("http://", "").replace("https://", "")
host_port = uri.split(":", 1)

LAVALINK_HOST = host_port[0]
LAVALINK_PORT = int(host_port[1]) if len(host_port) > 1 else 2333

URL_REGEX = re.compile(r"^https?://", re.IGNORECASE)


def format_duration(milliseconds):
    if not milliseconds:
        return "Unknown"

    seconds = int(milliseconds / 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours}:{minutes:02}:{seconds:02}"

    return f"{minutes}:{seconds:02}"


# ---------------------------------------------------------
# Discord <-> Lavalink Voice Protocol
# ---------------------------------------------------------

class LavalinkVoiceClient(discord.VoiceProtocol):

    def __init__(self, client, channel):
        self.client = client
        self.channel = channel

        self.guild_id = channel.guild.id
        self._destroyed = False

        if not hasattr(client, "lavalink"):
            raise RuntimeError("Lavalink client is not initialized.")

        self.lavalink = client.lavalink

    async def on_voice_server_update(self, data):
        lavalink_data = {
            "t": "VOICE_SERVER_UPDATE",
            "d": data
        }

        await self.lavalink.voice_update_handler(lavalink_data)

    async def on_voice_state_update(self, data):
        channel_id = data.get("channel_id")

        if not channel_id:
            await self._destroy()
            return

        self.channel = self.client.get_channel(int(channel_id))

        lavalink_data = {
            "t": "VOICE_STATE_UPDATE",
            "d": data
        }

        await self.lavalink.voice_update_handler(lavalink_data)

    async def connect(
        self,
        *,
        timeout,
        reconnect,
        self_deaf=False,
        self_mute=False
    ):
        self.lavalink.player_manager.create(self.guild_id)

        await self.channel.guild.change_voice_state(
            channel=self.channel,
            self_deaf=self_deaf,
            self_mute=self_mute
        )

    async def disconnect(self, *, force=False):
        player = self.lavalink.player_manager.get(self.guild_id)

        if player:
            player.queue.clear()
            await player.stop()

        await self.channel.guild.change_voice_state(channel=None)

        await self._destroy()

    async def move_to(self, channel):
        await self.channel.guild.change_voice_state(channel=channel)
        self.channel = channel

    async def _destroy(self):
        self.cleanup()

        if self._destroyed:
            return

        self._destroyed = True

        try:
            await self.lavalink.player_manager.destroy(self.guild_id)
        except Exception:
            pass


# ---------------------------------------------------------
# Bot
# ---------------------------------------------------------

class WuffleBot(commands.Bot):

    def __init__(self):
        intents = discord.Intents.default()
        intents.voice_states = True

        super().__init__(
            command_prefix="!",
            intents=intents
        )

        self.lavalink = lavalink.Client(0)

        self.lavalink.add_node(
            host=LAVALINK_HOST,
            port=LAVALINK_PORT,
            password=LAVALINK_PASSWORD,
            region="us",
            name="wuffle-main"
        )

        self.lavalink.add_event_hooks(self)

    async def setup_hook(self):
        synced = await self.tree.sync()
        print(f"Synced {len(synced)} global command(s)")

    async def on_ready(self):
        # Lavalink.py needs the actual Discord bot user ID.
        if self.lavalink.user_id != self.user.id:
            self.lavalink.user_id = self.user.id

        print(f"Wuffle Puffle logged in as {self.user}")
        print(f"Connected to {len(self.guilds)} server(s)")
        print(
            f"Lavalink target: "
            f"{LAVALINK_HOST}:{LAVALINK_PORT}"
        )

    @lavalink.listener(TrackStartEvent)
    async def on_lavalink_track_start(self, event):
        print(
            f"Now playing: {event.track.title} "
            f"in guild {event.player.guild_id}"
        )

    @lavalink.listener(QueueEndEvent)
    async def on_lavalink_queue_end(self, event):
        print(
            f"Queue finished in guild "
            f"{event.player.guild_id}"
        )


bot = WuffleBot()


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def get_player(interaction):
    if not interaction.guild:
        return None

    return bot.lavalink.player_manager.get(
        interaction.guild.id
    )


async def connect_to_user(interaction):
    if not interaction.guild:
        return None

    if not isinstance(interaction.user, discord.Member):
        return None

    if (
        not interaction.user.voice
        or not interaction.user.voice.channel
    ):
        return None

    channel = interaction.user.voice.channel
    voice_client = interaction.guild.voice_client

    player = bot.lavalink.player_manager.create(
        interaction.guild.id
    )

    if voice_client:
        if voice_client.channel.id != channel.id:
            await voice_client.move_to(channel)

        return player

    await channel.connect(
        cls=LavalinkVoiceClient,
        timeout=30.0,
        reconnect=True
    )

    return player


# ---------------------------------------------------------
# /ping
# ---------------------------------------------------------

@bot.tree.command(
    name="ping",
    description="Check whether Wuffle Puffle is online."
)
async def ping(interaction: discord.Interaction):

    latency = round(bot.latency * 1000)

    embed = discord.Embed(
        title="🐾 Wuffle Puffle",
        description=(
            "Online and ready! 🎵\n"
            f"Latency: **{latency} ms**"
        )
    )

    await interaction.response.send_message(embed=embed)


# ---------------------------------------------------------
# /join
# ---------------------------------------------------------

@bot.tree.command(
    name="join",
    description="Ask Wuffle Puffle to join your voice channel."
)
async def join(interaction: discord.Interaction):

    player = await connect_to_user(interaction)

    if not player:
        await interaction.response.send_message(
            "🐾 Join a voice channel first.",
            ephemeral=True
        )
        return

    channel = interaction.user.voice.channel

    await interaction.response.send_message(
        f"🎧 Joined **{channel.name}**."
    )


# ---------------------------------------------------------
# /leave
# ---------------------------------------------------------

@bot.tree.command(
    name="leave",
    description="Disconnect Wuffle Puffle from voice."
)
async def leave(interaction: discord.Interaction):

    if not interaction.guild or not interaction.guild.voice_client:
        await interaction.response.send_message(
            "I'm not connected to a voice channel.",
            ephemeral=True
        )
        return

    await interaction.guild.voice_client.disconnect(force=True)

    await interaction.response.send_message(
        "🐾 Wuffle Puffle disconnected."
    )


# ---------------------------------------------------------
# /play
# ---------------------------------------------------------

@bot.tree.command(
    name="play",
    description="Play a song, playlist, album, Spotify link or search."
)
@app_commands.describe(
    query="Song name, YouTube URL, Spotify URL, playlist or album"
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
            "🐾 Join a voice channel first.",
            ephemeral=True
        )
        return

    await interaction.response.defer()

    try:
        player = await connect_to_user(interaction)

        if not player:
            await interaction.followup.send(
                "❌ I couldn't connect to your voice channel.",
                ephemeral=True
            )
            return

        query = query.strip("<>")

        # Plain text searches use the official YouTube plugin.
        # Spotify/YouTube URLs are sent directly to Lavalink/LavaSrc.
        if not URL_REGEX.match(query):
            query = f"ytsearch:{query}"

        results = await player.node.get_tracks(query)

        if (
            results.load_type == LoadType.EMPTY
            or not results.tracks
        ):
            await interaction.followup.send(
                "❌ I couldn't find anything for that search.",
                ephemeral=True
            )
            return

        if results.load_type == LoadType.ERROR:
            error_message = "Unknown Lavalink error"

            if results.error:
                error_message = str(results.error)

            print(f"/play Lavalink error: {error_message}")

            await interaction.followup.send(
                "❌ Lavalink couldn't load that track.",
                ephemeral=True
            )
            return

        if results.load_type == LoadType.PLAYLIST:
            tracks = results.tracks

            for track in tracks:
                track.extra["requester"] = interaction.user.id
                player.add(track=track)

            playlist_name = (
                results.playlist_info.name
                if results.playlist_info
                else "Playlist"
            )

            embed = discord.Embed(
                title="🐾 Playlist Added",
                description=(
                    f"**{playlist_name}**\n"
                    f"Added **{len(tracks)} tracks**."
                )
            )

        else:
            track = results.tracks[0]

            track.extra["requester"] = interaction.user.id
            player.add(track=track)

            embed = discord.Embed(
                title="🎵 Added to Queue",
                description=f"**{track.title}**"
            )

            embed.add_field(
                name="Artist",
                value=track.author or "Unknown",
                inline=True
            )

            embed.add_field(
                name="Duration",
                value=format_duration(track.duration),
                inline=True
            )

            embed.add_field(
                name="Requested by",
                value=interaction.user.mention,
                inline=True
            )

            artwork = getattr(track, "artwork_url", None)

            if artwork:
                embed.set_thumbnail(url=artwork)

        if not player.is_playing:
            await player.play()

        await interaction.followup.send(embed=embed)

    except Exception as e:
        print(
            f"/play error: "
            f"{type(e).__name__}: {e}"
        )

        await interaction.followup.send(
            "❌ I couldn't load that track. "
            "Check the Railway logs for the exact error.",
            ephemeral=True
        )


# ---------------------------------------------------------
# /pause
# ---------------------------------------------------------

@bot.tree.command(
    name="pause",
    description="Pause the current song."
)
async def pause(interaction: discord.Interaction):

    player = get_player(interaction)

    if not player or not player.is_playing:
        await interaction.response.send_message(
            "Nothing is currently playing.",
            ephemeral=True
        )
        return

    await player.set_pause(True)

    await interaction.response.send_message(
        "⏸️ Music paused."
    )


# ---------------------------------------------------------
# /resume
# ---------------------------------------------------------

@bot.tree.command(
    name="resume",
    description="Resume the paused song."
)
async def resume(interaction: discord.Interaction):

    player = get_player(interaction)

    if not player or not player.paused:
        await interaction.response.send_message(
            "Nothing is currently paused.",
            ephemeral=True
        )
        return

    await player.set_pause(False)

    await interaction.response.send_message(
        "▶️ Music resumed."
    )


# ---------------------------------------------------------
# /skip
# ---------------------------------------------------------

@bot.tree.command(
    name="skip",
    description="Skip the current song."
)
async def skip(interaction: discord.Interaction):

    player = get_player(interaction)

    if not player or not player.current:
        await interaction.response.send_message(
            "Nothing is currently playing.",
            ephemeral=True
        )
        return

    await player.skip()

    await interaction.response.send_message(
        "⏭️ Skipped."
    )


# ---------------------------------------------------------
# /stop
# ---------------------------------------------------------

@bot.tree.command(
    name="stop",
    description="Stop playback and clear the queue."
)
async def stop(interaction: discord.Interaction):

    player = get_player(interaction)

    if not player:
        await interaction.response.send_message(
            "Nothing is currently playing.",
            ephemeral=True
        )
        return

    player.queue.clear()
    await player.stop()

    await interaction.response.send_message(
        "⏹️ Playback stopped and queue cleared."
    )


# ---------------------------------------------------------
# /nowplaying
# ---------------------------------------------------------

@bot.tree.command(
    name="nowplaying",
    description="Show the currently playing track."
)
async def nowplaying(interaction: discord.Interaction):

    player = get_player(interaction)

    if not player or not player.current:
        await interaction.response.send_message(
            "Nothing is currently playing.",
            ephemeral=True
        )
        return

    track = player.current

    embed = discord.Embed(
        title="🎧 Now Playing",
        description=f"**{track.title}**"
    )

    embed.add_field(
        name="Artist",
        value=track.author or "Unknown",
        inline=True
    )

    embed.add_field(
        name="Duration",
        value=format_duration(track.duration),
        inline=True
    )

    source = getattr(track, "source_name", None)

    embed.add_field(
        name="Source",
        value=source or "Unknown",
        inline=True
    )

    artwork = getattr(track, "artwork_url", None)

    if artwork:
        embed.set_thumbnail(url=artwork)

    await interaction.response.send_message(embed=embed)


# ---------------------------------------------------------
# /queue
# ---------------------------------------------------------

@bot.tree.command(
    name="queue",
    description="Show the current music queue."
)
async def show_queue(interaction: discord.Interaction):

    player = get_player(interaction)

    if not player:
        await interaction.response.send_message(
            "🎵 The queue is empty."
        )
        return

    current = player.current
    queued_tracks = list(player.queue)

    if not current and not queued_tracks:
        await interaction.response.send_message(
            "🎵 The queue is empty."
        )
        return

    lines = []

    if current:
        lines.append(
            f"**Now Playing:**\n"
            f"{current.title}\n"
        )

    if queued_tracks:
        lines.append("**Up Next:**")

        for index, track in enumerate(
            queued_tracks[:10],
            start=1
        ):
            lines.append(
                f"`{index}.` {track.title}"
            )

        if len(queued_tracks) > 10:
            lines.append(
                f"\n…and "
                f"**{len(queued_tracks) - 10}** more."
            )

    embed = discord.Embed(
        title="🐾 Wuffle Puffle Queue",
        description="\n".join(lines)
    )

    await interaction.response.send_message(embed=embed)


bot.run(TOKEN)
