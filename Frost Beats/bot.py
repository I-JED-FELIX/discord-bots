import os

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
import wavelink


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


def format_duration(milliseconds: int | None) -> str:
    if not milliseconds:
        return "Unknown"

    seconds = int(milliseconds / 1000)

    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours}:{minutes:02}:{seconds:02}"

    return f"{minutes}:{seconds:02}"


class WuffleBot(commands.Bot):

    def __init__(self):
        intents = discord.Intents.default()

        super().__init__(
            command_prefix="!",
            intents=intents
        )

    async def setup_hook(self):

        node = wavelink.Node(
            uri=LAVALINK_URI,
            password=LAVALINK_PASSWORD
        )

        await wavelink.Pool.connect(
            nodes=[node],
            client=self
        )

        synced = await self.tree.sync()

        print(f"Synced {len(synced)} global command(s)")

    async def on_ready(self):

        print(f"Wuffle Puffle logged in as {self.user}")
        print(f"Connected to {len(self.guilds)} server(s)")

    async def on_wavelink_node_ready(
        self,
        payload: wavelink.NodeReadyEventPayload
    ):

        print(
            f"Lavalink connected: "
            f"{payload.node.identifier}"
        )


bot = WuffleBot()


async def get_player(
    interaction: discord.Interaction
) -> wavelink.Player | None:

    if not interaction.guild:
        return None

    voice_client = interaction.guild.voice_client

    if isinstance(
        voice_client,
        wavelink.Player
    ):
        return voice_client

    return None


async def connect_to_user(
    interaction: discord.Interaction
) -> wavelink.Player | None:

    if not interaction.guild:
        return None

    if not isinstance(
        interaction.user,
        discord.Member
    ):
        return None

    if (
        not interaction.user.voice
        or not interaction.user.voice.channel
    ):
        return None

    channel = interaction.user.voice.channel

    player = await get_player(interaction)

    if player:

        if player.channel != channel:
            await player.move_to(channel)

        return player

    player = await channel.connect(
        cls=wavelink.Player
    )

    player.autoplay = (
        wavelink.AutoPlayMode.partial
    )

    return player


@bot.tree.command(
    name="ping",
    description="Check whether Wuffle Puffle is online."
)
async def ping(
    interaction: discord.Interaction
):

    latency = round(bot.latency * 1000)

    embed = discord.Embed(
        title="🐾 Wuffle Puffle",
        description=(
            "Online and ready! 🎵\n"
            f"Latency: **{latency} ms**"
        )
    )

    await interaction.response.send_message(
        embed=embed
    )


@bot.tree.command(
    name="join",
    description="Ask Wuffle Puffle to join your voice channel."
)
async def join(
    interaction: discord.Interaction
):

    player = await connect_to_user(
        interaction
    )

    if not player:

        await interaction.response.send_message(
            "🐾 Join a voice channel first.",
            ephemeral=True
        )

        return

    await interaction.response.send_message(
        f"🎧 Joined **{player.channel.name}**."
    )


@bot.tree.command(
    name="leave",
    description="Disconnect Wuffle Puffle from voice."
)
async def leave(
    interaction: discord.Interaction
):

    player = await get_player(interaction)

    if not player:

        await interaction.response.send_message(
            "I'm not connected to a voice channel.",
            ephemeral=True
        )

        return

    player.queue.clear()

    await player.disconnect()

    await interaction.response.send_message(
        "🐾 Wuffle Puffle disconnected."
    )


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
        not isinstance(
            interaction.user,
            discord.Member
        )
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

        player = await connect_to_user(
            interaction
        )

        if not player:

            await interaction.followup.send(
                "❌ I couldn't connect to your voice channel.",
                ephemeral=True
            )

            return

        results: wavelink.Search = (
            await wavelink.Playable.search(
                query
            )
        )

        if not results:

            await interaction.followup.send(
                "❌ I couldn't find anything for that search.",
                ephemeral=True
            )

            return

        if isinstance(
            results,
            wavelink.Playlist
        ):

            added = await player.queue.put_wait(
                results
            )

            embed = discord.Embed(
                title="🐾 Playlist Added",
                description=(
                    f"**{results.name}**\n"
                    f"Added **{added} tracks**."
                )
            )

        else:

            track = results[0]

            await player.queue.put_wait(
                track
            )

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
                value=format_duration(
                    track.length
                ),
                inline=True
            )

            embed.add_field(
                name="Requested by",
                value=interaction.user.mention,
                inline=True
            )

            if track.artwork:

                embed.set_thumbnail(
                    url=track.artwork
                )

        if not player.playing:

            next_track = player.queue.get()

            await player.play(
                next_track,
                volume=50
            )

        await interaction.followup.send(
            embed=embed
        )

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


@bot.tree.command(
    name="pause",
    description="Pause the current song."
)
async def pause(
    interaction: discord.Interaction
):

    player = await get_player(
        interaction
    )

    if not player or not player.playing:

        await interaction.response.send_message(
            "Nothing is currently playing.",
            ephemeral=True
        )

        return

    await player.pause(True)

    await interaction.response.send_message(
        "⏸️ Music paused."
    )


@bot.tree.command(
    name="resume",
    description="Resume the paused song."
)
async def resume(
    interaction: discord.Interaction
):

    player = await get_player(
        interaction
    )

    if not player or not player.paused:

        await interaction.response.send_message(
            "Nothing is currently paused.",
            ephemeral=True
        )

        return

    await player.pause(False)

    await interaction.response.send_message(
        "▶️ Music resumed."
    )


@bot.tree.command(
    name="skip",
    description="Skip the current song."
)
async def skip(
    interaction: discord.Interaction
):

    player = await get_player(
        interaction
    )

    if not player or not player.current:

        await interaction.response.send_message(
            "Nothing is currently playing.",
            ephemeral=True
        )

        return

    await player.skip(
        force=True
    )

    await interaction.response.send_message(
        "⏭️ Skipped."
    )


@bot.tree.command(
    name="stop",
    description="Stop playback and clear the queue."
)
async def stop(
    interaction: discord.Interaction
):

    player = await get_player(
        interaction
    )

    if not player:

        await interaction.response.send_message(
            "Nothing is currently playing.",
            ephemeral=True
        )

        return

    player.queue.clear()

    await player.skip(
        force=True
    )

    await interaction.response.send_message(
        "⏹️ Playback stopped and queue cleared."
    )


@bot.tree.command(
    name="nowplaying",
    description="Show the currently playing track."
)
async def nowplaying(
    interaction: discord.Interaction
):

    player = await get_player(
        interaction
    )

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
        value=format_duration(
            track.length
        ),
        inline=True
    )

    embed.add_field(
        name="Source",
        value=track.source or "Unknown",
        inline=True
    )

    if track.artwork:

        embed.set_thumbnail(
            url=track.artwork
        )

    await interaction.response.send_message(
        embed=embed
    )


@bot.tree.command(
    name="queue",
    description="Show the current music queue."
)
async def show_queue(
    interaction: discord.Interaction
):

    player = await get_player(
        interaction
    )

    if not player:

        await interaction.response.send_message(
            "🎵 The queue is empty."
        )

        return

    current = player.current

    queued_tracks = list(
        player.queue
    )

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

        lines.append(
            "**Up Next:**"
        )

        for index, track in enumerate(
            queued_tracks[:10],
            start=1
        ):

            lines.append(
                f"`{index}.` "
                f"{track.title}"
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

    await interaction.response.send_message(
        embed=embed
    )


bot.run(TOKEN)
