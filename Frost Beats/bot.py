import os

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is missing.")

intents = discord.Intents.default()

bot = commands.Bot(
    command_prefix="!",
    intents=intents
)


@bot.event
async def on_ready():
    print(f"Frost Beats logged in as {bot.user}")
    print(f"Connected to {len(bot.guilds)} server(s)")


@bot.event
async def setup_hook():
    synced = await bot.tree.sync()
    print(f"Synced {len(synced)} global command(s)")


@bot.tree.command(
    name="ping",
    description="Check whether Frost Beats is online."
)
async def ping(interaction: discord.Interaction):
    latency = round(bot.latency * 1000)

    embed = discord.Embed(
        title="❄️ Frost Beats",
        description=f"Online and ready! 🎵\nLatency: **{latency} ms**"
    )

    await interaction.response.send_message(embed=embed)


@bot.tree.command(
    name="join",
    description="Ask Frost Beats to join your voice channel."
)
async def join(interaction: discord.Interaction):

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message(
            "This command can only be used inside a server.",
            ephemeral=True
        )
        return

    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message(
            "❄️ Join a voice channel first.",
            ephemeral=True
        )
        return

    channel = interaction.user.voice.channel

    if interaction.guild.voice_client:
        await interaction.guild.voice_client.move_to(channel)
    else:
        await channel.connect()

    await interaction.response.send_message(
        f"🎧 Joined **{channel.name}**."
    )


@bot.tree.command(
    name="leave",
    description="Disconnect Frost Beats from voice."
)
async def leave(interaction: discord.Interaction):

    voice_client = interaction.guild.voice_client if interaction.guild else None

    if not voice_client:
        await interaction.response.send_message(
            "I'm not connected to a voice channel.",
            ephemeral=True
        )
        return

    await voice_client.disconnect()

    await interaction.response.send_message(
        "❄️ Frost Beats disconnected."
    )


bot.run(TOKEN)
