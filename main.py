import asyncio
import os
import random
from datetime import datetime, timezone

import discord
from discord.ext import commands

TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise RuntimeError("Missing TOKEN environment variable")

intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True

bot = commands.Bot(command_prefix="!", intents=intents)

TIME_MULTIPLIERS = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
}


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


@bot.command()
@commands.has_guild_permissions(manage_guild=True)
async def host(ctx: commands.Context, *args: str):
    if len(args) < 3:
        await ctx.send(
            "✨ **Giveaway Setup**\n"
            "Use: `!host <prize text> <winner_amount> <time>`\n"
            "Example: `!host Nitro 2 1h`\n"
            "⏳ Time format: `s`, `m`, `h`, `d`"
        )
        return

    time_str = args[-1]
    winners_str = args[-2]
    prize = " ".join(args[:-2]).strip()

    if not prize:
        await ctx.send("📝 Please include a prize description before the winner count.")
        return

    if not winners_str.isdigit():
        await ctx.send("🎯 Winner amount must be a whole number (1 or more).")
        return

    winner_amount = int(winners_str)
    if winner_amount < 1:
        await ctx.send("🎯 Winner amount must be at least 1.")
        return

    try:
        duration = parse_duration(time_str)
    except ValueError as exc:
        await ctx.send(f"⛔ Invalid time format: {exc}")
        return

    end_timestamp = int(datetime.now(timezone.utc).timestamp()) + duration

    embed = discord.Embed(
        title="🎁✨ Giveaway Time!",
        color=discord.Color.purple(),
        description="React with 🎉 to enter!",
    )
    embed.add_field(name="🏆 Prize", value=prize, inline=False)
    embed.add_field(name="👥 Winners", value=str(winner_amount), inline=True)
    embed.add_field(name="⏰ Ends", value=f"<t:{end_timestamp}:R>", inline=True)
    embed.set_footer(text=f"Hosted by {ctx.author.display_name}")

    giveaway_message = await ctx.send(embed=embed)
    await giveaway_message.add_reaction("🎉")

    await asyncio.sleep(duration)

    try:
        refreshed_message = await ctx.channel.fetch_message(giveaway_message.id)
    except discord.NotFound:
        return
    except discord.Forbidden:
        await ctx.send("🚫 I couldn't fetch the giveaway message due to missing permissions.")
        return

    reaction = next(
        (react for react in refreshed_message.reactions if str(react.emoji) == "🎉"),
        None,
    )

    participants = []
    if reaction:
        async for user in reaction.users():
            if not user.bot:
                participants.append(user)

    if participants:
        if len(participants) <= winner_amount:
            winners = participants
        else:
            winners = random.sample(participants, k=winner_amount)
        winner_mentions = "\n".join(user.mention for user in winners)
    else:
        winner_mentions = "😢 No winners this time."

    result_embed = discord.Embed(
        title="🎊 Giveaway Ended!",
        color=discord.Color.green(),
        description="Thanks for participating! ✨",
    )
    result_embed.add_field(name="🏆 Prize", value=prize, inline=False)
    result_embed.add_field(name="🥳 Winners", value=winner_mentions, inline=False)

    await ctx.send(embed=result_embed)


@host.error
async def host_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("🔒 You need the **Manage Server** permission to host a giveaway.")
        return
    await ctx.send("⚠️ Something went wrong while starting the giveaway. Please try again.")


bot.run(TOKEN)
