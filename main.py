import asyncio
import ast
import logging
import math
import operator
import os
import random
import re
import shlex
import time
import traceback

import aiohttp
import discord
import pytz
try:
    import chess as chess_lib
except ImportError:
    chess_lib = None
from collections import Counter
from datetime import datetime, timedelta, timezone
from discord import Embed, Emoji, File, Interaction, StickerItem, app_commands
from discord.ext import commands, tasks
from discord.ui import Button, Modal, Select, TextInput, View
try:
    from discord.ext.commands.view import StringView
except Exception:
    StringView = None
from flask import Flask
from io import BytesIO
from threading import Thread
from economy import (
    add_user_balance as economy_add_user_balance,
    award_chat_xp as economy_award_chat_xp,
    build_level_up_embed as economy_build_level_up_embed,
    ensure_db_ready as economy_ensure_db_ready,
    EXPLANATIONS as economy_explanations,
    format_balance as economy_format_balance,
    get_lottery_config as economy_get_lottery_config,
    get_game_stat as economy_get_game_stat,
    get_user as economy_get_user,
    log_transaction as economy_log_transaction,
    Q_ACCEPT as economy_q_accept,
    Q_ACTIVITY as economy_q_activity,
    Q_ALARM as economy_q_alarm,
    Q_ATTACHMENT as economy_q_attachment,
    Q_BELL as economy_q_bell,
    Q_BIRTHDAY as economy_q_birthday,
    Q_BOOK as economy_q_book,
    Q_BROOM as economy_q_broom,
    Q_CARDS as economy_q_cards,
    Q_CONFETTI as economy_q_confetti,
    Q_CONNECT_BLACK as economy_q_connect_black,
    Q_CONNECT_WHITE as economy_q_connect_white,
    Q_EDIT as economy_q_edit,
    Q_FILTER as economy_q_filter,
    Q_GAME_O as economy_q_game_o,
    Q_GAME_TIMEOUT as economy_q_game_timeout,
    Q_GAME_WIN as economy_q_game_win,
    Q_GAME_X as economy_q_game_x,
    Q_GIFT as economy_q_gift,
    Q_HAMMER as economy_q_hammer,
    Q_IMAGE as economy_q_image,
    Q_LEVEL_PULSE as economy_q_level_pulse,
    Q_LOCK as economy_q_lock,
    Q_PERMISSIONS as economy_q_permissions,
    Q_PERF as economy_q_perf,
    Q_POLL as economy_q_poll,
    Q_REACTION as economy_q_reaction,
    Q_REJECT as economy_q_reject,
    Q_ROLES as economy_q_roles,
    Q_SLEEP as economy_q_sleep,
    Q_THINKING as economy_q_thinking,
    Q_TIMEOUT as economy_q_timeout,
    Q_TIMER as economy_q_timer,
    Q_TIMER_TICK as economy_q_timer_tick,
    Q_TRASH as economy_q_trash,
    Q_USER_EDIT as economy_q_user_edit,
    Q_VOICE as economy_q_voice,
    Q_WARNING as economy_q_warning,
    risk_label as economy_risk_label,
    record_game_result as economy_record_game_result,
    setup as economy_setup,
    todays_daily_challenge as economy_todays_daily_challenge,
    track_daily_challenge_progress as economy_track_daily_challenge_progress,
    update_user as economy_update_user,
)
from pgdata import (
    add_guild_activity_counts,
    clear_guild_activity_counts,
    delete_guild_activity_channel,
    get_guild_activity_top,
    load_afk_users as pg_load_afk_users,
    load_active_polls,
    load_active_timers,
    load_autoban_ids,
    load_blacklisted_users,
    load_birthdays as pg_load_birthdays,
    load_censored_phrases,
    load_disabled_commands,
    load_guild_activity_channels,
    load_guild_birthday_channels,
    load_guild_prefixes,
    load_guild_log_config,
    load_reaction_shutdown_channels,
    load_reaction_watchlist,
    load_shutdown_channels,
    load_sleeping_users as pg_load_sleeping_users,
    load_watchlist,
    pg_init,
    remove_afk_user,
    remove_active_poll,
    remove_active_timer,
    remove_birthday,
    remove_sleeping_user,
    save_afk_user,
    save_active_poll,
    save_active_timer,
    save_autoban_ids,
    save_blacklisted_users,
    save_birthday,
    save_censored_phrases,
    save_disabled_commands,
    save_guild_activity_channel,
    save_guild_birthday_channel,
    save_guild_prefix,
    save_guild_log_config,
    save_reaction_shutdown_channels,
    save_reaction_watchlist,
    save_shutdown_channels,
    save_sleeping_user,
    update_guild_activity_next_report,
    update_guild_activity_message_id,
    save_watchlist,
)
last_message_time = 0
birthday_task = None
activity_task = None
app = Flask('')

# PostgreSQL is the single source of truth
pg_init()

birthdays = {
    str(uid): {"date": date}
    for uid, date in (pg_load_birthdays() or {}).items()
}

# Loaded from PostgreSQL
afk_users = pg_load_afk_users() or {}
sleeping_users = pg_load_sleeping_users() or {}

class MyBot(commands.Bot):
    async def setup_hook(self):
        pass
        
DEFAULT_PREFIX = "."

intents = discord.Intents.all()
def get_prefix(bot, message):
    """Use only the saved server prefix for commands."""
    guild_id = message.guild.id if message.guild else 0
    return guild_prefixes.get(guild_id, DEFAULT_PREFIX)

bot = MyBot(command_prefix=get_prefix, intents=intents, case_insensitive=True)
bot.remove_command("help")
print(f"Bot is starting with intents: {bot.intents}")

log_channel_id = None
rlog_channel_id = None
super_owner_id = 885548126365171824  
autoban_ids = load_autoban_ids()
blacklisted_users = load_blacklisted_users()
shutdown_channels = load_shutdown_channels()
reaction_shutdown_channels = load_reaction_shutdown_channels()
disabled_commands = load_disabled_commands()
guild_prefixes = load_guild_prefixes()
guild_birthday_channels = load_guild_birthday_channels()
guild_activity_channels = load_guild_activity_channels()
guild_log_configs = {}
censored_phrases = load_censored_phrases()
watchlist = load_watchlist()
reaction_watchlist = load_reaction_watchlist()
c4_games = {}
ttt_games = {}
chess_games = {}
edited_snipes = {}
deleted_snipes = {}
removed_reactions = {}
slash_commands_synced = False

TTT_EMPTY = "empty"
TTT_X = "x"
TTT_O = "o"
C4_NUMBER_EMOJIS = [
    "<:QC4One:1500858417810772160>",
    "<:QC4Two:1500858426698498219>",
    "<:QC4Three:1500858424999673956>",
    "<:QC4Four:1500858416220864622>",
    "<:QC4Five:1500858414660714717>",
    "<:QC4Six:1500858422264856738>",
    "<:QC4Seven:1500858419979096064>",
]
C4_COLUMN_LABELS = "".join(C4_NUMBER_EMOJIS)
TURN_TIMEOUT_SECONDS = 30
TURN_COUNTDOWN_EDIT_POINTS = {30, 20, 10, 5, 4, 3, 2, 1}
CHESS_CLOCK_SECONDS = 10 * 60
CHESS_CLOCK_LIVE_INTERVAL = 5

CHESS_PIECE_EMOJIS = {
    "P": "<:QChessWhitePawn:1500858450010312855>",
    "N": "<:QChessWhiteKnight:1500858448428925141>",
    "B": "<:QChessWhiteBishop:1500858444675289240>",
    "R": "<:QChessWhiteRook:1500858453424607262>",
    "Q": "<:QChessWhiteQueen:1500858451260215459>",
    "K": "<:QChessWhiteKing:1500858446642286644>",
    "p": "<:QChessBlackPawn:1500858437658087696>",
    "n": "<:QChessBlackKnight:1500858435925970975>",
    "b": "<:QChessBlackBishop:1500858431840452628>",
    "r": "<:QChessBlackRook:1500858442724675625>",
    "q": "<:QChessBlackQueen:1500858439197397034>",
    "k": "<:QChessBlackKing:1500858433786609874>",
}
CHESS_LIGHT = "<:QC4EmptyLight:1500878748537585715>"
CHESS_DARK = "<:QChessDark:1500878861653770271>"
CHESS_COORD_SPACER = CHESS_DARK
CHESS_FILE_EMOJIS = {
    "a": "<:QChessFileA:1501277403647967312>",
    "b": "<:QChessFileB:1501277406751752403>",
    "c": "<:QChessFileC:1501277408471289926>",
    "d": "<:QChessFileD:1501277410291879966>",
    "e": "<:QChessFileE:1501277412229382406>",
    "f": "<:QChessFileF:1501277413857034401>",
    "g": "<:QChessFileG:1501277426406264863>",
    "h": "<:QChessFileH:1501277428293832884>",
}
POLL_NUMBER_EMOJIS = [
    "<:QPollOne:1500881606389530724>",
    "<:QPollTwo:1500878993954705690>",
    "<:QPollThree:1500878992100688043>",
    "<:QPollFour:1500878979782152353>",
    "<:QPollFive:1500878977135284225>",
    "<:QPollSix:1500878987927224402>",
    "<:QPollSeven:1500878986107162755>",
    "<:QPollEight:1500883998723805346>",
    "<:QPollNine:1500884000355389563>",
    "<:QPollTen:1500884002507329636>",
]
CHESS_RANK_EMOJIS = POLL_NUMBER_EMOJIS[:8]
C4_EMPTY_LIGHT = "<:QC4EmptyLight:1500878748537585715>"
C4_EMPTY_DARK = "<:QC4EmptyDark:1500878746956202136>"

def custom_emoji(markdown):
    try:
        return discord.PartialEmoji.from_str(markdown)
    except Exception:
        return None

def reaction_emoji(markdown):
    return custom_emoji(markdown) or markdown

def same_emoji(left, right):
    return str(left) == str(right)

def set_embed_field(embed, index, name, value, inline=True):
    if index < len(embed.fields):
        embed.set_field_at(index, name=name, value=value, inline=inline)
    else:
        embed.add_field(name=name, value=value, inline=inline)

def embed_value(text, limit=1024):
    text = str(text or "None.")
    return text if len(text) <= limit else text[:limit - 1] + "…"

def joined_embed_value(lines, empty="None.", limit=1024):
    lines = [str(line) for line in lines if str(line)]
    if not lines:
        return empty
    value = "\n".join(lines)
    return embed_value(value, limit)

active_timers = load_active_timers()
active_polls = load_active_polls()
runtime_state_restored = False
user_mentions = {}
activity_buffer = Counter()
active_activity_status_messages = {}
command_timing_stats = {}
daily_cooldown = {}
weekly_cooldown = {}
monthly_cooldown = {}
chat_xp_memory = {}

class CommandDisabledError(commands.CheckFailure):
    def __init__(self, command_name):
        self.command_name = command_name

def get_command_case_insensitive(command_name):
    if not command_name:
        return None
    key = command_name.strip().casefold()
    for prefix in sorted({DEFAULT_PREFIX, *guild_prefixes.values()}, key=len, reverse=True):
        if prefix and key.startswith(prefix.casefold()):
            key = key[len(prefix):]
            break
    return next(
        (
            command
            for command in bot.walk_commands()
            if command.qualified_name.casefold() == key
            or command.name.casefold() == key
            or key in {alias.casefold() for alias in command.aliases}
        ),
        None
    )

def looks_like_command_message(message):
    content = message.content.strip()
    if not content:
        return False
    guild_id = message.guild.id if message.guild else 0
    prefix = guild_prefixes.get(guild_id, DEFAULT_PREFIX)
    if content.startswith(prefix):
        return True
    return False

def slash_runnable_commands():
    commands_ = []
    seen = set()
    for command in bot.walk_commands():
        if command.hidden or getattr(command, "parent", None):
            continue
        if command.name in seen:
            continue
        seen.add(command.name)
        commands_.append(command)
    return sorted(commands_, key=lambda c: c.name.casefold())

def slash_command_search(current):
    current = (current or "").casefold().strip()
    results = []
    for command in slash_runnable_commands():
        names = [command.name, *command.aliases]
        haystack = " ".join(names).casefold()
        if current and current not in haystack:
            continue
        alias_text = f" ({', '.join(command.aliases[:3])})" if command.aliases else ""
        label = f"{command.name}{alias_text}"
        if len(label) > 100:
            label = label[:97] + "..."
        results.append(app_commands.Choice(name=label, value=command.name))
        if len(results) >= 25:
            break
    return results

async def invoke_prefix_command_from_interaction(interaction, command_name, args=None):
    command = get_command_case_insensitive(command_name)
    if not command:
        await interaction.followup.send(
            f"Command `{command_name}` was not found. Try `/commands`.",
            ephemeral=True
        )
        return
    if StringView is None or not hasattr(commands.Context, "from_interaction"):
        await interaction.followup.send(
            "Slash forwarding is not available in this Discord library version.",
            ephemeral=True
        )
        return

    ctx = await commands.Context.from_interaction(interaction)
    ctx.command = command
    ctx.invoked_with = command.name
    ctx.prefix = "/run "
    raw_args = (args or "").strip()
    ctx.view = StringView(raw_args)
    ctx.view.skip_ws()
    try:
        await command.invoke(ctx)
    except commands.CommandError as e:
        await on_command_error(ctx, e)
    except Exception as e:
        print(f"Slash command bridge failed for {command.name}: {type(e).__name__} - {e}")
        if not interaction.response.is_done():
            await interaction.response.send_message("That slash command failed.", ephemeral=True)
        else:
            await interaction.followup.send("That slash command failed.", ephemeral=True)

async def sync_slash_commands_once():
    global slash_commands_synced
    if slash_commands_synced:
        return
    runnable_count = len(slash_runnable_commands())
    synced_guilds = 0
    failed_guilds = 0
    try:
        global_synced = await bot.tree.sync()
        print(f"Global slash commands synced: {len(global_synced)} top-level commands.")
    except Exception as e:
        print(f"Global slash command sync failed: {type(e).__name__} - {e}")

    for guild in bot.guilds:
        try:
            bot.tree.copy_global_to(guild=discord.Object(id=guild.id))
            synced = await bot.tree.sync(guild=discord.Object(id=guild.id))
            synced_guilds += 1
            print(f"Guild slash commands synced for {guild.name} ({guild.id}): {len(synced)} commands.")
        except Exception as e:
            failed_guilds += 1
            print(f"Guild slash command sync failed for {guild.name} ({guild.id}): {type(e).__name__} - {e}")

    if synced_guilds or not failed_guilds:
        slash_commands_synced = True
    print(
        f"Slash command sync complete: {synced_guilds} guild(s) synced, "
        f"{failed_guilds} failed. Use /run for all {runnable_count} prefix commands."
    )

def scoped_id(guild):
    return guild.id if guild else 0

def scoped_set(store, guild):
    return store.setdefault(scoped_id(guild), set())

def scoped_list(store, guild):
    return store.setdefault(scoped_id(guild), [])

def scoped_map(store, guild):
    return store.setdefault(scoped_id(guild), {})

def normalize(text):
    return (text or "").casefold()

def guild_autoban_ids(guild):
    return scoped_set(autoban_ids, guild)

def guild_blacklisted_users(guild):
    return scoped_set(blacklisted_users, guild)

def guild_shutdown_channels(guild):
    return scoped_set(shutdown_channels, guild)

def guild_reaction_shutdown_channels(guild):
    return scoped_set(reaction_shutdown_channels, guild)

def guild_disabled_commands(guild):
    return scoped_set(disabled_commands, guild)

def guild_censored_phrases(guild):
    return scoped_list(censored_phrases, guild)

def guild_watchlist(guild):
    return scoped_map(watchlist, guild)

def guild_reaction_watchlist(guild):
    return scoped_map(reaction_watchlist, guild)

@app.route('/')
def home():
    return "I'm alive", 200

def get_log_channel_id(guild_id, key):
    config = load_guild_log_config(guild_id)
    if config:
        guild_log_configs[int(guild_id)] = config
    else:
        config = guild_log_configs.get(int(guild_id))
    if not config:
        return None
    return config.get(key)

def guild_log_label(guild):
    if guild is None:
        return "unknown server"
    return f"{guild.name} ({guild.id})"

def log_user(value):
    user_id = getattr(value, "id", None)
    if user_id is None:
        return "Unknown"
    return f"<@{user_id}> ({user_id})"

async def send_log(embed, guild=None):
    try:
        if guild is None:
            print("Log skipped: missing guild context.")
            return False

        channel_id = get_log_channel_id(guild.id, "log_channel_id")
        if not channel_id:
            print(f"Log skipped: no normal log channel saved for guild {guild.id}.")
            return False

        channel = bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await bot.fetch_channel(channel_id)
            except Exception as e:
                print(f"Log skipped: could not fetch normal log channel {channel_id} for guild {guild.id}: {e}")
                return False

        if getattr(channel, "guild", None) is None or channel.guild.id != guild.id:
            print(f"Log skipped: saved normal log channel {channel_id} is not in guild {guild.id}.")
            return False

        bot_member = guild.get_member(bot.user.id) or guild.me
        perms = channel.permissions_for(bot_member)
        if not perms.view_channel or not perms.send_messages:
            print(f"Log skipped: missing send/view permission in normal log channel {channel_id}.")
            return False
        if not perms.embed_links:
            print(f"Log skipped: missing Embed Links permission in normal log channel {channel_id}.")
            return False

        await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        print(f"Sending log: {embed.title} | Server: {guild_log_label(guild)}")
        return True
    except Exception as e:
        print(f"Failed to send log for guild {guild.id if guild else 'unknown'}: {type(e).__name__} - {e}")
        return False

async def send_rlog(embed, guild=None):
    try:
        if guild is None:
            print("Reaction log skipped: missing guild context.")
            return False

        channel_id = get_log_channel_id(guild.id, "reaction_log_channel_id")
        if not channel_id:
            print(f"Reaction log skipped: no reaction log channel saved for guild {guild.id}.")
            return False

        channel = bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await bot.fetch_channel(channel_id)
            except Exception as e:
                print(f"Reaction log skipped: could not fetch channel {channel_id} for guild {guild.id}: {e}")
                return False

        if getattr(channel, "guild", None) is None or channel.guild.id != guild.id:
            print(f"Reaction log skipped: saved channel {channel_id} is not in guild {guild.id}.")
            return False

        bot_member = guild.get_member(bot.user.id) or guild.me
        perms = channel.permissions_for(bot_member)
        if not perms.view_channel or not perms.send_messages:
            print(f"Reaction log skipped: missing send/view permission in channel {channel_id}.")
            return False
        if not perms.embed_links:
            print(f"Reaction log skipped: missing Embed Links permission in channel {channel_id}.")
            return False

        await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        print(f"Sending reaction log: {embed.title} | Server: {guild_log_label(guild)}")
        return True
    except Exception as e:
        print(f"Failed to send reaction log for guild {guild.id if guild else 'unknown'}: {type(e).__name__} - {e}")
        return False

async def log_raw_reaction(payload, added=True):
    guild = bot.get_guild(payload.guild_id) if payload.guild_id else None
    if guild is None:
        return False

    user = guild.get_member(payload.user_id)
    if user is None:
        try:
            user = await guild.fetch_member(payload.user_id)
        except Exception:
            try:
                user = await bot.fetch_user(payload.user_id)
            except Exception:
                user = None
    if user and getattr(user, "bot", False) and user.id != super_owner_id:
        return False

    channel = bot.get_channel(payload.channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(payload.channel_id)
        except Exception:
            channel = None
    if channel is None:
        return False

    message_url = f"https://discord.com/channels/{payload.guild_id}/{payload.channel_id}/{payload.message_id}"
    message = None
    try:
        message = await channel.fetch_message(payload.message_id)
        message_url = message.jump_url
    except Exception:
        pass

    if added:
        title = f"{economy_q_reaction} Reaction Added"
        color = discord.Color.green()
    else:
        title = f"{economy_q_reaction} Reaction Removed"
        color = discord.Color.red()
        if message is not None:
            entry = (user, payload.emoji, message, datetime.now(timezone.utc).replace(tzinfo=timezone.utc))
            removed_reactions.setdefault(channel.id, []).insert(0, entry)
            removed_reactions[channel.id] = removed_reactions[channel.id][:50]

    embed = discord.Embed(title=title, color=color)
    embed.add_field(name="User", value=log_user(user), inline=False)
    embed.add_field(name="Emoji", value=str(payload.emoji), inline=True)
    embed.add_field(name="Message", value=f"[Jump to Message]({message_url})", inline=False)
    embed.add_field(name="Channel", value=channel.mention if hasattr(channel, "mention") else f"`{payload.channel_id}`", inline=False)
    embed.timestamp = datetime.now(timezone.utc)
    return await send_rlog(embed, guild)

def first_sendable_text_channel(guild):
    for channel in guild.text_channels:
        perms = channel.permissions_for(guild.me)
        if perms.send_messages and perms.view_channel:
            return channel
    return None

async def resolve_server_channel(guild, raw=None, mentioned_channels=None, allow_threads=False):
    for channel in mentioned_channels or []:
        if getattr(channel, "guild", None) == guild:
            return channel
    match = re.search(r"\d{15,25}", str(raw or ""))
    if not match:
        return None
    channel_id = int(match.group(0))
    channel = guild.get_channel(channel_id)
    if channel is None and allow_threads and hasattr(guild, "get_thread"):
        channel = guild.get_thread(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            channel = None
    if channel is None or getattr(channel, "guild", None) != guild:
        return None
    return channel

async def find_bot_adder(guild):
    try:
        async for entry in guild.audit_logs(limit=10, action=discord.AuditLogAction.bot_add):
            if entry.target and entry.target.id == bot.user.id:
                return entry.user
    except Exception:
        pass
    return guild.owner

async def prompt_log_setup(guild):
    channel = first_sendable_text_channel(guild)
    if channel is None:
        return

    requester = await find_bot_adder(guild)

    class SameChannelView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=120)
            self.same = None

        async def interaction_check(self, interaction):
            if requester is not None and interaction.user.id != requester.id:
                await interaction.response.send_message("Only the person who added me can choose this.", ephemeral=True)
                return False
            return True

        @discord.ui.button(label="Same channel", style=discord.ButtonStyle.success)
        async def same_channel(self, interaction, button):
            self.same = True
            await interaction.response.edit_message(content="Choose the log channel next.", view=None)
            self.stop()

        @discord.ui.button(label="Separate channels", style=discord.ButtonStyle.primary)
        async def separate_channels(self, interaction, button):
            self.same = False
            await interaction.response.edit_message(content="Choose the normal log channel next.", view=None)
            self.stop()

    async def ask_for_channel(prompt):
        def valid_log_channel(selected_channel):
            if not isinstance(selected_channel, discord.TextChannel):
                return False
            if selected_channel.guild.id != guild.id:
                return False
            bot_member = guild.get_member(bot.user.id) or guild.me
            perms = selected_channel.permissions_for(bot_member)
            return perms.view_channel and perms.send_messages

        class LogChannelSelect(discord.ui.ChannelSelect):
            def __init__(self):
                super().__init__(
                    placeholder="Select a text channel",
                    min_values=1,
                    max_values=1,
                    channel_types=[discord.ChannelType.text],
                )

            async def callback(self, interaction):
                selected_channel = self.values[0]
                if not valid_log_channel(selected_channel):
                    await interaction.response.send_message(
                        "I can't send messages in that channel.",
                        ephemeral=True,
                    )
                    return

                self.view.channel_id = selected_channel.id
                await interaction.response.edit_message(
                    content=f"{prompt} {selected_channel.mention}",
                    view=None,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                self.view.stop()

        class LogChannelView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=120)
                self.channel_id = None
                self.add_item(LogChannelSelect())

            async def interaction_check(self, interaction):
                if requester is not None and interaction.user.id != requester.id:
                    await interaction.response.send_message(
                        "Only the person who added me can choose this.",
                        ephemeral=True,
                    )
                    return False
                return True

        view = LogChannelView()
        message = await channel.send(
            f"{prompt}\nIf the channel is missing, reply with the channel mention or ID.",
            view=view,
        )

        def check(reply):
            if requester is not None and reply.author.id != requester.id:
                return False
            return reply.channel.id == channel.id

        message_task = asyncio.create_task(bot.wait_for("message", check=check, timeout=120))
        view_task = asyncio.create_task(view.wait())
        done, pending = await asyncio.wait(
            {message_task, view_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        if message_task in done:
            try:
                reply = message_task.result()
            except asyncio.TimeoutError:
                reply = None
            if reply is not None:
                selected_channel = await resolve_server_channel(guild, reply.content, reply.channel_mentions)
                if valid_log_channel(selected_channel):
                    view.channel_id = selected_channel.id
                    view.stop()
                    await message.edit(
                        content=f"{prompt} {selected_channel.mention}",
                        view=None,
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                else:
                    await message.edit(
                        content="I can't send messages in that channel.",
                        view=None,
                    )
                    return None

        if view.channel_id is None:
            await message.edit(content="Log setup timed out.", view=None)
        return view.channel_id

    mention = requester.mention if requester else guild.owner.mention
    same_view = SameChannelView()
    await channel.send(
        f"{mention} should normal logs and reaction logs use the same channel?",
        view=same_view,
        allowed_mentions=discord.AllowedMentions(users=True)
    )
    await same_view.wait()
    if same_view.same is None:
        return

    normal_channel_id = await ask_for_channel("Choose the normal log channel:")
    if normal_channel_id is None:
        return

    if same_view.same:
        reaction_channel_id = normal_channel_id
    else:
        reaction_channel_id = await ask_for_channel("Choose the reaction log channel:")
        if reaction_channel_id is None:
            return

    save_guild_log_config(guild.id, normal_channel_id, reaction_channel_id)
    guild_log_configs[guild.id] = {
        "log_channel_id": int(normal_channel_id),
        "reaction_log_channel_id": int(reaction_channel_id),
    }
    await channel.send("Log channels saved.")

async def prompt_birthday_setup(guild):
    channel = first_sendable_text_channel(guild)
    if channel is None:
        return
    requester = await find_bot_adder(guild)

    class BirthdayChannelSelect(discord.ui.ChannelSelect):
        def __init__(self):
            super().__init__(
                placeholder="Select birthday channel",
                min_values=1,
                max_values=1,
                channel_types=[discord.ChannelType.text],
            )

        async def callback(self, interaction):
            selected_channel = self.values[0]
            bot_member = guild.get_member(bot.user.id) or guild.me
            perms = selected_channel.permissions_for(bot_member)
            if not perms.view_channel or not perms.send_messages:
                return await interaction.response.send_message("I can't send birthday messages in that channel.", ephemeral=True)
            await interaction.response.defer()
            saved = await asyncio.to_thread(save_guild_birthday_channel, guild.id, selected_channel.id, interaction.user.id)
            if not saved:
                return await interaction.followup.send("Birthday channel save failed because the database is unavailable.", ephemeral=True)
            guild_birthday_channels[guild.id] = {"channel_id": selected_channel.id, "set_by_user_id": interaction.user.id}
            await interaction.edit_original_response(
                content=f"{economy_q_birthday} Birthday channel saved: {selected_channel.mention}",
                view=None,
                allowed_mentions=discord.AllowedMentions.none(),
            )

    class BirthdayChannelView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=180)
            self.add_item(BirthdayChannelSelect())

        async def interaction_check(self, interaction):
            if requester is not None and interaction.user.id != requester.id:
                await interaction.response.send_message("Only the person who added me can choose this.", ephemeral=True)
                return False
            return True

    mention = requester.mention if requester else guild.owner.mention
    message = await channel.send(
        f"{mention} choose the birthday announcement channel.",
        view=BirthdayChannelView(),
        allowed_mentions=discord.AllowedMentions(users=True),
    )

    def check(reply):
        if requester is not None and reply.author.id != requester.id:
            return False
        return reply.guild == guild and reply.channel.id == channel.id

    try:
        reply = await bot.wait_for("message", check=check, timeout=180)
    except asyncio.TimeoutError:
        return

    selected_channel = await resolve_server_channel(guild, reply.content, reply.channel_mentions)
    if selected_channel is None:
        return await message.edit(content="I couldn't find that channel. Run `.setbdaychannel #channel` or `.setbdaychannel <channel id>`.", view=None)
    bot_member = guild.get_member(bot.user.id) or guild.me
    perms = selected_channel.permissions_for(bot_member)
    if not perms.view_channel or not perms.send_messages:
        return await message.edit(content="I can't send birthday messages in that channel.", view=None)
    saved = await asyncio.to_thread(save_guild_birthday_channel, guild.id, selected_channel.id, reply.author.id)
    if not saved:
        return await message.edit(content="Birthday channel save failed because the database is unavailable.", view=None)
    guild_birthday_channels[guild.id] = {"channel_id": selected_channel.id, "set_by_user_id": reply.author.id}
    await message.edit(
        content=f"{economy_q_birthday} Birthday channel saved: {selected_channel.mention}",
        view=None,
        allowed_mentions=discord.AllowedMentions.none(),
    )

async def send_user_update_log(embed, user_id):
    for guild in bot.guilds:
        if guild.get_member(user_id) and load_guild_log_config(guild.id):
            await send_log(embed.copy(), guild)

async def safe_send(destination, *args, **kwargs):
    global last_message_time
    now = asyncio.get_event_loop().time()
    wait_time = 2 - (now - last_message_time)
    if wait_time > 0:
        await asyncio.sleep(wait_time)
    last_message_time = asyncio.get_event_loop().time()
    return await destination.send(*args, **kwargs)

async def safe_add_reaction(message, emoji):
    try:
        await message.add_reaction(reaction_emoji(emoji))
    except discord.HTTPException:
        pass

async def safe_delete_message(message):
    try:
        await message.delete()
    except discord.HTTPException:
        pass

async def safe_remove_reaction(message, emoji, member):
    try:
        await message.remove_reaction(emoji, member)
    except discord.HTTPException:
        pass

def run():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run)
    t.start()

def super_owner_in_guild(guild):
    return guild is not None and guild.get_member(super_owner_id) is not None

def has_super_owner_power(user, guild=None):
    return user is not None and user.id == super_owner_id

def has_admin_power(user, guild=None):
    if user is None or guild is None:
        return False
    permissions = getattr(user, "guild_permissions", None)
    return bool(permissions and permissions.administrator)

def has_owner_power(user, guild=None):
    if user is None:
        return False
    return has_super_owner_power(user, guild) or (guild is not None and guild.owner_id == user.id) or has_admin_power(user, guild)

def permission_rank(user, guild=None):
    if user is None:
        return 0
    if has_super_owner_power(user, guild):
        return 4
    if guild is not None and guild.owner_id == user.id:
        return 3
    if has_admin_power(user, guild):
        return 2
    return 0

def can_act_on(actor, target, guild=None):
    if target is None:
        return True
    return permission_rank(actor, guild) > permission_rank(target, guild)

def can_manage_prefix(user, guild):
    if guild is None or user is None:
        return False
    if super_owner_in_guild(guild):
        return user.id == super_owner_id
    permissions = getattr(user, "guild_permissions", None)
    return guild.owner_id == user.id or bool(permissions and permissions.administrator)

def is_admin_power():
    async def predicate(ctx):
        return has_owner_power(ctx.author, ctx.guild)
    return commands.check(predicate)

def is_super_owner():
    async def predicate(ctx):
        return has_super_owner_power(ctx.author, ctx.guild)
    return commands.check(predicate)

@tasks.loop(minutes=4)
async def keep_alive_task():
    print("Heartbeat")

@bot.event
async def on_ready():
    global birthday_task, activity_task, runtime_state_restored
    print(f'ProQue is online as {bot.user}')
    if not keep_alive_task.is_running():
        keep_alive_task.start()
    if birthday_task is None or birthday_task.done():
        birthday_task = asyncio.create_task(birthday_check_loop())
    if activity_task is None or activity_task.done():
        activity_task = asyncio.create_task(activity_report_loop())
    # Load economy cog
    try:
        await economy_setup(bot, send_log)
        economy_command_names = [
            "bal", "profile", "inventory", "quests", "dailychallenge", "streaks", "guide", "shop", "cooldowns", "transactions", "limits", "lottery", "editlottery", "stoplottery", "lotterystats", "buytick",
            "daily", "weekly", "monthly", "cf", "roulette", "slots",
            "blackjack", "scratch", "tower", "vault", "memory", "cardladder", "lockpick", "heist", "diceduel", "cases", "plinko", "luckynumber", "jackpotspin", "dungeon", "ms", "wheel", "give", "lb", "gamestats", "achievements", "setbadge", "gamebalance", "gamehistory",
            "qstats", "economyaudit", "add", "remove", "addtick", "settick", "setquesos", "econhelp", "explain"
        ]
        loaded_economy_commands = [name for name in economy_command_names if bot.get_command(name)]
        print(f"Quewo system loaded ({len(loaded_economy_commands)}/{len(economy_command_names)} commands)")
    except Exception as e:
        print(f"Quewo system not loaded: {e}")
    if not runtime_state_restored:
        await restore_persistent_runtime_state()
        runtime_state_restored = True
    await sync_slash_commands_once()


async def birthday_check_loop():
    await bot.wait_until_ready()
    already_sent = set()

    while not bot.is_closed():
        now = datetime.now(timezone.utc)
        today_str = now.strftime("%d/%m")

        if now.hour == 0 and now.minute == 0:
            for user_id, data in list(birthdays.items()):
                if data["date"] != today_str:
                    continue
                for guild in bot.guilds:
                    member = guild.get_member(int(user_id))
                    if member is None:
                        try:
                            member = await guild.fetch_member(int(user_id))
                        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                            member = None
                    if member is None:
                        continue
                    sent_key = (guild.id, int(user_id), now.date())
                    if sent_key in already_sent:
                        continue
                    config = guild_birthday_channels.get(guild.id)
                    if not config:
                        continue
                    channel = guild.get_channel(config["channel_id"]) or bot.get_channel(config["channel_id"])
                    if not channel:
                        continue
                    await channel.send(
                        f"@everyone {economy_q_birthday} it's {member.mention}'s birthday today!",
                        allowed_mentions=discord.AllowedMentions(everyone=True, users=True)
                    )
                    already_sent.add(sent_key)

        await asyncio.sleep(60)

async def flush_activity_buffer():
    if not activity_buffer:
        return
    pending = dict(activity_buffer)
    activity_buffer.clear()
    ok = await asyncio.to_thread(add_guild_activity_counts, pending)
    if not ok:
        activity_buffer.update(pending)

def activity_report_embed(guild, rows):
    embed = discord.Embed(
        title=f"{economy_q_activity} Activity Winners",
        description="Previous report results.",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc)
    )
    if not rows:
        embed.description = "Previous report ended with no tracked activity."
        embed.set_footer(text=f"{guild.name} • next report in 24 hours")
        return embed

    lines = []
    for index, row in enumerate(rows, 1):
        rank = POLL_NUMBER_EMOJIS[index - 1] if index <= len(POLL_NUMBER_EMOJIS) else f"**{index}.**"
        lines.append(
            f"{rank} <@{row['user_id']}> — **{row['messages']:,}** messages"
        )
    total_messages = sum(row["messages"] for row in rows)
    embed.add_field(name="Leaderboard", value="\n".join(lines), inline=False)
    embed.add_field(name="Messages", value=f"**{total_messages:,}**", inline=True)
    embed.set_footer(text=f"{guild.name} • next report in 24 hours")
    return embed

def activity_live_embed(guild, rows, next_report):
    embed = discord.Embed(
        title=f"{economy_q_activity} Current Activity Report",
        description="Live message leaderboard for this report window.",
        color=discord.Color.green(),
        timestamp=datetime.now(timezone.utc)
    )
    if next_report and next_report.tzinfo is None:
        next_report = next_report.replace(tzinfo=timezone.utc)
    if next_report:
        embed.add_field(name="Ends", value=f"<t:{int(next_report.timestamp())}:R>", inline=True)
        embed.add_field(name="Report Time", value=f"<t:{int(next_report.timestamp())}:f>", inline=True)
    if rows:
        lines = []
        for index, row in enumerate(rows, 1):
            rank = POLL_NUMBER_EMOJIS[index - 1] if index <= len(POLL_NUMBER_EMOJIS) else f"**{index}.**"
            lines.append(f"{rank} <@{row['user_id']}> — **{row['messages']:,}** messages")
        total_messages = sum(row["messages"] for row in rows)
        embed.add_field(name="Current Top 5", value="\n".join(lines), inline=False)
        embed.add_field(name="Tracked Messages", value=f"**{total_messages:,}**", inline=True)
    else:
        embed.add_field(name="Current Top 5", value="No messages tracked yet.", inline=False)
    embed.set_footer(text=f"{guild.name} • updates automatically")
    return embed

def activity_setup_embed(guild):
    embed = discord.Embed(
        title=f"{economy_q_activity} Activity Report Setup",
        description="Choose where the daily activity report should be posted.",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="Report Window", value="Every 24 hours", inline=True)
    embed.add_field(name="Tracks", value="Messages only", inline=True)
    embed.add_field(name="Channel", value="Use the dropdown, or reply here with a channel ID/mention.", inline=False)
    embed.set_footer(text=guild.name)
    return embed

def activity_saved_embed(guild, channel, next_report, user_id):
    embed = discord.Embed(
        title=f"{economy_q_activity} Activity Reports Enabled",
        description="Daily activity reports are now configured for this server.",
        color=discord.Color.green(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="Report Channel", value=channel.mention, inline=True)
    embed.add_field(name="First Report", value=f"<t:{int(next_report.timestamp())}:R>", inline=True)
    embed.add_field(name="Set By", value=f"<@{user_id}>", inline=True)
    embed.set_footer(text=guild.name)
    return embed

def activity_setup_error_embed(message):
    embed = discord.Embed(
        title=f"{economy_q_warning} Activity Setup",
        description=message,
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc)
    )
    return embed

def resolve_activity_report_channel(guild, raw, mentioned_channels=None):
    for channel in mentioned_channels or []:
        if getattr(channel, "guild", None) == guild:
            return channel
    match = re.search(r"\d{15,25}", str(raw or ""))
    if not match:
        return None
    channel_id = int(match.group(0))
    channel = guild.get_channel(channel_id)
    if channel is None and hasattr(guild, "get_thread"):
        channel = guild.get_thread(channel_id)
    if channel is None:
        channel = bot.get_channel(channel_id)
    if channel is None or getattr(channel, "guild", None) != guild:
        return None
    return channel

async def save_activity_report_config(guild, selected_channel, user_id, next_report=None):
    bot_member = guild.get_member(bot.user.id) or guild.me
    permission_channel = selected_channel
    if not hasattr(permission_channel, "permissions_for") and getattr(selected_channel, "parent", None) is not None:
        permission_channel = selected_channel.parent
    if not hasattr(permission_channel, "permissions_for"):
        return False, "I can't check permissions for that channel.", None
    perms = permission_channel.permissions_for(bot_member)
    if not perms.view_channel or not perms.send_messages:
        return False, "I can't send activity reports in that channel.", None
    next_report = next_report or (datetime.now(timezone.utc) + timedelta(hours=24))
    saved = await asyncio.to_thread(
        save_guild_activity_channel,
        guild.id,
        selected_channel.id,
        user_id,
        next_report,
        None,
    )
    if not saved:
        return False, "Activity channel save failed because the database is unavailable.", None
    guild_activity_channels[guild.id] = {
        "channel_id": selected_channel.id,
        "set_by_user_id": user_id,
        "next_report": next_report,
        "current_message_id": None,
    }
    return True, "", next_report

async def schedule_next_activity_report(guild_id, config, reason="completed"):
    next_report = datetime.now(timezone.utc) + timedelta(hours=24)
    guild_id = int(guild_id)
    config["next_report"] = next_report
    guild_activity_channels[guild_id] = {
        "channel_id": int(config["channel_id"]),
        "set_by_user_id": config.get("set_by_user_id"),
        "next_report": next_report,
        "current_message_id": config.get("current_message_id"),
    }
    updated = await asyncio.to_thread(update_guild_activity_next_report, guild_id, next_report)
    if not updated:
        saved = await asyncio.to_thread(
            save_guild_activity_channel,
            guild_id,
            int(config["channel_id"]),
            config.get("set_by_user_id"),
            next_report,
            config.get("current_message_id"),
        )
        if not saved:
            print(f"Activity next report save failed for guild {guild_id} after {reason}.")
    return next_report

async def save_activity_live_message_id(guild_id, config, message_id):
    config["current_message_id"] = message_id
    if int(guild_id) in guild_activity_channels:
        guild_activity_channels[int(guild_id)]["current_message_id"] = message_id
    updated = await asyncio.to_thread(update_guild_activity_message_id, int(guild_id), message_id)
    if not updated:
        print(f"Activity live message id save failed for guild {guild_id}.")

async def refresh_activity_live_message(guild_id, config, rows=None):
    guild = bot.get_guild(int(guild_id))
    if guild is None:
        return None
    channel = resolve_activity_report_channel(guild, str(config["channel_id"]))
    if channel is None:
        return None
    if rows is None:
        rows = await asyncio.to_thread(get_guild_activity_top, guild.id, 5)
    embed = activity_live_embed(guild, rows, config.get("next_report"))
    message = None
    message_id = config.get("current_message_id")
    if message_id:
        try:
            message = await channel.fetch_message(int(message_id))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            message = None
    if message is not None:
        try:
            await message.edit(embed=embed, allowed_mentions=discord.AllowedMentions.none())
            return message
        except discord.HTTPException as e:
            print(f"Activity live message edit failed for guild {guild.id}: {type(e).__name__} - {e}")
    try:
        message = await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        await save_activity_live_message_id(guild.id, config, message.id)
        return message
    except Exception as e:
        print(f"Activity live message send failed for guild {guild.id}: {type(e).__name__} - {e}")
        return None

def schedule_activity_live_refresh(guild_id, config, rows=None):
    async def runner():
        try:
            await refresh_activity_live_message(guild_id, config, rows=rows)
        except Exception as e:
            print(f"Activity background refresh failed for guild {guild_id}: {type(e).__name__} - {e}")
    asyncio.create_task(runner())

async def clear_activity_report_channel(channel):
    if channel is None:
        return 0
    try:
        deleted = await channel.purge(limit=None, check=lambda m: not m.pinned, reason="Activity report ended")
        return len(deleted)
    except Exception as e:
        print(f"Activity channel purge failed, trying manual delete: {type(e).__name__} - {e}")

    deleted_count = 0
    try:
        async for old_message in channel.history(limit=None):
            if old_message.pinned:
                continue
            try:
                await old_message.delete()
                deleted_count += 1
                if deleted_count % 10 == 0:
                    await asyncio.sleep(1)
            except Exception as e:
                print(f"Activity message delete skipped: {type(e).__name__} - {e}")
    except Exception as e:
        print(f"Activity manual clear failed: {type(e).__name__} - {e}")
    return deleted_count

async def activity_status_embed(guild, config, *, flush=True):
    if flush:
        await flush_activity_buffer()
    rows = await asyncio.to_thread(get_guild_activity_top, guild.id, 5)
    channel = guild.get_channel(config["channel_id"]) or bot.get_channel(config["channel_id"])
    next_report = config.get("next_report")
    if next_report and next_report.tzinfo is None:
        next_report = next_report.replace(tzinfo=timezone.utc)

    embed = discord.Embed(
        title=f"{economy_q_activity} Activity Report Status",
        description="Daily activity reports are enabled for this server.",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(
        name="Report Channel",
        value=channel.mention if channel else f"`{config['channel_id']}`",
        inline=True
    )
    embed.add_field(
        name="Next Report",
        value=f"<t:{int(next_report.timestamp())}:R>" if next_report else "Not scheduled",
        inline=True
    )
    if config.get("set_by_user_id"):
        embed.add_field(name="Set By", value=f"<@{config['set_by_user_id']}>", inline=True)
    if config.get("current_message_id") and channel:
        embed.add_field(
            name="Live Message",
            value=f"[Open](https://discord.com/channels/{guild.id}/{channel.id}/{config['current_message_id']})",
            inline=True
        )

    if rows:
        lines = [
            f"**{index}.** <@{row['user_id']}> - **{row['messages']:,}** messages"
            for index, row in enumerate(rows, 1)
        ]
        embed.add_field(name="Current Top 5", value="\n".join(lines), inline=False)
    else:
        embed.add_field(name="Current Top 5", value="No activity tracked in the current window yet.", inline=False)

    embed.set_footer(text="Use .activity setup to change the report channel.")
    return embed

def register_activity_status_message(message):
    if not message or not getattr(message, "guild", None):
        return
    active_activity_status_messages[int(message.id)] = {
        "guild_id": int(message.guild.id),
        "channel_id": int(message.channel.id),
        "expires_at": time.time() + 86400,
    }
    while len(active_activity_status_messages) > 100:
        oldest_id = next(iter(active_activity_status_messages))
        active_activity_status_messages.pop(oldest_id, None)

async def refresh_activity_status_messages():
    if not active_activity_status_messages:
        return
    now = time.time()
    for message_id, meta in list(active_activity_status_messages.items()):
        if meta.get("expires_at", 0) <= now:
            active_activity_status_messages.pop(message_id, None)
            continue
        guild_id = int(meta["guild_id"])
        config = guild_activity_channels.get(guild_id)
        if not config:
            active_activity_status_messages.pop(message_id, None)
            continue
        guild = bot.get_guild(guild_id)
        channel = bot.get_channel(int(meta["channel_id"]))
        if guild is None or channel is None:
            active_activity_status_messages.pop(message_id, None)
            continue
        try:
            message = await channel.fetch_message(int(message_id))
            embed = await activity_status_embed(guild, config, flush=False)
            await message.edit(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        except (discord.NotFound, discord.Forbidden):
            active_activity_status_messages.pop(message_id, None)
        except discord.HTTPException as e:
            print(f"Activity status message edit failed for guild {guild_id}: {type(e).__name__} - {e}")

async def send_activity_report(guild_id, config):
    guild = bot.get_guild(int(guild_id))
    if guild is None:
        await schedule_next_activity_report(guild_id, config, "missing guild")
        return
    channel = resolve_activity_report_channel(guild, str(config["channel_id"]))
    if channel is None:
        await schedule_next_activity_report(guild_id, config, "missing channel")
        return
    sent = False
    try:
        await flush_activity_buffer()
        rows = await asyncio.to_thread(get_guild_activity_top, guild.id, 5)
        await clear_activity_report_channel(channel)
        await channel.send(embed=activity_report_embed(guild, rows), allowed_mentions=discord.AllowedMentions.none())
        sent = True
        await asyncio.to_thread(clear_guild_activity_counts, guild.id)
    except Exception as e:
        print(f"Activity report failed for guild {guild.id}: {type(e).__name__} - {e}")
    next_report = await schedule_next_activity_report(guild.id, config, "sent" if sent else "failed send")
    if sent:
        await save_activity_live_message_id(guild.id, config, None)
        await refresh_activity_live_message(guild.id, config, rows=[])

async def activity_report_loop():
    await bot.wait_until_ready()
    last_flush = time.time()
    while not bot.is_closed():
        now = datetime.now(timezone.utc)
        if time.time() - last_flush >= 60:
            await flush_activity_buffer()
            for guild_id, config in list(guild_activity_channels.items()):
                await refresh_activity_live_message(guild_id, config)
            await refresh_activity_status_messages()
            last_flush = time.time()
        for guild_id, config in list(guild_activity_channels.items()):
            next_report = config.get("next_report")
            if next_report and next_report.tzinfo is None:
                next_report = next_report.replace(tzinfo=timezone.utc)
            if not next_report or now >= next_report:
                await send_activity_report(guild_id, config)
        await asyncio.sleep(30)

async def can_manage_birthday_channel(user, guild):
    if guild is None or user is None:
        return False
    if has_super_owner_power(user, guild):
        return True
    if guild.owner_id == user.id:
        return True
    permissions = getattr(user, "guild_permissions", None)
    if permissions and permissions.administrator:
        return True
    adder = await find_bot_adder(guild)
    return adder is not None and adder.id == user.id

async def can_manage_activity_channel(user, guild):
    return await can_manage_birthday_channel(user, guild)

@bot.event
async def on_member_join(member):
    if member.id in guild_autoban_ids(member.guild):
        try:
            await member.ban(reason="Autoban")
        except:
            pass

    embed = discord.Embed(
        title="Member Joined",
        color=discord.Color.green()
    )
    embed.add_field(name="User", value=log_user(member), inline=False)
    embed.timestamp = datetime.now(timezone.utc)
    await send_log(embed, member.guild)

@bot.event
async def on_member_ban(guild, user):
    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.ban):
        if entry.target.id == user.id:
            embed = discord.Embed(
                title=f"{economy_q_hammer} Member Banned",
                color=discord.Color.red()
            )
            embed.add_field(name="User", value=log_user(user), inline=False)
            embed.add_field(name="Banned by", value=log_user(entry.user), inline=False)
            embed.add_field(name="Reason", value=entry.reason or "No reason provided", inline=False)
            embed.timestamp = datetime.now(timezone.utc)
            try:
                await send_log(embed, guild)
            except Exception as e:
                print(f"Failed to send log: {e}")
            return

@bot.event
async def on_member_unban(guild, user):
    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.unban):
        if entry.target.id == user.id:
            embed = discord.Embed(
                title="Member Unbanned",
                color=discord.Color.green()
            )
            embed.add_field(name="User", value=log_user(user), inline=False)
            embed.add_field(name="Unbanned by", value=log_user(entry.user), inline=False)
            embed.add_field(name="Reason", value=entry.reason or "No reason provided", inline=False)
            embed.timestamp = datetime.now(timezone.utc)
            try:
                await send_log(embed, guild)
            except Exception as e:
                print(f"Failed to send log: {e}")
            return

@bot.event
async def on_guild_join(guild):
    print(f"Joined server: {guild.name} ({guild.id})")
    await prompt_log_setup(guild)

@bot.command()
@is_admin_power()
async def setlogs(ctx):
    await ctx.send("Starting log setup.")
    await prompt_log_setup(ctx.guild)

@bot.command(name="setbdaychannel", aliases=["bdaychannel", "birthdaychannel"])
async def set_birthday_channel(ctx, *, channel_arg: str = None):
    """Sets this server's birthday announcement channel."""
    if ctx.guild is None:
        return await ctx.send("Birthday channels only work in servers.")
    if not await can_manage_birthday_channel(ctx.author, ctx.guild):
        return await ctx.send("Only the person who added me, an admin, the server owner, or the superowner can set the birthday channel.")

    if channel_arg:
        channel = await resolve_server_channel(ctx.guild, channel_arg, ctx.message.channel_mentions)
        if channel is None:
            return await ctx.send("Mention a channel or send its channel ID.")
    else:
        channel = ctx.channel
    bot_member = ctx.guild.get_member(bot.user.id) or ctx.guild.me
    perms = channel.permissions_for(bot_member)
    if not perms.view_channel or not perms.send_messages:
        return await ctx.send("I can't send birthday messages in that channel.")

    saved = await asyncio.to_thread(save_guild_birthday_channel, ctx.guild.id, channel.id, ctx.author.id)
    if not saved:
        return await ctx.send("Birthday channel save failed because the database is unavailable.")

    guild_birthday_channels[ctx.guild.id] = {"channel_id": channel.id, "set_by_user_id": ctx.author.id}
    await ctx.send(
        f"{economy_q_birthday} Birthday announcements will be sent in {channel.mention}.",
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command(name="activity")
async def activity(ctx, action: str = None):
    """Sets this server's daily activity report channel."""
    if ctx.guild is None:
        return await ctx.send("Activity reports only work in servers.")

    existing_config = guild_activity_channels.get(ctx.guild.id)
    if existing_config is None:
        saved_configs = await asyncio.to_thread(load_guild_activity_channels)
        if saved_configs:
            guild_activity_channels.update(saved_configs)
        existing_config = guild_activity_channels.get(ctx.guild.id)
    setup_requested = action and action.casefold() in {"setup", "set", "config", "channel", "reset"}
    if existing_config and not setup_requested:
        embed = await activity_status_embed(ctx.guild, existing_config)
        message = await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        register_activity_status_message(message)
        return

    if not await can_manage_activity_channel(ctx.author, ctx.guild):
        return await ctx.send("Only the person who added me, an admin, the server owner, or the superowner can set the activity channel.")

    async def save_activity_channel(selected_channel, user_id):
        return await save_activity_report_config(ctx.guild, selected_channel, user_id)

    class ActivityChannelSelect(discord.ui.ChannelSelect):
        def __init__(self):
            channel_types = [discord.ChannelType.text]
            for channel_type_name in ("news", "public_thread", "private_thread"):
                channel_type = getattr(discord.ChannelType, channel_type_name, None)
                if channel_type is not None:
                    channel_types.append(channel_type)
            super().__init__(
                placeholder="Select activity report channel",
                min_values=1,
                max_values=1,
                channel_types=channel_types,
            )

        async def callback(self, interaction):
            selected_channel = self.values[0]
            await interaction.response.defer()
            ok, message, next_report = await save_activity_channel(selected_channel, interaction.user.id)
            if not ok:
                return await interaction.followup.send(message, ephemeral=True)
            self.view.saved = True
            schedule_activity_live_refresh(ctx.guild.id, guild_activity_channels[ctx.guild.id])
            await interaction.edit_original_response(
                embed=activity_saved_embed(ctx.guild, selected_channel, next_report, interaction.user.id),
                view=None,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            self.view.stop()

    class ActivityChannelView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=120)
            self.saved = False
            self.add_item(ActivityChannelSelect())

        async def interaction_check(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Only the setup user can choose this.", ephemeral=True)
                return False
            return True

    view = ActivityChannelView()
    prompt = await ctx.send(
        embed=activity_setup_embed(ctx.guild),
        view=view
    )

    def id_reply_check(message):
        return (
            message.author.id == ctx.author.id
            and message.guild == ctx.guild
            and message.channel.id == ctx.channel.id
        )

    reply_task = asyncio.create_task(bot.wait_for("message", check=id_reply_check, timeout=120))
    view_task = asyncio.create_task(view.wait())
    done, pending = await asyncio.wait({reply_task, view_task}, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    for task in pending:
        try:
            await task
        except asyncio.CancelledError:
            pass

    if view.saved:
        return
    if reply_task in done:
        try:
            reply = reply_task.result()
        except asyncio.TimeoutError:
            reply = None
        if reply is None:
            for item in view.children:
                item.disabled = True
            await prompt.edit(embed=activity_setup_error_embed("Activity channel setup timed out."), view=view)
            return
        selected_channel = resolve_activity_report_channel(ctx.guild, reply.content, reply.channel_mentions)
        if selected_channel is None:
            await prompt.edit(
                embed=activity_setup_error_embed("I couldn't find that channel in this server. Run `.activity setup` again and send a channel ID or mention."),
                view=None
            )
            return
        ok, message, next_report = await save_activity_channel(selected_channel, ctx.author.id)
        for item in view.children:
            item.disabled = True
        if ok:
            schedule_activity_live_refresh(ctx.guild.id, guild_activity_channels[ctx.guild.id])
            await prompt.edit(
                embed=activity_saved_embed(ctx.guild, selected_channel, next_report, ctx.author.id),
                view=None,
                allowed_mentions=discord.AllowedMentions.none()
            )
        else:
            await prompt.edit(embed=activity_setup_error_embed(message), view=view)
        return

    for item in view.children:
        item.disabled = True
    await prompt.edit(embed=activity_setup_error_embed("Activity channel setup timed out."), view=view)

@bot.command(name="activitystats", aliases=["astats"])
async def activitystats(ctx):
    """Shows this server's activity report status."""
    if ctx.guild is None:
        return await ctx.send("Activity reports only work in servers.")

    config = guild_activity_channels.get(ctx.guild.id)
    if config is None:
        saved_configs = await asyncio.to_thread(load_guild_activity_channels)
        if saved_configs:
            guild_activity_channels.update(saved_configs)
        config = guild_activity_channels.get(ctx.guild.id)
    if not config:
        return await ctx.send(f"{economy_q_warning} Activity reports are not set up. Use `.activity setup` first.")

    embed = await activity_status_embed(ctx.guild, config)
    message = await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    register_activity_status_message(message)

@bot.command(name="editactivity", aliases=["activityedit"])
async def editactivity(ctx, setting: str = None, *, value: str = None):
    """Edits this server's activity report settings."""
    if ctx.guild is None:
        return await ctx.send("Activity report editing only works in servers.")
    if not await can_manage_activity_channel(ctx.author, ctx.guild):
        return await ctx.send("Only the person who added me, an admin, the server owner, or the superowner can edit activity reports.")

    config = guild_activity_channels.get(ctx.guild.id)
    if config is None:
        saved_configs = await asyncio.to_thread(load_guild_activity_channels)
        if saved_configs:
            guild_activity_channels.update(saved_configs)
        config = guild_activity_channels.get(ctx.guild.id)
    if not config:
        return await ctx.send(f"{economy_q_warning} Activity reports are not set up. Use `.activity setup` first.")

    if not setting or not value:
        return await ctx.send(
            "Use `.editactivity <setting> <value>`.\n"
            "Settings: `channel`, `next`.\n"
            "Examples: `.editactivity channel #activity`, `.editactivity next 12h`"
        )

    setting = setting.casefold()
    if setting in {"channel", "chan"}:
        selected_channel = resolve_activity_report_channel(ctx.guild, value, ctx.message.channel_mentions)
        if selected_channel is None:
            return await ctx.send(f"{economy_q_warning} Mention a channel or send its channel ID.")
        next_report = config.get("next_report") or (datetime.now(timezone.utc) + timedelta(hours=24))
        if next_report.tzinfo is None:
            next_report = next_report.replace(tzinfo=timezone.utc)
        ok, message, _ = await save_activity_report_config(ctx.guild, selected_channel, ctx.author.id, next_report)
        if not ok:
            return await ctx.send(f"{economy_q_warning} {message}")
        schedule_activity_live_refresh(ctx.guild.id, guild_activity_channels[ctx.guild.id])
        embed = activity_saved_embed(ctx.guild, selected_channel, next_report, ctx.author.id)
        embed.title = f"{economy_q_activity} Activity Channel Updated"
        embed.description = "Daily activity reports were moved to a new channel."
        return await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

    if setting in {"next", "time", "timer", "delay", "reset"}:
        delay = parse_poll_duration(value)
        if delay is None or delay.total_seconds() < 300:
            return await ctx.send(f"{economy_q_warning} Invalid time. Use at least 5 minutes, like `30m`, `12h`, or `1d`.")
        next_report = datetime.now(timezone.utc) + delay
        channel = resolve_activity_report_channel(ctx.guild, str(config["channel_id"]))
        if channel is None:
            return await ctx.send(f"{economy_q_warning} Saved activity channel no longer exists. Use `.editactivity channel #channel`.")
        ok, message, _ = await save_activity_report_config(ctx.guild, channel, config.get("set_by_user_id") or ctx.author.id, next_report)
        if not ok:
            return await ctx.send(f"{economy_q_warning} {message}")
        schedule_activity_live_refresh(ctx.guild.id, guild_activity_channels[ctx.guild.id])
        return await ctx.send(
            f"{economy_q_activity} Next activity report set for <t:{int(next_report.timestamp())}:R>.",
            allowed_mentions=discord.AllowedMentions.none()
        )

    await ctx.send(f"{economy_q_warning} Unknown setting. Use `channel` or `next`.")

@bot.command(name="stopactivity", aliases=["activitystop"])
async def stopactivity(ctx):
    """Stops this server's activity reports and clears the current activity window."""
    if ctx.guild is None:
        return await ctx.send("Activity report stopping only works in servers.")
    if not await can_manage_activity_channel(ctx.author, ctx.guild):
        return await ctx.send("Only the person who added me, an admin, the server owner, or the superowner can stop activity reports.")

    saved_configs = await asyncio.to_thread(load_guild_activity_channels)
    if saved_configs:
        guild_activity_channels.update(saved_configs)
    config = guild_activity_channels.get(ctx.guild.id)
    if not config:
        return await ctx.send("Activity reports are not set up in this server.")

    channel = resolve_activity_report_channel(ctx.guild, str(config["channel_id"]))
    await flush_activity_buffer()
    deleted = await asyncio.to_thread(delete_guild_activity_channel, ctx.guild.id)
    if not deleted:
        return await ctx.send(f"{economy_q_warning} Activity report config could not be deleted because the database is unavailable.")
    await asyncio.to_thread(clear_guild_activity_counts, ctx.guild.id)
    guild_activity_channels.pop(ctx.guild.id, None)
    for key in list(activity_buffer):
        if key[0] == ctx.guild.id:
            activity_buffer.pop(key, None)

    embed = discord.Embed(
        title=f"{economy_q_activity} Activity Reports Stopped",
        description="Daily activity reports are now disabled for this server.",
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="Old Channel", value=channel.mention if channel else f"`{config['channel_id']}`", inline=True)
    embed.add_field(name="Current Window", value="Cleared", inline=True)
    embed.set_footer(text="Use .activity setup to enable reports again.")
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="endactivity", aliases=["stopcurrentactivity", "currentactivitystop", "resetactivity", "activityreset"])
async def endactivity(ctx):
    """Ends the current activity window, posts winners, and starts a fresh window."""
    if ctx.guild is None:
        return await ctx.send("Activity report ending only works in servers.")
    if not await can_manage_activity_channel(ctx.author, ctx.guild):
        return await ctx.send("Only the person who added me, an admin, the server owner, or the superowner can end the current activity report.")

    saved_configs = await asyncio.to_thread(load_guild_activity_channels)
    if saved_configs:
        guild_activity_channels.update(saved_configs)
    config = guild_activity_channels.get(ctx.guild.id)
    if not config:
        return await ctx.send(f"{economy_q_warning} Activity reports are not set up. Use `.activity setup` first.")

    channel = resolve_activity_report_channel(ctx.guild, str(config["channel_id"]))
    if channel is None:
        next_report = await schedule_next_activity_report(ctx.guild.id, config, "manual missing channel")
        return await ctx.send(
            f"{economy_q_warning} Saved activity channel no longer exists. I started a fresh timer for <t:{int(next_report.timestamp())}:R>. Use `.editactivity channel #channel`.",
            allowed_mentions=discord.AllowedMentions.none()
        )

    try:
        await flush_activity_buffer()
        rows = await asyncio.to_thread(get_guild_activity_top, ctx.guild.id, 5)
        await clear_activity_report_channel(channel)
        await channel.send(embed=activity_report_embed(ctx.guild, rows), allowed_mentions=discord.AllowedMentions.none())
        await asyncio.to_thread(clear_guild_activity_counts, ctx.guild.id)
        next_report = await schedule_next_activity_report(ctx.guild.id, config, "manual end")
        await save_activity_live_message_id(ctx.guild.id, config, None)
        await refresh_activity_live_message(ctx.guild.id, config, rows=[])
    except Exception as e:
        print(f"Manual activity report end failed for guild {ctx.guild.id}: {type(e).__name__} - {e}")
        return await ctx.send(f"{economy_q_warning} I couldn't end the current activity report: `{type(e).__name__}`")

    if ctx.channel.id != channel.id:
        await ctx.send(
            f"{economy_q_activity} Current activity report ended in {channel.mention}. Next report: <t:{int(next_report.timestamp())}:R>.",
            allowed_mentions=discord.AllowedMentions.none()
        )

@bot.event
async def on_guild_remove(guild):
    print(f"Left server: {guild.name} ({guild.id})")

@bot.event
async def on_guild_channel_create(channel):
    embed = discord.Embed(
        title=f"{economy_q_accept} Channel Created",
        color=discord.Color.green()
    )
    embed.add_field(name="Channel", value=channel.mention, inline=False)

    async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_create):
        if entry.target.id == channel.id:
            embed.add_field(name="By", value=log_user(entry.user), inline=False)
            break

    embed.timestamp = datetime.now(timezone.utc)
    try:
        await send_log(embed, channel.guild)
    except Exception as e:
        print(f"Failed to send log: {e}")

@bot.event
async def on_guild_channel_delete(channel):
    embed = discord.Embed(
        title=f"{economy_q_trash} Channel Deleted",
        color=discord.Color.red()
    )
    embed.add_field(name="Channel", value=channel.name, inline=False)

    async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_delete):
        if entry.target.id == channel.id:
            embed.add_field(name="By", value=log_user(entry.user), inline=False)
            break

    embed.timestamp = datetime.now(timezone.utc)
    try:
        await send_log(embed, channel.guild)
    except Exception as e:
        print(f"Failed to send log: {e}")

@bot.event
async def on_guild_role_create(role):
    embed = discord.Embed(
        title="Role Created",
        color=discord.Color.green()
    )
    embed.add_field(name="Role", value=role.name, inline=False)

    async for entry in role.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_create):
        if entry.target.id == role.id:
            embed.add_field(name="By", value=log_user(entry.user), inline=False)
            break

    embed.timestamp = datetime.now(timezone.utc)
    try:
        await send_log(embed, role.guild)
    except Exception as e:
        print(f"Failed to send log: {e}")

@bot.event
async def on_guild_role_delete(role):
    embed = discord.Embed(
        title=f"{economy_q_trash} Role Deleted",
        color=discord.Color.red()
    )
    embed.add_field(name="Role", value=role.name, inline=False)

    async for entry in role.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_delete):
        if entry.target.id == role.id:
            embed.add_field(name="By", value=log_user(entry.user), inline=False)
            break

    embed.timestamp = datetime.now(timezone.utc)
    try:
        await send_log(embed, role.guild)
    except Exception as e:
        print(f"Failed to send log: {e}")

@bot.event
async def on_guild_role_update(before, after):
    embed = discord.Embed(
        title=f"{economy_q_roles} Role Updated",
        color=discord.Color.blue()
    )
    embed.add_field(name="Role", value=f"{after.name} ({after.id})", inline=False)

    changes = []

    if before.name != after.name:
        changes.append(f"**Name:** `{before.name}` → `{after.name}`")
    if before.color != after.color:
        changes.append(f"**Color:** `{before.color}` → `{after.color}`")
    if before.permissions != after.permissions:
        before_perms = set(p[0] for p in before.permissions if p[1])
        after_perms = set(p[0] for p in after.permissions if p[1])
        added = after_perms - before_perms
        removed = before_perms - after_perms
        if added:
            changes.append(f"**Perms Added:** {', '.join(added)}")
        if removed:
            changes.append(f"**Perms Removed:** {', '.join(removed)}")

    if not changes:
        changes.append("No visible changes logged.")

    embed.add_field(name="Changes", value="\n".join(changes), inline=False)

    async for entry in after.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_update):
        if entry.target.id == after.id:
            embed.add_field(name="By", value=log_user(entry.user), inline=False)
            break

    embed.timestamp = datetime.now(timezone.utc)
    try:
        await send_log(embed, after.guild)
    except Exception as e:
        print(f"Failed to send log: {e}")

@bot.event
async def on_guild_update(before, after):
    guild = after.guild if hasattr(after, 'guild') else before
    entry = None
    async for log in guild.audit_logs(limit=5):
        if log.target.id == guild.id:
            entry = log
            break

    embed = None

    if before.name != after.name:
        embed = discord.Embed(
            title=f"{economy_q_edit} Server Name Changed",
            description=f"**Before:** {before.name}\n**After:** {after.name}",
            color=discord.Color.orange()
        )

    elif before.icon != after.icon:
        embed = discord.Embed(
            title=f"{economy_q_image} Server Icon Changed",
            color=discord.Color.orange()
        )
        if before.icon:
            embed.set_thumbnail(url=before.icon.url)
        if after.icon:
            embed.set_image(url=after.icon.url)

    elif before.verification_level != after.verification_level:
        embed = discord.Embed(
            title=f"{economy_q_lock} Verification Level Changed",
            description=f"**Before:** {before.verification_level.name}\n**After:** {after.verification_level.name}",
            color=discord.Color.orange()
        )

    if embed and entry:
        embed.add_field(name="By", value=log_user(entry.user), inline=False)
        embed.timestamp = datetime.now(timezone.utc)
        try:
            await send_log(embed, guild)
        except Exception as e:
            print(f"Failed to send log: {e}")

async def award_chat_xp_background(message):
    try:
        xp_result = await asyncio.to_thread(economy_award_chat_xp, message.author.id)
    except Exception as e:
        print(f"Chat XP skipped for {message.author.id}: {type(e).__name__} - {e}")
        return
    if not xp_result or xp_result["levels_gained"] <= 0:
        return
    try:
        data = await asyncio.to_thread(economy_get_user, message.author.id)
        await message.channel.send(
            f"{economy_q_level_pulse} <@{message.author.id}> leveled up.",
            embed=economy_build_level_up_embed(message.author, data, xp_result),
            allowed_mentions=discord.AllowedMentions.none()
        )
    except Exception as e:
        print(f"Level-up message skipped for {message.author.id}: {type(e).__name__} - {e}")

COMMAND_EXAMPLE_OVERRIDES = {
    "add": ".add @user 1000",
    "addrole": ".addrole @user @role",
    "addtick": ".addtick @user 5",
    "alarm": ".alarm 1h reminder",
    "analyse": ".analyse while replying to an image",
    "analyze": ".analyze while replying to an image",
    "ban": ".ban @user reason",
    "blackjack": ".blackjack 1000",
    "block": ".block @user",
    "buytick": ".buytick 3",
    "c4": ".c4 @user 1000",
    "cardladder": ".cardladder 1000",
    "cf": ".cf 1000 h",
    "chess": ".chess @user 1000",
    "define": ".define example",
    "deleterole": ".deleterole @role",
    "disable": ".disable command",
    "editactivity": ".editactivity channel #activity",
    "editlottery": ".editlottery duration 12h",
    "enable": ".enable command",
    "find": ".find 885548126365171824",
    "flagquiz": ".flagquiz",
    "fsleep": ".fsleep @user 1h",
    "give": ".give @user 1000",
    "giveaway": ".giveaway 10m prize",
    "kick": ".kick @user reason",
    "lockpick": ".lockpick 1000",
    "heist": ".heist 1000",
    "diceduel": ".diceduel 1000",
    "dice": ".dice 1000",
    "cases": ".cases 1000",
    "case": ".case 1000",
    "plinko": ".plinko 1000",
    "luckynumber": ".luckynumber 1000",
    "jackpotspin": ".jackpotspin 1000",
    "dungeon": ".dungeon",
    "gamestats": ".gamestats @user",
    "memory": ".memory 1000",
    "move": ".move e2e4",
    "ms": ".ms 1000",
    "poll": ".poll Best color? | Blue | Red | 10m",
    "prefix": ".prefix !",
    "preifx": ".preifx !",
    "purge": ".purge @user 20",
    "reopen": ".reopen",
    "remove": ".remove @user 1000",
    "removerole": ".removerole @user @role",
    "reply": ".reply <message id/link> message",
    "roulette": ".roulette 1000 red",
    "rpurge": ".rpurge @user 20",
    "rshut": ".rshut @user",
    "scratch": ".scratch 1000",
    "send": ".send #channel message",
    "setbday": ".setbday 25/12",
    "setbdaychannel": ".setbdaychannel #birthdays",
    "setnick": ".setnick @user new nickname",
    "setprefix": ".setprefix !",
    "setquesos": ".setquesos @user 1m",
    "settick": ".settick @user 10",
    "shut": ".shut @user",
    "slots": ".slots 1000",
    "steal": ".steal <:emoji:123456789>",
    "summon": ".summon @user",
    "summon2": ".summon2 @user",
    "timer": ".timer 10m study",
    "tower": ".tower 1000",
    "translate": ".translate hello to Italian",
    "ttt": ".ttt @user 1000",
    "unban": ".unban 885548126365171824",
    "unblock": ".unblock @user",
    "unmute": ".unmute @user",
    "unrshut": ".unrshut @user",
    "unshut": ".unshut @user",
    "vault": ".vault 1000",
    "wake": ".wake @user",
    "wheel": ".wheel 1000",
}

ARGUMENT_HINTS = {
    "amount": "Use a number like `1000`, `4k`, or `all` where allowed.",
    "member": "Mention a member or paste their user ID.",
    "user": "Mention a user or paste their user ID.",
    "target": "Mention the target or paste their ID.",
    "role": "Mention a role or paste its ID.",
    "channel": "Mention a channel like `#general` or paste its ID.",
    "time": "Use time like `30s`, `10m`, `2h`, or `1d`.",
    "duration": "Use time like `30s`, `10m`, `2h`, or `1d`.",
    "message": "Type the message text after the command.",
    "reason": "Type the reason after the member.",
    "choice": "Use one of the choices shown in the command example.",
}

GAMBLING_AMOUNT_COMMANDS = {
    "cf", "flip", "coinflip", "roulette", "slots", "slot", "blackjack", "bj",
    "scratch", "tower", "towers", "qtower", "vault", "memory", "mem",
    "cardladder", "ladder", "cards", "cladder", "lockpick", "lp", "picklock",
    "heist", "robbery", "qh", "diceduel", "dice", "dd", "cases", "case",
    "qcase", "open", "plinko", "plink", "drop", "luckynumber", "ln",
    "lucky", "number", "jackpotspin", "jackpot", "jspin", "jps", "ms",
    "minesweeper", "minesweepeer", "wheel", "spin",
}

def command_usage_example(ctx):
    prefix = getattr(ctx, "prefix", prefix_for_guild(ctx.guild))
    command = ctx.command
    if not command:
        return f"{prefix}help"
    example = COMMAND_EXAMPLE_OVERRIDES.get(command.name)
    if not example:
        for alias in command.aliases:
            example = COMMAND_EXAMPLE_OVERRIDES.get(alias)
            if example:
                break
    if example:
        return example.replace(".", prefix, 1)
    usage = f"{prefix}{command.qualified_name}"
    if command.signature:
        usage += f" {command.signature}"
    return usage

def command_argument_hint(error, ctx=None):
    param_name = getattr(getattr(error, "param", None), "name", "")
    command_name = getattr(getattr(ctx, "command", None), "name", "")
    command_aliases = set(getattr(getattr(ctx, "command", None), "aliases", []) or [])
    if param_name == "amount":
        command_keys = {command_name, *command_aliases}
        if command_keys & GAMBLING_AMOUNT_COMMANDS:
            return "Use a number like `1000`, `4k`, or `all`. Gambling commands cap normal users at `200k`."
        return "Use a number like `1000`, `4k`, `1m`, `1.5b`, or `all` where allowed."
    if param_name in ARGUMENT_HINTS:
        return ARGUMENT_HINTS[param_name]
    if isinstance(error, (commands.MemberNotFound, commands.UserNotFound)):
        return "Mention them or paste their user ID."
    if isinstance(error, commands.ChannelNotFound):
        return "Mention the channel like `#channel` or paste its ID."
    if isinstance(error, commands.RoleNotFound):
        return "Mention the role like `@role` or paste its ID."
    return None

async def send_command_usage_correction(ctx, error=None):
    usage = command_usage_example(ctx)
    hint = command_argument_hint(error, ctx)
    lines = [f"Type: `{usage}`"]
    if hint:
        lines.append(hint)
    if ctx.command:
        prefix = getattr(ctx, "prefix", prefix_for_guild(ctx.guild))
        lines.append(f"More help: `{prefix}help {ctx.command.qualified_name}` or `{prefix}explain {ctx.command.qualified_name}`")
    await ctx.send("\n".join(lines), allowed_mentions=discord.AllowedMentions.none())

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    is_command = looks_like_command_message(message)
    if is_command:
        if message.guild:
            if message.channel.id in guild_shutdown_channels(message.guild) and not has_owner_power(message.author, message.guild):
                try:
                    await message.delete()
                except:
                    pass
                return

            if not has_owner_power(message.author, message.guild):
                normalized_content = normalize(message.content)
                for phrase in guild_censored_phrases(message.guild):
                    if phrase in normalized_content:
                        try:
                            await message.delete()
                        except discord.Forbidden:
                            pass
                        return

            if message.author.id in guild_watchlist(message.guild) and not has_owner_power(message.author, message.guild):
                try:
                    await message.delete()
                except:
                    pass
                return

        ctx = await bot.get_context(message)
        if ctx.valid:
            print(
                f"Command received: {ctx.command} by {message.author} "
                f"({message.author.id}) in guild {message.guild.id if message.guild else 'DM'}"
            )
            started = time.perf_counter()
            await bot.invoke(ctx)
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            name = ctx.command.qualified_name if ctx.command else "unknown"
            stats = command_timing_stats.setdefault(name, {"count": 0, "total_ms": 0, "max_ms": 0})
            stats["count"] += 1
            stats["total_ms"] += elapsed_ms
            stats["max_ms"] = max(stats["max_ms"], elapsed_ms)
            if elapsed_ms >= 1500:
                print(f"Slow command: {name} took {elapsed_ms}ms in guild {message.guild.id if message.guild else 'DM'}")
        return

    # AI mention handling
    content = message.content.strip()
    is_mention = message.mentions and any(u.id == bot.user.id for u in message.mentions)
    
    mention_patterns = [f"<@{bot.user.id}>", f"<@!{bot.user.id}>", f"<@{bot.user.id}"]
    is_mention_start = any(content.startswith(p) for p in mention_patterns)
    
    if is_mention and is_mention_start and GROQ_API_KEY:
        # Extract question after mention
        for p in mention_patterns:
            if content.startswith(p):
                question = content[len(p):].strip()
                break
        
        if question:
            await message.channel.typing()
            
            messages = [
                {"role": "system", "content": "You are a helpful assistant. Answer clearly, simply, and briefly. If you use information from the web, cite your sources."}
            ]
            messages.append({"role": "user", "content": question})
            
            headers = {
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            }
            payload = {
                "messages": messages,
                "model": "llama-3.1-8b-instant",
                "temperature": 0.7,
                "max_tokens": 500
            }
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post("https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            answer = data["choices"][0]["message"]["content"]
                            if len(answer) > 1900:
                                answer = answer[:1897] + "..."
                            await message.channel.send(answer)
                        else:
                            await message.channel.send(f"Error: {resp.status}")
            except Exception as e:
                await message.channel.send(f"Error: {str(e)[:100]}")
            return

    if message.guild and message.channel.id in guild_shutdown_channels(message.guild) and not has_owner_power(message.author, message.guild):
        try:
            await message.delete()
        except:
            pass
        return

    if message.guild and not has_owner_power(message.author, message.guild):
        content = normalize(message.content)
        for phrase in guild_censored_phrases(message.guild):
            if phrase in content:
                try:
                    await message.delete()
                except discord.Forbidden:
                    pass
                return

    for mentioned_user in message.mentions:
        if mentioned_user.id in afk_users or mentioned_user.id in sleeping_users:
            if mentioned_user.id not in user_mentions:
                user_mentions[mentioned_user.id] = []
            user_mentions[mentioned_user.id].append((
                message.author.id,
                message.jump_url,
                int(message.created_at.timestamp())
            ))

    if message.author.id in sleeping_users:
        start = sleeping_users.pop(message.author.id)
        remove_sleeping_user(message.author.id)
        duration = datetime.now(timezone.utc) - start
        days, remainder = divmod(int(duration.total_seconds()), 86400)
        hours, remainder = divmod(remainder, 3600)
        mins = remainder // 60
        formatted = " ".join([f"{days}d" if days else "", f"{hours}h" if hours else "", f"{mins}m" if mins or not (days or hours) else ""]).strip()

        embed = discord.Embed(
            title=f"{economy_q_bell} Good morning",
            description=f"<@{message.author.id}> was sleeping for **{formatted}**.",
            color=0xF1C40F,
            timestamp=datetime.now(timezone.utc)
        )

        mentions_list = user_mentions.pop(message.author.id, [])
        if mentions_list:
            embed.add_field(name="Mentions received", value=f"You received **{len(mentions_list)}** mentions:", inline=False)
            for uid, link, ts in mentions_list:
                embed.add_field(
                    name="Mention",
                    value=f"<@{uid}> — <t:{ts}:R> — [Click to view message]({link})",
                    inline=True
                )

        await message.channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

    for uid in list(sleeping_users):
        if any(user.id == uid for user in message.mentions) or (
            message.reference and message.reference.resolved and message.reference.resolved.author.id == uid
        ):
            sleep_messages = [
                f"Shut up you’re gonna wake them up {economy_q_sleep}.",
                f"Let the thing sleep peacefully {economy_q_sleep}"
            ]
            chosen_msg = random.choice(sleep_messages)
            await message.reply(chosen_msg, mention_author=True)
            break

    for user in message.mentions:
        if user.id in afk_users:
            afk_data = afk_users[user.id]
            duration = datetime.now(timezone.utc) - afk_data["since"]
            days, remainder = divmod(int(duration.total_seconds()), 86400)
            hours, remainder = divmod(remainder, 3600)
            mins = remainder // 60
            formatted = " ".join([f"{days}d" if days else "", f"{hours}h" if hours else "", f"{mins}m" if mins or not (days or hours) else ""]).strip()
            reason = afk_data['reason']
            reason_text = f": **{reason}**" if reason.lower() != "afk" else ""
            await message.channel.send(
                f"<@{user.id}> is AFK{reason_text}",
                allowed_mentions=discord.AllowedMentions.none()
            )
            break

    if message.author.id in afk_users:
        afk_data = afk_users.pop(message.author.id)
        remove_afk_user(message.author.id)

        duration = datetime.now(timezone.utc) - afk_data["since"]
        days, remainder = divmod(int(duration.total_seconds()), 86400)
        hours, remainder = divmod(remainder, 3600)
        mins = remainder // 60
        formatted = " ".join([f"{days}d" if days else "", f"{hours}h" if hours else "", f"{mins}m" if mins or not (days or hours) else ""]).strip()
        reason = afk_data['reason']
        reason_text = f": **{reason}**" if reason.lower() != "afk" else ""

        embed = discord.Embed(
            title="Welcome back",
            description=f"<@{message.author.id}> was AFK for **{formatted}**{reason_text}",
            color=0x2ECC71,
            timestamp=datetime.now(timezone.utc)
        )

        mentions_list = user_mentions.pop(message.author.id, [])
        if mentions_list:
            embed.add_field(name="Mentions received", value=f"You received **{len(mentions_list)}** mentions:", inline=False)
            for uid, link, ts in mentions_list:
                embed.add_field(
                    name="Mention",
                    value=f"<@{uid}> — <t:{ts}:R> — [Click to view message]({link})",
                    inline=True
                )

        await message.channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

    if message.guild and message.author.id in guild_watchlist(message.guild) and not has_owner_power(message.author, message.guild):
        try:
            await message.delete()
        except:
            pass
        return

    if message.guild and message.author.id not in guild_blacklisted_users(message.guild):
        if message.guild.id in guild_activity_channels:
            activity_buffer[(message.guild.id, message.author.id, "messages")] += 1
        now_ts = time.time()
        last_xp = chat_xp_memory.get(message.author.id, 0)
        if now_ts - last_xp >= 60:
            chat_xp_memory[message.author.id] = now_ts
            asyncio.create_task(award_chat_xp_background(message))

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        print(f"Command not found: {ctx.message.content} by {ctx.author} ({ctx.author.id})")
        return

    elif isinstance(error, CommandDisabledError):
        print(f"Command disabled: {error.command_name} for {ctx.author} ({ctx.author.id})")
        await ctx.send(f"**{error.command_name}** is disabled.")

    elif isinstance(error, commands.CheckFailure):
        print(f"Command check failed: {ctx.command} for {ctx.author} ({ctx.author.id}) - {type(error).__name__}: {error}")
        if getattr(ctx, "quewo_cooldown_blocked", False):
            return
        if ctx.author.id in guild_blacklisted_users(ctx.guild):
            await ctx.send(f"LMAO you're blocked you can't use ts {economy_q_reject}")
        else:
            await ctx.send("You can't use that heh")

    elif isinstance(error, commands.MissingPermissions):
        print(f"Command missing permissions: {ctx.command} for {ctx.author} ({ctx.author.id}) - {error}")
        await ctx.send("You don’t have permission to do that.")

    elif isinstance(error, commands.MissingRequiredArgument):
        print(f"Command missing argument: {ctx.command} for {ctx.author} ({ctx.author.id}) - {error}")
        await send_command_usage_correction(ctx, error)

    elif isinstance(error, commands.MemberNotFound):
        await send_command_usage_correction(ctx, error)

    elif isinstance(error, commands.UserNotFound):
        await send_command_usage_correction(ctx, error)

    elif isinstance(error, commands.ChannelNotFound):
        await send_command_usage_correction(ctx, error)

    elif isinstance(error, commands.RoleNotFound):
        await send_command_usage_correction(ctx, error)

    elif isinstance(error, commands.BadArgument):
        print(f"Command bad argument: {ctx.command} for {ctx.author} ({ctx.author.id}) - {error}")
        await send_command_usage_correction(ctx, error)

    else:
        print(f"Unexpected error in {ctx.command}: {type(error).__name__} - {error}")
        if has_owner_power(ctx.author, ctx.guild):
            await ctx.send(fit_discord_content(f"Error: `{error}`"))
        else:
            await ctx.send("You can't use that heh")

HELP_CATEGORIES = {
    "Quewo": [
        "guide", "bal", "profile", "inventory", "quests", "dailychallenge", "streaks", "shop", "cooldowns", "transactions", "lottery", "lotterystats", "buytick",
        "daily", "weekly", "monthly", "cf", "roulette", "slots", "blackjack", "scratch", "tower", "vault", "memory", "cardladder", "lockpick",
        "heist", "diceduel", "cases", "plinko", "luckynumber", "jackpotspin", "dungeon", "ms", "wheel",
        "give", "lb", "gamestats", "achievements", "setbadge", "gamebalance", "gamehistory", "limits", "qstats", "economyaudit", "econhelp", "explain",
    ],
    "Games": ["games", "ttt", "c4", "chess", "move", "resign", "flagquiz", "flagstats", "q", "picker"],
    "Utility": ["help", "userinfo", "pfp", "calc", "define", "timer", "ctimer", "alarm", "poll", "epoll", "translate", "find"],
    "AI": ["ask", "generate", "analyse", "analyze"],
    "Server Tools": [
        "dsnipe", "esnipe", "rsnipe", "rolesinfo", "roleinfo", "purge", "rpurge", "steal",
        "giveaway", "listbans", "listblocks", "listtargets", "listcensors", "lists",
    ],
    "Status": ["afk", "sleep", "wake", "fsleep", "away", "setbday", "removebday", "setbdaychannel", "activity", "activitystats"],
    "Admin": [
        "settings", "setlogs", "prefix", "disable", "enable", "disableall", "enableall", "dclist", "perf", "test", "testlog", "testrlog",
        "endttt", "setnick", "unmute", "kick", "ban", "unban", "addrole", "removerole", "deleterole",
        "lock", "unlock", "lockdown", "reopen", "rlockdown", "runlock", "shut", "unshut", "clearwatchlist", "rshut", "unrshut",
        "send", "reply", "aban", "raban", "abanlist", "summon", "summon2", "block", "unblock",
        "censor", "uncensor", "clearcensors", "editlottery", "stoplottery", "editactivity", "endactivity", "stopactivity",
        "add", "remove", "addtick", "settick", "setquesos",
    ],
}

def prefix_for_guild(guild):
    guild_id = guild.id if guild else 0
    return guild_prefixes.get(guild_id, DEFAULT_PREFIX)

def render_help_embed(guild=None, category_name=None):
    current_prefix = prefix_for_guild(guild)
    if category_name:
        names = HELP_CATEGORIES.get(category_name, [])
        embed = discord.Embed(
            title=f"ProQue Help: {category_name}",
            description=f"Use `{current_prefix}help <command>` for usage or `{current_prefix}explain <command>` for details.",
            color=discord.Color.blurple()
        )
        commands_text = []
        for name in names:
            command = get_command_case_insensitive(name)
            if command and not command.hidden:
                commands_text.append(f"`{current_prefix}{command.name}`")
        embed.description += "\n\n" + (" ".join(commands_text) if commands_text else "No commands loaded for this category.")
        return embed

    embed = discord.Embed(
        title="ProQue Help",
        description=f"Pick a category below, or use `{current_prefix}help <command>`.",
        color=discord.Color.blurple()
    )
    for category, names in HELP_CATEGORIES.items():
        loaded = [name for name in names if get_command_case_insensitive(name)]
        if loaded:
            embed.add_field(name=category, value=f"{len(loaded)} commands", inline=True)
    return embed

def command_short_description(command):
    if not command:
        return "No extra help available."
    explanation = economy_explanations.get(command.name)
    if explanation:
        return explanation
    for alias in getattr(command, "aliases", []) or []:
        explanation = economy_explanations.get(alias)
        if explanation:
            return explanation
    help_text = (command.help or "").strip()
    if help_text:
        return help_text.splitlines()[0]
    return "No short explanation is written for this command yet."

def command_usage_text(command, prefix):
    example = COMMAND_EXAMPLE_OVERRIDES.get(command.name)
    if example:
        return example.replace(".", prefix, 1) if example.startswith(".") else example
    usage = f"{prefix}{command.qualified_name}"
    if command.signature:
        usage += f" {command.signature}"
    return usage

class HelpCategoryButton(Button):
    def __init__(self, category_name):
        super().__init__(label=category_name, style=discord.ButtonStyle.secondary)
        self.category_name = category_name

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if interaction.user.id != view.author_id:
            return await interaction.response.send_message("Use your own help menu.", ephemeral=True)
        await interaction.response.edit_message(embed=render_help_embed(interaction.guild, self.category_name), view=view)

class HelpHomeButton(Button):
    def __init__(self):
        super().__init__(label="Home", style=discord.ButtonStyle.primary)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if interaction.user.id != view.author_id:
            return await interaction.response.send_message("Use your own help menu.", ephemeral=True)
        await interaction.response.edit_message(embed=render_help_embed(interaction.guild), view=view)

class HelpView(View):
    def __init__(self, author_id):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.add_item(HelpHomeButton())
        for category in HELP_CATEGORIES:
            self.add_item(HelpCategoryButton(category))

@bot.command(name="help")
async def help_command(ctx, command_name: str = None):
    if command_name:
        command = get_command_case_insensitive(command_name)
        if not command:
            return await ctx.send("Command not found.", delete_after=30)

        current_prefix = prefix_for_guild(ctx.guild)
        usage = command_usage_text(command, current_prefix)
        aliases = f"\nAliases: {', '.join(command.aliases)}" if command.aliases else ""
        description = command_short_description(command)
        setup_note = "\nRun it with no arguments to open the setup UI." if command.name in SETUP_UI_COMMANDS else ""
        return await ctx.send(
            f"**{usage}**\n{description}{aliases}{setup_note}",
            view=command_setup_view(ctx.author.id, command.name)
        )

    await ctx.send(embed=render_help_embed(ctx.guild), view=HelpView(ctx.author.id))

def channel_status(guild, channel_id):
    if not channel_id:
        return "Not set"
    channel = guild.get_channel(int(channel_id)) if guild else None
    return channel.mention if channel else f"Missing channel (`{channel_id}`)"

async def build_settings_embed(guild):
    prefix = prefix_for_guild(guild)
    log_config = get_guild_log_config(guild.id) if guild else {}
    birthday_config = guild_birthday_channels.get(guild.id, {}) if guild else {}
    activity_config = guild_activity_channels.get(guild.id, {}) if guild else {}
    try:
        lottery_config = await asyncio.to_thread(economy_get_lottery_config, guild.id) if guild else None
    except Exception:
        lottery_config = None
    disabled = sorted(guild_disabled_commands(guild)) if guild else []

    embed = discord.Embed(
        title=f"{economy_q_permissions} Server Settings",
        description="Current bot setup for this server.",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="Prefix", value=f"`{prefix}`", inline=True)
    embed.add_field(name="Disabled Commands", value=f"{len(disabled)} disabled" if disabled else "None", inline=True)
    embed.add_field(name="Logs", value=channel_status(guild, log_config.get("log_channel_id")), inline=True)
    embed.add_field(name="Reaction Logs", value=channel_status(guild, log_config.get("reaction_log_channel_id")), inline=True)
    embed.add_field(name="Birthdays", value=channel_status(guild, birthday_config.get("channel_id")), inline=True)
    activity_value = channel_status(guild, activity_config.get("channel_id"))
    if activity_config.get("next_report"):
        activity_value += f"\nNext: {discord.utils.format_dt(activity_config['next_report'], 'R')}"
    embed.add_field(name="Activity", value=activity_value, inline=True)
    if lottery_config:
        next_draw = lottery_config.get("next_draw")
        lottery_value = (
            f"{channel_status(guild, lottery_config.get('channel_id'))}\n"
            f"Pot: {economy_format_balance(lottery_config.get('pot') or 0)}"
        )
        if next_draw:
            lottery_value += f"\nNext: {discord.utils.format_dt(next_draw, 'R')}"
    else:
        lottery_value = "Not set"
    embed.add_field(name="Lottery", value=lottery_value, inline=True)
    embed.set_footer(text="Use the buttons below for quick setup actions.")
    return embed

class PrefixSettingsModal(Modal):
    def __init__(self, author_id, current_prefix):
        super().__init__(title="Change Prefix")
        self.author_id = author_id
        self.prefix = TextInput(label="New prefix", placeholder=current_prefix, max_length=5)
        self.add_item(self.prefix)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own settings UI.", ephemeral=True)
        if not can_manage_prefix(interaction.user, interaction.guild):
            return await interaction.response.send_message("You can't change the prefix here.", ephemeral=True)
        new_prefix = str(self.prefix.value).strip()
        if not new_prefix or len(new_prefix) > 5 or any(char.isspace() for char in new_prefix) or new_prefix.startswith("<@"):
            return await interaction.response.send_message("Prefix must be 1-5 characters, no spaces, and not a user mention.", ephemeral=True)
        saved = await asyncio.to_thread(save_guild_prefix, interaction.guild.id, new_prefix)
        if not saved:
            return await interaction.response.send_message("Prefix save failed because the database is unavailable.", ephemeral=True)
        guild_prefixes[interaction.guild.id] = new_prefix
        await interaction.response.send_message(f"Prefix changed to `{new_prefix}`. Press Refresh on the settings panel to update it.", ephemeral=True)

class SettingsView(View):
    def __init__(self, author_id):
        super().__init__(timeout=180)
        self.author_id = author_id

    async def interaction_check(self, interaction):
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Use your own settings panel.", ephemeral=True)
        return False

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary)
    async def refresh(self, interaction, button):
        await interaction.response.edit_message(embed=await build_settings_embed(interaction.guild), view=self)

    @discord.ui.button(label="Prefix", style=discord.ButtonStyle.primary)
    async def prefix_button(self, interaction, button):
        current_prefix = prefix_for_guild(interaction.guild)
        await interaction.response.send_modal(PrefixSettingsModal(self.author_id, current_prefix))

    @discord.ui.button(label="Logs", style=discord.ButtonStyle.secondary)
    async def logs_button(self, interaction, button):
        if not has_owner_power(interaction.user, interaction.guild):
            return await interaction.response.send_message("Admin power only.", ephemeral=True)
        await interaction.response.send_message("Starting log setup in this server.", ephemeral=True)
        await prompt_log_setup(interaction.guild)

    @discord.ui.button(label="Birthdays Here", style=discord.ButtonStyle.secondary)
    async def birthday_button(self, interaction, button):
        if not await can_manage_birthday_channel(interaction.user, interaction.guild):
            return await interaction.response.send_message("You can't change the birthday channel here.", ephemeral=True)
        saved = await asyncio.to_thread(save_guild_birthday_channel, interaction.guild.id, interaction.channel.id, interaction.user.id)
        if not saved:
            return await interaction.response.send_message("Birthday channel save failed.", ephemeral=True)
        guild_birthday_channels[interaction.guild.id] = {"channel_id": interaction.channel.id, "set_by_user_id": interaction.user.id}
        await interaction.response.edit_message(embed=await build_settings_embed(interaction.guild), view=self)

    @discord.ui.button(label="Activity Here", style=discord.ButtonStyle.secondary)
    async def activity_button(self, interaction, button):
        if not await can_manage_activity_channel(interaction.user, interaction.guild):
            return await interaction.response.send_message("You can't change activity reports here.", ephemeral=True)
        ok, message, _ = await save_activity_report_config(interaction.guild, interaction.channel, interaction.user.id)
        if not ok:
            return await interaction.response.send_message(message, ephemeral=True)
        schedule_activity_live_refresh(interaction.guild.id, guild_activity_channels[interaction.guild.id])
        await interaction.response.edit_message(embed=await build_settings_embed(interaction.guild), view=self)

    @discord.ui.button(label="Admin Commands", style=discord.ButtonStyle.secondary)
    async def admin_commands_button(self, interaction, button):
        prefix = prefix_for_guild(interaction.guild)
        await interaction.response.send_message(
            "Quick setup commands:\n"
            f"`{prefix}prefix <new>` - change prefix\n"
            f"`{prefix}setlogs` - setup logs\n"
            f"`{prefix}setbdaychannel #channel` - birthdays\n"
            f"`{prefix}activity setup` - activity reports\n"
            f"`{prefix}lottery` - lottery panel\n"
            f"`{prefix}editlottery <setting> <value>` - lottery settings\n"
            f"`{prefix}disable <command>` / `{prefix}enable <command>` - command access\n"
            f"`{prefix}economyaudit` - economy audit",
            ephemeral=True,
        )

@bot.command(name="settings", aliases=["setup", "config"])
@is_admin_power()
async def settings_command(ctx):
    """Shows the server settings dashboard."""
    if ctx.guild is None:
        return await ctx.send("Settings only work in servers.")
    await ctx.send(embed=await build_settings_embed(ctx.guild), view=SettingsView(ctx.author.id), allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="perf", aliases=["performance", "slowcommands"])
@is_admin_power()
async def perf_command(ctx):
    """Shows slow command timing stats since the last restart."""
    if not command_timing_stats:
        return await ctx.send("No command timing data yet.")
    rows = []
    for name, stats in sorted(command_timing_stats.items(), key=lambda item: item[1]["max_ms"], reverse=True)[:15]:
        count = max(1, int(stats["count"]))
        avg = int(stats["total_ms"] / count)
        rows.append(f"**{name}** - avg `{avg}ms`, max `{int(stats['max_ms'])}ms`, runs `{count}`")
    embed = discord.Embed(
        title=f"{economy_q_perf} Command Performance",
        description="Tracked since this bot process started.",
        color=discord.Color.orange(),
    )
    embed.add_field(name="Slowest", value=joined_embed_value(rows), inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

GAME_MENU = [
    ("PvP", "Tic Tac Toe", None, "`.ttt @user [bet]`", "Quick 2-player strategy. Supports bets."),
    ("PvP", "Connect 4", None, "`.c4 @user [bet]`", "Column strategy game. Supports bets."),
    ("PvP", "Chess", None, "`.chess @user [bet]`", "Full chess with UI moves and 10-minute clocks."),
    ("Skill", "Tower", "tower", "`.tower <amount>`", "Climb floors or cash out."),
    ("Skill", "Vault", "vault", "`.vault <amount>`", "Think through code hints before tries run out."),
    ("Skill", "Memory", "memory", "`.memory <amount>`", "Match pairs before too many mistakes."),
    ("Skill", "Card Ladder", "cardladder", "`.cardladder <amount>`", "Higher/lower card climb with cash-out."),
    ("Skill", "Lockpick", "lockpick", "`.lockpick <amount>`", "Set pins using high/low hints before tries run out."),
    ("Skill", "Minesweeper", "ms", "`.ms <amount>`", "Reveal safe tiles and avoid bombs."),
    ("Luck", "Heist", "heist", "`.heist <amount>`", "Pick a route. Riskier routes pay more."),
    ("Luck", "Dice Duel", "diceduel", "`.diceduel <amount>`", "Roll against the dealer."),
    ("Luck", "Q Cases", "cases", "`.cases <amount>`", "Open weighted prize cases."),
    ("Luck", "Plinko", "plinko", "`.plinko <amount>`", "Drop into multiplier slots."),
    ("Luck", "Lucky Number", "luckynumber", "`.luckynumber <amount>`", "Choose mode, range, and limited tries."),
    ("Luck", "Jackpot Spin", "jackpotspin", "`.jackpotspin <amount>`", "Pick a target, then spin up to 3 times."),
    ("Solo", "Dungeon", "dungeon", "`.dungeon`", "Free solo rooms with HP, keys, relics, and loot."),
    ("Solo", "Flag Quiz", None, "`.flagquiz`", "Guess 10, 20, 50, or all 197 flags for points."),
    ("Utility", "Picker", None, "`.picker`", "Randomly picks from options."),
]

GAME_FILTERS = {
    "All": lambda item: True,
    "Solo": lambda item: item[0] == "Solo",
    "PvP": lambda item: item[0] == "PvP",
    "Skill": lambda item: item[0] == "Skill",
    "Luck": lambda item: item[0] == "Luck",
    "Free": lambda item: item[2] in {"dungeon"} or item[1] in {"Flag Quiz", "Picker"},
    "High Risk": lambda item: item[2] and economy_risk_label(item[2]).casefold() in {"high", "extreme", "medium/high", "skill/high"},
    "No Bet": lambda item: item[2] in {"dungeon"} or item[1] in {"Flag Quiz", "Picker"},
}

def games_embed(prefix=".", selected_filter="All"):
    filter_fn = GAME_FILTERS.get(selected_filter, GAME_FILTERS["All"])
    embed = discord.Embed(
        title=f"{economy_q_game_win} Games - {selected_filter}",
        description="Filter by solo, multiplayer, skill, luck, free, high risk, or no-bet games.",
        color=discord.Color.green()
    )
    for category in ["PvP", "Skill", "Luck", "Solo", "Utility"]:
        lines = []
        for item in GAME_MENU:
            item_category, name, game_key, usage, desc = item
            if item_category != category:
                continue
            if not filter_fn(item):
                continue
            risk = f" | Risk: **{economy_risk_label(game_key)}**" if game_key else ""
            lines.append(f"**{name}** - {usage.replace('`.', f'`{prefix}')} — {desc}{risk}")
        if lines:
            embed.add_field(name=category, value=joined_embed_value(lines), inline=False)
    if not embed.fields:
        embed.add_field(name="Games", value="No games match this filter.", inline=False)
    embed.set_footer(text="Use .explain <game> for full rules.")
    return embed

class GamesFilterButton(Button):
    def __init__(self, filter_name):
        super().__init__(label=filter_name, emoji=reaction_emoji(economy_q_filter), style=discord.ButtonStyle.primary if filter_name == "All" else discord.ButtonStyle.secondary)
        self.filter_name = filter_name

    async def callback(self, interaction):
        view = self.view
        if interaction.user.id != view.author_id:
            return await interaction.response.send_message("Use your own games menu.", ephemeral=True)
        view.selected_filter = self.filter_name
        for item in view.children:
            if isinstance(item, GamesFilterButton):
                item.style = discord.ButtonStyle.primary if item.filter_name == self.filter_name else discord.ButtonStyle.secondary
        await interaction.response.edit_message(embed=games_embed(view.prefix, self.filter_name), view=view)

class GamesView(View):
    def __init__(self, author_id, prefix):
        super().__init__(timeout=180)
        self.author_id = author_id
        self.prefix = prefix
        self.selected_filter = "All"
        for filter_name in ["All", "Solo", "PvP", "Skill", "Luck", "Free", "High Risk", "No Bet"]:
            self.add_item(GamesFilterButton(filter_name))

    async def interaction_check(self, interaction):
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Use your own games menu.", ephemeral=True)
        return False

    @discord.ui.select(
        placeholder="Choose a game",
        options=[
            discord.SelectOption(
                label=name,
                description=(f"{category} | Risk: {economy_risk_label(game_key)} | {desc}" if game_key else f"{category}: {desc}")[:95],
                value=name
            )
            for category, name, game_key, _, desc in GAME_MENU[:25]
        ]
    )
    async def select_game(self, interaction, select):
        selected = select.values[0]
        usage = next((usage for _, name, _, usage, _ in GAME_MENU if name == selected), None)
        usage = usage.replace("`.", f"`{self.prefix}") if usage else f"`{self.prefix}help games`"
        await interaction.response.send_message(f"Start with {usage}", ephemeral=True)

@bot.command(name="games", aliases=["gamelist"])
async def games_command(ctx):
    """Shows available games and how to start them."""
    prefix = prefix_for_guild(ctx.guild)
    await ctx.send(embed=games_embed(prefix), view=GamesView(ctx.author.id, prefix))

FLAG_COUNTRY_ROWS = """
AF|Afghanistan|
AL|Albania|
DZ|Algeria|
AD|Andorra|
AO|Angola|
AG|Antigua and Barbuda|
AR|Argentina|
AM|Armenia|
AU|Australia|
AT|Austria|
AZ|Azerbaijan|
BS|Bahamas|
BH|Bahrain|
BD|Bangladesh|
BB|Barbados|
BY|Belarus|
BE|Belgium|
BZ|Belize|
BJ|Benin|
BT|Bhutan|
BO|Bolivia|
BA|Bosnia and Herzegovina|bosnia
BW|Botswana|
BR|Brazil|
BN|Brunei|
BG|Bulgaria|
BF|Burkina Faso|
BI|Burundi|
CV|Cabo Verde|cape verde
KH|Cambodia|
CM|Cameroon|
CA|Canada|
CF|Central African Republic|car
TD|Chad|
CL|Chile|
CN|China|
CO|Colombia|
KM|Comoros|
CG|Congo|republic of the congo
CR|Costa Rica|
CI|Cote d'Ivoire|ivory coast
HR|Croatia|
CU|Cuba|
CY|Cyprus|
CZ|Czechia|czech republic
CD|Democratic Republic of the Congo|dr congo, drc, congo kinshasa
DK|Denmark|
DJ|Djibouti|
DM|Dominica|
DO|Dominican Republic|
EC|Ecuador|
EG|Egypt|
SV|El Salvador|
GQ|Equatorial Guinea|
ER|Eritrea|
EE|Estonia|
SZ|Eswatini|swaziland
ET|Ethiopia|
FJ|Fiji|
FI|Finland|
FR|France|
GA|Gabon|
GM|Gambia|the gambia
GE|Georgia|
DE|Germany|
GH|Ghana|
GR|Greece|
GD|Grenada|
GT|Guatemala|
GN|Guinea|
GW|Guinea-Bissau|guinea bissau
GY|Guyana|
HT|Haiti|
HN|Honduras|
HU|Hungary|
IS|Iceland|
IN|India|
ID|Indonesia|
IR|Iran|
IQ|Iraq|
IE|Ireland|
IL|Israel|
IT|Italy|
JM|Jamaica|
JP|Japan|
JO|Jordan|
KZ|Kazakhstan|
KE|Kenya|
KI|Kiribati|
XK|Kosovo|
KW|Kuwait|
KG|Kyrgyzstan|
LA|Laos|
LV|Latvia|
LB|Lebanon|
LS|Lesotho|
LR|Liberia|
LY|Libya|
LI|Liechtenstein|
LT|Lithuania|
LU|Luxembourg|
MG|Madagascar|
MW|Malawi|
MY|Malaysia|
MV|Maldives|
ML|Mali|
MT|Malta|
MH|Marshall Islands|
MR|Mauritania|
MU|Mauritius|
MX|Mexico|
FM|Micronesia|
MD|Moldova|
MC|Monaco|
MN|Mongolia|
ME|Montenegro|
MA|Morocco|
MZ|Mozambique|
MM|Myanmar|burma
NA|Namibia|
NR|Nauru|
NP|Nepal|
NL|Netherlands|holland
NZ|New Zealand|
NI|Nicaragua|
NE|Niger|
NG|Nigeria|
KP|North Korea|
MK|North Macedonia|macedonia
NO|Norway|
OM|Oman|
PK|Pakistan|
PW|Palau|
PS|Palestine|
PA|Panama|
PG|Papua New Guinea|
PY|Paraguay|
PE|Peru|
PH|Philippines|
PL|Poland|
PT|Portugal|
QA|Qatar|
RO|Romania|
RU|Russia|
RW|Rwanda|
KN|Saint Kitts and Nevis|st kitts and nevis
LC|Saint Lucia|st lucia
VC|Saint Vincent and the Grenadines|st vincent
WS|Samoa|
SM|San Marino|
ST|Sao Tome and Principe|sao tome
SA|Saudi Arabia|
SN|Senegal|
RS|Serbia|
SC|Seychelles|
SL|Sierra Leone|
SG|Singapore|
SK|Slovakia|
SI|Slovenia|
SB|Solomon Islands|
SO|Somalia|
ZA|South Africa|
KR|South Korea|
SS|South Sudan|
ES|Spain|
LK|Sri Lanka|
SD|Sudan|
SR|Suriname|
SE|Sweden|
CH|Switzerland|
SY|Syria|
TW|Taiwan|
TJ|Tajikistan|
TZ|Tanzania|
TH|Thailand|
TL|Timor-Leste|east timor, timor leste
TG|Togo|
TO|Tonga|
TT|Trinidad and Tobago|
TN|Tunisia|
TR|Turkey|turkiye
TM|Turkmenistan|
TV|Tuvalu|
UG|Uganda|
UA|Ukraine|
AE|United Arab Emirates|uae
GB|United Kingdom|uk, great britain, britain
US|United States|usa, us, america, united states of america
UY|Uruguay|
UZ|Uzbekistan|
VU|Vanuatu|
VA|Vatican City|vatican, holy see
VE|Venezuela|
VN|Vietnam|viet nam
YE|Yemen|
ZM|Zambia|
ZW|Zimbabwe|
""".strip()

def normalize_flag_answer(value):
    value = value.casefold().strip()
    value = re.sub(r"^the\s+", "", value)
    value = value.replace("&", "and")
    return re.sub(r"[^a-z0-9]+", "", value)

def flag_image_url(code):
    return f"https://flagcdn.com/w640/{code.lower()}.png"

def parse_flag_countries():
    countries = []
    for row in FLAG_COUNTRY_ROWS.splitlines():
        code, name, aliases = (row.split("|") + ["", ""])[:3]
        accepted = {normalize_flag_answer(name)}
        for alias in aliases.split(","):
            alias = alias.strip()
            if alias:
                accepted.add(normalize_flag_answer(alias))
        countries.append({
            "code": code,
            "name": name,
            "accepted": accepted,
        })
    return countries

FLAG_COUNTRIES = parse_flag_countries()
FLAG_QUIZ_ROUND_OPTIONS = {10: "10 Flags", 20: "20 Flags", 50: "50 Flags", len(FLAG_COUNTRIES): f"All ({len(FLAG_COUNTRIES)})"}
FLAG_QUIZ_REWARD_PER_POINT = 20_000
active_flag_quizzes = set()

def parse_flag_round_count(value):
    if value is None:
        return None
    value = str(value).strip().casefold()
    if value == "all":
        return len(FLAG_COUNTRIES)
    try:
        count = int(value)
    except ValueError:
        return None
    return count if count in FLAG_QUIZ_ROUND_OPTIONS else None

def edit_distance_limited(left, right, limit):
    if abs(len(left) - len(right)) > limit:
        return limit + 1
    previous = list(range(len(right) + 1))
    for i, left_char in enumerate(left, 1):
        current = [i]
        row_min = i
        for j, right_char in enumerate(right, 1):
            cost = 0 if left_char == right_char else 1
            current_value = min(
                previous[j] + 1,
                current[j - 1] + 1,
                previous[j - 1] + cost,
            )
            if i > 1 and j > 1 and left_char == right[j - 2] and left[i - 2] == right_char:
                current_value = min(current_value, previous[j - 2] + 1)
            current.append(current_value)
            row_min = min(row_min, current_value)
        if row_min > limit:
            return limit + 1
        previous = current
    return previous[-1]

def flag_answer_matches(guess, country):
    guess_norm = normalize_flag_answer(guess)
    if not guess_norm:
        return False
    if guess_norm in country["accepted"]:
        return True
    for accepted in country["accepted"]:
        if len(accepted) <= 4:
            continue
        if guess_norm[0] != accepted[0]:
            continue
        limit = 1 if len(accepted) <= 7 else 2
        if len(accepted) >= 16:
            limit = 3
        if edit_distance_limited(guess_norm, accepted, limit) <= limit:
            return True
    return False

def flag_country_hint(country):
    name = country["name"]
    parts = []
    for word in re.split(r"(\s+|-)", name):
        if not word or word.isspace() or word == "-":
            parts.append(word)
        elif len(word) <= 2:
            parts.append(word[0] + "_" * (len(word) - 1))
        else:
            parts.append(word[0] + "_" * (len(word) - 2) + word[-1])
    return "".join(parts)

def build_flag_quiz_embed(country, index, total, score_text, mode, hint=None, status=None, seconds_left=30):
    embed = discord.Embed(
        title=f"Flag Quiz {index}/{total}",
        description=(
            f"Mode: **{mode.title()}** | {score_text}\n"
            f"Time: **{int(seconds_left)}s**\n"
            "Type the country name. Mini typos are accepted.\n"
            "Each user gets **2 tries** per flag."
        ),
        color=discord.Color.blurple()
    )
    if hint:
        embed.add_field(name="Hint", value=f"`{hint}`", inline=False)
    if status:
        embed.add_field(name="Status", value=status, inline=False)
    embed.set_image(url=flag_image_url(country["code"]))
    embed.set_footer(text="Type skip to skip, or stop to finish early.")
    return embed

class FlagQuizHintButton(Button):
    def __init__(self):
        super().__init__(label="Show Hint", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction):
        view = self.view
        if view.mode == "solo" and interaction.user.id != view.author_id:
            return await interaction.response.send_message("Use your own flag quiz.", ephemeral=True)
        if view.mode == "public" and interaction.user.id not in view.eligible_user_ids:
            return await interaction.response.send_message("Guess once before using the hint.", ephemeral=True)
        view.hint_requested = True
        for item in view.children:
            item.disabled = True
        await interaction.response.edit_message(
            embed=build_flag_quiz_embed(
                view.country,
                view.index,
                view.total,
                view.score_text,
                view.mode,
                hint=flag_country_hint(view.country),
                status=view.status,
                seconds_left=max(0, int(view.end_time - time.monotonic())),
            ),
            view=view,
            allowed_mentions=discord.AllowedMentions.none(),
        )

class FlagQuizHintView(View):
    def __init__(self, author_id, mode, country, index, total, score_text, status, eligible_user_ids, end_time):
        super().__init__(timeout=30)
        self.author_id = author_id
        self.mode = mode
        self.country = country
        self.index = index
        self.total = total
        self.score_text = score_text
        self.status = status
        self.eligible_user_ids = set(eligible_user_ids)
        self.end_time = end_time
        self.hint_requested = False
        self.add_item(FlagQuizHintButton())

class FlagQuizModeButton(Button):
    def __init__(self, mode):
        super().__init__(label=mode.title(), style=discord.ButtonStyle.primary if mode == "solo" else discord.ButtonStyle.success)
        self.mode = mode

    async def callback(self, interaction):
        view = self.view
        if interaction.user.id != view.author_id:
            return await interaction.response.send_message("Use your own flag quiz menu.", ephemeral=True)
        view.mode = self.mode
        if view.rounds in FLAG_QUIZ_ROUND_OPTIONS:
            for item in view.children:
                item.disabled = True
            await interaction.response.edit_message(
                content=f"{economy_q_game_win} Starting **{view.rounds}** flag quiz rounds in **{self.mode.title()}** mode.",
                view=view
            )
            await run_flag_quiz(view.ctx, view.rounds, self.mode)
            return
        view.show_round_buttons()
        await interaction.response.edit_message(
            content=f"{economy_q_game_win} **FLAG QUIZ**\nMode: **{self.mode.title()}**\nChoose quiz length.",
            view=view
        )

class FlagQuizRoundsButton(Button):
    def __init__(self, rounds):
        super().__init__(label=FLAG_QUIZ_ROUND_OPTIONS[rounds], style=discord.ButtonStyle.primary if rounds < len(FLAG_COUNTRIES) else discord.ButtonStyle.success)
        self.rounds = rounds

    async def callback(self, interaction):
        view = self.view
        if interaction.user.id != view.author_id:
            return await interaction.response.send_message("Use your own flag quiz menu.", ephemeral=True)
        for item in view.children:
            item.disabled = True
        await interaction.response.edit_message(content=f"{economy_q_game_win} Starting **{self.rounds}** flag quiz rounds in **{view.mode.title()}** mode.", view=view)
        await run_flag_quiz(view.ctx, self.rounds, view.mode)

class FlagQuizSetupView(View):
    def __init__(self, ctx, rounds=None):
        super().__init__(timeout=60)
        self.ctx = ctx
        self.author_id = ctx.author.id
        self.mode = "solo"
        self.rounds = rounds
        self.add_item(FlagQuizModeButton("solo"))
        self.add_item(FlagQuizModeButton("public"))

    def show_round_buttons(self):
        self.clear_items()
        for rounds in FLAG_QUIZ_ROUND_OPTIONS:
            self.add_item(FlagQuizRoundsButton(rounds))
        if self.rounds in FLAG_QUIZ_ROUND_OPTIONS:
            for item in self.children:
                if isinstance(item, FlagQuizRoundsButton) and item.rounds != self.rounds:
                    item.disabled = True

def flag_quiz_score_text(scores, author_id, mode):
    if mode == "solo":
        return f"Points: **{scores.get(author_id, 0)}**"
    if not scores:
        return "No points yet."
    top = sorted(scores.items(), key=lambda item: item[1], reverse=True)[:3]
    return "Top: " + " | ".join(f"<@{user_id}> **{points}**" for user_id, points in top)

async def run_flag_quiz(ctx, rounds, mode="solo"):
    quiz_key = (ctx.channel.id, "public" if mode == "public" else ctx.author.id)
    if quiz_key in active_flag_quizzes:
        return await ctx.send(f"{economy_q_warning} You already have a flag quiz running in this channel.")
    active_flag_quizzes.add(quiz_key)
    scores = {}
    answered = {}
    countries = random.sample(FLAG_COUNTRIES, min(rounds, len(FLAG_COUNTRIES)))
    try:
        await ctx.send(
            f"{economy_q_game_win} **FLAG QUIZ**\n"
            f"Mode: **{mode.title()}** | Reward: **{economy_format_balance(FLAG_QUIZ_REWARD_PER_POINT)} per point**\n"
            "Type the country name. Each guess has **30s**. Everyone gets **2 tries** per flag in public mode. Type `skip` or `stop` anytime.",
            allowed_mentions=discord.AllowedMentions.none()
        )
        for index, country in enumerate(countries, 1):
            prompt = await ctx.send(embed=build_flag_quiz_embed(
                country,
                index,
                len(countries),
                flag_quiz_score_text(scores, ctx.author.id, mode),
                mode,
            ))
            stopped = False
            skipped = False
            winner_id = None
            tries_by_user = {}
            end_time = time.monotonic() + 30
            while time.monotonic() < end_time and winner_id is None and not skipped:
                remaining = max(1, end_time - time.monotonic())
                def check(message):
                    if message.channel.id != ctx.channel.id or message.author.bot:
                        return False
                    if mode == "solo" and message.author.id != ctx.author.id:
                        return False
                    return True
                try:
                    guess_message = await bot.wait_for("message", timeout=remaining, check=check)
                except asyncio.TimeoutError:
                    await prompt.edit(
                        embed=build_flag_quiz_embed(
                            country,
                            index,
                            len(countries),
                            flag_quiz_score_text(scores, ctx.author.id, mode),
                            mode,
                            status=f"{economy_q_timeout} Time. Answer: **{country['name']}**",
                            seconds_left=0,
                        ),
                        view=None,
                    )
                    break

                guess = guess_message.content.strip()
                if guess.casefold() == "stop":
                    if mode == "public" and guess_message.author.id != ctx.author.id:
                        continue
                    stopped = True
                    break
                if guess.casefold() == "skip":
                    if mode == "public" and guess_message.author.id != ctx.author.id:
                        continue
                    skipped = True
                    await prompt.edit(
                        embed=build_flag_quiz_embed(
                            country,
                            index,
                            len(countries),
                            flag_quiz_score_text(scores, ctx.author.id, mode),
                            mode,
                            status=f"{economy_q_warning} Skipped. Answer: **{country['name']}**",
                        ),
                        view=None,
                    )
                    break
                user_id = guess_message.author.id
                if tries_by_user.get(user_id, 0) >= 2:
                    continue
                tries_by_user[user_id] = tries_by_user.get(user_id, 0) + 1
                answered[user_id] = answered.get(user_id, 0) + 1
                if flag_answer_matches(guess, country):
                    scores[user_id] = scores.get(user_id, 0) + 1
                    winner_id = user_id
                    await prompt.edit(view=None)
                    await ctx.send(
                        f"{economy_q_accept} <@{user_id}> got it: **{country['name']}**\n"
                        f"{flag_quiz_score_text(scores, ctx.author.id, mode)}",
                        allowed_mentions=discord.AllowedMentions.none()
                    )
                    break
                status = f"{economy_q_reject} <@{user_id}> missed. "
                if tries_by_user[user_id] == 1:
                    status += "**1 try left.** Want help? Press **Show Hint**."
                else:
                    status += "**No tries left for this flag.**"
                end_time = time.monotonic() + 30
                hint_view = None
                eligible_hint_users = {
                    existing_user_id
                    for existing_user_id, tries in tries_by_user.items()
                    if tries >= 1
                }
                if any(tries < 2 for tries in tries_by_user.values()) and time.monotonic() < end_time:
                    hint_view = FlagQuizHintView(
                        ctx.author.id,
                        mode,
                        country,
                        index,
                        len(countries),
                        flag_quiz_score_text(scores, ctx.author.id, mode),
                        status,
                        eligible_hint_users,
                        end_time,
                    )
                await prompt.edit(
                    embed=build_flag_quiz_embed(
                        country,
                        index,
                        len(countries),
                        flag_quiz_score_text(scores, ctx.author.id, mode),
                        mode,
                        status=status,
                        seconds_left=max(0, int(end_time - time.monotonic())),
                    ),
                    view=hint_view,
                    allowed_mentions=discord.AllowedMentions.none()
                )
            if stopped:
                break
            if winner_id:
                await asyncio.sleep(0.4)

        reward_lines = []
        if not scores:
            reward_lines.append("No reward earned.")
        for user_id, points in sorted(scores.items(), key=lambda item: item[1], reverse=True):
            reward = points * FLAG_QUIZ_REWARD_PER_POINT
            try:
                old_balance, new_balance = await asyncio.to_thread(economy_add_user_balance, user_id, reward, reward)
                await asyncio.to_thread(economy_record_game_result, user_id, "flagquiz", points > 0, reward, reward)
                if points > 0 and economy_todays_daily_challenge()["game"] == "flagquiz":
                    await asyncio.to_thread(economy_track_daily_challenge_progress, user_id, "flagquiz", True, points)
                await asyncio.to_thread(economy_log_transaction, user_id, "flagquiz_reward", reward, f"{points} point(s) in {mode} flag quiz")
                reward_lines.append(
                    f"<@{user_id}>: **{points}** point(s), **{economy_format_balance(reward)}** "
                    f"({economy_format_balance(old_balance)} -> {economy_format_balance(new_balance)})"
                )
            except Exception as e:
                print(f"Flag quiz reward failed for {user_id}: {type(e).__name__} - {e}")
                reward_lines.append(f"<@{user_id}>: **{points}** point(s), reward could not be paid.")
        total_points = sum(scores.values())
        accuracy = (total_points / max(1, len(countries))) * 100
        await ctx.send(
            f"{economy_q_game_win} **FLAG QUIZ FINISHED**\n"
            f"Mode: **{mode.title()}** | Total Score: **{total_points}/{len(countries)}** ({accuracy:.1f}%)\n"
            + "\n".join(reward_lines),
            allowed_mentions=discord.AllowedMentions.none()
        )
    finally:
        active_flag_quizzes.discard(quiz_key)

@bot.command(name="flagquiz", aliases=["flags", "fq"])
async def flagquiz(ctx, rounds: str = None):
    """Starts a flag quiz. Pick 10, 20, 50, or all 197 flags, then guess country names with 2 tries per flag."""
    if not await economy_ensure_db_ready(ctx):
        return
    count = parse_flag_round_count(rounds)
    if count is not None:
        return await ctx.send(
            f"{economy_q_game_win} **FLAG QUIZ**\nChoose mode for **{count}** flag rounds.",
            view=FlagQuizSetupView(ctx, count)
        )
    prefix = prefix_for_guild(ctx.guild)
    await ctx.send(
        f"{economy_q_game_win} **FLAG QUIZ**\nChoose solo or public mode, then choose quiz length.\nYou can also use `{prefix}flagquiz 10`, `{prefix}flagquiz 20`, `{prefix}flagquiz 50`, or `{prefix}flagquiz all`.\nEach guess has **30s**. You get **2 tries** per flag, and small typos are accepted.\nReward: **{economy_format_balance(FLAG_QUIZ_REWARD_PER_POINT)} per correct flag**.",
        view=FlagQuizSetupView(ctx)
    )

@bot.command(name="flagstats", aliases=["flagscore", "fqstats"])
async def flagstats(ctx, member: discord.Member = None):
    user = member or ctx.author
    try:
        row = await asyncio.to_thread(economy_get_game_stat, user.id, "flagquiz")
    except Exception:
        await ctx.reply(f"{economy_q_warning} Flag quiz stats are unavailable right now.", mention_author=False)
        return
    if not row:
        await ctx.reply(f"{economy_q_game_win} {user.mention} has no Flag Quiz history yet.", mention_author=False, allowed_mentions=discord.AllowedMentions.none())
        return
    played = int(row["played"] or 0)
    earned = int(row["profit"] or 0)
    points = earned // FLAG_QUIZ_REWARD_PER_POINT if FLAG_QUIZ_REWARD_PER_POINT else 0
    embed = discord.Embed(
        title=f"{economy_q_game_win} Flag Quiz Stats",
        description=(
            f"{user.mention}\n"
            f"Quizzes: **{played:,}**\n"
            f"Scoring runs: **{points:,}**\n"
            f"Rewards Earned: **{economy_format_balance(earned)}**"
        ),
        color=discord.Color.green(),
    )
    await ctx.reply(embed=embed, mention_author=False, allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="run", description="Run any ProQue prefix command through slash commands.")
@app_commands.describe(command="Command name", args="Everything after the command, like @user 1000 or 10m title")
async def slash_run(interaction: discord.Interaction, command: str, args: str = ""):
    await interaction.response.defer(thinking=True)
    await invoke_prefix_command_from_interaction(interaction, command, args)

@slash_run.autocomplete("command")
async def slash_run_command_autocomplete(interaction: discord.Interaction, current: str):
    return slash_command_search(current)

@bot.tree.command(name="commands", description="List slash command access for all ProQue commands.")
async def slash_commands_list(interaction: discord.Interaction):
    commands_ = slash_runnable_commands()
    prefix = prefix_for_guild(interaction.guild)
    lines = []
    for command in commands_:
        alias_text = f" ({', '.join(command.aliases[:3])})" if command.aliases else ""
        lines.append(f"`{command.name}`{alias_text}")
    embed = discord.Embed(
        title="ProQue Slash Commands",
        description=(
            f"Use `/run command args` to run any of the **{len(commands_)}** commands.\n"
            f"Prefix commands still use `{prefix}` in this server."
        ),
        color=discord.Color.blurple()
    )
    for index in range(0, len(lines), 25):
        chunk = "\n".join(lines[index:index + 25])
        embed.add_field(name=f"Commands {index + 1}-{min(index + 25, len(lines))}", value=chunk, inline=True)
        if len(embed.fields) >= 6:
            break
    embed.set_footer(text="Discord limits top-level slash commands to 100, so /run covers every command.")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="help", description="Show ProQue help.")
@app_commands.describe(command="Optional command name")
async def slash_help(interaction: discord.Interaction, command: str = ""):
    if command:
        command_obj = get_command_case_insensitive(command)
        if not command_obj:
            return await interaction.response.send_message("Command not found.", ephemeral=True)
        prefix = prefix_for_guild(interaction.guild)
        usage = command_usage_text(command_obj, prefix)
        aliases = f"\nAliases: {', '.join(command_obj.aliases)}" if command_obj.aliases else ""
        description = command_short_description(command_obj)
        setup_note = "\nRun it through `/run`, or use the setup UI where available." if command_obj.name in SETUP_UI_COMMANDS else "\nRun it through `/run command args`."
        return await interaction.response.send_message(f"**{usage}**\n{description}{aliases}{setup_note}", ephemeral=True)
    await interaction.response.send_message(embed=render_help_embed(interaction.guild), ephemeral=True)

@slash_help.autocomplete("command")
async def slash_help_command_autocomplete(interaction: discord.Interaction, current: str):
    return slash_command_search(current)

@bot.tree.command(name="settings", description="Open the server settings dashboard.")
async def slash_settings(interaction: discord.Interaction):
    if interaction.guild is None:
        return await interaction.response.send_message("Settings only work in servers.", ephemeral=True)
    if not has_owner_power(interaction.user, interaction.guild):
        return await interaction.response.send_message("Admin power only.", ephemeral=True)
    await interaction.response.send_message(
        embed=await build_settings_embed(interaction.guild),
        view=SettingsView(interaction.user.id),
        ephemeral=True
    )

@bot.tree.command(name="games", description="Show ProQue games.")
async def slash_games(interaction: discord.Interaction):
    prefix = prefix_for_guild(interaction.guild)
    await interaction.response.send_message(embed=games_embed(prefix), view=GamesView(interaction.user.id, prefix), ephemeral=True)

class ConfirmActionView(View):
    def __init__(self, author_id, label="Confirm"):
        super().__init__(timeout=45)
        self.author_id = author_id
        self.label = label
        self.value = None

    async def interaction_check(self, interaction):
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("This confirmation is not for you.", ephemeral=True)
        return False

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction, button):
        self.value = True
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content=f"{economy_q_accept} Confirmed: {self.label}", view=self)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction, button):
        self.value = False
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Cancelled.", view=self)
        self.stop()

async def confirm_admin_action(ctx, title, details, danger=True):
    view = ConfirmActionView(ctx.author.id, title)
    color = discord.Color.red() if danger else discord.Color.orange()
    embed = discord.Embed(title=title, description=details, color=color)
    msg = await ctx.send(embed=embed, view=view, allowed_mentions=discord.AllowedMentions.none())
    await view.wait()
    if view.value is None:
        for item in view.children:
            item.disabled = True
        try:
            await msg.edit(content="Confirmation timed out.", embed=None, view=view)
        except discord.HTTPException:
            pass
        return False
    return bool(view.value)

@bot.event
async def on_message_delete(message):
    if not message.content and not message.attachments:
        return

    content = message.content or ""
    deleter = "Unknown"

    if message.guild:
        async for entry in message.guild.audit_logs(limit=5, action=discord.AuditLogAction.message_delete):
            if entry.target.id == message.author.id and (datetime.now(timezone.utc) - entry.created_at).total_seconds() < 5:
                deleter = log_user(entry.user)
                break

    embed = discord.Embed(
        title=f"{economy_q_trash} Message Deleted",
        color=discord.Color.red()
    )
    embed.add_field(name="User", value=log_user(message.author), inline=False)
    embed.add_field(name="Deleted by", value=deleter, inline=False)
    embed.add_field(name="Channel", value=message.channel.mention, inline=False)
    embed.timestamp = datetime.now(timezone.utc)

    if message.attachments:
        first = message.attachments[0]
        if first.content_type:
            if first.content_type.startswith("image"):
                embed.set_image(url=first.url)
            elif first.content_type.startswith("video"):
                embed.add_field(name="Video", value=first.url, inline=False)
            elif first.content_type.startswith("audio"):
                embed.add_field(name="Audio", value=first.url, inline=False)
            else:
                embed.add_field(name="Attachment", value=first.url, inline=False)
        else:
            embed.add_field(name="Attachment", value=first.url, inline=False)

        if len(message.attachments) > 1:
            other_urls = [att.url for att in message.attachments[1:]]
            embed.add_field(name="Other Attachments", value="\n".join(other_urls), inline=False)

        content += "\n" + "\n".join([att.url for att in message.attachments])

    embed.add_field(name="Content", value=content[:1024], inline=False)

    deleted_snipes.setdefault(message.channel.id, []).insert(0, (content, message.author, message.created_at))
    deleted_snipes[message.channel.id] = deleted_snipes[message.channel.id][:50]

    if (message.mention_everyone or message.mentions) and not message.author.bot:
        try:
            mentions = []
            if message.mention_everyone:
                mentions.append("@everyone or @here")
            if message.mentions:
                mentions.extend(m.mention for m in message.mentions)

            ghost_embed = discord.Embed(
                title=f"{economy_q_warning} Ghost Ping Detected!",
                description=f"**Author:** {message.author.mention} (`{message.author.id}`)\n"
                            f"**Channel:** {message.channel.mention}\n"
                            f"**Mentions:** {' '.join(mentions)}\n"
                            f"**Message:** {message.content or '*[No content]*'}",
                color=discord.Color.red()
            )
            ghost_embed.set_footer(text="Ghost Ping Log")
            ghost_embed.timestamp = message.created_at

            await send_log(ghost_embed, message.guild)
        except Exception as e:
            print(f"[Ghost Ping Log Error] {e}")

    try:
        await send_log(embed, message.guild)
    except Exception as e:
        print(f"Failed to send log: {e}")

@bot.event
async def on_bulk_message_delete(messages):
    if not messages:
        return

    messages.sort(key=lambda m: m.created_at)

    log_entries = []
    attachments = []

    for i, msg in enumerate(messages, 1):
        content = msg.content.strip() or "[No content]"
        log_entries.append(f"**{i}.** {log_user(msg.author)}: {content}")
        for j, att in enumerate(msg.attachments, 1):
            attachments.append((f"Attachment {i}.{j}", att.url))

    chunks = []
    current = []
    current_len = 0
    for entry in log_entries:
        if current_len + len(entry) > 3800:
            chunks.append(current)
            current = [entry]
            current_len = len(entry)
        else:
            current.append(entry)
            current_len += len(entry)
    if current:
        chunks.append(current)

    for i, chunk in enumerate(chunks, 1):
        embed = discord.Embed(
            title=f"{economy_q_broom} Bulk Messages Deleted (Part {i}/{len(chunks)})",
            description="\n".join(chunk),
            color=discord.Color.red()
        )
        embed.add_field(name="Channel", value=messages[0].channel.mention, inline=False)
        embed.timestamp = datetime.now(timezone.utc)
        try:
            await send_log(embed, messages[0].guild)
        except Exception as e:
            print(f"Failed to send log part {i}: {e}")

    if attachments:
        attach_chunks = [attachments[i:i+5] for i in range(0, len(attachments), 5)]
        for i, group in enumerate(attach_chunks, 1):
            embed = discord.Embed(
                title=f"{economy_q_attachment} Attachments from Purged Messages (Part {i}/{len(attach_chunks)})",
                color=discord.Color.orange()
            )
            for name, url in group:
                embed.add_field(name=name, value=url, inline=False)
            embed.add_field(name="Channel", value=messages[0].channel.mention, inline=False)
            embed.timestamp = datetime.now(timezone.utc)
            try:
                await send_log(embed, messages[0].guild)
            except Exception as e:
                print(f"Failed to send attachment log part {i}: {e}")
    

@bot.event
async def on_message_edit(before, after):
    if before.author.bot and before.author.id != super_owner_id:
        return

    if before.content != after.content:
        edited_snipes.setdefault(before.channel.id, []).insert(0, (
            before.content,
            after.content,
            before.author,
            before.jump_url,
            datetime.now(timezone.utc)
        ))
        edited_snipes[before.channel.id] = edited_snipes[before.channel.id][:50]

        embed = discord.Embed(
        title=f"{economy_q_edit} Message Edited",
            color=discord.Color.orange()
        )
        embed.add_field(name="Author", value=log_user(before.author), inline=False)
        embed.add_field(name="Before", value=embed_value(before.content), inline=False)
        embed.add_field(name="After", value=embed_value(after.content), inline=False)
        embed.add_field(name="Message", value=f"[Jump to Message]({before.jump_url})", inline=False)
        channel_value = before.channel.mention if hasattr(before.channel, "mention") else str(before.channel)
        embed.add_field(name="Channel", value=channel_value, inline=False)
        embed.timestamp = datetime.now(timezone.utc)

        try:
            await send_log(embed, before.guild)
        except Exception as e:
            print(f"Failed to send log: {e}")

async def update_poll_counts(message):
    if message.id not in active_polls:
        return
    
    poll_data = active_polls[message.id]
    
    if poll_data.get("ended"):
        return

    embed = message.embeds[0] if message.embeds else discord.Embed(title=poll_data["question"])
    
    options = poll_data.get("options", ["Yes", "No"])
    use_numbers = poll_data.get("use_numbers", False)
    for idx, opt in enumerate(options):
        count = poll_reaction_count(message, idx, use_numbers, opt)
        set_embed_field(embed, idx, opt, str(count), inline=True)

    try:
        await message.edit(embed=embed)
    except discord.HTTPException as e:
        print(f"Poll count update skipped: {type(e).__name__} - {e}")

def poll_reaction_count(message, option_index, use_numbers, option_name=None):
    if use_numbers:
        if option_index >= len(POLL_NUMBER_EMOJIS):
            return 0
        target = POLL_NUMBER_EMOJIS[option_index]
    elif option_name and option_name.lower() == "yes":
        target = economy_q_accept
    elif option_name and option_name.lower() == "no":
        target = economy_q_reject
    else:
        return 0

    for reaction in message.reactions:
        if same_emoji(reaction.emoji, target):
            return max(0, reaction.count - 1)
    return 0

def parse_poll_duration(value):
    if not value:
        return None
    raw = value.strip().lower()
    if not re.fullmatch(r"(?:\d+\s*[wdhms]\s*)+", raw):
        return None
    weeks = days = hours = minutes = seconds = 0
    for amount, unit in re.findall(r"(\d+)\s*([wdhms])", raw):
        amount = int(amount)
        if unit == "w":
            weeks += amount
        elif unit == "d":
            days += amount
        elif unit == "h":
            hours += amount
        elif unit == "m":
            minutes += amount
        elif unit == "s":
            seconds += amount
    delta = timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)
    return delta if delta.total_seconds() > 0 else None

def duration_seconds(value):
    delta = parse_poll_duration(value)
    if not delta:
        return None
    return int(delta.total_seconds())

def is_duration_piece(value):
    return bool(re.fullmatch(r"\d+\s*[wdhms]", value.strip().lower()))

def split_edge_duration(raw):
    text = raw.strip()
    tokens = split_friendly_words(text)
    if not tokens:
        return None, None

    leading = []
    for token in tokens:
        if not is_duration_piece(token):
            break
        leading.append(token)
    if leading:
        duration_text = " ".join(leading)
        rest = " ".join(tokens[len(leading):]).strip()
        return duration_text, rest

    trailing = []
    for token in reversed(tokens):
        if not is_duration_piece(token):
            break
        trailing.append(token)
    if trailing:
        trailing.reverse()
        duration_text = " ".join(trailing)
        rest = " ".join(tokens[:len(tokens) - len(trailing)]).strip()
        return duration_text, rest

    return None, text

def split_friendly_words(value):
    try:
        return shlex.split(value)
    except ValueError:
        return value.split()

def split_simple_options(value):
    raw = value.strip()
    if "|" in raw:
        return [p.strip() for p in raw.split("|") if p.strip()]
    if "," in raw:
        return [p.strip() for p in raw.split(",") if p.strip()]
    return [p.strip() for p in split_friendly_words(raw) if p.strip()]

def parse_poll_input(args):
    raw = (args or "").strip()
    if not raw:
        return None, None, None

    if "|" in raw:
        parts = [p.strip() for p in raw.split("|") if p.strip()]
        question = parts[0] if parts else None
        remaining = parts[1:]
        delta = parse_poll_duration(remaining[-1]) if remaining else None
        if delta:
            remaining.pop()
        options = remaining if remaining else None
        return question, options, delta

    delta = None
    tokens = split_friendly_words(raw)
    if tokens:
        possible_delta = parse_poll_duration(tokens[-1])
        if possible_delta:
            delta = possible_delta
            raw = raw[:raw.rfind(tokens[-1])].strip()
            tokens = tokens[:-1]

    if "?" in raw:
        question, _, options_text = raw.partition("?")
        question = (question.strip() + "?").strip()
        options = split_simple_options(options_text) if options_text.strip() else None
        return question, options or None, delta

    if "," in raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if len(parts) >= 3:
            return parts[0], parts[1:], delta

    return raw, None, delta

async def send_poll_message(channel, guild, author, question, options=None, delta=None):
    default_options = ["Yes", "No"]
    if not options:
        options = default_options
        use_numbers = False
    else:
        options = [str(opt).strip() for opt in options if str(opt).strip()]
        if len(options) < 2:
            raise ValueError("Custom polls need at least 2 options.")
        if len(options) > len(POLL_NUMBER_EMOJIS):
            raise ValueError(f"Maximum {len(POLL_NUMBER_EMOJIS)} options allowed.")
        use_numbers = True

    end_time = datetime.now(timezone.utc) + delta if delta else None

    embed = discord.Embed(title=f"{economy_q_poll} {question}", color=discord.Color.blue())
    for opt in options:
        embed.add_field(name=opt, value="0", inline=True)
    if end_time:
        embed.add_field(
            name="Ends",
            value=f"{discord.utils.format_dt(end_time, 'R')} ({discord.utils.format_dt(end_time, 'f')})",
            inline=False
        )
        embed.timestamp = end_time
    embed.set_footer(text="Poll")

    msg = await channel.send(embed=embed)

    if use_numbers:
        for i in range(len(options)):
            await msg.add_reaction(reaction_emoji(POLL_NUMBER_EMOJIS[i]))
    else:
        await msg.add_reaction(reaction_emoji(economy_q_accept))
        await msg.add_reaction(reaction_emoji(economy_q_reject))

    active_polls[msg.id] = {
        "question": question,
        "channel_id": channel.id,
        "author_id": author.id,
        "guild_id": guild.id,
        "options": options,
        "use_numbers": use_numbers,
        "end_time": end_time,
        "end_task": None,
        "ended": False
    }
    save_active_poll(msg.id, active_polls[msg.id])

    if end_time:
        async def end_poll_task():
            await asyncio.sleep(max(0, (end_time - datetime.now(timezone.utc)).total_seconds()))
            poll_data = active_polls.get(msg.id)
            if not poll_data or poll_data.get("ended"):
                return
            poll_data["ended"] = True
            save_active_poll(msg.id, poll_data)
            await finalize_poll(msg, poll_data)

        task = asyncio.create_task(end_poll_task())
        active_polls[msg.id]["end_task"] = task
    return msg

class PollSetupModal(Modal):
    def __init__(self, author_id):
        super().__init__(title="Create Poll")
        self.author_id = author_id
        self.question = TextInput(label="Question", placeholder="Best color?", max_length=200)
        self.options = TextInput(
            label="Options",
            placeholder="Leave blank for Yes/No, or enter: Blue, Red, Green",
            required=False,
            style=discord.TextStyle.paragraph,
            max_length=800
        )
        self.duration = TextInput(label="Auto-end time", placeholder="Optional: 10m, 2h, 1d", required=False, max_length=30)
        self.add_item(self.question)
        self.add_item(self.options)
        self.add_item(self.duration)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.defer(ephemeral=True, thinking=True)
        delta = parse_poll_duration(str(self.duration.value).strip()) if str(self.duration.value).strip() else None
        if str(self.duration.value).strip() and not delta:
            return await interaction.followup.send("Use a duration like `10m`, `2h`, or `1d`.", ephemeral=True)
        options = split_simple_options(str(self.options.value)) if str(self.options.value).strip() else None
        try:
            msg = await send_poll_message(interaction.channel, interaction.guild, interaction.user, str(self.question.value).strip(), options, delta)
        except ValueError as e:
            return await interaction.followup.send(str(e), ephemeral=True)
        await interaction.followup.send(f"Poll created: {msg.jump_url}", ephemeral=True)

class OpenPollSetupButton(Button):
    def __init__(self):
        super().__init__(label="Create Poll", style=discord.ButtonStyle.primary, emoji=reaction_emoji(economy_q_poll))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(PollSetupModal(self.view.author_id))

class SingleUserSetupView(View):
    def __init__(self, author_id, button):
        super().__init__(timeout=180)
        self.author_id = author_id
        self.add_item(button)

async def finalize_poll(msg, poll_data):
    """Updates the poll embed with final results."""
    poll_msg = None
    try:
        poll_msg = await bot.get_channel(poll_data["channel_id"]).fetch_message(msg.id)
    except:
        active_polls.pop(msg.id, None)
        remove_active_poll(msg.id)
        return

    try:
        embed = poll_msg.embeds[0] if poll_msg.embeds else discord.Embed(title=poll_data["question"])
        options = poll_data.get("options", ["Yes", "No"])
        use_numbers = poll_data.get("use_numbers", False)

        for idx, opt in enumerate(options):
            count = poll_reaction_count(poll_msg, idx, use_numbers, opt)
            set_embed_field(embed, idx, opt, str(count), inline=True)

        embed.color = discord.Color.green()
        embed.set_footer(text="Poll ended")
        embed.timestamp = datetime.now(timezone.utc)

        await poll_msg.edit(embed=embed)
        author = await bot.fetch_user(poll_data["author_id"])
        await poll_msg.reply(f"{author.mention} your poll has ended!", mention_author=True)
    except Exception as e:
        print(f"Poll finalize warning: {type(e).__name__} - {e}")
    finally:
        active_polls.pop(msg.id, None)
        remove_active_poll(msg.id)

async def restore_persistent_runtime_state():
    now = datetime.now(timezone.utc)

    for poll_id, poll_data in list(active_polls.items()):
        channel = bot.get_channel(poll_data["channel_id"])
        if channel is None:
            try:
                channel = await bot.fetch_channel(poll_data["channel_id"])
            except Exception:
                active_polls.pop(poll_id, None)
                remove_active_poll(poll_id)
                continue
        try:
            message = await channel.fetch_message(poll_id)
        except Exception:
            active_polls.pop(poll_id, None)
            remove_active_poll(poll_id)
            continue

        end_time = poll_data.get("end_time")
        if end_time and end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
            poll_data["end_time"] = end_time

        if end_time is None:
            continue
        if end_time <= now:
            poll_data["ended"] = True
            await finalize_poll(message, poll_data)
            continue

        async def restored_poll_task(message_id=poll_id, poll_message=message):
            poll_data = active_polls.get(message_id)
            if not poll_data:
                return
            end_time = poll_data["end_time"]
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            await asyncio.sleep(max(0, (end_time - datetime.now(timezone.utc)).total_seconds()))
            poll_data = active_polls.get(message_id)
            if not poll_data or poll_data.get("ended"):
                return
            poll_data["ended"] = True
            await finalize_poll(poll_message, poll_data)

        poll_data["end_task"] = asyncio.create_task(restored_poll_task())

    for timer_id, timer_data in list(active_timers.items()):
        channel = bot.get_channel(timer_data["channel_id"])
        if channel is None:
            try:
                channel = await bot.fetch_channel(timer_data["channel_id"])
            except Exception:
                active_timers.pop(timer_id, None)
                remove_active_timer(timer_id)
                continue
        try:
            message = await channel.fetch_message(timer_id)
        except Exception:
            active_timers.pop(timer_id, None)
            remove_active_timer(timer_id)
            continue

        end_time = timer_data["end_time"]
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
            timer_data["end_time"] = end_time
        timer_data["message"] = message
        timer_data["task"] = asyncio.create_task(
            timer_countdown_message(
                channel,
                channel.guild,
                message,
                end_time,
                timer_data["time_str"],
                timer_data["title"],
                timer_data["owner_id"],
            )
        )

@bot.event
async def on_reaction_remove(reaction, user):
    if user.bot and user.id != super_owner_id:
        return

    await update_poll_counts(reaction.message)
    
    msg = reaction.message
    entry = (user, reaction.emoji, msg, datetime.now(timezone.utc).replace(tzinfo=timezone.utc))
    removed_reactions.setdefault(msg.channel.id, []).insert(0, entry)
    removed_reactions[msg.channel.id] = removed_reactions[msg.channel.id][:50]

    # Reaction audit logs are sent from raw reaction events so uncached messages are covered too.

@bot.event
async def on_reaction_add(reaction, user):
    if user.bot and user.id != super_owner_id:
        return
    if reaction.message.channel.id in guild_reaction_shutdown_channels(reaction.message.guild) and not has_owner_power(user, reaction.message.guild):
        try:
            await reaction.remove(user)
        except:
            pass
        return

    if user.id in guild_reaction_watchlist(reaction.message.guild):
        try:
            await reaction.remove(user)
        except:
            pass
        return

    await update_poll_counts(reaction.message)

    # Reaction audit logs are sent from raw reaction events so uncached messages are covered too.

@bot.event
async def on_raw_reaction_add(payload):
    try:
        await log_raw_reaction(payload, added=True)
    except Exception as e:
        print(f"Raw reaction-add log skipped: {type(e).__name__} - {e}")

@bot.event
async def on_raw_reaction_remove(payload):
    try:
        await log_raw_reaction(payload, added=False)
    except Exception as e:
        print(f"Raw reaction-remove log skipped: {type(e).__name__} - {e}")

@bot.event
async def on_raw_reaction_clear(payload):
    channel = bot.get_channel(payload.channel_id)
    if not channel:
        return

    try:
        message = await channel.fetch_message(payload.message_id)
    except Exception as e:
        print(f"Failed to fetch message: {e}")
        return

    if not message.guild:
        return

    try:
        async for entry in message.guild.audit_logs(limit=1, action=discord.AuditLogAction.message_reaction_remove_all):
            remover = entry.user
            break
        else:
            remover = None
    except Exception as e:
        print(f"Failed to fetch audit log: {e}")
        remover = None

    embed = discord.Embed(
        title=f"{economy_q_reaction} All Reactions Removed",
        description=f"All reactions were removed from a [message]({message.jump_url}) in {channel.mention}.",
        color=discord.Color.red()
    )
    if remover:
        embed.add_field(name="By", value=log_user(remover), inline=False)
    else:
        embed.add_field(name="By", value="Unknown", inline=False)
    
    embed.set_footer(text=f"Message ID: {message.id}")
    embed.timestamp = datetime.now(timezone.utc)

    try:
        await send_rlog(embed, message.guild)
    except Exception as e:
        print(f"Failed to send log: {e}")
        
@bot.event
async def on_member_update(before, after):
    await asyncio.sleep(1)

    embed = None
    guild = after.guild

    async def get_action_by(action_types):
        async for entry in guild.audit_logs(limit=10, oldest_first=False):
            if entry.target.id == after.id and entry.action in action_types:
                return log_user(entry.user)
        return None

    if before.nick != after.nick:
        action_by = await get_action_by({discord.AuditLogAction.member_update})
        embed = discord.Embed(
            title=f"{economy_q_user_edit} Nickname Changed",
            color=discord.Color.blue()
        )
        embed.add_field(name="User", value=log_user(before), inline=False)
        embed.add_field(name="Before", value=before.nick or before.name, inline=True)
        embed.add_field(name="After", value=after.nick or after.name, inline=True)
        if action_by:
            embed.add_field(name="Changed by", value=action_by, inline=False)
        embed.timestamp = datetime.now(timezone.utc)
        await send_log(embed, guild)

    before_roles = set(before.roles)
    after_roles = set(after.roles)
    added = after_roles - before_roles
    removed = before_roles - after_roles
    if added or removed:
        action_by = await get_action_by({discord.AuditLogAction.member_role_update})
        embed = discord.Embed(
            title=f"{economy_q_roles} Roles Updated",
            color=discord.Color.teal()
        )
        embed.add_field(name="User", value=log_user(after), inline=False)
        if added:
            embed.add_field(name="Added", value=", ".join(role.name for role in added), inline=True)
        if removed:
            embed.add_field(name="Removed", value=", ".join(role.name for role in removed), inline=True)
        if action_by:
            embed.add_field(name="Updated by", value=action_by, inline=False)
        embed.timestamp = datetime.now(timezone.utc)
        await send_log(embed, guild)

    before_timeout = getattr(before, "communication_disabled_until", None)
    after_timeout = getattr(after, "communication_disabled_until", None)
    if before_timeout != after_timeout:
        action_by = await get_action_by({discord.AuditLogAction.member_update})
        if after_timeout and (after_timeout > datetime.now(timezone.utc)):
            embed = discord.Embed(
                title=f"{economy_q_timeout} Member Timed Out",
                color=discord.Color.orange()
            )
            embed.add_field(name="User", value=log_user(after), inline=False)
            embed.add_field(name="Until", value=f"<t:{int(after_timeout.timestamp())}:F>", inline=False)
            if action_by:
                embed.add_field(name="By", value=action_by, inline=False)
            embed.timestamp = datetime.now(timezone.utc)
        else:
            embed = discord.Embed(
                title=f"{economy_q_accept} Timeout Removed",
                color=discord.Color.green()
            )
            embed.add_field(name="User", value=log_user(after), inline=False)
            if action_by:
                embed.add_field(name="By", value=action_by, inline=False)
            embed.timestamp = datetime.now(timezone.utc)
        await send_log(embed, guild)

@bot.event
async def on_audit_log_entry_create(entry):
    if entry.action == discord.AuditLogAction.member_update:
        target = entry.target
        if not isinstance(target, discord.Member | discord.User):
            return

        after_timeout = getattr(target, "communication_disabled_until", None)

        if after_timeout and after_timeout.timestamp() > datetime.now(timezone.utc).timestamp():
            embed = discord.Embed(
                title=f"{economy_q_timeout} Member Timed Out",
                color=discord.Color.orange()
            )
            embed.add_field(name="User", value=log_user(target), inline=False)
            embed.add_field(name="Until", value=f"<t:{int(after_timeout.timestamp())}:F>", inline=False)
            embed.add_field(name="By", value=log_user(entry.user), inline=False)
            embed.timestamp = datetime.now(timezone.utc)
            try:
                await send_log(embed, entry.guild)
            except Exception as e:
                print(f"Failed to send timeout log: {e}")

        elif after_timeout is None:
            embed = discord.Embed(
                title=f"{economy_q_accept} Timeout Removed",
                color=discord.Color.green()
            )
            embed.add_field(name="User", value=log_user(target), inline=False)
            embed.add_field(name="By", value=log_user(entry.user), inline=False)
            embed.timestamp = datetime.now(timezone.utc)
            try:
                await send_log(embed, entry.guild)
            except Exception as e:
                print(f"Failed to send timeout removal log: {e}")

@bot.event
async def on_user_update(before, after):
    if before.name != after.name:
        embed = discord.Embed(title=f"{economy_q_user_edit} Username Changed", color=discord.Color.blue())
        embed.add_field(name="User", value=log_user(after), inline=False)
        embed.add_field(name="Before", value=before.name, inline=True)
        embed.add_field(name="After", value=after.name, inline=True)
        embed.timestamp = datetime.now(timezone.utc)
        try:
            await send_user_update_log(embed, after.id)
        except Exception as e:
            print(f"Failed to send log: {e}")

    if before.discriminator != after.discriminator:
        embed = discord.Embed(title="Discriminator Changed", color=discord.Color.purple())
        embed.add_field(name="User", value=log_user(after), inline=False)
        embed.add_field(name="Before", value=before.discriminator, inline=True)
        embed.add_field(name="After", value=after.discriminator, inline=True)
        embed.timestamp = datetime.now(timezone.utc)
        try:
            await send_user_update_log(embed, after.id)
        except Exception as e:
            print(f"Failed to send log: {e}")

    if before.avatar != after.avatar:
        embed = discord.Embed(title="Avatar Changed", color=discord.Color.gold())
        embed.add_field(name="User", value=log_user(after), inline=False)
        embed.set_thumbnail(url=before.avatar.url if before.avatar else discord.Embed.Empty)
        embed.set_image(url=after.avatar.url if after.avatar else discord.Embed.Empty)
        embed.timestamp = datetime.now(timezone.utc)
        try:
            await send_user_update_log(embed, after.id)
        except Exception as e:
            print(f"Failed to send log: {e}")

@bot.event
async def on_member_remove(member):
    guild = member.guild
    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.kick):
        if entry.target.id == member.id:
            embed = discord.Embed(
                title=f"{economy_q_hammer} Member Kicked",
                color=discord.Color.red()
            )
            embed.timestamp = datetime.now(timezone.utc)
            embed.add_field(name="User", value=log_user(member), inline=False)
            embed.add_field(name="Kicked by", value=log_user(entry.user), inline=False)
            embed.add_field(name="Reason", value=entry.reason or "No reason provided", inline=False)
            try:
                await send_log(embed, guild)
            except Exception as e:
                print(f"Failed to send log: {e}")
            return

    embed = discord.Embed(
        title="Member Left",
        color=discord.Color.orange()
    )
    embed.timestamp = datetime.now(timezone.utc)
    embed.add_field(name="User", value=log_user(member), inline=False)
    try:
        await send_log(embed, guild)
    except Exception as e:
        print(f"Failed to send log: {e}")
        
@bot.event
async def on_voice_state_update(member, before, after):
    changes = []
    if not before.channel and after.channel:
        changes.append(f"{economy_q_voice} **Joined voice:** {after.channel.mention}")
    elif before.channel and not after.channel:
        changes.append(f"**Left voice:** {before.channel.name}")
    elif before.channel != after.channel:
        changes.append(f"{economy_q_voice} **Moved voice:** {before.channel.name} -> {after.channel.name}")

    if before.self_mute != after.self_mute:
        changes.append(f"{economy_q_timeout if after.self_mute else economy_q_voice} {'Muted' if after.self_mute else 'Unmuted'}")

    if before.self_deaf != after.self_deaf:
        changes.append(f"{economy_q_timeout if after.self_deaf else economy_q_voice} {'Deafened' if after.self_deaf else 'Undeafened'}")

    if changes:
        embed = discord.Embed(
            title=f"{economy_q_voice} Voice State Changed",
            description="\n".join(changes),
            color=discord.Color.blurple()
        )
        embed.add_field(name="User", value=log_user(member), inline=False)
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.timestamp = datetime.now(timezone.utc)
        try:
            await send_log(embed, member.guild)
        except Exception as e:
            print(f"Failed to send log: {e}")

@bot.check
async def globally_block_disabled(ctx):
    disabled = guild_disabled_commands(ctx.guild)
    if ctx.command and ctx.command.name in disabled and not has_super_owner_power(ctx.author, ctx.guild):
        print(f"Disabled command blocked: {ctx.command.name} for {ctx.author} ({ctx.author.id}); disabled={sorted(disabled)}")
        raise CommandDisabledError(ctx.command.name)
    return True

@bot.check
async def block_blacklisted(ctx):
    blocked = guild_blacklisted_users(ctx.guild)
    if ctx.author.id in blocked and not has_owner_power(ctx.author, ctx.guild):
        print(f"Blacklisted user blocked: {ctx.author} ({ctx.author.id}) in guild {ctx.guild.id if ctx.guild else 'DM'}")
        return False
    return True

@bot.command(name="prefix", aliases=["preifx", "setprefix"])
async def prefix_command(ctx, new_prefix: str = None):
    """Shows or changes this server's command prefix."""
    if ctx.guild is None:
        await ctx.send(f"Current prefix: `{DEFAULT_PREFIX}`")
        return

    current_prefix = guild_prefixes.get(ctx.guild.id, DEFAULT_PREFIX)
    if new_prefix is None:
        await ctx.send(f"Current prefix: `{current_prefix}`\nUse `{current_prefix}prefix <new prefix>` to change it.")
        return

    if not can_manage_prefix(ctx.author, ctx.guild):
        await ctx.send("You can't change the prefix here.")
        return

    new_prefix = new_prefix.strip()
    if not new_prefix or len(new_prefix) > 5 or any(char.isspace() for char in new_prefix):
        await ctx.send("Prefix must be 1-5 characters with no spaces.")
        return
    if new_prefix.startswith("<@"):
        await ctx.send("Prefix can't be a user mention.")
        return

    saved = await asyncio.to_thread(save_guild_prefix, ctx.guild.id, new_prefix)
    if not saved:
        await ctx.send("Prefix save failed because the database is unavailable.")
        return

    guild_prefixes[ctx.guild.id] = new_prefix
    await ctx.send(f"Prefix changed to `{new_prefix}`")

@bot.command()
@is_admin_power()
async def disable(ctx, cmd: str):
    if not has_owner_power(ctx.author, ctx.guild):
        return

    command = get_command_case_insensitive(cmd)
    if not command:
        await ctx.send("Command not found.")
        return

    commands_for_guild = guild_disabled_commands(ctx.guild)
    commands_for_guild.add(command.name)
    save_disabled_commands(scoped_id(ctx.guild), commands_for_guild)
    await ctx.send(f"Disabled **{command.name}**")
    
@bot.command()
@is_admin_power()
async def enable(ctx, cmd: str):
    if not has_owner_power(ctx.author, ctx.guild):
        return

    command = get_command_case_insensitive(cmd)
    if not command:
        await ctx.send("Command not found.")
        return

    commands_for_guild = guild_disabled_commands(ctx.guild)
    if command.name in commands_for_guild:
        commands_for_guild.remove(command.name)
        save_disabled_commands(scoped_id(ctx.guild), commands_for_guild)
        await ctx.send(f"Enabled **{command.name}**")
    else:
        await ctx.send(f"**{command.name}** is not disabled.")

@bot.command()
@is_admin_power()
async def disableall(ctx):
    if not has_owner_power(ctx.author, ctx.guild):
        return

    ok = await confirm_admin_action(ctx, "Disable All Commands", "This disables every command except `enableall` for this server.")
    if not ok:
        return
    for command in bot.commands:
        if command.name != "enableall":
            guild_disabled_commands(ctx.guild).add(command.name)
    save_disabled_commands(scoped_id(ctx.guild), guild_disabled_commands(ctx.guild))
    await ctx.send("Disabled **all commands**")

@bot.command()
@is_admin_power()
async def enableall(ctx):
    if not has_owner_power(ctx.author, ctx.guild):
        return

    guild_disabled_commands(ctx.guild).clear()
    save_disabled_commands(scoped_id(ctx.guild), guild_disabled_commands(ctx.guild))
    await ctx.send("Enabled **all commands**")

@bot.command()
@is_admin_power()
async def dclist(ctx):
    commands_for_guild = guild_disabled_commands(ctx.guild)
    if not commands_for_guild:
        await ctx.send("No commands are disabled.")
    else:
        formatted = "\n".join(f"**{name}**" for name in commands_for_guild)
        await ctx.send(f"Disabled Commands:\n{formatted}")

@bot.command()
async def dsnipe(ctx, index: str = "1"):
    try:
        messages = deleted_snipes.get(ctx.channel.id, [])
        if not messages:
            return await ctx.send("Nothing to snipe.")

        n = int(index)
        if n == 0:
            return await ctx.send("Index cannot be 0.")
        if n < 0:
            n = len(messages) + n
        else:
            n -= 1

        if n < -len(messages) or n >= len(messages):
            return await ctx.send("Invalid index. Use a number like `.dsnipe 3` or `.dsnipe -3`.")

        content, author, timestamp = messages[n]
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        unix_time = int(timestamp.timestamp())

        await ctx.send(
            f"Deleted by <@{author.id}> at <t:{unix_time}:f>:\n{content}",
            allowed_mentions=discord.AllowedMentions.none()
        )
    except Exception:
        await ctx.send("Invalid index. Use a number like `.dsnipe 3` or `.dsnipe -3`.")


@bot.command()
async def esnipe(ctx, index: str = "1"):
    try:
        messages = edited_snipes.get(ctx.channel.id, [])
        if not messages:
            return await ctx.send("Nothing to snipe.")

        n = int(index)
        if n == 0:
            return await ctx.send("Index cannot be 0.")
        if n < 0:
            n = len(messages) + n
        else:
            n -= 1

        if n < -len(messages) or n >= len(messages):
            return await ctx.send("Invalid index. Use a number like `.esnipe 2` or `.esnipe -3`.")

        before, after, author, link, timestamp = messages[n]
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        unix_time = int(timestamp.timestamp())

        await ctx.send(
            f"Edited by <@{author.id}> at <t:{unix_time}:f>:\n**Before:** {before}\n**After:** {after}\n[Jump to message]({link})",
            allowed_mentions=discord.AllowedMentions.none()
        )
    except Exception:
        await ctx.send("Invalid index. Use a number like `.esnipe 2` or `.esnipe -3`.")


@bot.command()
async def rsnipe(ctx, index: str = "1"):
    try:
        logs = removed_reactions.get(ctx.channel.id, [])
        if not logs:
            return await ctx.send("Nothing to snipe.")

        n = int(index)
        if n == 0:
            return await ctx.send("Index cannot be 0.")
        if n < 0:
            n = len(logs) + n
        else:
            n -= 1

        if n < -len(logs) or n >= len(logs):
            return await ctx.send("Invalid index. Use a number like `.rsnipe 2` or `.rsnipe -3`.")

        user, emoji, msg, timestamp = logs[n]
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        unix_time = int(timestamp.timestamp())

        await ctx.send(
            f"<@{user.id}> removed {emoji} from [this message]({msg.jump_url}) at <t:{unix_time}:f>.",
            allowed_mentions=discord.AllowedMentions.none()
        )
    except Exception:
        await ctx.send("Invalid index. Use a number like `.rsnipe 2` or `.rsnipe -3`.")

@bot.command()
@is_admin_power()
async def rolesinfo(ctx):
    try:
        roles = ctx.guild.roles[1:]
        if not roles:
            return await ctx.send("No roles found.")

        powerful_roles = []
        bot_roles = []
        no_power_roles = []
        other_roles = []

        for role in roles:
            perms = role.permissions
            perms_list = []

            if perms.administrator:
                perms_list.append("Administrator")
            if perms.manage_guild:
                perms_list.append("Manage Server")
            if perms.kick_members:
                perms_list.append("Kick")
            if perms.ban_members:
                perms_list.append("Ban")
            if perms.manage_roles:
                perms_list.append("Manage Roles")

            perm_text = ", ".join(perms_list) if perms_list else "No powerful perms"

            if perms_list:
                powerful_roles.append(f"`{role.name}` - {perm_text}")
            elif any(member.bot for member in role.members):
                bot_roles.append(f"`{role.name}` - {perm_text}")
            elif not perms_list and not role.members:
                other_roles.append(f"`{role.name}` - {perm_text}")
            else:
                no_power_roles.append(f"`{role.name}` - {perm_text}")

        lines = []

        if powerful_roles:
            lines.append(f"**{economy_q_lock} Roles with Power:**")
            lines.extend(powerful_roles)

        if bot_roles:
            lines.append(f"**{economy_q_permissions} Bot Roles (No Power):**")
            lines.extend(bot_roles)

        if no_power_roles:
            lines.append(f"**{economy_q_roles} Custom Roles (No Power):**")
            lines.extend(no_power_roles)

        if other_roles:
            lines.append(f"**{economy_q_roles} Other Roles:**")
            lines.extend(other_roles)

        if not lines:
            return await ctx.send("No roles to show.")

        chunk = ""
        for line in lines:
            if len(chunk) + len(line) + 1 > 2000:
                await ctx.send(chunk)
                chunk = ""
            chunk += line + "\n"
        if chunk:
            await ctx.send(chunk)

    except Exception as e:
        await ctx.send(f"Error: `{type(e).__name__} - {e}`")

@bot.command()
@is_admin_power()
async def roleinfo(ctx, role: discord.Role):
    members = [member.mention for member in role.members]
    is_admin = role.permissions.administrator

    if is_admin:
        perms_text = f"**Admin Permissions:** {economy_q_accept}"
    else:
        perms = [name.replace('_', ' ').title() for name, value in role.permissions if value]
        perms_text = "**Permissions:**\n" + ", ".join(perms) if perms else "No special permissions."

    member_list = embed_value(", ".join(members) if members else "No members have this role.")
    embed = discord.Embed(title=f"Role Info: {role.name}", color=role.color)
    embed.add_field(name="Members", value=member_list, inline=False)
    embed.add_field(name="Permissions", value=embed_value(perms_text), inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_admin_power()
async def deleterole(ctx, *roles: discord.Role):
    if not roles:
        return await ctx.send("Mention at least one role to delete.")

    deleted = []
    failed = []

    for role in roles:
        try:
            await role.delete()
            deleted.append(role.name)
        except:
            failed.append(role.name)

    response = ""
    if deleted:
        response += f"{economy_q_accept} Deleted roles: {', '.join(deleted)}\n"
    if failed:
        response += f"{economy_q_reject} Failed to delete: {', '.join(failed)}"

    await ctx.send(response or "No roles processed.")

@bot.command()
@is_admin_power()
async def test(ctx):
    await ctx.send("I'm alive heh")

@bot.command()
async def testlog(ctx):
    embed = discord.Embed(title="Test Log", description="This is a test log.", color=discord.Color.green())
    try:
        sent = await send_log(embed, ctx.guild)
        print("DEBUG: testlog command used")
        if not sent:
            await ctx.send("Test log could not send. Check the saved log channel and my View Channel, Send Messages, and Embed Links permissions.")
    except Exception as e:
        print(f"Failed to send test log: {e}")
        await ctx.send("Failed to send test log.")

@bot.command()
async def testrlog(ctx):
    embed = discord.Embed(title="Test Reaction Log", description="This is a test reaction log.", color=discord.Color.green())
    try:
        sent = await send_rlog(embed, ctx.guild)
        print("DEBUG: testrlog command used")
        if not sent:
            await ctx.send("Test reaction log could not send. Check the saved reaction log channel and my View Channel, Send Messages, and Embed Links permissions.")
    except Exception as e:
        print(f"Failed to send test reaction log: {e}")
        await ctx.send("Failed to send test reaction log.")

@bot.command()
async def userinfo(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed = discord.Embed(title="User Info", color=0x3498db)
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="User", value=log_user(member), inline=True)
    embed.add_field(name="ID", value=member.id, inline=True)
    embed.add_field(
        name="Joined Server",
        value=member.joined_at.strftime("%d %b %Y • %H:%M UTC") if member.joined_at else "Unknown",
        inline=False
    )
    embed.add_field(
        name="Created Account",
        value=member.created_at.strftime("%d %b %Y • %H:%M UTC"),
        inline=False
    )
    try:
        user = await bot.fetch_user(member.id)
        if user.bio:
            embed.add_field(name="Bio", value=user.bio, inline=False)
        if user.pronouns:
            embed.add_field(name="Pronouns", value=user.pronouns, inline=True)
    except:
        pass
    await ctx.send(embed=embed)

@bot.command()
async def pfp(ctx, member: discord.Member = None):
    member = member or ctx.author
    await ctx.send(member.display_avatar.url)

class TicTacToeButton(Button):
    def __init__(self, row, col):
        super().__init__(style=discord.ButtonStyle.secondary, label="\u200b", row=row)
        self.row = row
        self.col = col

    async def callback(self, interaction: discord.Interaction):
        game = ttt_games.get(interaction.channel.id)
        if not game or interaction.user != game["players"][game["turn"]]:
            return await interaction.response.send_message("Not your turn.", ephemeral=True)

        board = game["board"]
        if board[self.row][self.col] != TTT_EMPTY:
            return await interaction.response.send_message("That spot is taken.", ephemeral=True)

        mark = TTT_X if game["turn"] == 0 else TTT_O
        board[self.row][self.col] = mark
        self.label = None
        self.emoji = custom_emoji(economy_q_game_x if mark == TTT_X else economy_q_game_o)
        self.disabled = True

        await interaction.response.edit_message(view=game["view"])

        await cancel_game_timeout(game)

        winner = check_winner(board)
        if winner:
            payout_text = await settle_game_bet(game, interaction.user)
            await disable_all_buttons(game["view"])
            await game["msg"].edit(
                content=f"{economy_q_game_win} <@{interaction.user.id}> wins!{payout_text}",
                view=game["view"],
                allowed_mentions=discord.AllowedMentions(users=True)
            )
            ttt_games.pop(interaction.channel.id, None)
            return

        if all(cell != TTT_EMPTY for row in board for cell in row):
            payout_text = await settle_game_bet(game, None)
            await disable_all_buttons(game["view"])
            await game["msg"].edit(content=f"It's a draw!{payout_text}", view=game["view"], allowed_mentions=discord.AllowedMentions.none())
            ttt_games.pop(interaction.channel.id, None)
            return

        game["turn"] = 1 - game["turn"]
        await update_turn(game, interaction.channel)

class TicTacToeView(View):
    def __init__(self):
        super().__init__(timeout=None)
        for row in range(3):
            for col in range(3):
                self.add_item(TicTacToeButton(row, col))

def check_winner(board):
    for i in range(3):
        if board[i][0] == board[i][1] == board[i][2] != TTT_EMPTY:
            return True
        if board[0][i] == board[1][i] == board[2][i] != TTT_EMPTY:
            return True
    if board[0][0] == board[1][1] == board[2][2] != TTT_EMPTY:
        return True
    if board[0][2] == board[1][1] == board[2][0] != TTT_EMPTY:
        return True
    return False

async def disable_all_buttons(view):
    for item in view.children:
        item.disabled = True

async def cancel_game_timeout(game):
    task = game.get("timeout_task")
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    game["timeout_task"] = None

class GameBetView(View):
    def __init__(self, ctx):
        super().__init__(timeout=30)
        self.ctx = ctx
        self.choice = None

    async def interaction_check(self, interaction):
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message("Use your own bet prompt.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success)
    async def yes(self, interaction: discord.Interaction, button: Button):
        self.choice = True
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Bet enabled. Type the amount now.", view=self)
        self.stop()

    @discord.ui.button(label="No", style=discord.ButtonStyle.secondary)
    async def no(self, interaction: discord.Interaction, button: Button):
        self.choice = False
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="No bet. Starting game.", view=self)
        self.stop()

async def ask_game_bet(ctx, opponent, game_name):
    view = GameBetView(ctx)
    msg = await ctx.send(f"Play **{game_name}** with a bet?", view=view)
    await view.wait()

    if view.choice is None:
        await msg.edit(content="Bet choice timed out. Game canceled.", view=None)
        return None
    if view.choice is False:
        return 0

    def check(message):
        return message.author.id == ctx.author.id and message.channel.id == ctx.channel.id

    try:
        amount_msg = await bot.wait_for("message", check=check, timeout=30)
    except asyncio.TimeoutError:
        await ctx.send("Bet amount timed out. Game canceled.")
        return None

    if not await economy_ensure_db_ready(ctx):
        return None

    try:
        author_data = economy_get_user(ctx.author.id)
        opponent_data = economy_get_user(opponent.id)
    except Exception:
        await ctx.send("Database unavailable. Try again shortly.")
        return None

    amount = parse_uncapped_game_amount(amount_msg.content.strip(), author_data["balance"])
    if amount is None or amount <= 0:
        await ctx.send("Invalid bet amount. Game canceled.")
        return None
    if amount > author_data["balance"]:
        await ctx.send(f"You only have {economy_format_balance(author_data['balance'])}. Game canceled.")
        return None
    if amount > opponent_data["balance"]:
        await ctx.send(
            f"<@{opponent.id}> only has {economy_format_balance(opponent_data['balance'])}. Game canceled.",
            allowed_mentions=discord.AllowedMentions.none()
        )
        return None

    bet_view = AcceptView(ctx, opponent)
    await ctx.send(
        f"<@{opponent.id}>, do you accept a **{economy_format_balance(amount)}** bet for **{game_name}**?",
        view=bet_view,
        allowed_mentions=discord.AllowedMentions(users=[opponent])
    )
    await bet_view.wait()
    if not bet_view.accepted:
        await ctx.send("Bet declined. Game canceled.")
        return None

    return amount

def parse_uncapped_game_amount(raw, balance):
    raw = str(raw).strip().lower().replace(",", "").replace("_", "")
    if raw in {"all", "max"}:
        return int(balance)
    multipliers = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000, "bn": 1_000_000_000}
    try:
        for suffix in sorted(multipliers, key=len, reverse=True):
            if raw.endswith(suffix):
                return int(float(raw[:-len(suffix)]) * multipliers[suffix])
        return int(float(raw))
    except (TypeError, ValueError):
        return None

def game_bet_line(game):
    amount = game.get("bet_amount", 0)
    if amount <= 0:
        return ""
    return f"\nBet: **{economy_format_balance(amount)}** each"

def fit_discord_content(content, limit=2000):
    content = str(content or "")
    if len(content) <= limit:
        return content
    suffix = "\n\n[message shortened]"
    return content[:limit - len(suffix)] + suffix

if not getattr(commands.Context.send, "_proque_safe_content", False):
    _original_context_send = commands.Context.send

    async def _safe_context_send(self, *args, **kwargs):
        if args and isinstance(args[0], str):
            args = (fit_discord_content(args[0]),) + args[1:]
        if isinstance(kwargs.get("content"), str):
            kwargs["content"] = fit_discord_content(kwargs["content"])
        return await _original_context_send(self, *args, **kwargs)

    _safe_context_send._proque_safe_content = True
    commands.Context.send = _safe_context_send

def c4_result_content(board, turn, result_text, payout_text=""):
    board_text = render_board(board, turn)
    full = f"{board_text}\n\n{result_text}{payout_text}"
    if len(full) <= 2000:
        return full, None
    compact = f"{board_text}\n\n{result_text}"
    if len(compact) <= 2000:
        return compact, payout_text.strip() or None
    return fit_discord_content(compact), payout_text.strip() or None

async def send_game_extra(channel, text, allowed_mentions=None):
    text = str(text or "").strip()
    if not text:
        return
    allowed_mentions = allowed_mentions or discord.AllowedMentions.none()
    while text:
        chunk = text[:2000]
        if len(text) > 2000:
            split_at = max(chunk.rfind("\n"), chunk.rfind(" "))
            if split_at > 1000:
                chunk = text[:split_at]
        await channel.send(chunk, allowed_mentions=allowed_mentions)
        text = text[len(chunk):].lstrip()

async def settle_game_bet(game, winner):
    amount = game.get("bet_amount", 0)
    if amount <= 0:
        return ""
    if winner is None:
        return f"\nBet: **{economy_format_balance(amount)}** each. Draw: no quesos moved."

    loser = game["players"][1] if winner.id == game["players"][0].id else game["players"][0]
    try:
        winner_data = economy_get_user(winner.id)
        loser_data = economy_get_user(loser.id)
        payout = min(amount, loser_data["balance"])
        economy_update_user(winner.id, balance=winner_data["balance"] + payout)
        economy_update_user(loser.id, balance=max(0, loser_data["balance"] - payout))
    except Exception:
        return "\nBet payout failed: database unavailable."

    note = "" if payout == amount else "\nLoser did not have the full bet anymore, so only their remaining balance was paid."
    return (
        f"\nBet paid: **{economy_format_balance(payout)}**"
        f"\nWinner: <@{winner.id}>"
        f"\nLoser: <@{loser.id}>"
        f"{note}"
    )

class AcceptView(View):
    def __init__(self, ctx, opponent):
        super().__init__(timeout=30) 
        self.ctx = ctx
        self.opponent = opponent
        self.accepted = False
        self.declined = False

    @discord.ui.button(label="Accept", style=discord.ButtonStyle.success)
    async def accept(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.opponent:
            return await interaction.response.send_message("You're not the challenged player.", ephemeral=True)
        self.accepted = True
        self.stop()
        await interaction.response.edit_message(content=f"{economy_q_accept} Challenge accepted!", view=None)

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.danger)
    async def decline(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.opponent:
            return await interaction.response.send_message("You're not the challenged player.", ephemeral=True)
        self.declined = True
        self.stop()
        await interaction.response.edit_message(content=f"{economy_q_reject} Challenge declined.", view=None)

    async def on_timeout(self):
        if not self.accepted and not self.declined:
            await self.ctx.send(
                f"<@{self.opponent.id}> didn't respond in time. Game canceled.",
                allowed_mentions=discord.AllowedMentions.none()
            )

def chess_rank_label(rank):
    if 1 <= rank <= len(CHESS_RANK_EMOJIS):
        return CHESS_RANK_EMOJIS[rank - 1]
    return str(rank)

def chess_file_label(file_index):
    name = chr(ord("a") + file_index)
    return CHESS_FILE_EMOJIS.get(name, name)

def render_chess_board(game):
    board = game["board"]
    lines = []
    black_perspective = board.turn == chess_lib.BLACK
    rank_order = range(8) if black_perspective else range(7, -1, -1)
    file_order = range(7, -1, -1) if black_perspective else range(8)
    for rank in rank_order:
        cells = []
        for file in file_order:
            square = chess_lib.square(file, rank)
            piece = board.piece_at(square)
            if piece:
                cells.append(CHESS_PIECE_EMOJIS[piece.symbol()])
            else:
                cells.append(CHESS_LIGHT if (rank + file) % 2 == 0 else CHESS_DARK)
        lines.append(chess_rank_label(rank + 1) + "".join(cells))
    lines.append(CHESS_COORD_SPACER + "".join(chess_file_label(file) for file in file_order))
    return "\n".join(lines)

def chess_current_player(game):
    return game["white"] if game["board"].turn == chess_lib.WHITE else game["black"]

def chess_opponent(game, player):
    return game["black"] if player.id == game["white"].id else game["white"]

def format_chess_clock(seconds):
    seconds = max(0, int(seconds))
    minutes, secs = divmod(seconds, 60)
    return f"{minutes}:{secs:02d}"

def chess_clock_remaining(game, player):
    remaining = float(game["clocks"].get(player.id, CHESS_CLOCK_SECONDS))
    current = chess_current_player(game)
    if player.id == current.id and game.get("last_turn_started") is not None:
        remaining -= time.monotonic() - game["last_turn_started"]
    return max(0, remaining)

def chess_clock_line(game):
    white_time = format_chess_clock(chess_clock_remaining(game, game["white"]))
    black_time = format_chess_clock(chess_clock_remaining(game, game["black"]))
    return f"White <@{game['white'].id}>: **{white_time}** | Black <@{game['black'].id}>: **{black_time}**"

def chess_message_kwargs(game, result_text=None, view=None):
    return {
        "content": fit_discord_content(result_text or chess_status(game)),
        "embed": chess_embed(game, result_text),
        "view": game.get("view") if view is None else view,
        "allowed_mentions": discord.AllowedMentions.none(),
    }

def apply_chess_elapsed(game):
    current = chess_current_player(game)
    started = game.get("last_turn_started")
    if started is None:
        game["last_turn_started"] = time.monotonic()
        return
    elapsed = time.monotonic() - started
    game["clocks"][current.id] = max(0, float(game["clocks"].get(current.id, CHESS_CLOCK_SECONDS)) - elapsed)
    game["last_turn_started"] = time.monotonic()

async def stop_chess_clock(game):
    task = game.get("clock_task")
    if task and not task.done():
        if task is asyncio.current_task():
            game["clock_task"] = None
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    game["clock_task"] = None

async def stop_chess_live_clock(game):
    task = game.get("live_clock_task")
    if task and not task.done():
        if task is asyncio.current_task():
            game["live_clock_task"] = None
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    game["live_clock_task"] = None

async def start_chess_live_clock(game):
    if game.get("live_clock_task") and not game["live_clock_task"].done():
        return

    async def live_task():
        try:
            while not game.get("ended"):
                await asyncio.sleep(CHESS_CLOCK_LIVE_INTERVAL)
                active = chess_games.get(game["channel_id"])
                if active is not game or game.get("ended"):
                    return
                message = game.get("message")
                if not message:
                    continue
                try:
                    await message.edit(**chess_message_kwargs(game))
                except discord.NotFound:
                    return
                except Exception as e:
                    print(f"Chess live clock edit error: {type(e).__name__} - {e}")
        except asyncio.CancelledError:
            raise

    game["live_clock_task"] = asyncio.create_task(live_task())

async def start_chess_clock(game):
    await stop_chess_clock(game)
    if game.get("ended"):
        return
    current = chess_current_player(game)
    game["last_turn_started"] = time.monotonic()

    async def timeout_task(player_id=current.id, channel_id=game["channel_id"]):
        try:
            await asyncio.sleep(chess_clock_remaining(game, current))
            active = chess_games.get(channel_id)
            if active is not game or game.get("ended"):
                return
            if chess_current_player(game).id != player_id:
                return
            game["clocks"][player_id] = 0
            loser = game["white"] if game["white"].id == player_id else game["black"]
            winner = chess_opponent(game, loser)
            game["ended"] = True
            await stop_chess_clock(game)
            await stop_chess_live_clock(game)
            if game.get("view"):
                game["view"].disable_all_items()
            payout_text = await settle_game_bet(game, winner)
            result_text = f"{economy_q_game_timeout} <@{loser.id}> ran out of time.\n{economy_q_game_win} <@{winner.id}> wins on time!{payout_text}"
            await game["message"].edit(
                content=result_text,
                embed=chess_embed(game, result_text),
                view=game.get("view"),
                allowed_mentions=discord.AllowedMentions(users=True)
            )
            chess_games.pop(channel_id, None)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Chess clock error: {type(e).__name__} - {e}")

    game["clock_task"] = asyncio.create_task(timeout_task())

def chess_move_label(board, move):
    try:
        return board.san(move)
    except Exception:
        return move.uci()

def chess_move_options(board, from_square=None):
    moves = sorted(
        list(board.legal_moves),
        key=lambda move: (move.from_square, move.to_square, move.promotion or 0)
    )
    if from_square is not None:
        moves = [move for move in moves if move.from_square == from_square]
    return moves

def chess_option_emoji(value):
    if isinstance(value, str) and value.startswith("<"):
        return custom_emoji(value)
    return value

def chess_status(game):
    board = game["board"]
    current = chess_current_player(game)
    color = "White" if board.turn == chess_lib.WHITE else "Black"
    status = f"{economy_q_cards} **Chess** - {color} to move: <@{current.id}>"
    if board.is_check():
        status += f"\n{economy_q_warning} Check."
    selected = game.get("selected_from")
    if selected is not None:
        moves = chess_move_options(board, selected)
        destinations = ", ".join(chess_lib.square_name(move.to_square) for move in moves[:12])
        if len(moves) > 12:
            destinations += f", +{len(moves) - 12} more"
        status += f"\nSelected: `{chess_lib.square_name(selected)}`"
        if destinations:
            status += f" → legal squares: {destinations}"
    pending = game.get("pending_move")
    if pending is not None:
        status += f"\nPending move: **{chess_move_label(board, pending)}**. Press **Move** to play it."
    status += f"\n{chess_clock_line(game)}"
    status += game_bet_line(game)
    status += "\nPick a piece, pick its square, then press **Move**."
    return status

def chess_embed(game, result_text=None):
    embed = discord.Embed(
        title=f"{economy_q_cards} Chess",
        description=render_chess_board(game),
        color=discord.Color.blurple()
    )
    embed.add_field(name="Clock", value=chess_clock_line(game), inline=False)
    bet_line = game_bet_line(game).strip()
    if bet_line:
        embed.add_field(name="Bet", value=bet_line.removeprefix("Bet: "), inline=False)
    if result_text:
        embed.add_field(name="Result", value=result_text, inline=False)
    return embed

def parse_chess_move(board, raw_move):
    text = raw_move.strip()
    try:
        return board.parse_san(text)
    except ValueError:
        pass
    try:
        move = chess_lib.Move.from_uci(text.lower())
    except ValueError:
        return None
    return move if move in board.legal_moves else None

class ChessFromSelect(Select):
    def __init__(self, game):
        board = game["board"]
        from_squares = sorted({move.from_square for move in board.legal_moves})
        options = []
        for square in from_squares[:25]:
            piece = board.piece_at(square)
            square_name = chess_lib.square_name(square)
            legal_count = sum(1 for move in board.legal_moves if move.from_square == square)
            options.append(
                discord.SelectOption(
                    label=f"{square_name} ({legal_count})",
                    value=str(square),
                    emoji=chess_option_emoji(CHESS_PIECE_EMOJIS.get(piece.symbol())) if piece else None,
                    description=f"{piece.symbol().upper()} legal moves" if piece else "Legal moves",
                )
            )
        if not options:
            options = [discord.SelectOption(label="No legal moves", value="-1")]
        super().__init__(placeholder="1. Pick your piece", min_values=1, max_values=1, options=options, row=0)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        game = view.game
        current = chess_current_player(game)
        if interaction.user.id != current.id:
            return await interaction.response.send_message("Not your turn.", ephemeral=True)
        selected = int(self.values[0])
        if selected < 0:
            return await interaction.response.send_message("No legal moves are available.", ephemeral=True)
        game["selected_from"] = selected
        game["pending_move"] = None
        new_view = ChessView(game)
        game["view"] = new_view
        await interaction.response.edit_message(
            content=chess_status(game),
            embed=chess_embed(game),
            view=new_view,
            allowed_mentions=discord.AllowedMentions(users=True)
        )

class ChessMoveSelect(Select):
    def __init__(self, game, moves, row):
        board = game["board"]
        options = []
        for move in moves[:25]:
            to_name = chess_lib.square_name(move.to_square)
            label = chess_move_label(board, move)
            options.append(
                discord.SelectOption(
                    label=label[:100],
                    value=move.uci(),
                    description=f"Move to {to_name}",
                )
            )
        super().__init__(placeholder="2. Pick where it moves", min_values=1, max_values=1, options=options, row=row)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        game = view.game
        current = chess_current_player(game)
        if interaction.user.id != current.id:
            return await interaction.response.send_message("Not your turn.", ephemeral=True)

        move = chess_lib.Move.from_uci(self.values[0])
        if move not in game["board"].legal_moves:
            return await interaction.response.send_message("That move is no longer legal.", ephemeral=True)
        game["pending_move"] = move
        new_view = ChessView(game)
        game["view"] = new_view
        await interaction.response.edit_message(
            content=chess_status(game),
            embed=chess_embed(game),
            view=new_view,
            allowed_mentions=discord.AllowedMentions(users=True)
        )

class ChessResignButton(Button):
    def __init__(self):
        super().__init__(label="Resign", style=discord.ButtonStyle.danger, row=4)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        game = view.game
        if interaction.user.id not in {game["white"].id, game["black"].id}:
            return await interaction.response.send_message("Only chess players can resign.", ephemeral=True)
        await interaction.response.defer()
        winner = game["black"] if interaction.user.id == game["white"].id else game["white"]
        game["ended"] = True
        await stop_chess_clock(game)
        await stop_chess_live_clock(game)
        view.disable_all_items()
        chess_games.pop(game["channel_id"], None)
        payout_text = await settle_game_bet(game, winner)
        result_text = f"{economy_q_game_win} <@{winner.id}> wins by resignation.{payout_text}"
        await interaction.edit_original_response(
            content=result_text,
            embed=chess_embed(game, result_text),
            view=view,
            allowed_mentions=discord.AllowedMentions(users=True)
        )

class ChessClearSelectionButton(Button):
    def __init__(self):
        super().__init__(label="Clear Piece", style=discord.ButtonStyle.secondary, row=4)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        game = view.game
        current = chess_current_player(game)
        if interaction.user.id != current.id:
            return await interaction.response.send_message("Not your turn.", ephemeral=True)
        game["selected_from"] = None
        game["pending_move"] = None
        new_view = ChessView(game)
        game["view"] = new_view
        await interaction.response.edit_message(
            content=chess_status(game),
            embed=chess_embed(game),
            view=new_view,
            allowed_mentions=discord.AllowedMentions(users=True)
        )

class ChessConfirmMoveButton(Button):
    def __init__(self):
        super().__init__(label="Move", style=discord.ButtonStyle.success, row=4)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        game = view.game
        current = chess_current_player(game)
        if interaction.user.id != current.id:
            return await interaction.response.send_message("Not your turn.", ephemeral=True)
        move = game.get("pending_move")
        if move is None:
            return await interaction.response.send_message("No pending move to confirm.", ephemeral=True)
        if move not in game["board"].legal_moves:
            game["pending_move"] = None
            game["selected_from"] = None
            new_view = ChessView(game)
            game["view"] = new_view
            return await interaction.response.edit_message(
                content=chess_status(game),
                embed=chess_embed(game),
                view=new_view,
                allowed_mentions=discord.AllowedMentions(users=True)
            )
        await view.play_move(interaction, move)

class ChessCancelMoveButton(Button):
    def __init__(self):
        super().__init__(label="Cancel", style=discord.ButtonStyle.secondary, row=4)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        game = view.game
        current = chess_current_player(game)
        if interaction.user.id != current.id:
            return await interaction.response.send_message("Not your turn.", ephemeral=True)
        game["pending_move"] = None
        new_view = ChessView(game)
        game["view"] = new_view
        await interaction.response.edit_message(
            content=chess_status(game),
            embed=chess_embed(game),
            view=new_view,
            allowed_mentions=discord.AllowedMentions(users=True)
        )

class ChessView(View):
    def __init__(self, game):
        super().__init__(timeout=None)
        self.game = game
        self.add_item(ChessFromSelect(game))
        selected = game.get("selected_from")
        if selected is not None:
            moves = chess_move_options(game["board"], selected)
            for idx in range(0, len(moves), 25):
                row = 1 + idx // 25
                if row >= 4:
                    break
                self.add_item(ChessMoveSelect(game, moves[idx:idx + 25], row=row))
        if game.get("pending_move") is not None:
            self.add_item(ChessConfirmMoveButton())
            self.add_item(ChessCancelMoveButton())
        elif selected is not None:
            self.add_item(ChessClearSelectionButton())
        self.add_item(ChessResignButton())

    async def interaction_check(self, interaction):
        if interaction.user.id not in {self.game["white"].id, self.game["black"].id}:
            await interaction.response.send_message("Only the chess players can use this board.", ephemeral=True)
            return False
        return True

    def disable_all_items(self):
        for item in self.children:
            item.disabled = True

    async def play_move(self, interaction, move):
        await interaction.response.defer()
        board = self.game["board"]
        apply_chess_elapsed(self.game)
        board.push(move)
        self.game["selected_from"] = None
        self.game["pending_move"] = None

        if board.is_checkmate():
            winner = self.game["black"] if board.turn == chess_lib.WHITE else self.game["white"]
            self.game["ended"] = True
            await stop_chess_clock(self.game)
            await stop_chess_live_clock(self.game)
            self.disable_all_items()
            chess_games.pop(self.game["channel_id"], None)
            payout_text = await settle_game_bet(self.game, winner)
            result_text = f"{economy_q_game_win} Checkmate. <@{winner.id}> wins!{payout_text}"
            return await interaction.edit_original_response(
                content=result_text,
                embed=chess_embed(self.game, result_text),
                view=self,
                allowed_mentions=discord.AllowedMentions(users=True)
            )

        if board.is_stalemate() or board.is_insufficient_material() or board.can_claim_draw():
            self.game["ended"] = True
            await stop_chess_clock(self.game)
            await stop_chess_live_clock(self.game)
            self.disable_all_items()
            chess_games.pop(self.game["channel_id"], None)
            payout_text = await settle_game_bet(self.game, None)
            result_text = f"Game drawn.{payout_text}"
            return await interaction.edit_original_response(
                content=result_text,
                embed=chess_embed(self.game, result_text),
                view=self,
                allowed_mentions=discord.AllowedMentions.none()
            )

        new_view = ChessView(self.game)
        self.game["view"] = new_view
        await start_chess_clock(self.game)
        await interaction.edit_original_response(
            content=chess_status(self.game),
            embed=chess_embed(self.game),
            view=new_view,
            allowed_mentions=discord.AllowedMentions(users=True)
        )

@bot.command()
async def chess(ctx, opponent: discord.Member):
    if chess_lib is None:
        return await ctx.send("Chess needs `python-chess` installed. Redeploy after installing requirements.")
    if ctx.channel.id in chess_games:
        return await ctx.send("A chess game is already in progress here.")
    if opponent.bot or opponent == ctx.author:
        return await ctx.send("Choose a real opponent.")

    view = AcceptView(ctx, opponent)
    await ctx.send(
        f"<@{opponent.id}>, <@{ctx.author.id}> challenged you to **Chess**.\nClick below to accept or decline:",
        view=view,
        allowed_mentions=discord.AllowedMentions(users=[opponent])
    )
    await view.wait()
    if not view.accepted:
        return

    bet_amount = await ask_game_bet(ctx, opponent, "Chess")
    if bet_amount is None:
        return

    game = {
        "white": ctx.author,
        "black": opponent,
        "players": [ctx.author, opponent],
        "board": chess_lib.Board(),
        "channel_id": ctx.channel.id,
        "message": None,
        "selected_from": None,
        "pending_move": None,
        "view": None,
        "bet_amount": bet_amount,
        "clocks": {
            ctx.author.id: CHESS_CLOCK_SECONDS,
            opponent.id: CHESS_CLOCK_SECONDS,
        },
        "last_turn_started": None,
        "clock_task": None,
        "live_clock_task": None,
        "ended": False,
    }
    game["view"] = ChessView(game)
    msg = await ctx.send(
        chess_status(game),
        embed=chess_embed(game),
        view=game["view"],
        allowed_mentions=discord.AllowedMentions(users=True)
    )
    game["message"] = msg
    chess_games[ctx.channel.id] = game
    await start_chess_clock(game)
    await start_chess_live_clock(game)

@bot.command(name="move", aliases=["chessmove"])
async def chess_move(ctx, *, move: str):
    game = chess_games.get(ctx.channel.id)
    if not game:
        return await ctx.send("No chess game is active in this channel.")

    board = game["board"]
    current = game["white"] if board.turn == chess_lib.WHITE else game["black"]
    if ctx.author.id != current.id:
        return await ctx.send("Not your turn.")

    parsed = parse_chess_move(board, move)
    if parsed is None:
        return await ctx.send(f"Illegal move. Use the chess UI or something like `{ctx.prefix}move e2e4`.")

    apply_chess_elapsed(game)
    board.push(parsed)
    game["selected_from"] = None
    game["pending_move"] = None
    if board.is_checkmate():
        winner = game["black"] if board.turn == chess_lib.WHITE else game["white"]
        game["ended"] = True
        await stop_chess_clock(game)
        await stop_chess_live_clock(game)
        payout_text = await settle_game_bet(game, winner)
        result_text = f"{economy_q_game_win} Checkmate. <@{winner.id}> wins!{payout_text}"
        await game["message"].edit(
            content=result_text,
            embed=chess_embed(game, result_text),
            view=None,
            allowed_mentions=discord.AllowedMentions(users=True)
        )
        chess_games.pop(ctx.channel.id, None)
        return
    if board.is_stalemate() or board.is_insufficient_material() or board.can_claim_draw():
        game["ended"] = True
        await stop_chess_clock(game)
        await stop_chess_live_clock(game)
        payout_text = await settle_game_bet(game, None)
        result_text = f"Game drawn.{payout_text}"
        await game["message"].edit(
            content=result_text,
            embed=chess_embed(game, result_text),
            view=None,
            allowed_mentions=discord.AllowedMentions.none()
        )
        chess_games.pop(ctx.channel.id, None)
        return

    game["view"] = ChessView(game)
    await start_chess_clock(game)
    await start_chess_live_clock(game)
    await game["message"].edit(content=chess_status(game), embed=chess_embed(game), view=game["view"], allowed_mentions=discord.AllowedMentions(users=True))

@bot.command()
async def resign(ctx):
    game = chess_games.get(ctx.channel.id)
    if not game:
        return await ctx.send("No chess game is active in this channel.")
    if ctx.author.id not in {game["white"].id, game["black"].id}:
        return await ctx.send("Only a chess player can resign.")

    winner = game["black"] if ctx.author.id == game["white"].id else game["white"]
    game["ended"] = True
    await stop_chess_clock(game)
    await stop_chess_live_clock(game)
    payout_text = await settle_game_bet(game, winner)
    result_text = f"{economy_q_game_win} <@{winner.id}> wins by resignation.{payout_text}"
    await game["message"].edit(
        content=result_text,
        embed=chess_embed(game, result_text),
        view=None,
        allowed_mentions=discord.AllowedMentions(users=True)
    )
    chess_games.pop(ctx.channel.id, None)

@bot.command()
async def ttt(ctx, opponent: discord.Member):
    if ctx.channel.id in ttt_games:
        return await ctx.send("A game is already in progress in this channel.")
    if opponent.bot or opponent == ctx.author:
        return await ctx.send("Choose a real opponent.")

    view = AcceptView(ctx, opponent)
    await ctx.send(
        f"<@{opponent.id}>, <@{ctx.author.id}> challenged you to a game of **Tic Tac Toe**.\nClick below to accept or decline:",
        view=view,
        allowed_mentions=discord.AllowedMentions(users=[opponent])
    )
    await view.wait()

    if not view.accepted:
        return

    bet_amount = await ask_game_bet(ctx, opponent, "Tic Tac Toe")
    if bet_amount is None:
        return

    board = [[TTT_EMPTY] * 3 for _ in range(3)]
    game_view = TicTacToeView()
    msg = await ctx.send(f"Game started!{game_bet_line({'bet_amount': bet_amount})}", view=game_view)
    game = {
        "players": [ctx.author, opponent],
        "turn": 0,
        "board": board,
        "view": game_view,
        "msg": msg,
        "timeout_task": None,
        "bet_amount": bet_amount
    }
    ttt_games[ctx.channel.id] = game

    await update_turn(game, ctx.channel)

async def update_turn(game, channel):
    current = game["players"][game["turn"]]
    opponent = game["players"][1 - game["turn"]]
    time_left = TURN_TIMEOUT_SECONDS

    async def countdown():
        nonlocal time_left
        while time_left > 0:
            if time_left in TURN_COUNTDOWN_EDIT_POINTS:
                await game["msg"].edit(
                    content=f"<@{current.id}>, it's your turn! ({time_left}s){game_bet_line(game)}",
                    view=game["view"],
                    allowed_mentions=discord.AllowedMentions.none()
                )
            await asyncio.sleep(1)
            time_left -= 1

        payout_text = await settle_game_bet(game, opponent)
        await disable_all_buttons(game["view"])
        await game["msg"].edit(
            content=f"{economy_q_timer} <@{current.id}> took too long.\n{economy_q_game_timeout} <@{opponent.id}> wins by timeout!{payout_text}",
            view=game["view"],
            allowed_mentions=discord.AllowedMentions(users=True)
        )
        ttt_games.pop(channel.id, None)

    game["timeout_task"] = asyncio.create_task(countdown())

class Connect4Button(Button):
    def __init__(self, col):
        super().__init__(style=discord.ButtonStyle.secondary, label=str(col + 1))
        self.col = col

    async def callback(self, interaction: discord.Interaction):
        try:
            game = c4_games.get(interaction.channel.id)
            if not game or interaction.user != game["players"][game["turn"]]:
                return await interaction.response.send_message("Not your turn.", ephemeral=True)

            board = game["board"]

            for row in reversed(range(6)):
                if board[row][self.col] == " ":
                    piece = economy_q_connect_black if game["turn"] == 0 else economy_q_connect_white
                    board[row][self.col] = piece
                    break
            else:
                return await interaction.response.send_message("Column full.", ephemeral=True)

            await interaction.response.defer()
            await cancel_game_timeout(game)

            if check_c4_winner(board, piece):
                render = render_board(board, game["turn"])
                payout_text = await settle_game_bet(game, interaction.user)
                await disable_all_buttons(game["view"])
                content, extra = c4_result_content(board, game["turn"], f"{economy_q_game_win} <@{interaction.user.id}> wins!", payout_text)
                await interaction.message.edit(
                    content=content,
                    view=game["view"],
                    allowed_mentions=discord.AllowedMentions(users=True)
                )
                await send_game_extra(interaction.channel, extra, discord.AllowedMentions(users=True))
                c4_games.pop(interaction.channel.id, None)
                return

            if all(cell != " " for row in board for cell in row):
                render = render_board(board, game["turn"])
                payout_text = await settle_game_bet(game, None)
                await disable_all_buttons(game["view"])
                content, extra = c4_result_content(board, game["turn"], "It's a draw!", payout_text)
                await interaction.message.edit(
                    content=content,
                    view=game["view"],
                    allowed_mentions=discord.AllowedMentions.none()
                )
                await send_game_extra(interaction.channel, extra, discord.AllowedMentions.none())
                c4_games.pop(interaction.channel.id, None)
                return

            game["turn"] = 1 - game["turn"]
            game["msg"] = interaction.message
            game["board"] = board
            game["view"] = Connect4View()

            render = render_board(board, game["turn"])
            await interaction.message.edit(content=fit_discord_content(render), view=game["view"], allowed_mentions=discord.AllowedMentions.none())
            await update_c4_turn(game, interaction.channel)

        except Exception as e:
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[ERROR in Connect4 callback]\n{traceback_str}")
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(fit_discord_content(f"Error:\n```{e}```"), ephemeral=True)
                else:
                    await interaction.response.send_message(fit_discord_content(f"Error:\n```{e}```"), ephemeral=True)
            except Exception as err:
                print(f"Failed to send error: {err}")

class Connect4View(View):
    def __init__(self):
        super().__init__(timeout=None)
        for col in range(7):
            self.add_item(Connect4Button(col))

def check_c4_winner(board, piece):
    for r in range(6):
        for c in range(4):
            if all(board[r][c+i] == piece for i in range(4)):
                return True
    for r in range(3):
        for c in range(7):
            if all(board[r+i][c] == piece for i in range(4)):
                return True
    for r in range(3):
        for c in range(4):
            if all(board[r+i][c+i] == piece for i in range(4)):
                return True
    for r in range(3):
        for c in range(3, 7):
            if all(board[r+i][c-i] == piece for i in range(4)):
                return True
    return False

async def update_c4_turn(game, channel):
    current = game["players"][game["turn"]]
    opponent = game["players"][1 - game["turn"]]
    msg = game["msg"]
    board = game["board"]
    time_left = TURN_TIMEOUT_SECONDS

    async def countdown():
        nonlocal time_left
        try:
            while time_left > 0:
                if time_left in TURN_COUNTDOWN_EDIT_POINTS:
                    await msg.edit(
                        content=fit_discord_content(f"{render_board(board, game['turn'])}\n\n<@{current.id}>, it's your turn! ({time_left}s){game_bet_line(game)}"),
                        view=game["view"],
                        allowed_mentions=discord.AllowedMentions.none()
                    )
                await asyncio.sleep(1)
                time_left -= 1

            payout_text = await settle_game_bet(game, opponent)
            await disable_all_buttons(game["view"])
            content, extra = c4_result_content(
                board,
                game["turn"],
                f"{economy_q_timer} <@{current.id}> took too long.\n{economy_q_game_timeout} <@{opponent.id}> wins by timeout!",
                payout_text
            )
            await msg.edit(
                content=content,
                view=game["view"],
                allowed_mentions=discord.AllowedMentions(users=True)
            )
            await send_game_extra(channel, extra, discord.AllowedMentions(users=True))
            c4_games.pop(channel.id, None)
        except Exception as e:
            print("Error in countdown:", e)

    game["timeout_task"] = asyncio.create_task(countdown())

def render_board(board, turn):
    bg = C4_EMPTY_LIGHT if turn == 0 else C4_EMPTY_DARK
    rendered = ""
    for row in board:
        for cell in row:
            rendered += cell if cell in (economy_q_connect_black, economy_q_connect_white) else bg
        rendered += "\n"
    rendered += C4_COLUMN_LABELS
    return rendered

@bot.command()
async def c4(ctx, opponent: discord.Member):
    if ctx.channel.id in c4_games:
        return await ctx.send("A Connect 4 game is already in progress here.")
    if opponent.bot or opponent == ctx.author:
        return await ctx.send("Choose a real opponent.")

    view = AcceptView(ctx, opponent)
    await ctx.send(
        f"<@{opponent.id}>, <@{ctx.author.id}> challenged you to a game of **Connect 4**.\nClick below to accept or decline:",
        view=view,
        allowed_mentions=discord.AllowedMentions(users=[opponent])
    )
    await view.wait()

    if not view.accepted:
        return

    bet_amount = await ask_game_bet(ctx, opponent, "Connect 4")
    if bet_amount is None:
        return

    board = [[" "] * 7 for _ in range(6)]
    render = render_board(board, 0)
    game_view = Connect4View()
    msg = await ctx.send(fit_discord_content(f"{render}{game_bet_line({'bet_amount': bet_amount})}"), view=game_view)

    game = {
        "players": [ctx.author, opponent],
        "turn": 0,
        "board": board,
        "view": game_view,
        "msg": msg,
        "timeout_task": None,
        "bet_amount": bet_amount
    }
    c4_games[ctx.channel.id] = game
    await update_c4_turn(game, ctx.channel)

@bot.command()
@is_admin_power()
async def endttt(ctx):
    if ctx.channel.id in ttt_games:
        ttt_games.pop(ctx.channel.id, None)
        await ctx.send("Tic-Tac-Toe game ended.")
    else:
        await ctx.send("No Tic-Tac-Toe game is currently active in this channel.")

@bot.command()
async def q(ctx):
    answer = random.choice(["Yes", "No"])
    await ctx.send(f"**{answer}**")

@bot.command()
@is_admin_power()
async def setnick(ctx, member: discord.Member, *, nickname: str):
    if not can_act_on(ctx.author, member, ctx.guild):
        return await ctx.send("You can't change that user's nickname.")
    try:
        await member.edit(nick=nickname)
        await ctx.send(
            f"Changed <@{member.id}>'s nickname to **{nickname}**.",
            allowed_mentions=discord.AllowedMentions.none()
        )
    except discord.Forbidden:
        await ctx.send("I don't have permission to change that user's nickname.")
    except Exception as e:
        await ctx.send(f"Error: {e}")

@bot.command()
@is_admin_power()
async def shut(ctx, member: discord.Member):
    print(f"shut command used by {ctx.author} on {member}")
    if not can_act_on(ctx.author, member, ctx.guild):
        return await ctx.send("You can't silence that user.")
    targets = guild_watchlist(ctx.guild)
    targets[member.id] = ctx.author.id
    save_watchlist(scoped_id(ctx.guild), targets)
    await ctx.send(f"<@{member.id}> has been silenced.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_admin_power()
async def unshut(ctx, member: discord.Member):
    targets = guild_watchlist(ctx.guild)
    if not can_act_on(ctx.author, member, ctx.guild) and targets.get(member.id) != ctx.author.id:
        return await ctx.send("You can't unshut that user.")
    targets.pop(member.id, None)
    save_watchlist(scoped_id(ctx.guild), targets)

@bot.command()
@is_admin_power()
async def clearwatchlist(ctx):
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send("Only 𝚀𝚞𝚎 can clear the watchlist.")
    guild_watchlist(ctx.guild).clear()
    save_watchlist(scoped_id(ctx.guild), guild_watchlist(ctx.guild))
    await ctx.send("Watchlist cleared.")

@bot.command()
@is_admin_power()
async def rshut(ctx, member: discord.Member):
    """Silence a user's reactions."""
    if not can_act_on(ctx.author, member, ctx.guild):
        return await ctx.send("You can't silence that user's reactions.")
    targets = guild_reaction_watchlist(ctx.guild)
    targets[member.id] = ctx.author.id
    save_reaction_watchlist(scoped_id(ctx.guild), targets)
    await ctx.send(f"<@{member.id}>'s reactions have been silenced.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_admin_power()
async def unrshut(ctx, member: discord.Member):
    """Allow a user's reactions again (silent unless protected owner)."""
    targets = guild_reaction_watchlist(ctx.guild)
    if not can_act_on(ctx.author, member, ctx.guild) and targets.get(member.id) != ctx.author.id:
        return await ctx.send("You can't unshut that user.")
    targets.pop(member.id, None)
    save_reaction_watchlist(scoped_id(ctx.guild), targets)

@bot.command()
@is_admin_power()
async def lockdown(ctx):
    """Bot-level channel lockdown. Non-admin messages are deleted."""
    channels = guild_shutdown_channels(ctx.guild)
    channels.add(ctx.channel.id)
    save_shutdown_channels(scoped_id(ctx.guild), channels)
    await ctx.send("This channel is now in lockdown mode. Only admins can speak.")

@bot.command(name="reopen")
@is_admin_power()
async def reopen(ctx):
    """Reopen a bot-level locked-down channel."""
    channels = guild_shutdown_channels(ctx.guild)
    channels.discard(ctx.channel.id)
    save_shutdown_channels(scoped_id(ctx.guild), channels)
    await ctx.send("This channel has been reopened. All users may speak now.")

@bot.command()
@is_admin_power()
async def rlockdown(ctx):
    channels = guild_reaction_shutdown_channels(ctx.guild)
    channels.add(ctx.channel.id)
    save_reaction_shutdown_channels(scoped_id(ctx.guild), channels)
    await ctx.send("Reactions are now disabled in this channel.", delete_after=5)

@bot.command()
@is_admin_power()
async def runlock(ctx):
    channels = guild_reaction_shutdown_channels(ctx.guild)
    channels.discard(ctx.channel.id)
    save_reaction_shutdown_channels(scoped_id(ctx.guild), channels)
    await ctx.send("Reactions are now enabled in this channel.", delete_after=5)

from collections import Counter

async def parse_member_count_args(ctx, args):
    try:
        tokens = shlex.split(str(args or ""))
    except ValueError:
        return None, None
    if not tokens:
        return None, None

    count_indexes = []
    for index, token in enumerate(tokens):
        if token.isdigit():
            count_indexes.append(index)
    if not count_indexes:
        return None, None

    count_index = len(tokens) - 1 if len(tokens) - 1 in count_indexes else count_indexes[0]
    amount = int(tokens[count_index])
    member_text = " ".join(tokens[:count_index] + tokens[count_index + 1:]).strip()
    if not member_text:
        return amount, None
    try:
        member = await commands.MemberConverter().convert(ctx, member_text)
    except commands.BadArgument:
        return amount, False
    return amount, member

async def parse_member_role_args(ctx, args):
    try:
        tokens = shlex.split(str(args or ""))
    except ValueError:
        return None, None
    if len(tokens) < 2:
        return None, None

    member = None
    role = None
    used = set()
    for index, token in enumerate(tokens):
        if member is None:
            try:
                member = await commands.MemberConverter().convert(ctx, token)
                used.add(index)
                continue
            except commands.BadArgument:
                pass
        if role is None:
            try:
                role = await commands.RoleConverter().convert(ctx, token)
                used.add(index)
            except commands.BadArgument:
                pass
    if member is not None and role is not None:
        return member, role

    remaining = [token for index, token in enumerate(tokens) if index not in used]
    remaining_text = " ".join(remaining)
    if member is None and remaining_text:
        try:
            member = await commands.MemberConverter().convert(ctx, remaining_text)
        except commands.BadArgument:
            pass
    if role is None and remaining_text:
        try:
            role = await commands.RoleConverter().convert(ctx, remaining_text)
        except commands.BadArgument:
            pass
    return member, role

@bot.command()
@is_admin_power()
async def purge(ctx, *, args: str = None):
    amount, member = await parse_member_count_args(ctx, args)
    if amount is None:
        return await ctx.send("Use `.purge 20`, `.purge @user 20`, or `.purge 20 @user`.", delete_after=10)
    if member is False:
        return await ctx.send("I couldn't find that member. Use `.purge @user 20` or `.purge 20 @user`.", delete_after=10)
    if amount <= 0:
        return await ctx.send("Amount must be greater than 0.", delete_after=5)
    await safe_delete_message(ctx.message)
    deleted = []
    try:
        if member is None:
            deleted = await ctx.channel.purge(limit=amount)
        else:
            async for message in ctx.channel.history(limit=1000):
                if message.author == member:
                    deleted.append(message)
                    if len(deleted) >= amount:
                        break
            if deleted:
                await ctx.channel.delete_messages(deleted)

        if not deleted:
            return await ctx.send("No messages found to delete.", delete_after=5)

        counts = Counter([msg.author for msg in deleted])
        lines = [f"<@{user.id}>: {count}" for user, count in counts.items()]
        summary = (
            f"**{len(deleted)} messages** were purged by <@{ctx.author.id}>:\n\n"
            + "\n".join(lines)
        )

        await ctx.send(summary, delete_after=10, allowed_mentions=discord.AllowedMentions.none())

    except discord.Forbidden:
        await ctx.send("I don’t have permission to delete messages.", delete_after=5)
    except discord.HTTPException as e:
        await ctx.send(f"Error: {type(e).__name__} - {e}", delete_after=5)


@bot.command()
@is_admin_power()
async def rpurge(ctx, *, args: str = None):
    amount, member = await parse_member_count_args(ctx, args)
    if amount is None:
        return await ctx.send("Use `.rpurge 20`, `.rpurge @user 20`, or `.rpurge 20 @user`.", delete_after=10)
    if member is False:
        return await ctx.send("I couldn't find that member. Use `.rpurge @user 20` or `.rpurge 20 @user`.", delete_after=10)
    if amount <= 0:
        return await ctx.send("Amount must be greater than 0.", delete_after=5)

    await safe_delete_message(ctx.message)
    removed = 0
    reaction_owners = []

    try:
        async for message in ctx.channel.history(limit=1000):
            if removed >= amount:
                break

            if member is None or message.author == member:
                for reaction in message.reactions:
                    async for user in reaction.users():
                        if removed >= amount:
                            break
                        try:
                            await message.remove_reaction(reaction.emoji, user)
                            removed += 1
                            reaction_owners.append(user)
                        except discord.Forbidden:
                            return await ctx.send("I don’t have permission to remove reactions.", delete_after=5)
                        except Exception:
                            continue

        if removed == 0:
            return await ctx.send("No reactions removed.", delete_after=5)

        counts = Counter(reaction_owners)
        lines = [f"<@{user.id}>: {count}" for user, count in counts.items()]
        summary = (
            f"**{removed} reactions** were purged by <@{ctx.author.id}>:\n\n"
            + "\n".join(lines)
        )

        await ctx.send(summary, delete_after=10, allowed_mentions=discord.AllowedMentions.none())

    except discord.HTTPException as e:
        await ctx.send(f"Failed to remove reactions: {type(e).__name__} - {e}", delete_after=5)
    except Exception as e:
        print(f"[RPURGE ERROR] {type(e).__name__} - {e}")
        await ctx.send(f"An unexpected error occurred: {type(e).__name__}", delete_after=5)

@bot.command(name="lock")
@is_admin_power()
async def lock_channel(ctx):
    """Permission-lock this channel for @everyone."""
    overwrite = ctx.channel.overwrites_for(ctx.guild.default_role)
    overwrite.send_messages = False
    await ctx.channel.set_permissions(ctx.guild.default_role, overwrite=overwrite)
    await ctx.send("Channel locked.")

@bot.command(name="unlock")
@is_admin_power()
async def unlock_channel(ctx):
    """Undo `.lock` and allow @everyone to send messages again."""
    overwrite = ctx.channel.overwrites_for(ctx.guild.default_role)
    overwrite.send_messages = None
    await ctx.channel.set_permissions(ctx.guild.default_role, overwrite=overwrite)
    await ctx.send("Channel unlocked.")


@bot.command()
@is_admin_power()
async def unmute(ctx, member: discord.Member):
    if not can_act_on(ctx.author, member, ctx.guild):
        return await ctx.send("You can't unmute that member.")
    try:
        await member.timeout(None)
        await ctx.send(
            f"<@{member.id}> has been unmuted.",
            allowed_mentions=discord.AllowedMentions.none()
        )
    except discord.Forbidden:
        await ctx.send("Missing permissions to unmute this member.")
    except Exception as e:
        await ctx.send(f"Failed to unmute: {e}")

@bot.command()
@is_admin_power()
async def ban(ctx, user: discord.User):
    member = ctx.guild.get_member(user.id) if ctx.guild else None
    if member is not None and not can_act_on(ctx.author, member, ctx.guild):
        return await ctx.send("You can't ban that user.")
    ok = await confirm_admin_action(ctx, "Ban User", f"Ban <@{user.id}> from **{ctx.guild.name}**?")
    if not ok:
        return
    try:
        await user.send(f"LMAO you got banned from **{ctx.guild.name}** {economy_q_reject}")
    except Exception:
        pass

    await ctx.guild.ban(user)
    await ctx.send(
        f"<@{user.id}> has been banned.",
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
@is_admin_power()
async def unban(ctx, *, user: str):
    try:
        user_obj = None

        if user.isdigit():
            user_obj = await bot.fetch_user(int(user))
        elif "#" in user:
            name, discriminator = user.split("#")
            bans = await ctx.guild.bans()
            user_obj = next((ban.user for ban in bans if ban.user.name == name and ban.user.discriminator == discriminator), None)
        else:
            return await ctx.send("Please provide a username or their user ID.")

        if not user_obj:
            return await ctx.send("User not found in ban list.")

        await ctx.guild.unban(user_obj)
        await ctx.send(
            f"{user_obj.mention} has been unbanned.",
            allowed_mentions=discord.AllowedMentions.none()
        )
    except Exception:
        await ctx.send("Failed to unban user.")

@bot.command()
@is_admin_power()
async def kick(ctx, member: discord.Member, *, reason=None):
    if not can_act_on(ctx.author, member, ctx.guild):
        return await ctx.send("You can't kick that member.")
    reason_text = f"\nReason: {reason}" if reason else ""
    ok = await confirm_admin_action(ctx, "Kick User", f"Kick <@{member.id}> from **{ctx.guild.name}**?{reason_text}")
    if not ok:
        return
    await member.kick(reason=reason)
    await ctx.send(f"<@{member.id}> has been kicked.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_admin_power()
async def addrole(ctx, *, args: str = None):
    """Adds a role to a member. Member and role can be in either order."""
    member, role = await parse_member_role_args(ctx, args)
    if member is None or role is None:
        return await ctx.send("Use `.addrole @user @role` or `.addrole @role @user`.")
    if not can_act_on(ctx.author, member, ctx.guild):
        return await ctx.send("You can't edit that member's roles.")
    await member.add_roles(role)
    await ctx.send(f"Added **{role.name}** to <@{member.id}>.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_admin_power()
async def removerole(ctx, *, args: str = None):
    """Removes a role from a member. Member and role can be in either order."""
    member, role = await parse_member_role_args(ctx, args)
    if member is None or role is None:
        return await ctx.send("Use `.removerole @user @role` or `.removerole @role @user`.")
    if not can_act_on(ctx.author, member, ctx.guild):
        return await ctx.send("You can't edit that member's roles.")
    await member.remove_roles(role)
    await ctx.send(f"Removed **{role.name}** from <@{member.id}>.", allowed_mentions=discord.AllowedMentions.none())

class NameModal(Modal):
    def __init__(self, type_: str, asset: BytesIO, emoji_char=None, default_name: str = None):
        super().__init__(title=f"Name your {type_}")
        self.type_ = type_
        self.asset = asset
        self.emoji_char = emoji_char
        self.name_input = TextInput(
            label="Name",
            placeholder=f"Enter a name for the {type_}",
            default_value=default_name or "",
            max_length=32
        )
        self.add_item(self.name_input)

    async def on_submit(self, interaction: discord.Interaction):
        name = self.name_input.value.strip()
        if not name:
            await interaction.response.send_message("Please enter a name.", ephemeral=True)
            return
        await interaction.response.defer()
        
        # Sanitize name - Discord emoji/sticker names only allow alphanumeric, underscores
        name = re.sub(r'[^\w]+', '_', name)
        name = name[:32]  # Max length
        
        guild = interaction.guild
        if self.type_ == "sticker":
            try:
                self.asset.seek(0)
                sticker = await guild.create_sticker(
                    name=name,
                    image=self.asset,
                    description=f"Sticker stolen via bot",
                    reason=None
                )
                await interaction.followup.send(f"{economy_q_accept} Sticker `{name}` added successfully! {sticker}", ephemeral=True)
            except discord.Forbidden:
                await interaction.followup.send(f"{economy_q_reject} I don't have permission to create stickers.", ephemeral=True)
            except discord.HTTPException as e:
                await interaction.followup.send(f"{economy_q_reject} Failed to add sticker: {e.text[:100] if e.text else e}", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"{economy_q_reject} Failed to add sticker: {e}", ephemeral=True)
        elif self.type_ in ["emoji", "animated_emoji"]:
            is_animated = getattr(self.emoji_char, "animated", False) if self.emoji_char else False
            try:
                self.asset.seek(0)
                emoji = await guild.create_custom_emoji(
                    name=name,
                    image=self.asset,
                    reason=None
                )
                await interaction.followup.send(f"{economy_q_accept} Emoji `{name}` added successfully! {emoji}", ephemeral=True)
            except discord.Forbidden:
                await interaction.followup.send(f"{economy_q_reject} I don't have permission to create emojis.", ephemeral=True)
            except discord.HTTPException as e:
                await interaction.followup.send(f"{economy_q_reject} Failed to add emoji: {e.text[:100] if e.text else e}", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"{economy_q_reject} Failed to add emoji: {e}", ephemeral=True)

class StealView(View):
    def __init__(self, type_: str, asset: BytesIO, emoji_char=None, preview_url: str = None):
        super().__init__(timeout=120)
        self.type_ = type_
        self.asset = asset
        self.emoji_char = emoji_char
        self.preview_url = preview_url

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Allow only the person who triggered the command
        return True

    @discord.ui.button(label="Add to server", style=discord.ButtonStyle.green)
    async def add_button(self, interaction: discord.Interaction, button: Button):
        default_name = self.emoji_char.name if self.emoji_char else None
        await interaction.response.send_modal(
            NameModal(self.type_, self.asset, self.emoji_char, default_name)
        )
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.send_message(f"{economy_q_reject} Cancelled.", ephemeral=True)
        self.stop()

@bot.command()
@is_admin_power()
async def steal(ctx):
    await safe_delete_message(ctx.message)
    
    ref = ctx.message.reference
    if not ref:
        await ctx.send("Reply to a message containing a sticker, emoji, or image.", delete_after=5)
        return
    
    try:
        msg = await ctx.channel.fetch_message(ref.message_id)
    except discord.NotFound:
        await ctx.send("Message not found.", delete_after=5)
        return
    except discord.Forbidden:
        await ctx.send("I can't access that message.", delete_after=5)
        return

    sticker = msg.stickers[0] if msg.stickers else None
    attachments = msg.attachments
    content = msg.content

    # Match custom emoji: <:name:id>
    emoji_match = re.findall(r'<:(\w+):(\d+)>', content)
    # Match animated emoji: <a:name:id>
    animated_emoji_match = re.findall(r'<a:(\w+):(\d+)>', content)

    if sticker:
        try:
            buffer = BytesIO(await sticker.read())
            buffer.seek(0)
            preview_url = str(sticker.url)
            embed = discord.Embed(title="Sticker Detected", color=discord.Color.blurple())
            embed.set_image(url=preview_url)
            embed.add_field(name="Name", value=sticker.name, inline=True)
            embed.add_field(name="Format", value=sticker.format.name, inline=True)
            await ctx.send(embed=embed, view=StealView("sticker", buffer, preview_url=preview_url))
        except Exception as e:
            await ctx.send(f"Error reading sticker: {e}", delete_after=5)
            return
    
    elif attachments:
        first = attachments[0]
        content_type = first.content_type or ""
        
        if content_type.startswith("image/"):
            try:
                buffer = BytesIO()
                async with aiohttp.ClientSession() as session:
                    async with session.get(first.url) as resp:
                        buffer.write(await resp.read())
                buffer.seek(0)
                
                embed = discord.Embed(title="Image Detected", color=discord.Color.blurple())
                embed.set_image(url=first.url)
                embed.add_field(name="Filename", value=first.filename, inline=True)
                embed.add_field(name="Size", value=f"{first.size / 1024:.1f} KB", inline=True)
                await ctx.send(embed=embed, view=StealView("sticker", buffer, preview_url=first.url))
            except Exception as e:
                await ctx.send(f"Error downloading image: {e}", delete_after=5)
                return
        else:
            await ctx.send("Attachment is not an image.", delete_after=5)
            return
    
    elif animated_emoji_match:
        # Handle animated emoji
        name, emoji_id = animated_emoji_match[0]
        emoji_obj = ctx.bot.get_emoji(int(emoji_id))
        
        if emoji_obj and emoji_obj.animated:
            try:
                buffer = BytesIO()
                async with aiohttp.ClientSession() as session:
                    async with session.get(str(emoji_obj.url)) as resp:
                        buffer.write(await resp.read())
                buffer.seek(0)
                
                embed = discord.Embed(title="Animated Emoji Detected", color=discord.Color.blurple())
                embed.add_field(name="Name", value=name, inline=True)
                embed.add_field(name="Animated", value="Yes", inline=True)
                embed.add_field(name="Preview", value=str(emoji_obj), inline=False)
                await ctx.send(embed=embed, view=StealView("emoji", buffer, emoji_obj))
            except Exception as e:
                await ctx.send(f"Error fetching animated emoji: {e}", delete_after=5)
                return
        else:
            await ctx.send("Could not fetch emoji.", delete_after=5)
            return
    
    elif emoji_match:
        name, emoji_id = emoji_match[0]
        emoji_obj = ctx.bot.get_emoji(int(emoji_id))
        
        if emoji_obj:
            try:
                buffer = BytesIO()
                async with aiohttp.ClientSession() as session:
                    async with session.get(str(emoji_obj.url)) as resp:
                        buffer.write(await resp.read())
                buffer.seek(0)
                
                embed = discord.Embed(title="Emoji Detected", color=discord.Color.blurple())
                embed.add_field(name="Name", value=name, inline=True)
                embed.add_field(name="Animated", value="No", inline=True)
                embed.add_field(name="Preview", value=str(emoji_obj), inline=False)
                await ctx.send(embed=embed, view=StealView("emoji", buffer, emoji_obj))
            except Exception as e:
                await ctx.send(f"Error fetching emoji: {e}", delete_after=5)
                return
        else:
            await ctx.send("Could not fetch emoji - it may not be in this server.", delete_after=5)
            return
    else:
        await ctx.send("No sticker, emoji, or image found in the replied message.", delete_after=5)
            
@bot.command()
@is_admin_power()
async def send(ctx, target=None, *, msg=None):
    await safe_delete_message(ctx.message)
    attachments = ctx.message.attachments

    target_channel = ctx.channel
    if target:
        raw_target = target.strip()
        channel = None
        channel_lookup = raw_target
        if raw_target.startswith("<#") and raw_target.endswith(">"):
            channel_lookup = raw_target[2:-1]

        if channel_lookup.isdigit():
            try:
                channel = bot.get_channel(int(channel_lookup)) or await bot.fetch_channel(int(channel_lookup))
            except Exception:
                channel = None
            if channel is None:
                return await ctx.send("Channel not found.", delete_after=5)
        else:
            try:
                channel = await commands.TextChannelConverter().convert(ctx, raw_target)
            except commands.BadArgument:
                channel = None

        if channel is not None:
            target_channel = channel
        else:
            msg = f"{target} {msg}".strip() if msg else target

    if not msg and not attachments:
        return await ctx.send("Provide a message or attachment to send.", delete_after=5)

    try:
        files = [await a.to_file() for a in attachments] if attachments else None
        await target_channel.send(content=msg, files=files)
    except Exception as e:
        print(f"[SEND ERROR] {type(e).__name__}: {e}")
        return await ctx.send("Failed to send message.", delete_after=5)

    if target_channel.id != ctx.channel.id:
        await ctx.send(
            f"Message sent to {target_channel.mention}.",
            delete_after=3,
            allowed_mentions=discord.AllowedMentions.none()
        )


@bot.command()
@is_admin_power()
async def reply(ctx, message_id: int, *, text=None):
    await safe_delete_message(ctx.message)
    attachments = ctx.message.attachments

    msg = None
    for guild in bot.guilds:
        for channel in guild.text_channels:
            try:
                msg = await channel.fetch_message(message_id)
                break
            except (discord.NotFound, discord.Forbidden):
                continue
        if msg:
            break

    if not msg:
        for channel in bot.private_channels:
            if isinstance(channel, discord.DMChannel):
                try:
                    msg = await channel.fetch_message(message_id)
                    break
                except (discord.NotFound, discord.Forbidden):
                    continue

    if not msg:
        return await ctx.send("Message not found in any accessible channel.", delete_after=5)

    if not text and not attachments:
        return await ctx.send("Provide a message or attachment to reply with.", delete_after=5)

    try:
        files = [await a.to_file() for a in attachments] if attachments else None
        await msg.reply(content=text, files=files)
    except discord.Forbidden:
        return await ctx.send("I cannot reply to that message.", delete_after=5)
    except discord.HTTPException as e:
        print(f"[REPLY ERROR] {type(e).__name__}: {e}")
        return await ctx.send("Failed to reply to the message.", delete_after=5)

    if getattr(msg.channel, "id", None) != ctx.channel.id:
        await ctx.send(
            f"Replied to message in {msg.channel.mention}.",
            delete_after=3,
            allowed_mentions=discord.AllowedMentions.none()
        )

@bot.command()
async def poll(ctx, *, args: str = None):
    """Create a poll. Examples: .poll Is this good? yes no 10m OR .poll Question | A | B | 10m"""
    if ctx.guild is None:
        return await ctx.send("Polls can only be used in a server.")

    question, remaining, delta = parse_poll_input(args)
    if not question:
        return await ctx.send(
            "Set up your poll here, or type `.poll Best color? blue red 10m`.",
            view=SingleUserSetupView(ctx.author.id, OpenPollSetupButton())
        )
    try:
        await send_poll_message(ctx.channel, ctx.guild, ctx.author, question, remaining, delta)
    except ValueError as e:
        await ctx.send(str(e))


class ConfirmEndPollView(View):
    def __init__(self, poll_id, ctx, poll_data, parent_view):
        super().__init__(timeout=120)
        self.poll_id = poll_id
        self.ctx = ctx
        self.poll_data = poll_data
        self.parent_view = parent_view
        self.value = None
        self.message = None

    async def on_timeout(self):
        try:
            await self.message.edit(content="Poll end confirmation timed out.", view=None)
        except:
            pass
        if self.parent_view:
            self.parent_view.enable_all_items()
            try:
                await self.parent_view.message.edit(view=self.parent_view)
            except:
                pass

    async def interaction_check(self, interaction):
        if interaction.user.id == self.ctx.author.id or interaction.user.id == super_owner_id:
            return True
        await interaction.response.send_message("Only the poll owner or superowner can use this.", ephemeral=True)
        return False

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.danger)
    async def yes_button(self, interaction, button):
        await interaction.response.defer()
        poll_id = self.poll_id
        poll_data = active_polls.pop(poll_id, None)
        if not poll_data:
            await interaction.edit_original_response(content="Poll no longer exists.", view=None)
            return
        remove_active_poll(poll_id)
        end_task = poll_data.get("end_task")
        if end_task and not end_task.done():
            end_task.cancel()
        channel = bot.get_channel(poll_data["channel_id"])
        if not channel:
            await interaction.edit_original_response(content="Poll channel not found.", view=None)
            return
        try:
            poll_msg = await channel.fetch_message(poll_id)
        except:
            await interaction.edit_original_response(content="Poll message not found.", view=None)
            return
        await finalize_poll(poll_msg, poll_data)
        if self.parent_view and self.parent_view.message:
            self.parent_view.disable_all_items()
            try:
                await self.parent_view.message.edit(
                    content=f"Poll ended: [{poll_data['question']}]({poll_msg.jump_url})",
                    view=None
                )
            except:
                pass
        await interaction.edit_original_response(
            content=f"Poll ended: [{poll_data['question']}]({poll_msg.jump_url})",
            view=None
        )
        self.value = True
        self.stop()

    @discord.ui.button(label="No", style=discord.ButtonStyle.secondary)
    async def no_button(self, interaction, button):
        await interaction.response.edit_message(content="Poll end aborted.", view=None)
        if self.parent_view:
            self.parent_view.enable_all_items()
            try:
                await self.parent_view.message.edit(view=self.parent_view)
            except:
                pass
        self.value = False
        self.stop()


class EndPollSelect(Select):
    def __init__(self, ctx, options, parent_view):
        super().__init__(placeholder="Select a poll to end...", min_values=1, max_values=1, options=options)
        self.ctx = ctx
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        poll_id = int(self.values[0])
        if poll_id not in active_polls:
            await interaction.response.send_message("Poll no longer exists.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        poll_data = active_polls[poll_id]
        self.parent_view.disable_all_items()
        await interaction.message.edit(view=self.parent_view)
        confirm_view = ConfirmEndPollView(poll_id, self.ctx, poll_data, parent_view=self.parent_view)
        confirm_msg = await interaction.followup.send(
            f"Are you sure you want to end this poll? [Jump to poll message](https://discord.com/channels/{poll_data['guild_id']}/{poll_data['channel_id']}/{poll_id})",
            view=confirm_view,
            ephemeral=True,
            wait=True
        )
        confirm_view.message = confirm_msg


class EndPollSelectView(View):
    def __init__(self, ctx, polls_list):
        super().__init__(timeout=60)
        self.ctx = ctx
        self.polls_list = polls_list[:25]
        self.message = None
        options = [
            discord.SelectOption(label=(data["question"][:97] + "..." if len(data["question"]) > 100 else data["question"]), value=str(msg_id))
            for msg_id, data in self.polls_list
        ]
        self.add_item(EndPollSelect(ctx, options, parent_view=self))

    def disable_all_items(self):
        for item in self.children:
            item.disabled = True

    def enable_all_items(self):
        for item in self.children:
            item.disabled = False


@bot.command()
async def epoll(ctx):
    if ctx.guild is None:
        return await ctx.send("Polls can only be ended in a server.")
    if has_owner_power(ctx.author, ctx.guild):
        polls_list = list(active_polls.items())
    else:
        polls_list = [(pid, pdata) for pid, pdata in active_polls.items() if pdata["author_id"] == ctx.author.id]
    if not polls_list:
        return await ctx.send("No active polls found.")
    view = EndPollSelectView(ctx, polls_list)
    note = " Showing the first 25 active polls." if len(polls_list) > 25 else ""
    sent_msg = await ctx.send(f"Select a poll to end:{note}", view=view)
    view.message = sent_msg

async def run_giveaway(channel, seconds, prize):
    end_time = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    embed = discord.Embed(
        title=f"{economy_q_gift} Giveaway!",
        description=(
            f"Prize: **{prize}**\n"
            f"Ends: {discord.utils.format_dt(end_time, 'R')} ({discord.utils.format_dt(end_time, 'f')})\n"
            f"React with {economy_q_confetti} to enter!"
        ),
        color=0x00ff00
    )
    embed.timestamp = end_time
    embed.set_footer(text="Ends at")
    msg = await channel.send(embed=embed)
    await msg.add_reaction(reaction_emoji(economy_q_confetti))

    remaining = seconds
    while remaining > 0:
        if remaining <= 60:
            embed.set_footer(text=f"Ends in {remaining}s")
            try:
                await msg.edit(embed=embed)
            except discord.HTTPException:
                pass
        await asyncio.sleep(1)
        remaining = int((end_time - datetime.now(timezone.utc)).total_seconds())

    try:
        new_msg = await channel.fetch_message(msg.id)
    except discord.HTTPException:
        return
    entry_reaction = next((r for r in new_msg.reactions if same_emoji(r.emoji, economy_q_confetti)), None)
    users = [u async for u in entry_reaction.users() if not u.bot] if entry_reaction else []
    if users:
        winner = random.choice(users)
        await channel.send(f"{economy_q_confetti} Congratulations {winner.mention}! You won **{prize}**!")
    else:
        await channel.send("No one entered the giveaway.")
    try:
        await msg.clear_reactions()
    except discord.HTTPException:
        pass

async def start_giveaway(channel, raw):
    duration_text, prize = split_edge_duration(raw)
    seconds = duration_seconds(duration_text) if duration_text else None
    if not seconds:
        raise ValueError("Use a time like `30s`, `10m`, `2h`, `1d`, or `1w`.")
    if not prize:
        raise ValueError("Add a prize name. Example: `.giveaway 10m Nitro`")
    asyncio.create_task(run_giveaway(channel, seconds, prize))
    return seconds, prize

class GiveawaySetupModal(Modal):
    def __init__(self, author_id):
        super().__init__(title="Create Giveaway")
        self.author_id = author_id
        self.duration = TextInput(label="Time", placeholder="10m, 2h, 1d", max_length=30)
        self.prize = TextInput(label="Prize", placeholder="Nitro, role, custom prize", max_length=160)
        self.add_item(self.duration)
        self.add_item(self.prize)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        try:
            seconds, prize = await start_giveaway(interaction.channel, f"{self.duration.value} {self.prize.value}".strip())
        except ValueError as e:
            return await interaction.response.send_message(str(e), ephemeral=True)
        await interaction.response.send_message(f"Giveaway started for **{prize}**. Ends in {format_remaining(seconds)}.", ephemeral=True)

class OpenGiveawaySetupButton(Button):
    def __init__(self):
        super().__init__(label="Create Giveaway", style=discord.ButtonStyle.primary, emoji=reaction_emoji(economy_q_gift))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(GiveawaySetupModal(self.view.author_id))

@bot.command()
@is_admin_power()
async def giveaway(ctx, time: str = None, *, prize: str = None):
    """Start a giveaway. Examples: .giveaway 10m Nitro OR .giveaway Nitro 10m"""
    raw = f"{time or ''} {prize or ''}".strip()
    if not raw:
        return await ctx.send(
            "Set up your giveaway here, or type `.giveaway 10m prize`.",
            view=SingleUserSetupView(ctx.author.id, OpenGiveawaySetupButton())
        )
    try:
        seconds, parsed_prize = await start_giveaway(ctx.channel, raw)
    except ValueError as e:
        return await ctx.send(str(e))
    await ctx.send(f"{economy_q_accept} Giveaway started for **{parsed_prize}**. Ends in {format_remaining(seconds)}.", delete_after=5)

class PickerSetupModal(Modal):
    def __init__(self, author_id):
        super().__init__(title="Pick Random Option")
        self.author_id = author_id
        self.options = TextInput(
            label="Options",
            placeholder='apple banana orange, or "ice cream", pizza, sushi',
            style=discord.TextStyle.paragraph,
            max_length=1000
        )
        self.add_item(self.options)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        opts = split_simple_options(str(self.options.value))
        if len(opts) < 2:
            return await interaction.response.send_message("Add at least 2 options.", ephemeral=True)
        await interaction.response.send_message(f"**{random.choice(opts)}**")

class OpenPickerSetupButton(Button):
    def __init__(self):
        super().__init__(label="Add Options", style=discord.ButtonStyle.primary, emoji=reaction_emoji(economy_q_thinking))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(PickerSetupModal(self.view.author_id))

@bot.command()
async def picker(ctx, *, options: str = None):
    """Pick one option. Supports spaces, commas, pipes, and quoted multi-word options."""
    if not options:
        return await ctx.send(
            "Add options here, or type `.picker apple banana orange`.",
            view=SingleUserSetupView(ctx.author.id, OpenPickerSetupButton())
        )
    opts = split_simple_options(options)
    if len(opts) < 2:
        return await ctx.send("Add at least 2 options. Example: `.picker apple banana orange`.")
    choice = random.choice(opts)
    await ctx.send(f"**{choice}**")

@bot.command()
@is_admin_power()
async def aban(ctx, target):
    ids = guild_autoban_ids(ctx.guild)
    try:
        user = await commands.UserConverter().convert(ctx, target)
        ids.add(user.id)
        save_autoban_ids(scoped_id(ctx.guild), ids)
        await ctx.send(f"<@{user.id}> added to the autoban list.", allowed_mentions=discord.AllowedMentions.none())
    except:
        try:
            user_id = int(target)
            ids.add(user_id)
            save_autoban_ids(scoped_id(ctx.guild), ids)
            await ctx.send(f"User ID `{user_id}` added to the autoban list.")
        except:
            await ctx.send("Invalid user or ID.")

@bot.command()
@is_admin_power()
async def raban(ctx, target):
    ids = guild_autoban_ids(ctx.guild)
    try:
        user = await commands.UserConverter().convert(ctx, target)
        ids.discard(user.id)
        save_autoban_ids(scoped_id(ctx.guild), ids)
        await ctx.send(
           f"<@{user.id}> removed from autoban list.",
            allowed_mentions=discord.AllowedMentions.none()
        )
    except:
        try:
            user_id = int(target)
            ids.discard(user_id)
            save_autoban_ids(scoped_id(ctx.guild), ids)
            await ctx.send(f"User ID `{user_id}` removed from autoban list.")
        except:
            await ctx.send("Invalid user or ID.")

@bot.command()
@is_admin_power()
async def abanlist(ctx):
    ids = guild_autoban_ids(ctx.guild)
    if not ids:
        return await ctx.send("No autobanned users.")
    results = []
    for uid in ids:
        member = ctx.guild.get_member(uid)
        if member:
            results.append(f"<@{uid}>")
        else:
            results.append(f"<@{uid}>")
    await ctx.send("Autoban List:\n" + "\n".join(results), allowed_mentions=discord.AllowedMentions.none())

def parse_time_string(time_str: str) -> int:
    pattern = r'(\d+)\s*([smhd])'
    matches = re.findall(pattern, time_str.lower())
    if not matches:
        return None
    unit_map = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    total_seconds = 0
    for amount, unit in matches:
        total_seconds += int(amount) * unit_map.get(unit, 0)
    return total_seconds

def is_timer_duration_piece(value):
    return bool(re.fullmatch(r"\d+\s*[smhd]", value.strip().lower()))

def parse_timer_input(raw):
    text = raw.strip()
    if not text:
        return None, None

    quote_match = re.match(r'^(\S+)\s+[\"“”\'‘’](.+?)[\"“”\'‘’]$', text)
    if quote_match:
        return quote_match.group(1), quote_match.group(2)

    tokens = split_friendly_words(text)
    if not tokens:
        return None, None

    leading = []
    for token in tokens:
        if not is_timer_duration_piece(token):
            break
        leading.append(token)
    if leading:
        time_str = " ".join(leading)
        title = " ".join(tokens[len(leading):]) or None
        return time_str, title

    trailing = []
    for token in reversed(tokens):
        if not is_timer_duration_piece(token):
            break
        trailing.append(token)
    if trailing:
        trailing.reverse()
        time_str = " ".join(trailing)
        title = " ".join(tokens[:len(tokens) - len(trailing)]) or None
        return time_str, title

    parts = text.split(maxsplit=1)
    return parts[0], parts[1] if len(parts) > 1 else None

def parse_alarm_datetime(raw):
    text = raw.strip()
    if not text:
        return None

    seconds = duration_seconds(text)
    if seconds:
        return datetime.now(timezone.utc) + timedelta(seconds=seconds)

    formats = [
        "%d/%m/%Y %H:%M",
        "%d/%m/%y %H:%M",
        "%d/%m/%Y",
        "%d/%m/%y",
        "%d/%m %H:%M",
        "%d/%m",
    ]
    now = datetime.now(timezone.utc)
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if "%Y" not in fmt and "%y" not in fmt:
            parsed = parsed.replace(year=now.year)
        if "%H" not in fmt:
            parsed = parsed.replace(hour=9, minute=0)
        parsed = parsed.replace(tzinfo=timezone.utc)
        if parsed <= now and ("%Y" not in fmt and "%y" not in fmt):
            parsed = parsed.replace(year=parsed.year + 1)
        return parsed
    return None

def format_remaining(seconds: int) -> str:
    if seconds < 0:
        seconds = 0
    hrs, rem = divmod(seconds, 3600)
    mins, secs = divmod(rem, 60)
    parts = []
    if hrs > 0: parts.append(f"{hrs}h")
    if mins > 0: parts.append(f"{mins}m")
    parts.append(f"{secs}s")
    return " ".join(parts)

async def timer_countdown_message(channel, guild, message, end_time, time_str, title, owner_id):
    while True:
        now = datetime.now(timezone.utc)
        remaining = int((end_time - now).total_seconds())
        if remaining <= 0:
            break

        time_left = format_remaining(remaining)
        description = f"{economy_q_timer_tick} Time remaining:\n```{time_left}```"
        embed = message.embeds[0]
        embed.description = description
        try:
            await message.edit(embed=embed)
        except Exception:
            break
        await asyncio.sleep(1)

    embed = message.embeds[0]
    if title and title != "Timer":
        embed.description = f"{economy_q_alarm} Time's up!\n```{title}``` timer has ended"
    else:
        embed.description = f"{economy_q_alarm} Time's up!"

    embed.set_footer(text=f"Ended at:")
    embed.timestamp = datetime.now(timezone.utc)
    try:
        await message.edit(embed=embed)
    except Exception:
        pass

    active_timers.pop(message.id, None)
    remove_active_timer(message.id)

    user = guild.get_member(owner_id) if guild else None
    if user is None:
        try:
            user = await bot.fetch_user(owner_id)
        except Exception:
            user = None
    mention = user.mention if user else f"<@{owner_id}>"
    if title:
        await channel.send(f"{economy_q_alarm} {mention} Your **{title}** timer for **{time_str}** is over!")
    else:
        await channel.send(f"{economy_q_alarm} {mention} Your timer for **{time_str}** is over!")

async def timer_countdown(ctx, message, end_time, time_str, title, owner_id):
    await timer_countdown_message(ctx.channel, ctx.guild, message, end_time, time_str, title, owner_id)

async def start_timer_for_user(channel, guild, author, time_str, title):
    seconds = parse_time_string(time_str)
    if seconds is None or seconds <= 0:
        raise ValueError("Use a time like `30s`, `10m`, `1h 20m`, or `2d`.")

    title_display = title if title else "Timer"
    end_time = datetime.now(timezone.utc) + timedelta(seconds=seconds)

    embed = Embed(
        title=title_display,
        description=f"{economy_q_timer_tick} Time remaining:\n```{format_remaining(seconds)}```",
        color=0x00ff00
    )
    embed.set_footer(text="Ends at:")
    embed.timestamp = end_time
    message = await channel.send(embed=embed)

    task = asyncio.create_task(
        timer_countdown_message(channel, guild, message, end_time, time_str, title, author.id)
    )

    active_timers[message.id] = {
        "owner_id": author.id,
        "channel_id": channel.id,
        "guild_id": guild.id if guild else 0,
        "message": message,
        "title": title,
        "time_str": time_str,
        "end_time": end_time,
        "task": task
    }
    save_active_timer(message.id, active_timers[message.id])
    return message

class TimerSetupModal(Modal):
    def __init__(self, author_id):
        super().__init__(title="Create Timer")
        self.author_id = author_id
        self.duration = TextInput(label="Time", placeholder="10m, 1h 20m, 30s", max_length=40)
        self.title_input = TextInput(label="Title", placeholder="Optional: study, food, break", required=False, max_length=120)
        self.add_item(self.duration)
        self.add_item(self.title_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            message = await start_timer_for_user(
                interaction.channel,
                interaction.guild,
                interaction.user,
                str(self.duration.value).strip(),
                str(self.title_input.value).strip() or None
            )
        except ValueError as e:
            return await interaction.followup.send(str(e), ephemeral=True)
        await interaction.followup.send(f"Timer created: {message.jump_url}", ephemeral=True)

class OpenTimerSetupButton(Button):
    def __init__(self):
        super().__init__(label="Create Timer", style=discord.ButtonStyle.primary, emoji=reaction_emoji(economy_q_timer))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(TimerSetupModal(self.view.author_id))

@bot.command()
async def timer(ctx, *, args: str = None):
    """Start a timer. Examples: .timer 10m, .timer 10m study, .timer study 10m"""
    if ctx.guild is None:
        return await ctx.send("Timers can only be used in a server.")

    args = (args or "").strip()
    if not args:
        return await ctx.send(
            "Set up your timer here, or type `.timer 10m study`.",
            view=SingleUserSetupView(ctx.author.id, OpenTimerSetupButton())
        )

    time_str, title = parse_timer_input(args)
    try:
        await start_timer_for_user(ctx.channel, ctx.guild, ctx.author, time_str, title)
    except ValueError as e:
        await ctx.send(str(e))


class CancelConfirmView(View):
    def __init__(self, timer_id, ctx, timer_data, parent_view):
        super().__init__(timeout=120)
        self.timer_id = timer_id
        self.ctx = ctx
        self.timer_data = timer_data
        self.parent_view = parent_view
        self.value = None

    async def on_timeout(self):
        try:
            await self.message.edit(content="Cancel confirmation timed out.", view=None)
        except:
            pass
        if self.parent_view:
            self.parent_view.enable_all_items()
            try:
                await self.parent_view.message.edit(view=self.parent_view)
            except:
                pass

    async def interaction_check(self, interaction):
        if interaction.user.id == self.ctx.author.id or interaction.user.id == super_owner_id:
            return True
        await interaction.response.send_message("Only the timer owner or superowner can use this.", ephemeral=True)
        return False

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.danger)
    async def yes_button(self, interaction, button):
        await interaction.response.defer()
        timer_task = self.timer_data.get("task")
        if timer_task and not timer_task.done():
            timer_task.cancel()

        embed = self.timer_data["message"].embeds[0]
        now = datetime.now(timezone.utc)
        remaining = int((self.timer_data["end_time"] - now).total_seconds())
        remaining_text = format_remaining(remaining)
        embed.description = f"Timer cancelled with {remaining_text} left."
        embed.set_footer(text="Cancelled at:")
        embed.timestamp = now
        try:
            await self.timer_data["message"].edit(embed=embed)
        except:
            pass

        active_timers.pop(self.timer_id, None)
        remove_active_timer(self.timer_id)

        if self.parent_view and self.parent_view.message:
            try:
                self.parent_view.disable_all_items()
                await self.parent_view.message.edit(
                    content=f"Timer cancelled with `{remaining_text}` left: [Timer]({self.timer_data['message'].jump_url})",
                    view=None
                )
            except:
                pass

        await interaction.edit_original_response(
            content=f"Timer cancelled with `{remaining_text}` left: [Timer]({self.timer_data['message'].jump_url})",
            view=None
        )

        self.value = True
        self.stop()

    @discord.ui.button(label="No", style=discord.ButtonStyle.secondary)
    async def no_button(self, interaction, button):
        await interaction.response.edit_message(content="Cancel aborted.", view=None)
        if self.parent_view:
            self.parent_view.enable_all_items()
            try:
                await self.parent_view.message.edit(view=self.parent_view)
            except:
                pass
        self.value = False
        self.stop()

class CancelSelectView(View):
    def __init__(self, ctx, timers):
        super().__init__(timeout=120)
        self.ctx = ctx
        self.timers = timers
        self.selected_timer_id = None
        self.message = None

        options = []
        for tid, data in timers:
            title = data["title"]
            if not title or title == "Timer":
                title = ""
            else:
                title = f"{title} | "
            remaining = int((data["end_time"] - datetime.now(timezone.utc)).total_seconds())
            remaining_text = format_remaining(remaining)
            label = f"{title}{data['time_str']} — {remaining_text} left"
            if len(label) > 100:
                label = label[:97] + "..."
            options.append(discord.SelectOption(label=label, value=str(tid)))

        self.select = discord.ui.Select(
            placeholder="Select a timer to cancel...",
            options=options,
            min_values=1,
            max_values=1
        )
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction):
        timer_id = int(self.select.values[0])
        if timer_id not in active_timers:
            await interaction.response.send_message("This timer has already ended or been cancelled. Refreshing list...", ephemeral=True)
            await self.refresh_view(interaction)
            return

        timer_data = active_timers[timer_id]

        self.disable_all_items()
        await interaction.response.edit_message(view=self)

        confirm_view = CancelConfirmView(timer_id, self.ctx, timer_data, parent_view=self)
        confirm_msg = await interaction.followup.send(
            f"Are you sure you want to cancel: [Jump to timer message]({timer_data['message'].jump_url})?",
            view=confirm_view,
            ephemeral=True,
            wait=True
        )
        confirm_view.message = confirm_msg

    async def refresh_view(self, interaction):
        new_timers = []
        for tid, data in active_timers.items():
            if self.ctx.author.id == super_owner_id or data["owner_id"] == self.ctx.author.id:
                new_timers.append((tid, data))

        if not new_timers:
            await interaction.edit_original_response(content="No active timers found.", view=None)
            return

        self.timers = new_timers
        options = []
        for tid, data in new_timers:
            title = data["title"]
            if not title or title == "Timer":
                title = ""
            else:
                title = f"{title} | "
            remaining = int((data["end_time"] - datetime.now(timezone.utc)).total_seconds())
            remaining_text = format_remaining(remaining)
            label = f"{title}{data['time_str']} — {remaining_text} left"
            if len(label) > 100:
                label = label[:97] + "..."
            options.append(discord.SelectOption(label=label, value=str(tid)))

        self.select.options = options
        self.enable_all_items()
        await interaction.edit_original_response(content="Select a timer to cancel:", view=self)

    def disable_all_items(self):
        for item in self.children:
            item.disabled = True

    def enable_all_items(self):
        for item in self.children:
            item.disabled = False

@bot.command()
async def ctimer(ctx):
    if ctx.author.id == super_owner_id:
        timers_list = list(active_timers.items())
    else:
        timers_list = [(tid, data) for tid, data in active_timers.items() if data["owner_id"] == ctx.author.id]

    if not timers_list:
        return await ctx.send("No active timers found.")

    view = CancelSelectView(ctx, timers_list)
    sent_msg = await ctx.send("Select a timer to cancel:", view=view)
    view.message = sent_msg

async def alarm_wait_and_send(channel, user_id, alarm_time, title):
    await asyncio.sleep(max(0, (alarm_time - datetime.now(timezone.utc)).total_seconds()))
    if title:
        await channel.send(f"{economy_q_bell} <@{user_id}> **{title}**")
    else:
        await channel.send(f"{economy_q_bell} <@{user_id}> Here's your alarm.")

async def schedule_alarm_for_user(channel, author, raw):
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("Use a time like `1h`, `30m`, `25/12`, or `25/12/2026 18:30`.")

    tokens = split_friendly_words(raw)
    title = None
    alarm_time = None

    for span in (2, 1):
        if len(tokens) >= span:
            candidate = " ".join(tokens[:span])
            parsed = parse_alarm_datetime(candidate)
            if parsed:
                alarm_time = parsed
                title = raw[len(candidate):].strip() or None
                break
    if alarm_time is None and tokens:
        candidate = tokens[-1]
        parsed = parse_alarm_datetime(candidate)
        if parsed:
            alarm_time = parsed
            title = raw[:raw.rfind(candidate)].strip() or None
    if alarm_time is None:
        raise ValueError("Use a time like `1h`, `30m`, `25/12`, or `25/12/2026 18:30`.")
    if alarm_time <= datetime.now(timezone.utc):
        raise ValueError("Alarm time must be in the future.")

    asyncio.create_task(alarm_wait_and_send(channel, author.id, alarm_time, title))
    return alarm_time, title

class AlarmSetupModal(Modal):
    def __init__(self, author_id):
        super().__init__(title="Create Alarm")
        self.author_id = author_id
        self.when = TextInput(label="When", placeholder="1h, 30m, 25/12 18:00", max_length=60)
        self.title_input = TextInput(label="Reminder", placeholder="Optional: check oven", required=False, max_length=140)
        self.add_item(self.when)
        self.add_item(self.title_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        raw = f"{self.when.value} {self.title_input.value}".strip()
        try:
            alarm_time, title = await schedule_alarm_for_user(interaction.channel, interaction.user, raw)
        except ValueError as e:
            return await interaction.response.send_message(str(e), ephemeral=True)
        title_text = f" for **{title}**" if title else ""
        await interaction.response.send_message(
            f"{economy_q_alarm} Alarm set{title_text}: {discord.utils.format_dt(alarm_time, 'R')} ({discord.utils.format_dt(alarm_time, 'f')}).",
            ephemeral=True
        )

class OpenAlarmSetupButton(Button):
    def __init__(self):
        super().__init__(label="Create Alarm", style=discord.ButtonStyle.primary, emoji=reaction_emoji(economy_q_alarm))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(AlarmSetupModal(self.view.author_id))

@bot.command()
async def alarm(ctx, *, args: str = None):
    """Set an alarm. Examples: .alarm 1h check oven OR .alarm 25/12 18:00"""
    raw = (args or "").strip()
    if not raw:
        return await ctx.send(
            "Set up your alarm here, or type `.alarm 1h reminder`.",
            view=SingleUserSetupView(ctx.author.id, OpenAlarmSetupButton())
        )
    try:
        alarm_time, title = await schedule_alarm_for_user(ctx.channel, ctx.author, raw)
    except ValueError as e:
        return await ctx.send(str(e))
    title_text = f" for **{title}**" if title else ""
    await ctx.send(
        f"{economy_q_alarm} Alarm set{title_text}: {discord.utils.format_dt(alarm_time, 'R')} ({discord.utils.format_dt(alarm_time, 'f')}).",
        allowed_mentions=discord.AllowedMentions.none()
    )

_allowed_funcs = {
    'sin': math.sin, 'cos': math.cos, 'tan': math.tan,
    'asin': math.asin, 'acos': math.acos, 'atan': math.atan,
    'atan2': math.atan2, 'sinh': math.sinh, 'cosh': math.cosh, 'tanh': math.tanh,
    'asinh': math.asinh, 'acosh': math.acosh, 'atanh': math.atanh,
    'sqrt': math.sqrt, 'cbrt': lambda x: x ** (1/3),
    'log': lambda x, base=math.e: math.log(x, base) if base != math.e else math.log(x),
    'ln': math.log, 'log10': math.log10, 'log2': math.log2,
    'exp': math.exp, 'pow': pow, 'abs': abs, 'round': round,
    'floor': math.floor, 'ceil': math.ceil, 'trunc': math.trunc,
    'factorial': math.factorial, 'gamma': math.gamma, 'lgamma': math.lgamma,
    'degrees': math.degrees, 'radians': math.radians
}
_allowed_consts = {
    'pi': math.pi, 'e': math.e, 'tau': math.tau, 'inf': math.inf, 'nan': math.nan
}
_operators = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.LShift: operator.lshift,
    ast.RShift: operator.rshift,
    ast.BitOr: operator.or_,
    ast.BitAnd: operator.and_,
    ast.BitXor: operator.xor
}
_unary_ops = {
    ast.UAdd: lambda x: +x,
    ast.USub: lambda x: -x,
    ast.Invert: operator.invert
}

class _SafeEval(ast.NodeVisitor):
    def visit(self, node):
        return super().visit(node)

    def visit_Expression(self, node):
        return self.visit(node.body)

    def visit_Constant(self, node):
        if isinstance(node.value, (int, float, complex)):
            return node.value
        raise ValueError("Unsupported constant type")

    def visit_Num(self, node):
        return node.n

    def visit_Name(self, node):
        if node.id in _allowed_consts:
            return _allowed_consts[node.id]
        raise NameError(f"Unknown identifier: {node.id}")

    def visit_BinOp(self, node):
        left = self.visit(node.left)
        right = self.visit(node.right)
        op_type = type(node.op)
        if op_type in _operators:
            func = _operators[op_type]
            return func(left, right)
        raise ValueError("Unsupported binary operator")

    def visit_UnaryOp(self, node):
        operand = self.visit(node.operand)
        op_type = type(node.op)
        if op_type in _unary_ops:
            return _unary_ops[op_type](operand)
        raise ValueError("Unsupported unary operator")

    def visit_Call(self, node):
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            if func_name not in _allowed_funcs:
                raise NameError(f"Function not allowed: {func_name}")
            func = _allowed_funcs[func_name]
            args = [self.visit(a) for a in node.args]
            kwargs = {}
            for kw in node.keywords:
                if kw.arg is None:
                    raise ValueError("No kwargs unpacking allowed")
                kwargs[kw.arg] = self.visit(kw.value)
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise type(e)(f"{e}")
        raise ValueError("Only simple function calls are allowed")

    def visit_Expr(self, node):
        return self.visit(node.value)

    def visit_List(self, node):
        return [self.visit(e) for e in node.elts]

    def visit_Tuple(self, node):
        return tuple(self.visit(e) for e in node.elts)

    def generic_visit(self, node):
        raise ValueError(f"Unsupported expression: {type(node).__name__}")

def safe_eval(expr: str):
    tree = ast.parse(expr, mode='eval')
    visitor = _SafeEval()
    return visitor.visit(tree)

def calculate_expression_text(expression):
    expr = expression.replace('^', '**')
    result = safe_eval(expr)
    if isinstance(result, float):
        if math.isinf(result):
            resa = "Infinity"
        elif math.isnan(result):
            resa = "NaN"
        else:
            resa = repr(result)
    else:
        resa = repr(result)
    return f"Input: `{expression}`\nResult: `{resa}`"

class CalcSetupModal(Modal):
    def __init__(self, author_id):
        super().__init__(title="Calculator")
        self.author_id = author_id
        self.expression = TextInput(label="Expression", placeholder="2+2*5, sqrt(144), sin(pi/2)", max_length=300)
        self.add_item(self.expression)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        try:
            response = calculate_expression_text(str(self.expression.value))
        except Exception as e:
            response = f"Error: {e}"
        await interaction.response.send_message(response)

class OpenCalcSetupButton(Button):
    def __init__(self):
        super().__init__(label="Calculate", style=discord.ButtonStyle.primary)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(CalcSetupModal(self.view.author_id))

@bot.command(name="calc")
async def calc(ctx, *, expression: str = None):
    if not expression:
        return await ctx.send(
            "Enter an expression here, or type `.calc 2+2*5`.",
            view=SingleUserSetupView(ctx.author.id, OpenCalcSetupButton())
        )
    try:
        await ctx.send(calculate_expression_text(expression))
    except Exception as e:
        await ctx.send(f"Error: {e}")

async def define_word_text(word):
    async with aiohttp.ClientSession() as session:
        url = f"https://api.dictionaryapi.dev/api/v2/entries/en/{word}"
        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    definitions = []
    for meaning in data[0]["meanings"]:
        part_of_speech = meaning["partOfSpeech"]
        for d in meaning["definitions"]:
            definition = d["definition"]
            definitions.append(f"**({part_of_speech})** {definition}")
    unique_defs = list(dict.fromkeys(definitions))
    return f"{economy_q_book} **Definition of `{word}`:**\n" + "\n".join(unique_defs[:3])

class DefineSetupModal(Modal):
    def __init__(self, author_id):
        super().__init__(title="Define Word")
        self.author_id = author_id
        self.word = TextInput(label="Word", placeholder="example", max_length=80)
        self.add_item(self.word)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.defer()
        response = await define_word_text(str(self.word.value).strip())
        await interaction.followup.send(response or "Couldn't find that word.")

class OpenDefineSetupButton(Button):
    def __init__(self):
        super().__init__(label="Define", style=discord.ButtonStyle.primary, emoji=reaction_emoji(economy_q_book))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(DefineSetupModal(self.view.author_id))

@bot.command()
async def define(ctx, *, word: str = None):
    if not word:
        return await ctx.send(
            "Enter a word here, or type `.define example`.",
            view=SingleUserSetupView(ctx.author.id, OpenDefineSetupButton())
        )
    try:
        response = await define_word_text(word.strip())
        await ctx.send(response or "Couldn't find that word.")
    except Exception:
        await ctx.send("Error.")

@bot.command()
@is_admin_power()
async def summon(ctx, *, message: str = "h-hi"):
    await safe_delete_message(ctx.message)
    await ctx.send(f"@everyone {message}")

@bot.command()
@is_admin_power()
async def summon2(ctx, *, message: str):
    role = discord.utils.get(ctx.guild.roles, name="everyone2")
    if role is None:
        return await ctx.send("Role not found.")
    if not role.mentionable:
        return await ctx.send("The role is not mentionable.")

    await ctx.send(f"{role.mention} {message}")

@bot.command()
@is_admin_power()
async def block(ctx, member: discord.Member):
    if not can_act_on(ctx.author, member, ctx.guild):
        return await ctx.send("You can't block that member.")
    blocked = guild_blacklisted_users(ctx.guild)
    blocked.add(member.id)
    save_blacklisted_users(scoped_id(ctx.guild), blocked)
    await ctx.send(
        f"<@{member.id}> is now blocked from using commands.",
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
@is_admin_power()
async def unblock(ctx, member: discord.Member):
    if not can_act_on(ctx.author, member, ctx.guild) and member.id not in guild_blacklisted_users(ctx.guild):
        return await ctx.send("You can't unblock that member.")
    blocked = guild_blacklisted_users(ctx.guild)
    blocked.discard(member.id)
    save_blacklisted_users(scoped_id(ctx.guild), blocked)
    await ctx.send(
        f"<@{member.id}> is now unblocked.",
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
async def sleep(ctx):
    sleeping_users[ctx.author.id] = datetime.now(timezone.utc)
    save_sleeping_user(ctx.author.id, sleeping_users[ctx.author.id])
    await ctx.send(f"You’re now in sleep mode. {economy_q_sleep} Good night!")

@bot.command()
async def fsleep(ctx, members: commands.Greedy[discord.Member], *, time: str = None):
    if not has_super_owner_power(ctx.author, ctx.guild):
        return

    if not members:
        return await ctx.send("No members provided.", delete_after=5)

    results = []

    for member in members:
        start_time = datetime.now(timezone.utc)

        if time:
            matches = re.findall(r'(\d+)\s*(h|m|s)', time.lower())
            if not matches:
                results.append(f"{economy_q_warning} Invalid time format: `{time}` (skipped {member.mention})")
                continue

            total_seconds = 0
            for value, unit in matches:
                v = int(value)
                if unit == 'h':
                    total_seconds += v * 3600
                elif unit == 'm':
                    total_seconds += v * 60
                elif unit == 's':
                    total_seconds += v

            if total_seconds > 0:
                start_time -= timedelta(seconds=total_seconds)

        sleeping_users[member.id] = start_time
        save_sleeping_user(member.id, start_time)
        await ctx.send(
            f"Marked {member.mention} as asleep since <t:{int(start_time.timestamp())}:F>"
        )

    if results:
        await ctx.send("\n".join(results), delete_after=10)

@bot.command()
async def wake(ctx, members: commands.Greedy[discord.Member]):
    if not has_super_owner_power(ctx.author, ctx.guild):
        return

    for member in members:
        sleeping_users.pop(member.id, None)
        remove_sleeping_user(member.id)

@bot.command()
async def afk(ctx, *, reason="AFK"):
    afk_users[ctx.author.id] = {
        "reason": reason,
        "since": datetime.now(timezone.utc)
    }
    save_afk_user(ctx.author.id, reason, afk_users[ctx.author.id]["since"])

    reason_text = f": **{reason}**" if reason.lower() != "afk" else ""
    await ctx.send(f"{ctx.author.mention} You're now AFK {reason_text}", allowed_mentions=discord.AllowedMentions.none())

def save_user_birthday(user_id, date):
    datetime.strptime(date, "%d/%m")
    birthdays[str(user_id)] = {"date": date}
    save_birthday(user_id, date)

class BirthdaySetupModal(Modal):
    def __init__(self, author_id):
        super().__init__(title="Set Birthday")
        self.author_id = author_id
        self.date = TextInput(label="Birthday", placeholder="DD/MM, example: 25/12", max_length=5)
        self.add_item(self.date)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        try:
            save_user_birthday(interaction.user.id, str(self.date.value).strip())
        except ValueError:
            return await interaction.response.send_message("Use `DD/MM`, example: `25/12`.", ephemeral=True)
        await interaction.response.send_message(f"{economy_q_accept} Birthday saved!", ephemeral=True)

class OpenBirthdaySetupButton(Button):
    def __init__(self):
        super().__init__(label="Set Birthday", style=discord.ButtonStyle.primary, emoji=reaction_emoji(economy_q_birthday))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(BirthdaySetupModal(self.view.author_id))

@bot.command()
async def setbday(ctx, date: str = None):
    if not date:
        return await ctx.send(
            "Set your birthday here, or type `.setbday 25/12`.",
            view=SingleUserSetupView(ctx.author.id, OpenBirthdaySetupButton())
        )
    try:
        save_user_birthday(ctx.author.id, date)
        await ctx.send(f"{economy_q_accept} Birthday saved!")
    except ValueError:
        await ctx.send("Invalid date format. Use DD/MM.")

@bot.command()
async def removebday(ctx):
    user_id = str(ctx.author.id)
    if user_id in birthdays:
        del birthdays[user_id]
        remove_birthday(ctx.author.id)
        await ctx.send("Birthday removed.")
    else:
        await ctx.send("You haven’t set a birthday.")

@bot.command(name="away")
async def away(ctx):
    now = datetime.now(timezone.utc)

    async def format_status_embed():
        embed = discord.Embed(title="AFK & Sleeping Users", color=0x3498db)
        now = datetime.now(timezone.utc)

        if afk_users:
            afk_text = ""
            for uid, data in afk_users.items():
                user = bot.get_user(uid) or await bot.fetch_user(uid)
                duration = now - data["since"]
                days, remainder = divmod(int(duration.total_seconds()), 86400)
                hours, remainder = divmod(remainder, 3600)
                mins = remainder // 60

                time_parts = []
                if days: time_parts.append(f"{days}d")
                if hours: time_parts.append(f"{hours}h")
                if mins or not time_parts:
                    time_parts.append(f"{mins}m")
                formatted = " ".join(time_parts)

                afk_text += f"<@!{user.id}> — been AFK for {formatted}\n"

            embed.add_field(name="AFK Users", value=embed_value(afk_text), inline=False)

        if sleeping_users:
            sleep_text = ""
            for uid, start in sleeping_users.items():
                user = bot.get_user(uid) or await bot.fetch_user(uid)
                duration = now - start
                days, remainder = divmod(int(duration.total_seconds()), 86400)
                hours, remainder = divmod(remainder, 3600)
                mins = remainder // 60

                time_parts = []
                if days: time_parts.append(f"{days}d")
                if hours: time_parts.append(f"{hours}h")
                if mins or not time_parts:
                    time_parts.append(f"{mins}m")
                formatted = " ".join(time_parts)

                sleep_text += f"<@!{user.id}> — been asleep for {formatted}\n"

            embed.add_field(name="Sleeping Users", value=embed_value(sleep_text), inline=False)

        if not afk_users and not sleeping_users:
            embed.description = "No users are currently AFK or sleeping."

        embed.timestamp = now
        return embed

    status_msg = await ctx.send(embed=await format_status_embed())

    while afk_users or sleeping_users:
        await asyncio.sleep(10)
        await status_msg.edit(embed=await format_status_embed())

@bot.command()
async def find(ctx, user_id: int):
    member = ctx.guild.get_member(user_id) if ctx.guild else None
    if member is None and ctx.guild:
        try:
            member = await ctx.guild.fetch_member(user_id)
        except discord.NotFound:
            member = None
        except discord.HTTPException as e:
            return await ctx.send(f"Could not fetch server member: {e}")

    if member is None:
        try:
            user = await bot.fetch_user(user_id)
        except discord.NotFound:
            return await ctx.send(f"User not found: `{user_id}`")
        except discord.HTTPException as e:
            return await ctx.send(f"Could not fetch user: {e}")
        return await ctx.send(
            f"User found globally: {user.mention} (`{user.id}`)",
            allowed_mentions=discord.AllowedMentions.none()
        )

    await ctx.send(
        f"User found: {member.mention} (`{member.id}`)",
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
@is_admin_power()
async def censor(ctx, *, phrase: str):
    phrase = normalize(phrase)
    phrases = guild_censored_phrases(ctx.guild)
    if phrase in phrases:
        return await ctx.send(f"'{phrase}' is already being censored.")
    
    phrases.append(phrase)
    save_censored_phrases(scoped_id(ctx.guild), phrases)
    await ctx.send(f"Now censoring messages containing: `{phrase}`")

@bot.command()
@is_admin_power()
async def uncensor(ctx, *, phrase: str):
    phrase = normalize(phrase)
    phrases = guild_censored_phrases(ctx.guild)
    if phrase not in phrases:
        return await ctx.send(f"'{phrase}' is not currently censored.")
    
    phrases.remove(phrase)
    save_censored_phrases(scoped_id(ctx.guild), phrases)
    await ctx.send(f"Stopped censoring: `{phrase}`")

@bot.command()
@is_admin_power()
async def clearcensors(ctx):
    guild_censored_phrases(ctx.guild).clear()
    save_censored_phrases(scoped_id(ctx.guild), guild_censored_phrases(ctx.guild))
    await ctx.send("All censors have been cleared.")

def generate_list_embed(title, user_ids, guild=None, show_names=False):
    embed = discord.Embed(title=title, color=0x3498db, timestamp=datetime.now(timezone.utc))
    if not user_ids:
        embed.description = "None."
        return embed

    lines = []
    for uid in user_ids:
        if guild:
            member = guild.get_member(uid)
            if member:
                mention = f"<@{member.id}>"
                lines.append(mention)
            else:
                lines.append(f"<@{uid}>")
        else:
            lines.append(f"<@{uid}>")
    embed.description = "\n".join(lines)
    return embed

@bot.command()
async def listtargets(ctx):
    embed = generate_list_embed("Watched Targets", guild_watchlist(ctx.guild), guild=ctx.guild)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="listbans")
@is_admin_power()
async def listbans(ctx):
    try:
        bans = await ctx.guild.bans()
        if not bans:
            return await ctx.send("No banned users in this server.")

        user_ids = [ban.user.id for ban in bans]
        embed = generate_list_embed(f"Banned Users ({len(bans)})", user_ids, guild=ctx.guild)
        await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

    except discord.Forbidden:
        await ctx.send("I don’t have permission to view bans.")
    except Exception as e:
        await ctx.send(f"Error: {type(e).__name__} - {e}")

@bot.command()
@is_admin_power()
async def listblocks(ctx):
    embed = generate_list_embed("Blocked Users", guild_blacklisted_users(ctx.guild), guild=ctx.guild)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command()
async def listcensors(ctx):
    phrases = guild_censored_phrases(ctx.guild)
    if not phrases:
        return await ctx.send("No censors are active.")
    
    formatted = "\n".join(f"- {p}" for p in phrases)
    await ctx.send(f"Active censors:\n{formatted}")

@bot.command()
@is_admin_power()
async def lists(ctx):
    embed = discord.Embed(title="Server Lists", color=0x3498db, timestamp=datetime.now(timezone.utc))

    targets = guild_watchlist(ctx.guild)
    blocked = guild_blacklisted_users(ctx.guild)
    phrases = guild_censored_phrases(ctx.guild)

    targets_text = joined_embed_value([f"<@{uid}>" for uid in targets])
    embed.add_field(name="Watched Targets", value=targets_text, inline=True)

    try:
        bans_list = [ban async for ban in ctx.guild.bans()]
        banned_text = joined_embed_value([f"<@{ban.user.id}>" for ban in bans_list])
    except discord.Forbidden:
        banned_text = "Cannot view bans."
    embed.add_field(name="Banned Users", value=banned_text, inline=True)

    blocked_text = joined_embed_value([f"<@{uid}>" for uid in blocked])
    embed.add_field(name="Blocked Users", value=blocked_text, inline=True)

    censored_text = joined_embed_value([f"- {p}" for p in phrases])
    embed.add_field(name="Censored Phrases", value=censored_text, inline=True)

    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

def home():
    return "Bot alive"

def run_flask():
    app.run(host="0.0.0.0", port=8080)

Thread(target=run_flask).start()

def run_bot_with_retry():
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        print("ERROR: DISCORD_TOKEN not set!")
        return
    
    max_retries = 5
    base_delay = 5
    
    for attempt in range(max_retries):
        try:
            print(f"Attempting to login (attempt {attempt + 1}/{max_retries})...")
            bot.run(token, reconnect=True)
            return
        except discord.errors.HTTPException as e:
            if e.status == 429:
                delay = base_delay * (2 ** attempt)
                print(f"Rate limited! Waiting {delay} seconds before retry...")
                time.sleep(delay)
            else:
                print(f"HTTP error: {e}")
                raise
        except RuntimeError as e:
            if "Session is closed" in str(e):
                delay = base_delay * (2 ** attempt)
                print(f"Session closed! Waiting {delay} seconds before retry...")
                time.sleep(delay)
            else:
                raise
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise
    
    print("Max retries reached. Exiting.")

# === AI COMMANDS ===

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
CLOUDFLARE_API_KEY = os.getenv("CLOUDFLARE_API_KEY", "")
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")

@bot.command(name="ask")
async def ask_command(ctx, *, question: str):
    """Ask AI anything - answers simply, clearly, with sources"""
    if not GROQ_API_KEY:
        return await ctx.send("API not configured. Set GROQ_API_KEY.")
    
    await safe_add_reaction(ctx.message, economy_q_timer_tick)

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "messages": [
            {"role": "system", "content": "You are a helpful assistant. Answer clearly, simply, and briefly. If you use information from the web, cite your sources."},
            {"role": "user", "content": question}
        ],
        "model": "llama-3.1-8b-instant",
        "temperature": 0.7,
        "max_tokens": 500
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers) as resp:
                if resp.status != 200:
                    await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
                    return await ctx.send(f"Error: {resp.status}")

                data = await resp.json(content_type=None)
                answer = data["choices"][0]["message"]["content"]

                if len(answer) > 1900:
                    answer = answer[:1897] + "..."

                await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
                await ctx.send(answer)

    except Exception as e:
        await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
        await ctx.send(f"Error: {str(e)[:100]}")

@bot.command(name="generate")
async def generate_command(ctx, *, prompt: str):
    """Generate an image from text"""
    if not CLOUDFLARE_API_KEY or not CLOUDFLARE_ACCOUNT_ID:
        return await ctx.send("API not configured. Set CLOUDFLARE_API_KEY and CLOUDFLARE_ACCOUNT_ID.")
    
    await safe_add_reaction(ctx.message, economy_q_timer_tick)

    url = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/ai/run/@cf/black-forest-labs/flux-1-schnell"

    headers = {
        "Authorization": f"Bearer {CLOUDFLARE_API_KEY}"
    }

    payload = {
        "prompt": prompt,
        "num_steps": 4
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status != 200:
                    await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
                    return await ctx.send(f"Error: {resp.status}")

                data = await resp.json()
                image_data = data["result"]["image"]

                import base64
                from io import BytesIO
                image_bytes = base64.b64decode(image_data)
                file = discord.File(BytesIO(image_bytes), filename="generated.png")

                await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
                await ctx.send(file=file)

    except Exception as e:
        await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
        await ctx.send(f"Error: {str(e)[:100]}")


# === IMAGE ANALYSIS COMMAND ===
async def image_url_to_data_uri(session, image_url):
    import base64

    async with session.get(image_url) as resp:
        if resp.status != 200:
            raise ValueError(f"image download failed: {resp.status}")
        image_bytes = await resp.read()
        if len(image_bytes) > 8 * 1024 * 1024:
            raise ValueError("image is too large")
        content_type = resp.headers.get("Content-Type", "image/png").split(";", 1)[0].strip().lower()
        if not content_type.startswith("image/"):
            content_type = "image/png"
        encoded = base64.b64encode(image_bytes).decode("ascii")
        return f"data:{content_type};base64,{encoded}"

@bot.command(name="analyse", aliases=["analyze", "analyseimage", "analyzeimage", "vision"])
async def analyse_command(ctx):
    """Analyse an image - reply to an image or attachment"""
    if not ctx.message.reference or not ctx.message.reference.resolved:
        return await ctx.send("Reply to an image to analyse it.")
    
    ref_msg = ctx.message.reference.resolved
    image_url = None
    
    # Check attachments
    if ref_msg.attachments:
        for att in ref_msg.attachments:
            if att.content_type and att.content_type.startswith("image/"):
                image_url = att.url
                break
    
    # Check embeds
    if not image_url and ref_msg.embeds:
        for emb in ref_msg.embeds:
            if emb.image:
                image_url = emb.image.url
            elif emb.thumbnail:
                image_url = emb.thumbnail.url
    
    if not image_url:
        return await ctx.send("No image found in the replied message.")
    
    if not CLOUDFLARE_API_KEY or not CLOUDFLARE_ACCOUNT_ID:
        return await ctx.send("API not configured.")
    
    await safe_add_reaction(ctx.message, economy_q_timer_tick)
    
    url = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/ai/run/@cf/meta/llama-3.2-11b-vision-instruct"
    
    headers = {"Authorization": f"Bearer {CLOUDFLARE_API_KEY}"}
    
    try:
        async with aiohttp.ClientSession() as session:
            try:
                image_data_uri = await image_url_to_data_uri(session, image_url)
            except ValueError as e:
                await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
                return await ctx.send(f"Could not read image: {e}")

            payload = {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "Describe this image in detail."},
                            {"type": "image_url", "image_url": {"url": image_data_uri}}
                        ]
                    }
                ],
                "max_tokens": 500
            }
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
                    if resp.status == 400 and "agree" in error_text.lower():
                        return await ctx.send("Error: Cloudflare needs the Meta vision model license accepted first.")
                    return await ctx.send(f"Error: {resp.status} — {error_text[:180]}")
                
                data = await resp.json(content_type=None)
                result = data["result"]["response"]
                
                if len(result) > 1900:
                    result = result[:1897] + "..."
                
                await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
                await ctx.send(result)
    except Exception as e:
        await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
        await ctx.send(f"Error: {str(e)[:100]}")

# === TRANSLATE COMMAND ===
LANGUAGE_ALIASES = {
    "arabic": "ar", "ar": "ar",
    "english": "en", "en": "en",
    "spanish": "es", "es": "es",
    "french": "fr", "fr": "fr",
    "german": "de", "de": "de",
    "italian": "it", "italy": "it", "it": "it",
    "portuguese": "pt", "pt": "pt",
    "russian": "ru", "ru": "ru",
    "japanese": "ja", "jp": "ja", "ja": "ja",
    "korean": "ko", "ko": "ko",
    "chinese": "zh-cn", "mandarin": "zh-cn", "zh": "zh-cn", "zh-cn": "zh-cn",
    "hindi": "hi", "hi": "hi",
    "turkish": "tr", "tr": "tr",
    "dutch": "nl", "nl": "nl",
    "urdu": "ur", "ur": "ur",
}

def normalize_language_code(value):
    if not value:
        return None
    cleaned = value.strip().lower().replace("_", "-")
    return LANGUAGE_ALIASES.get(cleaned, cleaned if re.fullmatch(r"[a-z]{2,3}(?:-[a-z]{2,4})?", cleaned) else None)

def parse_translate_args(raw):
    text = raw.strip()
    if not text:
        return "auto", "en", ""

    match = re.match(r"^to\s+([a-zA-Z-]+)$", text, flags=re.I)
    if match:
        target = normalize_language_code(match.group(1))
        if target:
            return "auto", target, ""

    match = re.match(r"^to\s+([a-zA-Z-]+)\s+(.+)$", text, flags=re.I)
    if match:
        target = normalize_language_code(match.group(1))
        if target:
            return "auto", target, match.group(2).strip()

    match = re.match(r"^(.+?)\s+to\s+([a-zA-Z-]+)$", text, flags=re.I)
    if match:
        target = normalize_language_code(match.group(2))
        if target:
            return "auto", target, match.group(1).strip()

    first, _, rest = text.partition(" ")
    if "|" in first and rest:
        left, right = first.split("|", 1)
        source = normalize_language_code(left) or "auto"
        target = normalize_language_code(right) or "en"
        return source, target, rest.strip()

    target = normalize_language_code(first)
    if target and rest:
        return "auto", target, rest.strip()

    return "auto", "en", text

async def translate_text_api(source_lang, target_lang, text):
    async with aiohttp.ClientSession() as session:
        params = {
            "client": "gtx",
            "sl": source_lang,
            "tl": target_lang,
            "dt": "t",
            "q": text,
        }
        async with session.get("https://translate.googleapis.com/translate_a/single", params=params) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Error: {resp.status}")
            data = await resp.json()
    result = "".join(part[0] for part in data[0] if part and part[0])
    detected_lang = data[2] if len(data) > 2 and data[2] else source_lang
    response = f"**Detected: {detected_lang.upper()} → {target_lang.upper()}**\n{result}"
    return response[:1897] + "..." if len(response) > 1900 else response

class TranslateSetupModal(Modal):
    def __init__(self, author_id):
        super().__init__(title="Translate")
        self.author_id = author_id
        self.text = TextInput(label="Text", placeholder="hello", style=discord.TextStyle.paragraph, max_length=1500)
        self.language = TextInput(label="To", placeholder="English, Spanish, Italian, ar, fr...", required=False, max_length=40)
        self.add_item(self.text)
        self.add_item(self.language)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        target_lang = normalize_language_code(str(self.language.value).strip()) or "en"
        await interaction.response.defer()
        try:
            response = await translate_text_api("auto", target_lang, str(self.text.value).strip())
        except Exception as e:
            response = f"Error: {str(e)[:100]}"
        await interaction.followup.send(response)

class OpenTranslateSetupButton(Button):
    def __init__(self):
        super().__init__(label="Translate", style=discord.ButtonStyle.primary)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(TranslateSetupModal(self.view.author_id))

SETUP_UI_COMMANDS = {"poll", "timer", "alarm", "picker", "giveaway", "calc", "define", "setbday", "translate"}

def command_setup_view(author_id, command_name):
    buttons = {
        "poll": OpenPollSetupButton,
        "timer": OpenTimerSetupButton,
        "alarm": OpenAlarmSetupButton,
        "picker": OpenPickerSetupButton,
        "giveaway": OpenGiveawaySetupButton,
        "calc": OpenCalcSetupButton,
        "define": OpenDefineSetupButton,
        "setbday": OpenBirthdaySetupButton,
        "translate": OpenTranslateSetupButton,
    }
    button_cls = buttons.get(command_name)
    if not button_cls:
        return None
    return SingleUserSetupView(author_id, button_cls())

@bot.command(name="translate")
async def translate_command(ctx, *, args: str = None):
    """Translate text. Examples: .translate hello to Italian OR reply with .translate to Spanish"""

    source_lang = "auto"
    target_lang = "en"

    if args:
        source_lang, target_lang, text = parse_translate_args(args)
        if not text and ctx.message.reference and ctx.message.reference.resolved:
            ref_msg = ctx.message.reference.resolved
            text = ref_msg.content
            if not text:
                return await ctx.send("No text found in the replied message.")
    elif ctx.message.reference and ctx.message.reference.resolved:
        ref_msg = ctx.message.reference.resolved
        text = ref_msg.content
        if not text:
            return await ctx.send("No text found in the replied message.")
    else:
        return await ctx.send(
            "Translate text here, or type `.translate hello to Italian`.",
            view=SingleUserSetupView(ctx.author.id, OpenTranslateSetupButton())
        )
    
    await safe_add_reaction(ctx.message, economy_q_timer_tick)
    
    try:
        response = await translate_text_api(source_lang, target_lang, text)
        await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
        await ctx.send(response)
            
    except Exception as e:
        await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
        await ctx.send(f"Error: {str(e)[:100]}")

# === RUN BOT ===


# Start the bot
run_bot_with_retry()
