import asyncio
import ast
import concurrent.futures
import colorsys
import json
import logging
import math
import operator
import os
import random
import re
import shlex
import struct
import time
import traceback
import zlib

import aiohttp
import discord
import economy as economy_module
import pytz
try:
    import chess as chess_lib
except ImportError:
    chess_lib = None
try:
    from PIL import Image
except ImportError:
    Image = None
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from discord import Embed, Emoji, File, Interaction, StickerItem
from discord.ext import commands, tasks
from discord.ui import Button, Modal, Select, TextInput, View
try:
    from discord.ext.commands.view import StringView
except Exception:
    StringView = None
from flask import Flask
from io import BytesIO
from threading import Thread

def load_local_env_file(path=".env"):
    """Tiny dotenv fallback for local runs; Railway should still use real env vars."""
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        pass

load_local_env_file()

from economy import (
    add_user_balance as economy_add_user_balance,
    award_chat_xp as economy_award_chat_xp,
    build_level_up_embed as economy_build_level_up_embed,
    bulk_add_users as economy_bulk_add_users,
    bulk_adjust_lottery_tickets as economy_bulk_adjust_lottery_tickets,
    DETAILED_EXPLANATIONS as economy_detailed_explanations,
    ensure_db_ready as economy_ensure_db_ready,
    EXPLANATIONS as economy_explanations,
    format_balance as economy_format_balance,
    get_lottery_config as economy_get_lottery_config,
    get_economy_channel_id as economy_get_economy_channel_id,
    get_leaderboard_user_ids as economy_get_leaderboard_user_ids,
    get_game_stat as economy_get_game_stat,
    get_user as economy_get_user,
    lottery_ticket_rows as economy_lottery_ticket_rows,
    log_transaction as economy_log_transaction,
    parse_amount as economy_parse_amount,
    Q_ACCEPT as economy_q_accept,
    Q_ACTIVITY as economy_q_activity,
    Q_ALARM as economy_q_alarm,
    Q_AUDIT as economy_q_audit,
    Q_AI_HISTORY as economy_q_ai_history,
    Q_ARCHIVE as economy_q_archive,
    Q_ATTACHMENT as economy_q_attachment,
    Q_BELL as economy_q_bell,
    Q_BIRTHDAY as economy_q_birthday,
    Q_BIRTHDAY_BALLOONS as economy_q_birthday_balloons,
    Q_BIRTHDAY_CAKE as economy_q_birthday_cake,
    Q_BANK as economy_q_bank,
    Q_BOOK as economy_q_book,
    Q_BROOM as economy_q_broom,
    Q_CARDS as economy_q_cards,
    Q_COLOUR as economy_q_colour,
    Q_COLOUR_PICKER as economy_q_colour_picker,
    Q_CONFETTI as economy_q_confetti,
    Q_CONNECT_BLACK as economy_q_connect_black,
    Q_CONNECT_WHITE as economy_q_connect_white,
    Q_DENIED as economy_q_denied,
    Q_EDIT as economy_q_edit,
    Q_EVENT as economy_q_event,
    Q_FILTER as economy_q_filter,
    Q_GAME_AUDIT as economy_q_game_audit,
    Q_GAME_O as economy_q_game_o,
    Q_GAME_STATS as economy_q_game_stats,
    Q_GAME_TIMEOUT as economy_q_game_timeout,
    Q_GAME_WIN as economy_q_game_win,
    Q_GAME_X as economy_q_game_x,
    Q_GIFT as economy_q_gift,
    Q_HAMMER as economy_q_hammer,
    Q_HISTORY as economy_q_history,
    Q_IMAGE as economy_q_image,
    Q_QUOTE as economy_q_quote,
    Q_COMMAND_CHECK as economy_q_command_check,
    Q_LEVEL_PULSE as economy_q_level_pulse,
    Q_LOCK as economy_q_lock,
    Q_PERMISSIONS as economy_q_permissions,
    Q_PERF as economy_q_perf,
    Q_POLL as economy_q_poll,
    Q_PALETTE as economy_q_palette,
    Q_DATABASE as economy_q_database,
    Q_ERROR_LOG as economy_q_error_log,
    Q_REACTION as economy_q_reaction,
    Q_RECOMMEND as economy_q_recommend,
    Q_RECOVERY as economy_q_recovery,
    Q_REJECT as economy_q_reject,
    Q_REFRESH as economy_q_refresh,
    Q_ROB as economy_q_rob,
    Q_ROLES as economy_q_roles,
    Q_QUEUE as economy_q_queue,
    Q_SLEEP as economy_q_sleep,
    Q_SNIPE as economy_q_snipe,
    Q_SETUP as economy_q_setup,
    Q_SEASON_PASS as economy_q_season_pass,
    Q_TARGET as economy_q_target,
    Q_THINKING as economy_q_thinking,
    Q_TICKET as economy_q_ticket,
    Q_TIMEOUT as economy_q_timeout,
    Q_TIMER as economy_q_timer,
    Q_TIMER_TICK as economy_q_timer_tick,
    Q_TRASH as economy_q_trash,
    Q_TRUST as economy_q_trust,
    Q_USER_EDIT as economy_q_user_edit,
    Q_VOICE as economy_q_voice,
    Q_WARNING as economy_q_warning,
    Q_TUTORIAL as economy_q_tutorial,
    get_receipt as economy_get_receipt,
    get_receipts_for_user as economy_get_receipts_for_user,
    risk_label as economy_risk_label,
    record_game_result as economy_record_game_result,
    setup as economy_setup,
    set_economy_channel_id as economy_set_economy_channel_id,
    todays_daily_challenge as economy_todays_daily_challenge,
    track_daily_challenge_progress as economy_track_daily_challenge_progress,
    update_user as economy_update_user,
)
from pgdata import (
    add_guild_activity_counts,
    add_message_activity_events,
    clear_away_mentions,
    clear_guild_activity_counts,
    delete_guild_activity_channel,
    delete_guild_birthday_channel,
    delete_active_game_session,
    get_guild_activity_top,
    get_ai_control_settings,
    get_bot_receipt,
    get_command_usage_stats,
    get_message_activity_count,
    get_message_activity_top,
    get_message_activity_top_between,
    get_message_event,
    load_afk_users as pg_load_afk_users,
    load_active_polls,
    load_active_timers,
    load_away_mentions,
    load_active_giveaways,
    load_active_alarms,
    load_ai_channel_memory,
    load_ai_user_memory,
    load_active_game_sessions,
    load_autoban_ids,
    load_blacklisted_users,
    load_birthday_cards,
    load_birthdays as pg_load_birthdays,
    load_censored_phrases,
    load_disabled_commands,
    load_guild_activity_channels,
    load_guild_birthday_channels,
    load_guild_prefixes,
    load_guild_log_config,
    load_message_events,
    load_reaction_shutdown_channels,
    load_truth_or_dare_channels,
    load_reaction_watchlist,
    load_shutdown_channels,
    load_sleeping_users as pg_load_sleeping_users,
    load_watchlist,
    pg_init,
    record_command_usage,
    remove_afk_user,
    remove_active_poll,
    remove_active_timer,
    remove_active_giveaway,
    remove_active_alarm,
    remove_birthday_card,
    remove_birthday,
    remove_sleeping_user,
    delete_message_event,
    save_afk_user,
    save_active_poll,
    save_active_timer,
    save_away_mention,
    save_active_giveaway,
    save_active_alarm,
    save_active_game_session,
    save_autoban_ids,
    save_ai_channel_memory,
    save_ai_user_memory,
    save_birthday_card,
    save_blacklisted_users,
    save_birthday,
    save_censored_phrases,
    save_disabled_commands,
    save_guild_activity_channel,
    save_guild_birthday_channel,
    save_guild_prefix,
    save_guild_log_config,
    save_message_event,
    save_bot_receipt,
    set_ai_control_setting,
    delete_ai_control_setting,
    save_reaction_shutdown_channels,
    save_truth_or_dare_channels,
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
presence_task = None
app = Flask('')
LONG_HELP_VIEW_TIMEOUT = 24 * 60 * 60
LONG_SETUP_VIEW_TIMEOUT = 60 * 60
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
CLOUDFLARE_API_KEY = os.getenv("CLOUDFLARE_API_KEY", "")
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")
HF_TOKEN = os.getenv("HF_TOKEN", "")
HF_IMAGE_MODEL = os.getenv("HF_IMAGE_MODEL", "black-forest-labs/FLUX.1-schnell")
HF_IMAGE_ENDPOINT = f"https://router.huggingface.co/hf-inference/models/{HF_IMAGE_MODEL}"
HF_IMAGE_TIMEOUT_SECONDS = int(os.getenv("PROQUE_IMAGE_TIMEOUT", "120"))
HF_IMAGE_COOLDOWN_SECONDS = int(os.getenv("PROQUE_IMAGE_COOLDOWN", "45"))
AI_MODEL_TIMEOUT_SECONDS = 10
AI_SUMMARY_MAX_MESSAGES = 250
AI_SUMMARY_DEFAULT_MESSAGES = 80
AI_SUMMARY_MAX_CHARS = 4500
AI_REQUEST_MAX_CHARS = 5200
AI_SYSTEM_CONTEXT_MAX_CHARS = 1900
AI_MESSAGE_CONTEXT_MAX_CHARS = 1800
AI_DETECT_MAX_CHARS = 9000
COMMAND_CONCURRENCY_LIMIT = int(os.getenv("PROQUE_COMMAND_CONCURRENCY", "48"))
HEAVY_COMMAND_CONCURRENCY_LIMIT = int(os.getenv("PROQUE_HEAVY_COMMAND_CONCURRENCY", "8"))
BULK_COMMAND_CONCURRENCY_LIMIT = int(os.getenv("PROQUE_BULK_COMMAND_CONCURRENCY", "2"))
AI_COMMAND_CONCURRENCY_LIMIT = int(os.getenv("PROQUE_AI_COMMAND_CONCURRENCY", "4"))
IMAGE_COMMAND_CONCURRENCY_LIMIT = int(os.getenv("PROQUE_IMAGE_CONCURRENCY", "2"))
DB_WORKER_LIMIT = int(os.getenv("PROQUE_DB_WORKERS", "24"))

command_semaphore = asyncio.Semaphore(COMMAND_CONCURRENCY_LIMIT)
heavy_command_semaphore = asyncio.Semaphore(HEAVY_COMMAND_CONCURRENCY_LIMIT)
bulk_command_semaphore = asyncio.Semaphore(BULK_COMMAND_CONCURRENCY_LIMIT)
ai_command_semaphore = asyncio.Semaphore(AI_COMMAND_CONCURRENCY_LIMIT)
image_command_semaphore = asyncio.Semaphore(IMAGE_COMMAND_CONCURRENCY_LIMIT)
AI_COMMAND_NAMES = {
    "ask", "generate", "imagine", "image", "aiimage", "genimg", "profilebanner", "banner",
    "profileart", "makeemoji", "genemoji", "emojiart", "eventposter", "poster", "eventart",
    "bdaycard", "birthdaycard", "viewbdaycard", "bdaypreview", "birthdaypreview", "cardpreview",
    "shoppreview", "itempreview", "shopart", "gameart",
    "gameimage", "reactionimage", "aireaction", "reactimage", "analyse", "analyze", "summarize", "summarise", "summary",
    "aisummary", "tldr", "recap", "aidetect", "aicheck", "detectai",
    "authenticity", "authcheck", "essaycheck",
}
BULK_COMMAND_NAMES = {
    "add", "remove", "move", "setquesos", "addtick", "removetick", "movetick", "settick", "purge",
    "rpurge", "archive", "fwd", "forward", "send", "reply",
}
HEAVY_COMMAND_NAMES = {
    "shop", "help", "econhelp", "quewohelp", "lb", "leaderboard", "activity",
    "messages", "economyaudit", "abuseaudit", "gameaudit", "balancedashboard",
    "flagquiz", "giveaway", "steal", "lottery", "lotterystats",
}

# PostgreSQL is the single source of truth
pg_init()

birthdays = {
    str(uid): {"date": date}
    for uid, date in (pg_load_birthdays() or {}).items()
}
birthday_card_prompts = load_birthday_cards() or {}

# Loaded from PostgreSQL
afk_users = pg_load_afk_users() or {}
sleeping_users = pg_load_sleeping_users() or {}

class MyBot(commands.Bot):
    async def setup_hook(self):
        loop = asyncio.get_running_loop()
        loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=DB_WORKER_LIMIT))

    async def close(self):
        session = getattr(self, "proque_http_session", None)
        if session and not session.closed:
            await session.close()
        await super().close()
        
DEFAULT_PREFIX = "."

intents = discord.Intents.all()
def get_prefix(bot, message):
    """Use only the saved server prefix for commands."""
    guild_id = message.guild.id if message.guild else 0
    return guild_prefixes.get(guild_id, DEFAULT_PREFIX)

bot = MyBot(command_prefix=get_prefix, intents=intents, case_insensitive=True)
bot.remove_command("help")
print(f"Bot is starting with intents: {bot.intents}")

async def get_http_session():
    session = getattr(bot, "proque_http_session", None)
    if session is None or session.closed:
        timeout = aiohttp.ClientTimeout(total=max(15, AI_MODEL_TIMEOUT_SECONDS + 5))
        connector = aiohttp.TCPConnector(limit=64, limit_per_host=12, ttl_dns_cache=300)
        session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        bot.proque_http_session = session
    return session

IMAGE_STYLE_PRESETS = {
    "general": "clean ProQue themed digital illustration, polished, high quality, subtle queso coins, no text, no watermark",
    "banner": "wide Discord profile banner, clean ProQue themed digital art, elegant composition, subtle queso coins, no text, no watermark",
    "emoji": "single custom Discord emoji icon, centered subject, transparent-looking simple background, bold readable silhouette, no text, no watermark",
    "event": "Discord event poster background, cinematic clean composition, ProQue themed accents, no text, no watermark",
    "game": "polished Discord game card art, dynamic but clean, ProQue themed accents, no text, no watermark",
    "birthday": "premium birthday card illustration, cake, balloons, soft confetti, ProQue themed queso coins, no text, no watermark",
    "reaction": "fun expressive reaction image, clean animated-style character energy, ProQue themed accents, no text, no watermark",
    "shop": "premium shop item preview, product render style, clean dark background, subtle queso coins, no text, no watermark",
}
image_generation_cooldowns = {}

def image_generation_available():
    return bool(HF_TOKEN or (CLOUDFLARE_API_KEY and CLOUDFLARE_ACCOUNT_ID))

def clean_image_prompt(prompt, *, max_chars=700):
    prompt = re.sub(r"\s+", " ", str(prompt or "")).strip()
    return prompt[:max_chars]

def image_prompt_for(kind, prompt):
    base = IMAGE_STYLE_PRESETS.get(kind, IMAGE_STYLE_PRESETS["general"])
    prompt = clean_image_prompt(prompt)
    return f"{base}. User request: {prompt}"

def image_cooldown_left(user_id):
    if int(user_id) == super_owner_id:
        return 0.0
    ready_at = image_generation_cooldowns.get(int(user_id), 0.0)
    return max(0.0, ready_at - time.monotonic())

def touch_image_cooldown(user_id):
    if int(user_id) != super_owner_id:
        image_generation_cooldowns[int(user_id)] = time.monotonic() + HF_IMAGE_COOLDOWN_SECONDS

def image_extension(content_type):
    content_type = (content_type or "").split(";", 1)[0].strip().lower()
    if content_type.endswith("png"):
        return "png"
    if content_type.endswith("webp"):
        return "webp"
    return "jpg"

async def generate_cloudflare_image_bytes(prompt):
    import base64

    if not CLOUDFLARE_API_KEY or not CLOUDFLARE_ACCOUNT_ID:
        raise RuntimeError("Image generation is not configured.")
    url = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/ai/run/@cf/black-forest-labs/flux-1-schnell"
    headers = {"Authorization": f"Bearer {CLOUDFLARE_API_KEY}"}
    session = await get_http_session()
    async with session.post(url, json={"prompt": prompt, "num_steps": 4}, headers=headers, timeout=HF_IMAGE_TIMEOUT_SECONDS) as resp:
        body = await resp.read()
        if resp.status != 200:
            raise RuntimeError(ai_http_error_message(resp.status, body.decode("utf-8", errors="ignore")))
        data = json.loads(body.decode("utf-8", errors="ignore"))
        return base64.b64decode(data["result"]["image"]), "png", "Cloudflare"

async def generate_hf_image_bytes(prompt, *, width=1024, height=576, steps=4):
    if not HF_TOKEN:
        return await generate_cloudflare_image_bytes(prompt)
    payload = {
        "inputs": prompt,
        "parameters": {
            "width": int(width),
            "height": int(height),
            "num_inference_steps": int(steps),
        },
        "options": {"wait_for_model": True},
    }
    headers = {
        "Authorization": f"Bearer {HF_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "image/*",
    }
    session = await get_http_session()
    try:
        response_cm = session.post(HF_IMAGE_ENDPOINT, json=payload, headers=headers, timeout=HF_IMAGE_TIMEOUT_SECONDS)
        resp = await response_cm.__aenter__()
    except (aiohttp.ClientConnectorCertificateError, aiohttp.ClientSSLError):
        response_cm = session.post(HF_IMAGE_ENDPOINT, json=payload, headers=headers, timeout=HF_IMAGE_TIMEOUT_SECONDS, ssl=False)
        resp = await response_cm.__aenter__()
    try:
        body = await resp.read()
        content_type = resp.headers.get("Content-Type", "")
        if resp.status != 200 or not content_type.startswith("image/"):
            text = body.decode("utf-8", errors="ignore")
            raise RuntimeError(ai_http_error_message(resp.status, text))
        return body, image_extension(content_type), "Hugging Face"
    finally:
        await response_cm.__aexit__(None, None, None)

async def run_image_generation(ctx, prompt, *, kind="general", title="Generated Image", width=1024, height=576):
    prompt = clean_image_prompt(prompt)
    if not prompt:
        return await send_command_input_ui(ctx, ctx.command.name, note="Enter what you want generated.")
    if not image_generation_available():
        return await ctx.send("Image generation is not configured. Add `HF_TOKEN` to Railway.")
    left = image_cooldown_left(ctx.author.id)
    if left > 0:
        return await ctx.send(f"{economy_q_timer_tick} Image generation cooldown: **{left:.0f}s**.")
    touch_image_cooldown(ctx.author.id)
    await safe_add_reaction(ctx.message, economy_q_timer_tick)
    try:
        if image_command_semaphore.locked():
            await ctx.reply(
                f"{economy_q_timer_tick} Image queue is busy. I’m keeping this one in line.",
                mention_author=False,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        styled_prompt = image_prompt_for(kind, prompt)
        async with image_command_semaphore:
            image_bytes, ext, provider = await generate_hf_image_bytes(styled_prompt, width=width, height=height)
        file = discord.File(BytesIO(image_bytes), filename=f"proque-{kind}.{ext}")
        embed = standard_embed(
            title,
            description=f"{economy_q_image} Prompt: {embed_value(prompt, limit=700)}",
            color=0x2A8FDA,
            icon=economy_q_image,
        )
        embed.set_footer(text=f"Generated with {provider}")
        embed.set_image(url=f"attachment://proque-{kind}.{ext}")
        await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
        await ctx.reply(embed=embed, file=file, mention_author=False, allowed_mentions=discord.AllowedMentions.none())
        return image_bytes, ext
    except Exception as e:
        await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
        await ctx.send(clean_user_error(e, ai_exception_message(e)))
        return None

async def polished_view_error(self, interaction: discord.Interaction, error: Exception, item):
    text = clean_user_error(error, "That panel had a problem. Run the command again for a fresh one.")
    if "This interaction failed" in text:
        text = "That panel expired or restarted. Run the command again for a fresh one."
    try:
        if interaction.response.is_done():
            await interaction.followup.send(text, ephemeral=True)
        else:
            await interaction.response.send_message(text, ephemeral=True)
    except Exception:
        pass
    try:
        await notify_superowner_error(interaction, error)
    except Exception:
        pass

discord.ui.View.on_error = polished_view_error

log_channel_id = None
rlog_channel_id = None
super_owner_id = 885548126365171824  
QUE_OWNER_DISPLAY = f"𝚀𝚞𝚎 (<@{super_owner_id}>)"
autoban_ids = load_autoban_ids()
blacklisted_users = load_blacklisted_users()
shutdown_channels = load_shutdown_channels()
reaction_shutdown_channels = load_reaction_shutdown_channels()
truth_or_dare_channels = load_truth_or_dare_channels()
disabled_commands = load_disabled_commands()
bot_maintenance_mode = False
bot_maintenance_reason = ""
maintenance_notice_times = {}
guild_prefixes = load_guild_prefixes()
guild_birthday_channels = load_guild_birthday_channels()
guild_activity_channels = load_guild_activity_channels()
guild_log_configs = {}
guild_invite_cache = {}
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
stale_game_messages_cleaned = False

AI_MEMORY_MAX_MESSAGES = 60
AI_MEMORY_TTL_SECONDS = 7 * 24 * 60 * 60
AI_MEMORY_DB_MAX_MESSAGES = 120
AI_USER_MEMORY_MAX_FACTS = 24
ai_conversation_memory = {}
pending_ai_batch_actions = {}
ai_action_history = deque(maxlen=80)
background_jobs = {}
background_job_counter = 0
command_error_events = deque(maxlen=80)
recent_log_event_keys = deque(maxlen=200)

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

def compact_ai_payload_text(text, limit):
    text = str(text or "")
    if len(text) <= limit:
        return text
    if limit <= 20:
        return text[:limit]
    return text[:limit - 14].rstrip() + "\n...[trimmed]"

def fit_ai_messages(messages, max_chars=AI_REQUEST_MAX_CHARS):
    """Keep AI requests under the provider TPM limit by trimming hidden context first."""
    cleaned = []
    for msg in messages:
        role = msg.get("role", "user")
        content = str(msg.get("content") or "").strip()
        if not content:
            continue
        per_message_limit = AI_SYSTEM_CONTEXT_MAX_CHARS if role == "system" else AI_MESSAGE_CONTEXT_MAX_CHARS
        cleaned.append({"role": role, "content": compact_ai_payload_text(content, per_message_limit)})
    if not cleaned:
        return [{"role": "user", "content": "hi"}]

    first_system = cleaned[0] if cleaned[0]["role"] == "system" else None
    rest = cleaned[1:] if first_system else cleaned
    result_reversed = []
    used = 0

    if first_system:
        first = dict(first_system)
        first["content"] = compact_ai_payload_text(first["content"], min(AI_SYSTEM_CONTEXT_MAX_CHARS, max_chars // 2))
        used += len(first["content"])

    for msg in reversed(rest):
        remaining = max_chars - used
        if remaining <= 200:
            break
        content = msg["content"]
        if len(content) > remaining:
            if msg["role"] == "user" or not result_reversed:
                content = compact_ai_payload_text(content, remaining)
            else:
                continue
        used += len(content)
        result_reversed.append({"role": msg["role"], "content": content})

    result = []
    if first_system:
        result.append(first)
    result.extend(reversed(result_reversed))
    if not any(msg["role"] == "user" for msg in result):
        last_user = next((msg for msg in reversed(cleaned) if msg["role"] == "user"), None)
        if last_user:
            result.append({"role": "user", "content": compact_ai_payload_text(last_user["content"], AI_MESSAGE_CONTEXT_MAX_CHARS)})
    return result

def ai_memory_key(message):
    guild_id = message.guild.id if message.guild else 0
    return (int(guild_id), int(message.channel.id))

def prune_ai_memory(key):
    now = time.time()
    entries = [
        entry for entry in ai_conversation_memory.get(key, [])
        if now - float(entry.get("ts", 0)) <= AI_MEMORY_TTL_SECONDS
    ]
    if len(entries) > AI_MEMORY_MAX_MESSAGES:
        entries = entries[-AI_MEMORY_MAX_MESSAGES:]
    if entries:
        ai_conversation_memory[key] = entries
    else:
        ai_conversation_memory.pop(key, None)
    return entries

def ai_memory_messages(key):
    if key not in ai_conversation_memory:
        guild_id, channel_id = key
        stored = load_ai_channel_memory(guild_id, channel_id, AI_MEMORY_MAX_MESSAGES)
        if stored:
            ai_conversation_memory[key] = stored
    entries = prune_ai_memory(key)
    messages = []
    for entry in entries[-8:]:
        role = entry.get("role")
        content = str(entry.get("content") or "").strip()
        if role not in {"user", "assistant"} or not content:
            continue
        messages.append({"role": role, "content": content[:360]})
    return messages

def ai_channel_context_text(key, limit=6):
    if key not in ai_conversation_memory:
        guild_id, channel_id = key
        stored = load_ai_channel_memory(guild_id, channel_id, AI_MEMORY_MAX_MESSAGES)
        if stored:
            ai_conversation_memory[key] = stored
    entries = prune_ai_memory(key)
    context_lines = []
    for entry in entries:
        if entry.get("role") != "context":
            continue
        content = str(entry.get("content") or "").strip()
        if content:
            context_lines.append(content[:180])
    if not context_lines:
        return ""
    return "\n".join(context_lines[-limit:])[:1000]

def remember_ai_message(key, role, content):
    content = str(content or "").strip()
    if not content:
        return
    ai_conversation_memory.setdefault(key, []).append({
        "role": role,
        "content": content[:900],
        "ts": time.time(),
    })
    prune_ai_memory(key)
    guild_id, channel_id = key
    asyncio.create_task(asyncio.to_thread(save_ai_channel_memory, guild_id, channel_id, role, content[:1000], AI_MEMORY_DB_MAX_MESSAGES))

def remember_chat_context(message):
    if not message.guild or not message.content or message.author.bot:
        return
    content = message.content.strip()
    if not content:
        return
    author_name = getattr(message.author, "display_name", message.author.name)
    channel_name = getattr(message.channel, "name", str(message.channel.id))
    key = ai_memory_key(message)
    ai_conversation_memory.setdefault(key, []).append({
        "role": "context",
        "content": f"[#{channel_name}] {author_name} ({message.author.id}): {content[:450]}",
        "ts": time.time(),
    })
    prune_ai_memory(key)
    remember_user_facts_from_message(message)

AI_MEMORY_SKIP_PATTERNS = re.compile(
    r"\b(password|passcode|token|api\s*key|secret|private\s*key|2fa|otp|phone|email|address)\b",
    re.IGNORECASE,
)

def clean_memory_fact(text, limit=220):
    text = re.sub(r"\s+", " ", str(text or "")).strip(" .")
    if not text or AI_MEMORY_SKIP_PATTERNS.search(text):
        return ""
    if len(text) > limit:
        text = text[:limit].rstrip()
    return text

def extract_user_memory_facts(content):
    text = str(content or "").strip()
    if not text:
        return []
    lowered = text.casefold()
    facts = []

    explicit = re.search(r"\bremember(?:\s+that)?\s+(.+)", text, flags=re.IGNORECASE | re.DOTALL)
    if explicit:
        fact = clean_memory_fact(explicit.group(1))
        if fact:
            facts.append(f"They asked me to remember: {fact}.")

    patterns = [
        (r"\b(?:my name is|call me)\s+(.+)", "They prefer to be called {value}."),
        (r"\bmy birthday is\s+(.+)", "Their birthday is {value}."),
        (r"\bmy timezone is\s+(.+)", "Their timezone is {value}."),
        (r"\b(?:i am|i'm|im) from\s+(.+)", "They are from {value}."),
        (r"\bi live in\s+(.+)", "They live in {value}."),
        (r"\bi(?: really)? (?:like|love|enjoy)\s+(.+)", "They like {value}."),
        (r"\bi(?: really)? (?:hate|dislike)\s+(.+)", "They dislike {value}."),
        (r"\bi prefer\s+(.+)", "They prefer {value}."),
        (r"\b(?:be|keep it)\s+(brief|short|simple|serious|funny|casual)\s+(?:with me|for me)?\b", "They prefer AI replies to be {value}."),
        (r"\b(?:don't|do not)\s+(joke|make jokes|be funny)\s+(?:with me|for me)?\b", "They prefer fewer jokes in AI replies."),
        (r"\bexplain(?: things)?\s+(simply|in detail|step by step)\s+(?:to me|for me)?\b", "They prefer explanations {value}."),
    ]
    for pattern, template in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        value = clean_memory_fact(match.group(1))
        if value:
            facts.append(template.format(value=value) + ".")

    if "forget" in lowered and "remember" in lowered:
        facts = []

    unique = []
    seen = set()
    for fact in facts:
        key = fact.casefold()
        if key not in seen:
            unique.append(fact)
            seen.add(key)
    return unique[:3]

def remember_user_facts_from_message(message):
    if not message.guild or message.author.bot:
        return
    facts = extract_user_memory_facts(message.content)
    if not facts:
        return
    guild_id = message.guild.id
    user_id = message.author.id
    try:
        existing = load_ai_user_memory(guild_id, user_id, AI_USER_MEMORY_MAX_FACTS)
        existing_keys = {str(item.get("fact", "")).casefold() for item in existing}
    except Exception:
        existing_keys = set()
    for fact in facts:
        if fact.casefold() in existing_keys:
            continue
        asyncio.create_task(asyncio.to_thread(
            save_ai_user_memory,
            guild_id,
            user_id,
            fact,
            f"#{getattr(message.channel, 'name', message.channel.id)}",
            AI_USER_MEMORY_MAX_FACTS,
        ))

def is_ai_forget_memory_request(text):
    lowered = str(text or "").casefold()
    return any(phrase in lowered for phrase in (
        "forget me",
        "forget what you know about me",
        "forget everything about me",
        "delete my memory",
        "clear my memory",
        "remove my memory",
    ))

def is_ai_memory_query(text):
    lowered = str(text or "").casefold()
    return any(phrase in lowered for phrase in (
        "what do you know about me",
        "what do you remember about me",
        "show my memory",
        "what's in my memory",
        "whats in my memory",
        "what have you saved about me",
    ))

async def ai_user_memory_text(message, referenced_message=None, limit_users=6, limit_facts=8):
    if not message.guild:
        return ""
    user_ids = []
    for user in [message.author, *(getattr(message, "mentions", []) or [])]:
        if user and not getattr(user, "bot", False) and user.id not in user_ids:
            user_ids.append(user.id)
    if referenced_message and not getattr(referenced_message.author, "bot", False):
        if referenced_message.author.id not in user_ids:
            user_ids.append(referenced_message.author.id)

    lines = []
    for user_id in user_ids[:limit_users]:
        facts, bot_facts = await asyncio.gather(
            asyncio.to_thread(load_ai_user_memory, message.guild.id, user_id, limit_facts),
            asyncio.to_thread(bot_saved_user_facts, message.guild, user_id),
        )
        if not facts:
            facts = []
        member = message.guild.get_member(user_id)
        display = getattr(member, "display_name", str(user_id))
        fact_values = [str(item.get("fact", "")).strip() for item in facts if item.get("fact")]
        fact_values.extend(bot_facts)
        fact_text = "; ".join(fact_values)
        if fact_text:
            lines.append(f"{display} ({user_id}): {fact_text}")
    return "\n".join(lines)[:1800]

def bot_saved_user_facts(guild, user_id):
    facts = []
    user_key = str(int(user_id))
    birthday = birthdays.get(user_key, {}).get("date")
    if birthday:
        facts.append(f"Saved birthday: {birthday}.")
    if int(user_id) in afk_users:
        reason = str(afk_users[int(user_id)].get("reason") or "AFK")
        facts.append(f"Current bot status: AFK ({reason}).")
    if int(user_id) in sleeping_users:
        facts.append("Current bot status: sleeping.")
    try:
        data = economy_get_user(int(user_id))
        if data:
            level = int(data.get("level", 1))
            balance = economy_format_balance(int(data.get("balance", 0)))
            bank = economy_format_balance(int(data.get("bank_balance", 0) or 0))
            daily = int(data.get("daily_streak", 0) or 0)
            weekly = int(data.get("weekly_streak", 0) or 0)
            monthly = int(data.get("monthly_streak", 0) or 0)
            facts.append(f"𝚀𝚞𝚎wo profile: level {level}, cash {balance}, bank {bank}, streaks daily {daily}/weekly {weekly}/monthly {monthly}.")
            try:
                inventory = economy_module.user_inventory(data)
                achievements = economy_module.achievement_ids(data)
                if inventory:
                    facts.append(f"Inventory count: {len(inventory)} item(s).")
                if achievements:
                    facts.append(f"Achievement count: {len(achievements)} badge(s).")
            except Exception:
                pass
    except Exception:
        pass
    if guild:
        member = guild.get_member(int(user_id))
        if member:
            facts.append(f"Server display name: {member.display_name}.")
    return facts[:6]

async def send_ai_memory_summary(destination, guild, user, *, ephemeral=False):
    facts, bot_facts = await asyncio.gather(
        asyncio.to_thread(load_ai_user_memory, guild.id if guild else 0, user.id, AI_USER_MEMORY_MAX_FACTS),
        asyncio.to_thread(bot_saved_user_facts, guild, user.id),
    )
    lines = [str(item.get("fact", "")).strip() for item in facts if item.get("fact")]
    lines.extend(bot_facts)
    embed = discord.Embed(
        title=f"{economy_q_book} AI Memory",
        description=f"What I know about <@{user.id}>:",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(
        name="Saved Facts",
        value=joined_embed_value([f"- {line}" for line in lines], empty="Nothing saved yet.", limit=3500),
        inline=False,
    )
    embed.set_footer(text="AI memory stays on so Pro𝚀𝚞𝚎 can keep bot help and user context consistent.")
    kwargs = {"embed": embed, "allowed_mentions": discord.AllowedMentions.none()}
    if isinstance(destination, discord.Interaction):
        return await destination.response.send_message(**kwargs, ephemeral=ephemeral)
    return await destination.reply(**kwargs, mention_author=False)

async def notify_superowner_error(ctx, error):
    command_name = ctx.command.qualified_name if getattr(ctx, "command", None) else "unknown"
    guild_id = ctx.guild.id if ctx.guild else 0
    channel_id = getattr(ctx.channel, "id", None)
    receipt_id = f"QERR-{int(time.time())}-{random.randint(1000, 9999)}"
    error_summary = f"{type(error).__name__}: {str(error)[:900]}"
    command_error_events.appendleft({
        "receipt_id": receipt_id,
        "command": command_name,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "user_id": ctx.author.id,
        "error": error_summary,
        "created_at": datetime.now(timezone.utc),
    })
    asyncio.create_task(asyncio.to_thread(
        save_bot_receipt,
        receipt_id,
        guild_id,
        channel_id,
        ctx.author.id,
        [],
        "command_error",
        None,
        error_summary,
    ))
    try:
        user = bot.get_user(super_owner_id) or await bot.fetch_user(super_owner_id)
    except Exception:
        return
    guild_name = ctx.guild.name if ctx.guild else "DM"
    channel_name = getattr(ctx.channel, "name", str(getattr(ctx.channel, "id", "unknown")))
    content = getattr(getattr(ctx, "message", None), "content", "") or ""
    jump = getattr(getattr(ctx, "message", None), "jump_url", "")
    embed = discord.Embed(
        title=f"{economy_q_warning} Pro𝚀𝚞𝚎 Command Error",
        description=(
            f"Command: `{command_name}`\n"
            f"Error: `{type(error).__name__}: {str(error)[:700]}`\n"
            f"User: <@{ctx.author.id}> ({ctx.author.id})\n"
            f"Server: **{guild_name}**\n"
            f"Channel: `#{channel_name}`\n"
            f"Receipt: `{receipt_id}`"
        ),
        color=discord.Color.red(),
        timestamp=datetime.now(timezone.utc),
    )
    if content:
        embed.add_field(name="Message", value=embed_value(f"`{content[:900]}`", 1000), inline=False)
    if jump:
        embed.add_field(name="Jump", value=f"[Open message]({jump})", inline=False)
    embed.set_footer(text="AI doctor context can help diagnose this.")
    try:
        await user.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    except Exception:
        pass

def command_permission_note(command):
    name = command.name.casefold()
    aliases = {alias.casefold() for alias in getattr(command, "aliases", []) or []}
    names = {name, *aliases}
    if names & SUPEROWNER_HIDDEN_COMMANDS:
        return f"{QUE_OWNER_DISPLAY} only."
    if any(name in {item.casefold() for item in HELP_CATEGORIES.get(category, [])} for category in ADMIN_HELP_CATEGORIES):
        return "Requires admin/server-owner power unless the command itself says otherwise."
    if name in {"editlottery", "stoplottery", "editactivity", "endactivity", "stopactivity"}:
        return "Server owner/admin command."
    return "Usually available."

def command_risk_note(command):
    names = {command.name.casefold(), *(alias.casefold() for alias in getattr(command, "aliases", []) or [])}
    if names & {"ban", "kick", "purge", "rpurge", "lock", "unlock", "lockdown", "reopen", "block", "unblock", "send", "reply", "speak", "relay"}:
        return "High impact moderation/server action."
    if names & {"add", "remove", "move", "give", "addtick", "removetick", "movetick", "settick", "lotterypot", "lotteryprize", "prizepool", "setpot", "addpot", "removepot", "setquesos"}:
        return "Changes 𝚀𝚞𝚎wo money or lottery entries."
    if names & GAMBLING_AMOUNT_COMMANDS:
        return "Gambling/game action; may spend 𝚀𝚞𝚎wo balance."
    if names & {"settings", "prefix", "setprefix", "setlogs", "setbdaychannel", "activity"}:
        return "Changes or opens server setup."
    return "Low impact."

def command_safety_label(command):
    names = {command.name.casefold(), *(alias.casefold() for alias in getattr(command, "aliases", []) or [])}
    if names & AI_BLOCKED_COMMANDS:
        return f"{economy_q_reject} AI blocked"
    if names & AI_SUPEROWNER_ONLY_COMMANDS:
        return f"{economy_q_warning} {QUE_OWNER_DISPLAY} only + confirmation"
    if names & AI_CONFIRM_COMMANDS:
        return f"{economy_q_thinking} Confirmation required"
    if names & AI_SAFE_COMMANDS:
        return f"{economy_q_accept} Read-only / low impact"
    return f"{economy_q_warning} Confirmation required"

def command_plan_embed(message, command, args, display):
    prefix = prefix_for_guild(message.guild)
    embed = discord.Embed(
        title=f"{economy_q_thinking} AI Command Plan",
        description=f"I think you want me to run:\n`{display}`",
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    aliases = ", ".join(getattr(command, "aliases", []) or []) or "None"
    detail = economy_detailed_explanations.get(command.name) or economy_explanations.get(command.name) or getattr(command, "help", "") or "Runs this bot command."
    embed.add_field(name="Command", value=f"`{prefix}{command.qualified_name}`", inline=True)
    embed.add_field(name="Aliases", value=embed_value(aliases, 500), inline=True)
    embed.add_field(name="Input", value=f"`{args}`" if args else "No extra input.", inline=False)
    embed.add_field(name="Permission", value=command_permission_note(command), inline=True)
    embed.add_field(name="Impact", value=command_risk_note(command), inline=True)
    embed.add_field(name="AI Safety", value=command_safety_label(command), inline=True)
    embed.add_field(name="What It Does", value=embed_value(detail, 900), inline=False)
    embed.set_footer(text="Confirm only if this is exactly what you wanted.")
    return embed

def bot_doctor_context(guild, active_sessions=None):
    guild_id = guild.id if guild else 0
    slow = []
    for name, stats in sorted(command_timing_stats.items(), key=lambda item: item[1]["max_ms"], reverse=True)[:6]:
        count = max(1, int(stats.get("count", 1)))
        avg = int(stats.get("total_ms", 0) / count)
        slow.append(f"{name}: avg {avg}ms, max {int(stats.get('max_ms', 0))}ms, runs {count}")
    if active_sessions is None:
        active_sessions = "not checked"
    return (
        f"Bot doctor snapshot: config DB ready=yes; 𝚀𝚞𝚎wo DB ready={getattr(economy_module, 'db_ready', False)}; "
        f"birthday task={'running' if birthday_task and not birthday_task.done() else 'stopped'}; "
        f"activity task={'running' if activity_task and not activity_task.done() else 'stopped'}; "
        f"presence task={'running' if presence_rotation_task.is_running() else 'stopped'}; "
        f"slash commands={'cleared' if slash_commands_synced else 'pending clear'}; active game sessions={active_sessions}; "
        f"guild disabled commands={len(guild_disabled_commands(guild)) if guild else 0}; "
        f"slow commands={'; '.join(slow) if slow else 'none tracked yet'}."
    )

async def bot_doctor_context_async(guild):
    try:
        sessions = await asyncio.to_thread(load_active_game_sessions)
        active_sessions = len([session for session in sessions if not guild or session.get("guild_id") == guild.id])
    except Exception:
        active_sessions = -1
    return bot_doctor_context(guild, active_sessions=active_sessions)

def compact_ai_text(text, limit=220):
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(text) <= limit:
        return text
    return text[:limit - 1].rstrip() + "…"

def ai_http_error_message(status, body=""):
    status = int(status or 0)
    if status == 429:
        return "AI is rate-limited right now. Give it a minute and try again."
    if status == 413:
        return "AI context got too large, so I trimmed it. Try that again."
    if status in {401, 403}:
        return "AI API key/config is being rejected. Check the AI provider key in Railway."
    if status >= 500:
        return "AI provider is having trouble right now. Try again in a bit."
    detail = clean_provider_error(body)
    return detail or f"AI request failed with HTTP {status}."

def clean_provider_error(body):
    text = str(body or "").strip()
    if not text:
        return ""
    candidates = []
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            for key in ("error", "errors", "messages"):
                value = data.get(key)
                if isinstance(value, dict):
                    msg = value.get("message") or value.get("error") or value.get("detail")
                    if msg:
                        candidates.append(str(msg))
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            msg = item.get("message") or item.get("error") or item.get("detail")
                            if msg:
                                candidates.append(str(msg))
                        elif item:
                            candidates.append(str(item))
            if data.get("message"):
                candidates.append(str(data["message"]))
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("message"):
                    candidates.append(str(item["message"]))
    except Exception:
        candidates.append(text)

    message = next((item for item in candidates if item.strip()), text)
    message = re.sub(r"^(AiError:\s*)+", "", message, flags=re.IGNORECASE).strip()
    message = re.sub(r"\s*\([0-9a-f]{8}-[0-9a-f-]{27,}\)\s*$", "", message, flags=re.IGNORECASE).strip()
    message = re.sub(r"\s+", " ", message).strip()
    if not message:
        return ""
    if "nsfw" in message.casefold():
        return "Input prompt contains NSFW content."
    if "rate limit" in message.casefold() or "too many requests" in message.casefold():
        return "AI is rate-limited right now. Give it a minute and try again."
    if "request too large" in message.casefold() or "tokens per minute" in message.casefold() or "tpm" in message.casefold():
        return "AI context got too large, so I trimmed it. Try that again."
    return compact_ai_text(message, 220)

def ai_likelihood_heuristic(text):
    text = str(text or "").strip()
    words = re.findall(r"[A-Za-z']+", text.lower())
    sentences = re.split(r"(?<=[.!?])\s+", text)
    word_count = len(words)
    unique_ratio = len(set(words)) / max(1, word_count)
    avg_sentence = word_count / max(1, len([s for s in sentences if s.strip()]))
    transitions = sum(1 for phrase in (
        "in conclusion", "moreover", "furthermore", "overall", "it is important to note",
        "this essay will", "in today's society", "a significant impact", "plays a crucial role",
    ) if phrase in text.lower())
    personal_markers = sum(1 for word in {"i", "me", "my", "we", "our"} if word in words)
    score = 35
    if word_count < 80:
        score -= 10
    if unique_ratio < 0.42:
        score += 12
    if 18 <= avg_sentence <= 28:
        score += 8
    if transitions:
        score += min(20, transitions * 5)
    if personal_markers == 0 and word_count > 180:
        score += 8
    if re.search(r"\b(?:specific example|real life example|personal experience)\b", text, re.IGNORECASE):
        score -= 6
    score = max(5, min(90, score))
    if score >= 70:
        label = "High"
    elif score >= 45:
        label = "Medium"
    else:
        label = "Low"
    return {
        "label": label,
        "score": score,
        "signals": [
            f"Word count: {word_count}",
            f"Vocabulary variety: {unique_ratio:.2f}",
            f"Average sentence length: {avg_sentence:.1f} words",
            f"Template-like transition phrases: {transitions}",
        ],
        "notes": "Heuristic fallback only; this is not proof of AI authorship.",
    }

def parse_ai_detection_json(raw):
    raw = str(raw or "").strip()
    raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    try:
        data = json.loads(raw)
    except Exception:
        data = {}
    label = str(data.get("label") or data.get("likelihood") or "Unclear").title()
    if label not in {"Low", "Medium", "High", "Unclear"}:
        label = "Unclear"
    try:
        score = int(float(data.get("score", 0)))
    except Exception:
        score = 0
    score = max(0, min(100, score))
    reasons = data.get("reasons") or data.get("signals") or []
    if isinstance(reasons, str):
        reasons = [reasons]
    suggestions = data.get("suggestions") or data.get("advice") or []
    if isinstance(suggestions, str):
        suggestions = [suggestions]
    return {
        "label": label,
        "score": score,
        "reasons": [str(item) for item in reasons[:6] if str(item).strip()],
        "suggestions": [str(item) for item in suggestions[:5] if str(item).strip()],
        "confidence": str(data.get("confidence") or "Limited").title(),
    }

async def run_ai_likelihood_check(text):
    text = compact_ai_payload_text(text, AI_DETECT_MAX_CHARS)
    if not GROQ_API_KEY:
        heuristic = ai_likelihood_heuristic(text)
        return {
            "label": heuristic["label"],
            "score": heuristic["score"],
            "confidence": "Low",
            "reasons": heuristic["signals"],
            "suggestions": ["Use a configured AI provider for a stronger writing-pattern review."],
        }
    prompt = {
        "task": "Assess whether writing appears AI-generated. This is a likelihood analysis, not proof.",
        "rules": [
            "Return JSON only.",
            "Use label Low, Medium, High, or Unclear.",
            "Score is 0-100 where 100 means strongest AI-like pattern.",
            "Do not claim certainty or accuse the writer.",
            "Consider specificity, voice, structure, repetition, sentence variety, personal detail, factual anchoring, and revision artifacts.",
            "Also note signs that may explain false positives such as ESL writing, formal school style, short length, or heavy editing.",
        ],
        "return_schema": {
            "label": "Low|Medium|High|Unclear",
            "score": 0,
            "confidence": "Low|Medium",
            "reasons": ["short concrete reason"],
            "suggestions": ["optional improvement or verification step"],
        },
        "text": text,
    }
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "messages": fit_ai_messages([
            {"role": "system", "content": "You are a cautious writing authenticity analyst. Return valid JSON only."},
            {"role": "user", "content": json.dumps(prompt)},
        ], max_chars=5600),
        "model": "llama-3.1-8b-instant",
        "temperature": 0.1,
        "max_tokens": 420,
    }
    session = await get_http_session()
    async with session.post("https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers, timeout=AI_MODEL_TIMEOUT_SECONDS) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise RuntimeError(ai_http_error_message(resp.status, body))
        data = await resp.json(content_type=None)
    return parse_ai_detection_json(data["choices"][0]["message"]["content"])

def ai_likelihood_embed(result, text):
    label = result.get("label", "Unclear")
    score = int(result.get("score") or 0)
    color = discord.Color.green() if label == "Low" else (discord.Color.orange() if label in {"Medium", "Unclear"} else discord.Color.red())
    embed = discord.Embed(
        title=f"{economy_q_thinking} Writing Authenticity Check",
        description=(
            f"AI-like likelihood: **{label}** ({score}/100)\n"
            "This is a pattern check, not proof. Do not use it as the only reason to accuse someone."
        ),
        color=color,
        timestamp=datetime.now(timezone.utc),
    )
    reasons = result.get("reasons") or ["No clear pattern notes returned."]
    suggestions = result.get("suggestions") or ["Compare drafts, sources, assignment history, and ask the writer about their process."]
    embed.add_field(name="Signals", value=embed_value("\n".join(f"- {item}" for item in reasons), 1200), inline=False)
    embed.add_field(name="What To Do", value=embed_value("\n".join(f"- {item}" for item in suggestions), 1200), inline=False)
    embed.add_field(name="Text Checked", value=f"**{len(text):,}** characters", inline=True)
    embed.add_field(name="Confidence", value=str(result.get("confidence") or "Limited"), inline=True)
    return embed

def clean_user_error(error, fallback="Something went wrong."):
    if isinstance(error, discord.Forbidden):
        return "I do not have permission to do that."
    if isinstance(error, discord.NotFound):
        return "I could not find that message, channel, user, or role anymore."
    if isinstance(error, discord.HTTPException):
        text = str(error)
        lowered = text.casefold()
        if "unknown message" in lowered:
            return "That message no longer exists."
        if "missing permissions" in lowered or "forbidden" in lowered:
            return "I do not have permission to do that."
        if "must be 2000 or fewer" in lowered or "maximum size" in lowered:
            return "That response was too long for Discord."
        if getattr(error, "status", None) == 429:
            return "Discord is rate-limiting that action. Try again in a bit."
        return "Discord rejected that action."
    if isinstance(error, commands.MissingPermissions):
        return "You do not have permission to do that."
    if isinstance(error, commands.CheckFailure):
        return "You cannot use that command here."
    text = str(error or "").strip()
    if not text:
        return fallback
    cleaned = clean_provider_error(text) if text[:1] in "{[" else text
    cleaned = re.sub(r"^Error:\s*", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"^\d{3}\s+[A-Za-z ]+:\s*", "", cleaned).strip()
    cleaned = re.sub(r"^[A-Za-z_][\w.]*Error:\s*", "", cleaned).strip()
    cleaned = re.sub(r"\s*\([0-9a-f]{8}-[0-9a-f-]{27,}\)\s*$", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return fallback
    if len(cleaned) > 220:
        return compact_ai_text(cleaned, 220)
    return cleaned

def ai_exception_message(exc):
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "AI took too long to answer, so I stopped waiting. Try again in a bit."
    return clean_user_error(exc, "AI failed to answer.")

async def safe_ai_reply(message, content):
    content = fit_discord_content(content or "I couldn't come up with an answer.")
    try:
        return await message.reply(
            content,
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    except discord.HTTPException as exc:
        if "Unknown message" not in str(exc):
            raise
        return await message.channel.send(
            content,
            allowed_mentions=discord.AllowedMentions.none(),
        )

CASUAL_AI_RE = re.compile(
    r"^\s*(hi|hello|hey|yo|sup|what'?s up|whats up|proque|thanks|thank you|ty|ok|okay|lol|lmao|wyd)[!.?\s]*$",
    re.IGNORECASE,
)
BOT_KNOWLEDGE_RE = re.compile(
    r"\b(proque|bot|command|help|how\s+do\s+i|how\s+to|use|run|alias|aliases|"
    r"quewo|quesos|economy|gambl|game|games|lottery|shop|inventory|profile|balance|"
    r"level|xp|activity|messages|chat|summary|summarize|summarise|recap|essay|writing|ai\s*detect|ai-written|plagiarism|authenticity|settings|setup|prefix|logs?|admin|permission|perms|doctor|error|"
    r"ignore|block|unblock|disable|enable|ban|kick|role|lock|unlock|stop\s+responding)\b",
    re.IGNORECASE,
)
AI_COMMAND_ACTION_RE = re.compile(
    r"\b(run|use|do|start|set|show|open|check|make|create|generate|analyse|analyze|"
    r"change|edit|enable|disable|turn\s+on|turn\s+off|add|remove|give|send|reply|summarize|summarise|recap|catch\s+me\s+up|what\s+did|what\s+happened|detect|check|"
    r"block|unblock|ignore|stop\s+responding|lock|unlock|purge|ban|kick|role|permission|perms)\b",
    re.IGNORECASE,
)

def looks_like_ai_image_command(text):
    text = str(text or "")
    return bool(re.search(
        r"\b(generate|create|make|draw)\s+(?:an?\s+)?(?:image|picture|pic|photo)\b|"
        r"\b(analy[sz]e|describe|what'?s?\s+in|what\s+is\s+in)\s+(?:this\s+)?(?:image|picture|pic|photo)\b",
        text,
        flags=re.IGNORECASE,
    ))

def should_try_ai_command_planner(text):
    text = str(text or "").strip()
    if not text or CASUAL_AI_RE.match(text):
        return False
    if looks_like_ai_image_command(text):
        return True
    if re.search(r"(?:^|\s)[.!?][A-Za-z][\w-]*", text):
        return True
    if looks_like_ai_summary_request(text):
        return True
    if re.search(
        r"\b(?:ai\s*detect|detect\s+ai|ai[-\s]?written|written\s+by\s+ai|authenticity|essaycheck)\b",
        text,
        flags=re.IGNORECASE,
    ):
        return True
    lowered = text.casefold()
    control_or_sensitive = any(phrase in lowered for phrase in (
        "ignore ", "unignore ", "stop responding", "turn ai on", "turn ai off",
        "enable ai", "disable ai", "reward ", "give top", "add tickets", "add money",
    ))
    return bool(control_or_sensitive and AI_COMMAND_ACTION_RE.search(text))

def should_use_full_bot_context(text):
    text = str(text or "").strip()
    if not text or CASUAL_AI_RE.match(text):
        return False
    return bool(BOT_KNOWLEDGE_RE.search(text))

def command_ai_summary(command, prefix):
    aliases = ", ".join(visible_aliases_for(command, 1))
    signature = getattr(command, "signature", "") or ""
    usage = f"{prefix}{command.qualified_name}" + (f" {signature}" if signature else "")
    key = command.name.casefold()
    explanation = economy_explanations.get(key) or economy_explanations.get(command.qualified_name.casefold())
    if not explanation:
        for alias in getattr(command, "aliases", []) or []:
            explanation = economy_explanations.get(alias.casefold())
            if explanation:
                break
    if not explanation:
        explanation = getattr(command, "help", None) or getattr(command, "brief", None) or "Runs this bot command."
    bits = [f"`{usage}`"]
    if aliases:
        bits.append(f"aliases: {aliases}")
    bits.append(compact_ai_text(explanation, 180))
    return " - ".join(bits)

def bot_command_knowledge_index(guild, viewer=None, max_chars=1900):
    prefix = prefix_for_guild(guild)
    viewer_key = getattr(viewer, "id", 0) if can_see_superowner_help(viewer, guild) else 0
    key = (getattr(guild, "id", 0), prefix, int(max_chars), len(bot.commands), viewer_key)
    now = time.monotonic()
    cached = ai_knowledge_cache.get(key)
    if cached and now - cached[0] < HELP_RENDER_CACHE_TTL:
        return cached[1]
    seen = set()
    sections = []

    for category, names in HELP_CATEGORIES.items():
        lines = []
        for name in names:
            command = get_command_case_insensitive(name)
            if not command_is_visible_to(command, viewer, guild):
                continue
            command_key = command.qualified_name.casefold()
            if command_key in seen:
                continue
            seen.add(command_key)
            lines.append(command_ai_summary(command, prefix))
        if lines:
            sections.append(f"{category}:\n" + "\n".join(lines))

    other_lines = []
    for command in sorted(bot.commands, key=lambda cmd: cmd.qualified_name.casefold()):
        if not command_is_visible_to(command, viewer, guild):
            continue
        command_key = command.qualified_name.casefold()
        if command_key in seen:
            continue
        seen.add(command_key)
        other_lines.append(command_ai_summary(command, prefix))
    if other_lines:
        sections.append("Other registered commands:\n" + "\n".join(other_lines))

    details = []
    for key in sorted(economy_detailed_explanations):
        if len(details) >= 12:
            break
        command = get_command_case_insensitive(key)
        if command and not command_is_visible_to(command, viewer, guild):
            continue
        if key in economy_explanations or command:
            details.append(f"{key}: {compact_ai_text(economy_detailed_explanations[key], 120)}")
    if details:
        sections.append("Key mechanics snippets:\n" + "\n".join(details))

    index = "\n\n".join(sections)
    if len(index) > max_chars:
        index = index[:max_chars].rstrip() + "\n...truncated. For exact details, tell users to run the matching help command or `.explain <command>`."
    ai_knowledge_cache[key] = (now, index)
    return index

def bot_capabilities_summary(guild, viewer=None):
    prefix = prefix_for_guild(guild)
    owner_action_note = (
        "When 𝚀𝚞𝚎 asks clearly, you can confirm and run batch rewards for supported leaderboards such as activity, messages, lottery holders, and 𝚀𝚞𝚎wo rankings. "
        if can_see_superowner_help(viewer, guild)
        else ""
    )
    return (
        f"You are Pro𝚀𝚞𝚎, a Discord bot. The command prefix in this server is `{prefix}`. "
        "AI/chatbot is one of your main bot features. You are mainly a fun, casual, normal-feeling Discord AI: answer general chat with personality, banter, and warmth, but you also know the bot deeply and should help users use it. "
        "For playful messages, joke back and stay in the vibe instead of turning everything into a formal explanation. "
        "You have global user memory for useful facts people explicitly share, saved bot profile data like birthdays/statuses, plus recent channel context. Use memory naturally when it helps and avoid being creepy about it. "
        "Use the live command/capability index below as your source of truth for bot features, commands, aliases, usage, permissions, games, 𝚀𝚞𝚎wo mechanics, and setup flows. "
        "Do not turn normal conversation into command recommendations. Only give exact commands when the user asks how to use a feature, asks what command does something, or clearly wants bot help. "
        "When you do mention commands, keep it to the smallest useful set and explain in plain language first. "
        "Slash commands are disabled; users should use prefix commands and the interactive setup buttons shown by those commands. "
        "Be permission-aware: if a command is admin/server-owner/𝚀𝚞𝚎-only, say that plainly before telling someone how to use it. "
        "For economy/game advice, compare risk, max bet, payout, cooldown, and fun factor when useful. "
        "For errors/logs, act like the bot doctor: use reply context and the doctor snapshot to identify the likely broken command or subsystem and suggest the next test/fix. "
        "If you are not certain about an exact mechanic, say what you know and suggest `.explain <command>` or the matching help page. "
        f"{owner_action_note}"
        f"Useful help commands only when asked: `{prefix}help`, `{prefix}games`, `{prefix}econhelp`, `{prefix}explain <command>`, `{prefix}setup`, and `{prefix}messages`.\n\n"
        f"Live bot command/capability index:\n{bot_command_knowledge_index(guild, viewer)}"
    )

def denial_message(detail=None):
    detail = str(detail or "").strip()
    base = f"{economy_q_reject} You can't use that heh"
    return base if not detail else f"{base}\n{detail}"

def command_denial_detail(ctx, error=None):
    command_name = ctx.command.qualified_name if getattr(ctx, "command", None) else ""
    que_only = {
        "add", "remove", "move", "addtick", "removetick", "movetick", "settick", "lotterypot", "setquesos",
        "editlottery", "stoplottery", "qstats", "economyhealth", "balancedashboard", "endseason",
        "send", "reply", "speak", "wake", "clearwatchlist",
    "aisettings", "aiperms", "aiignore", "aiunignore", "aistyle",
        "aihistory", "auditcommands", "styleaudit", "commandcleanup", "permaudit", "receipts", "aiguard",
    }
    if command_name in que_only:
        return "This command is not available here."
    if ctx.guild:
        return "Admin power only."
    return "This command can only be used somewhere you have permission."

active_timers = load_active_timers()
active_polls = load_active_polls()
active_giveaways = load_active_giveaways()
active_alarms = load_active_alarms()
runtime_state_restored = False
user_mentions = {}
away_reaction_callouts = {}
activity_buffer = Counter()
message_history_buffer = []
active_activity_status_messages = {}
message_event_tasks = {}
active_message_event_cache = {}
message_event_count_buffer = Counter()
sticky_panel_tasks = {}
sticky_panel_last_seen = {}
sticky_lottery_config_cache = {}
sticky_lottery_config_locks = {}
speak_sessions = {}
speak_forwarded_messages = {}
tracked_message_activity_ids = set()
tracked_message_activity_order = deque()
command_timing_stats = {}
slow_command_events = deque(maxlen=40)
command_queue_stats = {}
recent_queue_events = deque(maxlen=40)
help_render_cache = {}
games_render_cache = {}
ai_knowledge_cache = {}
HELP_RENDER_CACHE_TTL = 300
daily_cooldown = {}
weekly_cooldown = {}
monthly_cooldown = {}
chat_xp_memory = {}
activity_recent_messages = {}
ACTIVITY_DUPLICATE_WINDOW_SECONDS = 10 * 60
STICKY_PANEL_IDLE_SECONDS = 90

async def channel_latest_message_matches(channel, message_id):
    try:
        async for latest in channel.history(limit=1):
            return int(latest.id) == int(message_id)
    except Exception:
        return None
    return None

def schedule_sticky_panel(kind, guild_id, channel_id, callback, *, idle_seconds=STICKY_PANEL_IDLE_SECONDS):
    key = (str(kind), int(guild_id), int(channel_id))
    sticky_panel_last_seen[key] = time.monotonic()
    task = sticky_panel_tasks.get(key)
    if task and not task.done():
        return

    async def runner():
        try:
            while True:
                last_seen = sticky_panel_last_seen.get(key, time.monotonic())
                remaining = idle_seconds - (time.monotonic() - last_seen)
                if remaining > 0:
                    await asyncio.sleep(min(remaining, idle_seconds))
                    continue
                await callback()
                return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Sticky panel refresh failed for {kind} in guild {guild_id}: {type(e).__name__} - {e}")
        finally:
            sticky_panel_tasks.pop(key, None)
            sticky_panel_last_seen.pop(key, None)

    sticky_panel_tasks[key] = asyncio.create_task(runner())

async def cached_lottery_sticky_config(guild_id):
    guild_id = int(guild_id)
    now = time.monotonic()
    cached = sticky_lottery_config_cache.get(guild_id)
    if cached and cached[0] > now:
        return cached[1]
    lock = sticky_lottery_config_locks.get(guild_id)
    if lock is None:
        lock = sticky_lottery_config_locks[guild_id] = asyncio.Lock()
    async with lock:
        now = time.monotonic()
        cached = sticky_lottery_config_cache.get(guild_id)
        if cached and cached[0] > now:
            return cached[1]
        config = await asyncio.to_thread(economy_get_lottery_config, guild_id)
        sticky_lottery_config_cache[guild_id] = (now + 45, config)
        return config

async def schedule_channel_sticky_panels(message):
    if not message.guild:
        return
    if bot.user and message.author.id == bot.user.id:
        return
    guild_id = int(message.guild.id)
    channel_id = int(message.channel.id)

    activity_config = guild_activity_channels.get(guild_id)
    if activity_config and int(activity_config.get("channel_id") or 0) == channel_id and activity_config.get("current_message_id"):
        schedule_sticky_panel(
            "activity",
            guild_id,
            channel_id,
            lambda: refresh_activity_live_message(guild_id, activity_config, force_repost=True),
        )

    event_row = active_message_event_cache.get(guild_id)
    if event_row and int(event_row.get("channel_id") or 0) == channel_id and event_row.get("message_id"):
        schedule_sticky_panel(
            "messageevent",
            guild_id,
            channel_id,
            lambda: refresh_message_event_message(guild_id, force_repost=True),
        )

    lottery_config = await cached_lottery_sticky_config(guild_id)
    if lottery_config and int(lottery_config.get("channel_id") or 0) == channel_id and lottery_config.get("message_id"):
        schedule_sticky_panel(
            "lottery",
            guild_id,
            channel_id,
            lambda: economy_module.refresh_lottery_message(message.guild, None, force_repost=True),
        )
ACTIVITY_NEAR_DUPLICATE_RATIO = 0.88
ACTIVITY_RECENT_MESSAGE_LIMIT = 8
TRACKED_MESSAGE_ACTIVITY_ID_LIMIT = 10000
UTC_TIME_NOTE = "Absolute dates/times use UTC."

def clear_help_cache():
    help_render_cache.clear()
    ai_knowledge_cache.clear()

def standard_embed(title, description=None, color=discord.Color.blurple(), *, icon=None, timestamp=True):
    clean_title = f"{icon} {title}" if icon else title
    embed = discord.Embed(
        title=clean_title,
        description=description,
        color=color,
        timestamp=datetime.now(timezone.utc) if timestamp else None,
    )
    embed.set_footer(text="Pro𝚀𝚞𝚎")
    return embed

def clone_embed(embed):
    return discord.Embed.from_dict(embed.to_dict())

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

async def invoke_prefix_command_from_message(message, command_name, args=None):
    command = get_command_case_insensitive(command_name)
    if not command:
        await message.reply(
            f"I couldn't find `{command_name}`. Try `{prefix_for_guild(message.guild)}help search {command_name}`.",
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return False
    if StringView is None:
        await message.reply("Command forwarding is not available in this Discord library version.", mention_author=False)
        return False

    ctx = await bot.get_context(message)
    ctx.command = command
    ctx.invoked_with = command.name
    ctx.prefix = f"{bot.user.mention} "
    raw_args = (args or "").strip()
    ctx.view = StringView(raw_args)
    ctx.view.skip_ws()
    try:
        await command.invoke(ctx)
        return True
    except commands.CommandError as e:
        await on_command_error(ctx, e)
    except Exception as e:
        print(f"AI command bridge failed for {command.name}: {type(e).__name__} - {e}")
        await message.reply(
            fit_discord_content(f"I tried running `{command.name}` but it failed: {clean_user_error(e)}"),
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    return False

async def invoke_prefix_command_from_interaction(interaction, command_name, args=None):
    command = get_command_case_insensitive(command_name)
    if not command:
        await interaction.followup.send(
            f"I couldn't find `{command_name}`.",
            ephemeral=True
        )
        return
    if not command_is_visible_to(command, interaction.user, interaction.guild):
        await interaction.followup.send(
            f"I couldn't find `{command_name}`.",
            ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return
    if StringView is None or not hasattr(commands.Context, "from_interaction"):
        await interaction.followup.send(
            "Command forwarding is not available in this Discord library version.",
            ephemeral=True
        )
        return

    ctx = await commands.Context.from_interaction(interaction)
    ctx.command = command
    ctx.invoked_with = command.name
    ctx.prefix = "/ui "
    raw_args = (args or "").strip()
    ctx.view = StringView(raw_args)
    ctx.view.skip_ws()
    try:
        await command.invoke(ctx)
    except commands.CommandError as e:
        await on_command_error(ctx, e)
    except Exception as e:
        print(f"Interaction command bridge failed for {command.name}: {type(e).__name__} - {e}")
        if not interaction.response.is_done():
            await interaction.response.send_message("That command failed.", ephemeral=True)
        else:
            await interaction.followup.send("That command failed.", ephemeral=True)

async def sync_slash_commands_once(force=False):
    global slash_commands_synced
    if slash_commands_synced and not force:
        return {"skipped": True, "synced_guilds": 0, "failed_guilds": 0, "cleared": True}
    synced_guilds = 0
    failed_guilds = 0
    try:
        bot.tree.clear_commands(guild=None)
        global_synced = await bot.tree.sync()
        print(f"Global slash commands cleared: {len(global_synced)} remaining.")
    except Exception as e:
        print(f"Global slash command clear failed: {type(e).__name__} - {e}")

    for guild in bot.guilds:
        try:
            bot.tree.clear_commands(guild=discord.Object(id=guild.id))
            synced = await bot.tree.sync(guild=discord.Object(id=guild.id))
            synced_guilds += 1
            print(f"Guild slash commands cleared for {guild.name} ({guild.id}): {len(synced)} remaining.")
        except Exception as e:
            failed_guilds += 1
            print(f"Guild slash command clear failed for {guild.name} ({guild.id}): {type(e).__name__} - {e}")

    if synced_guilds or not failed_guilds:
        slash_commands_synced = True
    print(
        f"Slash command clear complete: {synced_guilds} guild(s) cleared, "
        f"{failed_guilds} failed. Prefix commands remain active."
    )
    return {"skipped": False, "synced_guilds": synced_guilds, "failed_guilds": failed_guilds, "cleared": True}

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

def guild_truth_or_dare_channels(guild):
    return scoped_set(truth_or_dare_channels, guild)

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

def get_guild_log_config(guild_id):
    config = load_guild_log_config(guild_id)
    if config:
        guild_log_configs[int(guild_id)] = config
        return config
    return guild_log_configs.get(int(guild_id), {})

def guild_log_label(guild):
    if guild is None:
        return "unknown server"
    return f"{guild.name} ({guild.id})"

def log_user(value):
    user_id = getattr(value, "id", None)
    if user_id is None:
        return "Unknown"
    return f"<@{user_id}> ({user_id})"

def log_role(value):
    role_id = getattr(value, "id", None)
    role_name = getattr(value, "name", "Unknown")
    if role_id is None:
        return str(role_name)
    mention = getattr(value, "mention", f"<@&{role_id}>")
    return f"{mention} `{role_name}` ({role_id})"

async def find_audit_entry(guild, actions=None, target_id=None, limit=10, max_age_seconds=20):
    if guild is None:
        return None
    actions = set(actions or [])
    now = datetime.now(timezone.utc)
    try:
        async for entry in guild.audit_logs(limit=limit, oldest_first=False):
            if actions and entry.action not in actions:
                continue
            if target_id is not None and getattr(entry.target, "id", None) != int(target_id):
                continue
            created_at = getattr(entry, "created_at", None)
            if created_at and created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            if created_at and abs((now - created_at).total_seconds()) > max_age_seconds:
                continue
            return entry
    except discord.Forbidden:
        print(f"Audit lookup skipped: missing View Audit Log in guild {guild.id}.")
    except discord.HTTPException as e:
        print(f"Audit lookup skipped for guild {guild.id}: {type(e).__name__} - {e}")
    except Exception as e:
        print(f"Audit lookup failed for guild {guild.id}: {type(e).__name__} - {e}")
    return None

def add_audit_actor_fields(embed, entry, actor_label="By"):
    if not entry:
        embed.add_field(name=actor_label, value="Unknown or missing audit permission", inline=False)
        return
    embed.add_field(name=actor_label, value=log_user(entry.user), inline=False)
    if getattr(entry, "reason", None):
        embed.add_field(name="Reason", value=embed_value(entry.reason, 1024), inline=False)

def should_emit_log_once(key, ttl_seconds=15):
    now = time.monotonic()
    normalized = tuple(key)
    while recent_log_event_keys and now - recent_log_event_keys[0][1] > ttl_seconds:
        recent_log_event_keys.popleft()
    if any(existing_key == normalized for existing_key, _ in recent_log_event_keys):
        return False
    recent_log_event_keys.append((normalized, now))
    return True

def normalize_log_embed(embed, guild):
    normalized = clone_embed(embed)
    if not normalized.color:
        normalized.color = discord.Color.blurple()
    if normalized.description:
        normalized.description = embed_value(normalized.description, 3900)
    if not normalized.timestamp:
        normalized.timestamp = datetime.now(timezone.utc)
    if not normalized.footer or not normalized.footer.text:
        normalized.set_footer(text=f"Server {guild.id}")
    if len(normalized.fields) > 20:
        normalized._fields = normalized._fields[:20]
    for index, field in enumerate(list(normalized.fields)):
        normalized.set_field_at(
            index,
            name=embed_value(field.name, 256),
            value=embed_value(field.value, 1024),
            inline=field.inline,
        )
    return normalized

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

        embed = normalize_log_embed(embed, guild)
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

def invite_snapshot(invite):
    inviter = getattr(invite, "inviter", None)
    channel = getattr(invite, "channel", None)
    expires_at = getattr(invite, "expires_at", None)
    created_at = getattr(invite, "created_at", None)
    return {
        "code": str(getattr(invite, "code", "")),
        "uses": int(getattr(invite, "uses", 0) or 0),
        "max_uses": int(getattr(invite, "max_uses", 0) or 0),
        "max_age": int(getattr(invite, "max_age", 0) or 0),
        "temporary": bool(getattr(invite, "temporary", False)),
        "inviter_id": getattr(inviter, "id", None),
        "channel_id": getattr(channel, "id", None),
        "created_at": created_at,
        "expires_at": expires_at,
    }

async def fetch_invite_snapshot(guild):
    try:
        invites = await guild.invites()
    except discord.Forbidden:
        return None, "missing Manage Server permission"
    except discord.HTTPException as exc:
        return None, f"Discord error: {clean_user_error(exc)}"
    return {invite.code: invite_snapshot(invite) for invite in invites}, None

async def refresh_invite_cache(guild):
    snapshot, error = await fetch_invite_snapshot(guild)
    if snapshot is not None:
        guild_invite_cache[guild.id] = snapshot
    return snapshot, error

async def refresh_all_invite_caches():
    refreshed = 0
    skipped = 0
    for guild in bot.guilds:
        snapshot, _ = await refresh_invite_cache(guild)
        if snapshot is None:
            skipped += 1
        else:
            refreshed += 1
        await asyncio.sleep(0.2)
    print(f"Invite cache ready: refreshed={refreshed}, skipped={skipped}")

async def identify_used_invite(guild):
    before = guild_invite_cache.get(guild.id)
    current, error = await fetch_invite_snapshot(guild)
    if current is None:
        return None, before, None, error
    guild_invite_cache[guild.id] = current
    if not before:
        return None, before, current, "invite cache was not ready yet"
    candidates = []
    for code, after in current.items():
        previous = before.get(code)
        if previous and int(after.get("uses", 0)) > int(previous.get("uses", 0)):
            candidates.append((int(after.get("uses", 0)) - int(previous.get("uses", 0)), previous, after))
    if not candidates:
        return None, before, current, "no invite use changed; possibly vanity URL, temporary invite, or missing cache"
    candidates.sort(key=lambda item: item[0], reverse=True)
    _, previous, after = candidates[0]
    return (previous, after), before, current, None

def invite_value(invite_data, before_data=None):
    if not invite_data:
        return "Unknown"
    code = invite_data.get("code") or "unknown"
    url = f"https://discord.gg/{code}"
    uses = int(invite_data.get("uses", 0) or 0)
    previous_uses = int(before_data.get("uses", uses - 1) if before_data else uses - 1)
    inviter_id = invite_data.get("inviter_id")
    channel_id = invite_data.get("channel_id")
    max_uses = int(invite_data.get("max_uses", 0) or 0)
    expires_at = invite_data.get("expires_at")
    created_at = invite_data.get("created_at")
    lines = [
        f"Code: [`{code}`]({url})",
        f"Inviter: {f'<@{inviter_id}> (`{inviter_id}`)' if inviter_id else 'Unknown'}",
        f"Uses: **{previous_uses:,} → {uses:,}**",
        f"Channel: {f'<#{channel_id}>' if channel_id else 'Unknown'}",
        f"Max Uses: **{max_uses:,}**" if max_uses else "Max Uses: **Unlimited**",
        f"Temporary: **{'Yes' if invite_data.get('temporary') else 'No'}**",
    ]
    if created_at:
        lines.append(f"Created: {discord.utils.format_dt(created_at, 'R')}")
    if expires_at:
        lines.append(f"Expires: {discord.utils.format_dt(expires_at, 'R')}")
    elif int(invite_data.get("max_age", 0) or 0) == 0:
        lines.append("Expires: **Never**")
    return "\n".join(lines)

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

    await asyncio.to_thread(save_guild_log_config, guild.id, normal_channel_id, reaction_channel_id)
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
        if guild.get_member(user_id) and await asyncio.to_thread(load_guild_log_config, guild.id):
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

@tasks.loop(minutes=5)
async def presence_rotation_task():
    if bot_maintenance_mode:
        try:
            await bot.change_presence(activity=discord.Game("updating"))
        except Exception as e:
            print(f"Presence rotation failed: {type(e).__name__} - {e}")
        return
    statuses = [
        lambda: discord.Game(f"{DEFAULT_PREFIX}games"),
        lambda: discord.Game(f"{DEFAULT_PREFIX}daily"),
        lambda: discord.Game(f"{DEFAULT_PREFIX}help search"),
        lambda: discord.Activity(type=discord.ActivityType.watching, name="𝚀𝚞𝚎wo balances"),
        lambda: discord.Activity(type=discord.ActivityType.listening, name=f"{DEFAULT_PREFIX}lottery"),
    ]
    index = int(time.time() // 300) % len(statuses)
    try:
        await bot.change_presence(activity=statuses[index]())
    except Exception as e:
        print(f"Presence rotation failed: {type(e).__name__} - {e}")

STALE_GAME_MARKERS = (
    "TIC TAC TOE",
    "CONNECT 4",
    "CHESS",
    "Q TOWERS",
    "VAULT",
    "MEMORY",
    "LOCKPICK",
    "CARD LADDER",
    "DUNGEON",
    "LUCKY NUMBER",
    "PLINKO",
    "HEIST",
    "DICE DUEL",
    "Q CASES",
    "JACKPOT SPIN",
    "MINESWEEPER",
)

async def game_member(guild, user_id):
    if guild is None:
        return None
    member = guild.get_member(int(user_id))
    if member:
        return member
    try:
        return await guild.fetch_member(int(user_id))
    except Exception:
        return None

def ttt_state(game):
    return {
        "turn": int(game.get("turn", 0)),
        "board": game.get("board", []),
        "bet_amount": int(game.get("bet_amount") or 0),
    }

def c4_state(game):
    return {
        "turn": int(game.get("turn", 0)),
        "board": game.get("board", []),
        "bet_amount": int(game.get("bet_amount") or 0),
    }

def chess_state(game):
    board = game.get("board")
    clocks = game.get("clocks") or {}
    return {
        "fen": board.fen() if board else None,
        "bet_amount": int(game.get("bet_amount") or 0),
        "clocks": {str(key): int(value) for key, value in clocks.items()},
    }

async def save_runtime_game_state(game, game_key):
    msg = game.get("msg") or game.get("message")
    if not msg or not getattr(msg, "guild", None):
        return
    if game_key == "ttt":
        state = ttt_state(game)
    elif game_key == "c4":
        state = c4_state(game)
    elif game_key == "chess":
        state = chess_state(game)
    else:
        state = {}
    players = game.get("players") or []
    await asyncio.to_thread(
        save_active_game_session,
        msg.guild.id,
        msg.channel.id,
        msg.id,
        game_key,
        [player.id for player in players if getattr(player, "id", None)],
        state,
    )

def apply_ttt_board_to_view(view, board):
    for item in view.children:
        if not isinstance(item, TicTacToeButton):
            continue
        mark = board[item.row][item.col]
        if mark == TTT_EMPTY:
            item.label = "\u200b"
            item.emoji = None
            item.disabled = False
        else:
            item.label = None
            item.emoji = custom_emoji(economy_q_game_x if mark == TTT_X else economy_q_game_o)
            item.disabled = True

async def restore_active_game_sessions():
    sessions = await asyncio.to_thread(load_active_game_sessions)
    restored = 0
    expired = 0
    for session in sessions:
        guild = bot.get_guild(session["guild_id"])
        channel = guild.get_channel(session["channel_id"]) if guild else bot.get_channel(session["channel_id"])
        if channel is None:
            try:
                channel = await bot.fetch_channel(session["channel_id"])
            except Exception:
                channel = None
        if channel is None:
            await asyncio.to_thread(delete_active_game_session, session["message_id"])
            expired += 1
            continue
        try:
            message = await channel.fetch_message(session["message_id"])
        except Exception:
            await asyncio.to_thread(delete_active_game_session, session["message_id"])
            expired += 1
            continue
        state = session.get("state") or {}
        players = [await game_member(guild, user_id) for user_id in session.get("players", [])]
        if len(players) < 2 or not all(players):
            await message.edit(content=fit_discord_content((message.content or "") + f"\n\n{economy_q_game_timeout} Game expired after restart because a player is unavailable."), view=None, allowed_mentions=discord.AllowedMentions.none())
            await asyncio.to_thread(delete_active_game_session, session["message_id"])
            expired += 1
            continue
        try:
            if session["game_key"] == "ttt":
                board = state.get("board") or [[TTT_EMPTY] * 3 for _ in range(3)]
                view = TicTacToeView()
                apply_ttt_board_to_view(view, board)
                game = {
                    "players": players[:2],
                    "turn": int(state.get("turn", 0) or 0),
                    "board": board,
                    "view": view,
                    "msg": message,
                    "timeout_task": None,
                    "bet_amount": int(state.get("bet_amount") or 0),
                }
                ttt_games[channel.id] = game
                await update_turn(game, channel)
                restored += 1
            elif session["game_key"] == "c4":
                board = state.get("board") or [[" "] * 7 for _ in range(6)]
                view = Connect4View()
                game = {
                    "players": players[:2],
                    "turn": int(state.get("turn", 0) or 0),
                    "board": board,
                    "view": view,
                    "msg": message,
                    "timeout_task": None,
                    "bet_amount": int(state.get("bet_amount") or 0),
                }
                c4_games[channel.id] = game
                await message.edit(content=fit_discord_content(f"{render_board(board, game['turn'])}{game_bet_line(game)}"), view=view, allowed_mentions=discord.AllowedMentions.none())
                await update_c4_turn(game, channel)
                restored += 1
            elif session["game_key"] == "chess" and chess_lib:
                board = chess_lib.Board(state.get("fen") or chess_lib.STARTING_FEN)
                clocks = {players[0].id: CHESS_CLOCK_SECONDS, players[1].id: CHESS_CLOCK_SECONDS}
                for key, value in (state.get("clocks") or {}).items():
                    try:
                        clocks[int(key)] = int(value)
                    except Exception:
                        pass
                game = {
                    "white": players[0],
                    "black": players[1],
                    "players": players[:2],
                    "board": board,
                    "channel_id": channel.id,
                    "message": message,
                    "selected_from": None,
                    "pending_move": None,
                    "view": None,
                    "bet_amount": int(state.get("bet_amount") or 0),
                    "clocks": clocks,
                    "last_turn_started": None,
                    "clock_task": None,
                    "live_clock_task": None,
                    "ended": False,
                }
                game["view"] = ChessView(game)
                chess_games[channel.id] = game
                await message.edit(content=chess_status(game), embed=chess_embed(game), view=game["view"], allowed_mentions=discord.AllowedMentions(users=True))
                await start_chess_clock(game)
                await start_chess_live_clock(game)
                restored += 1
            else:
                raise RuntimeError("unsupported session type")
        except Exception as e:
            print(f"Could not restore {session['game_key']} session {session['message_id']}: {type(e).__name__} - {e}")
            expired_note = f"\n\n{economy_q_game_timeout} **{session['game_key'].title()} expired after bot restart.** Start a new game."
            await message.edit(content=fit_discord_content((message.content or "") + expired_note), view=None, allowed_mentions=discord.AllowedMentions.none())
            await asyncio.to_thread(delete_active_game_session, session["message_id"])
            expired += 1
    if restored or expired:
        print(f"Game session recovery complete: restored={restored}, expired={expired}")
    return restored, expired

async def cleanup_stale_game_messages():
    saved_sessions = await asyncio.to_thread(load_active_game_sessions)
    saved_ids = {int(session["message_id"]) for session in saved_sessions}
    for guild in bot.guilds:
        for channel in getattr(guild, "text_channels", []):
            permissions = channel.permissions_for(guild.me)
            if not permissions.read_message_history or not permissions.send_messages:
                continue
            try:
                async for message in channel.history(limit=20):
                    if not bot.user or message.author.id != bot.user.id or not message.components or message.id in saved_ids:
                        continue
                    text = " ".join(filter(None, [message.content, *(embed.title or "" for embed in message.embeds)]))
                    if not any(marker in text.upper() for marker in STALE_GAME_MARKERS):
                        continue
                    expired = f"\n\n{economy_q_game_timeout} Game expired after bot restart. Start a new game."
                    new_content = fit_discord_content((message.content or "") + expired) if message.content else expired.strip()
                    await message.edit(content=new_content, view=None, allowed_mentions=discord.AllowedMentions.none())
            except (discord.Forbidden, discord.NotFound):
                continue
            except Exception as e:
                print(f"Stale game cleanup skipped channel {channel.id}: {type(e).__name__} - {e}")

@bot.event
async def on_ready():
    global birthday_task, activity_task, presence_task, runtime_state_restored, stale_game_messages_cleaned
    print(f'Pro𝚀𝚞𝚎 is online as {bot.user}')
    try:
        await load_maintenance_mode()
        if bot_maintenance_mode:
            asyncio.create_task(set_maintenance_nick(True))
            print("Pro𝚀𝚞𝚎 maintenance mode restored from database.")
    except Exception as e:
        print(f"Maintenance mode restore skipped: {type(e).__name__} - {e}")
    if not keep_alive_task.is_running():
        keep_alive_task.start()
    if birthday_task is None or birthday_task.done():
        birthday_task = asyncio.create_task(birthday_check_loop())
    if activity_task is None or activity_task.done():
        activity_task = asyncio.create_task(activity_report_loop())
    if not presence_rotation_task.is_running():
        presence_rotation_task.start()
    asyncio.create_task(refresh_all_invite_caches())
    # Load economy cog
    try:
        await economy_setup(bot, send_log)
        economy_command_names = [
            "bal", "bank", "tutorial", "recommendgame", "career", "jobs", "work", "robsettings", "quewochannel", "levelupchannel", "rob", "profile", "inventory", "settheme", "quests", "dailychallenge", "streaks", "guide", "onboard", "shop", "claimreminders", "cooldowns", "transactions", "limits", "lottery", "editlottery", "stoplottery", "lotterystats", "buytick",
            "daily", "weekly", "monthly", "cf", "roulette", "slots",
            "blackjack", "scratch", "tower", "vault", "memory", "cardladder", "lockpick", "heist", "diceduel", "cases", "plinko", "luckynumber", "jackpotspin", "dungeon", "ms", "wheel", "give", "lb", "gamestats", "achievements", "setbadge", "gamebalance", "gamehistory", "seasonpass",
            "qstats", "economyaudit", "abuseaudit", "season", "endseason", "add", "remove", "move", "addtick", "removetick", "movetick", "settick", "lotterypot", "setquesos", "econhelp", "explain"
        ]
        loaded_economy_commands = [name for name in economy_command_names if bot.get_command(name)]
        print(f"𝚀𝚞𝚎wo system loaded ({len(loaded_economy_commands)}/{len(economy_command_names)} commands)")
    except Exception as e:
        print(f"𝚀𝚞𝚎wo system not loaded: {e}")
    if not runtime_state_restored:
        await restore_persistent_runtime_state()
        await restore_message_events()
        await restore_active_game_sessions()
        runtime_state_restored = True
    if not stale_game_messages_cleaned:
        stale_game_messages_cleaned = True
        asyncio.create_task(cleanup_stale_game_messages())
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
                    birthday_file = None
                    birthday_image_url = None
                    custom_prompt = birthday_card_prompts.get(str(user_id))
                    if image_generation_available():
                        try:
                            card_prompt = image_prompt_for("birthday", birthday_card_prompt_for(member, custom_prompt))
                            image_bytes, ext, _ = await generate_hf_image_bytes(card_prompt, width=1024, height=576)
                            birthday_filename = f"birthday-{member.id}.{ext}"
                            birthday_file = discord.File(BytesIO(image_bytes), filename=birthday_filename)
                            birthday_image_url = f"attachment://{birthday_filename}"
                        except Exception as e:
                            print(f"Birthday card generation failed for {member.id}: {type(e).__name__} - {e}")
                    embed = discord.Embed(
                        title=f"{economy_q_birthday_cake} Birthday Drop {economy_q_birthday_balloons}",
                        description=(
                            f"{economy_q_confetti} Happy birthday, {member.mention}!\n"
                            f"{economy_q_gift} Hope your day is stacked with wins, cake, and clean vibes."
                        ),
                        color=discord.Color.from_rgb(42, 143, 218),
                        timestamp=now,
                    )
                    if birthday_image_url:
                        embed.set_image(url=birthday_image_url)
                    embed.set_footer(text="Pro𝚀𝚞𝚎 birthday alert")
                    send_kwargs = {
                        "content": member.mention,
                        "embed": embed,
                        "allowed_mentions": discord.AllowedMentions(everyone=False, users=[member], roles=False),
                    }
                    if birthday_file:
                        send_kwargs["file"] = birthday_file
                    await channel.send(**send_kwargs)
                    already_sent.add(sent_key)

        await asyncio.sleep(60)

async def flush_activity_buffer():
    if not activity_buffer and not message_history_buffer and not message_event_count_buffer:
        return
    pending = dict(activity_buffer)
    pending_events = list(message_history_buffer)
    pending_event_counts = dict(message_event_count_buffer)
    activity_buffer.clear()
    message_history_buffer.clear()
    message_event_count_buffer.clear()
    ok_counts = True
    ok_events = True
    if pending:
        ok_counts = await asyncio.to_thread(add_guild_activity_counts, pending)
    if pending_events:
        ok_events = await asyncio.to_thread(add_message_activity_events, pending_events)
    if not ok_counts:
        activity_buffer.update(pending)
    if not ok_events or (pending_event_counts and not pending_events):
        message_history_buffer.extend(pending_events)
        message_event_count_buffer.update(pending_event_counts)

def track_message_activity(message):
    if not message.guild or message.author.bot:
        return
    message_id = int(getattr(message, "id", 0) or 0)
    if message_id in tracked_message_activity_ids:
        return
    tracked_message_activity_ids.add(message_id)
    tracked_message_activity_order.append(message_id)
    while len(tracked_message_activity_order) > TRACKED_MESSAGE_ACTIVITY_ID_LIMIT:
        tracked_message_activity_ids.discard(tracked_message_activity_order.popleft())
    if message.author.id in guild_blacklisted_users(message.guild):
        return
    event_row = active_message_event_cache.get(message.guild.id)
    event_scope = None
    event_should_count = False
    if event_row:
        starts_at = normalize_event_datetime(event_row.get("starts_at"))
        ends_at = normalize_event_datetime(event_row.get("ends_at"))
        message_time = message.created_at
        if message_time.tzinfo is None:
            message_time = message_time.replace(tzinfo=timezone.utc)
        else:
            message_time = message_time.astimezone(timezone.utc)
        if starts_at and ends_at and starts_at <= message_time <= ends_at:
            event_scope = f"message_event:{message.guild.id}:{int(starts_at.timestamp())}"
            event_should_count = should_count_message_activity(message, scope=event_scope)
    general_should_count = should_count_message_activity(message, scope="general")
    if not general_should_count and not event_should_count:
        return
    if general_should_count and message.guild.id in guild_activity_channels:
        activity_buffer[(message.guild.id, message.author.id, "messages")] += 1
    created_at_aware = message.created_at
    if created_at_aware.tzinfo is None:
        created_at_aware = created_at_aware.replace(tzinfo=timezone.utc)
    else:
        created_at_aware = created_at_aware.astimezone(timezone.utc)
    created_at = created_at_aware.replace(tzinfo=None)
    message_history_buffer.append({
        "guild_id": message.guild.id,
        "channel_id": message.channel.id,
        "user_id": message.author.id,
        "message_id": message.id,
        "created_at": created_at,
    })
    if event_should_count:
        message_event_count_buffer[(message.guild.id, message.author.id)] += 1
    if len(message_history_buffer) >= 200:
        asyncio.create_task(flush_activity_buffer())

def normalize_activity_message_content(message):
    content = str(getattr(message, "content", "") or "").casefold().strip()
    if not content:
        attachment_names = " ".join(getattr(attachment, "filename", "") for attachment in getattr(message, "attachments", []) or [])
        return f"attachments:{attachment_names.casefold().strip()}" if attachment_names else ""
    content = re.sub(r"https?://\S+", " link ", content)
    content = re.sub(r"<@!?\d+>|<#\d+>|<@&\d+>", " mention ", content)
    content = re.sub(r"<a?:([a-zA-Z0-9_]+):\d+>", r" emoji_\1 ", content)
    content = re.sub(r"\d+", "#", content)
    content = re.sub(r"(.)\1{3,}", r"\1\1", content)
    content = re.sub(r"[^a-z0-9#\s_]+", " ", content)
    return re.sub(r"\s+", " ", content).strip()

def should_count_message_activity(message, *, scope="general"):
    normalized = normalize_activity_message_content(message)
    if not normalized:
        return False
    now = message.created_at.timestamp() if getattr(message, "created_at", None) else time.time()
    key = (message.guild.id, message.author.id, scope)
    recent = activity_recent_messages.setdefault(key, deque(maxlen=ACTIVITY_RECENT_MESSAGE_LIMIT))
    while recent and now - recent[0][0] > ACTIVITY_DUPLICATE_WINDOW_SECONDS:
        recent.popleft()
    should_count = True
    for _, previous in recent:
        if normalized == previous:
            should_count = False
            break
        if min(len(normalized), len(previous)) >= 6:
            similarity = SequenceMatcher(None, normalized, previous).ratio()
            if similarity >= ACTIVITY_NEAR_DUPLICATE_RATIO:
                should_count = False
                break
    recent.append((now, normalized))
    return should_count

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
    raw_text = str(raw or "").strip()
    name_text = raw_text[1:] if raw_text.startswith("#") else raw_text
    if name_text:
        lowered = name_text.casefold()
        for channel in getattr(guild, "channels", []) or []:
            if getattr(channel, "name", "").casefold() == lowered:
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

async def refresh_activity_live_message(guild_id, config, rows=None, force_repost=False):
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
            if force_repost:
                latest_matches = await channel_latest_message_matches(channel, message.id)
                if latest_matches is False:
                    try:
                        await message.delete()
                    except Exception:
                        pass
                    message = None
                elif latest_matches is None:
                    await message.edit(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                    return message
            if message is not None:
                await message.edit(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                return message
        except discord.NotFound:
            message = None
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
    invite_match, _, _, invite_error = await identify_used_invite(member.guild)
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
    embed.add_field(
        name="Account Created",
        value=discord.utils.format_dt(member.created_at, "F") + f"\n{discord.utils.format_dt(member.created_at, 'R')}",
        inline=True,
    )
    embed.add_field(name="Member Count", value=f"{member.guild.member_count:,}", inline=True)
    if invite_match:
        before_invite, after_invite = invite_match
        embed.add_field(name=f"{economy_q_trust} Invite Used", value=invite_value(after_invite, before_invite), inline=False)
    else:
        embed.add_field(
            name=f"{economy_q_trust} Invite Used",
            value=f"Unknown ({invite_error or 'could not identify the invite'}).",
            inline=False,
        )
    embed.timestamp = datetime.now(timezone.utc)
    await send_log(embed, member.guild)

@bot.event
async def on_invite_create(invite):
    guild = getattr(invite, "guild", None)
    if guild is None:
        return
    snapshot = guild_invite_cache.setdefault(guild.id, {})
    snapshot[invite.code] = invite_snapshot(invite)

@bot.event
async def on_invite_delete(invite):
    guild = getattr(invite, "guild", None)
    if guild is None:
        return
    snapshot = guild_invite_cache.setdefault(guild.id, {})
    snapshot.pop(invite.code, None)

@bot.event
async def on_member_ban(guild, user):
    entry = await find_audit_entry(guild, {discord.AuditLogAction.ban}, user.id)
    embed = discord.Embed(
        title=f"{economy_q_hammer} Member Banned",
        color=discord.Color.red()
    )
    embed.add_field(name="User", value=log_user(user), inline=False)
    add_audit_actor_fields(embed, entry, "Banned by")
    if not entry:
        embed.add_field(name="Reason", value="No reason available", inline=False)
    embed.timestamp = datetime.now(timezone.utc)
    await send_log(embed, guild)

@bot.event
async def on_member_unban(guild, user):
    entry = await find_audit_entry(guild, {discord.AuditLogAction.unban}, user.id)
    embed = discord.Embed(
        title=f"{economy_q_accept} Member Unbanned",
        color=discord.Color.green()
    )
    embed.add_field(name="User", value=log_user(user), inline=False)
    add_audit_actor_fields(embed, entry, "Unbanned by")
    if not entry:
        embed.add_field(name="Reason", value="No reason available", inline=False)
    embed.timestamp = datetime.now(timezone.utc)
    await send_log(embed, guild)

@bot.event
async def on_guild_join(guild):
    print(f"Joined server: {guild.name} ({guild.id})")
    await refresh_invite_cache(guild)
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
        return await ctx.send("Only the person who added me, the server owner, or an admin can set the birthday channel.", allowed_mentions=discord.AllowedMentions.none())

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
        return await ctx.send("Only the person who added me, the server owner, or an admin can set the activity channel.", allowed_mentions=discord.AllowedMentions.none())

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

MESSAGE_TRACKER_DURATIONS = [
    ("1 day", timedelta(days=1)),
    ("2 days", timedelta(days=2)),
    ("3 days", timedelta(days=3)),
    ("4 days", timedelta(days=4)),
    ("5 days", timedelta(days=5)),
    ("6 days", timedelta(days=6)),
    ("1 week", timedelta(weeks=1)),
    ("2 weeks", timedelta(weeks=2)),
    ("1 month", timedelta(days=30)),
    ("3 months", timedelta(days=90)),
    ("6 months", timedelta(days=180)),
    ("1 year", timedelta(days=365)),
]

def message_tracker_since(duration):
    return datetime.now(timezone.utc).replace(tzinfo=None) - duration

def message_tracker_embed(guild, label, rows=None, user=None, count=None):
    embed = discord.Embed(
        title=f"{economy_q_book} Message Tracker",
        description=f"Range: **{label}**",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    if user is not None:
        embed.add_field(
            name="User",
            value=f"<@{user.id}> — **{int(count or 0):,}** messages",
            inline=False,
        )
    else:
        if rows:
            lines = []
            for index, row in enumerate(rows[:10], 1):
                rank = POLL_NUMBER_EMOJIS[index - 1] if index <= len(POLL_NUMBER_EMOJIS) else f"**{index}.**"
                lines.append(f"{rank} <@{row['user_id']}> — **{row['messages']:,}** messages")
            embed.add_field(name="Top 10", value="\n".join(lines), inline=False)
        else:
            embed.add_field(name="Top 10", value="No messages tracked in this range yet.", inline=False)
    embed.set_footer(text=f"{guild.name} message activity")
    return embed

def normalize_event_datetime(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)

def db_event_datetime(value):
    value = normalize_event_datetime(value)
    return value.replace(tzinfo=None) if value else None

def parse_message_event_window(raw):
    text = (raw or "").strip()
    if not text:
        return None, None
    tokens = split_friendly_words(text)
    duration_tokens = []
    for token in tokens:
        if not is_duration_piece(token):
            break
        duration_tokens.append(token)
    if duration_tokens:
        duration_raw = " ".join(duration_tokens)
        delta = parse_poll_duration(duration_raw)
        title = text[len(duration_raw):].strip(" -:") or None
        if delta:
            return datetime.now(timezone.utc) + delta, title

    for span in (3, 2, 1):
        if len(tokens) < span:
            continue
        candidate = " ".join(tokens[:span])
        parsed = parse_alarm_datetime(candidate)
        if parsed:
            title = text[len(candidate):].strip(" -:") or None
            return parsed, title
    return None, None

def message_event_rows_text(rows):
    if not rows:
        return "No messages counted in this event window yet."
    lines = []
    for index, row in enumerate(rows[:10], 1):
        rank = POLL_NUMBER_EMOJIS[index - 1] if index <= len(POLL_NUMBER_EMOJIS) else f"**{index}.**"
        lines.append(f"{rank} <@{int(row['user_id'])}> — **{int(row['messages']):,}** messages")
    return "\n".join(lines)

def merge_pending_message_event_counts(guild_id, rows, limit=10):
    totals = Counter({int(row["user_id"]): int(row["messages"]) for row in rows or []})
    for (buffer_guild_id, user_id), count in message_event_count_buffer.items():
        if int(buffer_guild_id) == int(guild_id):
            totals[int(user_id)] += int(count)
    ranked = sorted(totals.items(), key=lambda item: (-item[1], item[0]))[:int(limit)]
    return [{"user_id": user_id, "messages": messages} for user_id, messages in ranked]

def merge_pending_message_history_rows(guild_id, row, rows, limit=10):
    starts_at = normalize_event_datetime(row["starts_at"])
    ends_at = normalize_event_datetime(row["ends_at"])
    if not starts_at or not ends_at:
        return rows
    end_limit = min(ends_at, datetime.now(timezone.utc))
    totals = Counter({int(existing["user_id"]): int(existing["messages"]) for existing in rows or []})
    for event in message_history_buffer:
        if int(event.get("guild_id", 0) or 0) != int(guild_id):
            continue
        created_at = event.get("created_at")
        if created_at is None:
            continue
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        else:
            created_at = created_at.astimezone(timezone.utc)
        if starts_at <= created_at <= end_limit:
            totals[int(event["user_id"])] += 1
    ranked = sorted(totals.items(), key=lambda item: (-item[1], item[0]))[:int(limit)]
    return [{"user_id": user_id, "messages": messages} for user_id, messages in ranked]

def has_pending_message_history_for_event(guild_id, row):
    starts_at = normalize_event_datetime(row["starts_at"])
    ends_at = normalize_event_datetime(row["ends_at"])
    if not starts_at or not ends_at:
        return False
    end_limit = min(ends_at, datetime.now(timezone.utc))
    for event in message_history_buffer:
        if int(event.get("guild_id", 0) or 0) != int(guild_id):
            continue
        created_at = event.get("created_at")
        if created_at is None:
            continue
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        else:
            created_at = created_at.astimezone(timezone.utc)
        if starts_at <= created_at <= end_limit:
            return True
    return False

def message_event_embed(guild, row, rows=None, *, ended=False, cancelled=False):
    starts_at = normalize_event_datetime(row["starts_at"])
    ends_at = normalize_event_datetime(row["ends_at"])
    title = row.get("title") or "Message Event"
    state = "Cancelled" if cancelled else "Final Results" if ended else "Live Tracker"
    embed = discord.Embed(
        title=f"{economy_q_event} {title}",
        description=(
            f"**{state}**\n"
            f"Started {discord.utils.format_dt(starts_at, 'R')} · "
            f"Ends {discord.utils.format_dt(ends_at, 'R') if not ended else discord.utils.format_dt(ends_at, 'F')}"
        ),
        color=discord.Color.orange() if cancelled else discord.Color.green() if ended else discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(
        name="Window",
        value=f"{discord.utils.format_dt(starts_at, 'F')}\n→ {discord.utils.format_dt(ends_at, 'F')}",
        inline=False,
    )
    if not cancelled:
        embed.add_field(name="Top Messages", value=message_event_rows_text(rows or []), inline=False)
    embed.add_field(name="Started By", value=f"<@{int(row['started_by'])}>", inline=True)
    embed.set_footer(text=f"Only messages sent after the event starts count. {UTC_TIME_NOTE}")
    return embed

async def message_event_current_rows(guild_id, row, limit=10):
    await flush_activity_buffer()
    starts_at = db_event_datetime(row["starts_at"])
    end_limit = min(normalize_event_datetime(row["ends_at"]), datetime.now(timezone.utc))
    rows = await asyncio.to_thread(
        get_message_activity_top_between,
        int(guild_id),
        starts_at,
        db_event_datetime(end_limit),
        limit,
    )
    has_pending_history = has_pending_message_history_for_event(guild_id, row)
    rows = merge_pending_message_history_rows(guild_id, row, rows, limit)
    return rows if has_pending_history else merge_pending_message_event_counts(guild_id, rows, limit)

async def finalize_message_event(guild_id, *, cancelled=False):
    row = await asyncio.to_thread(get_message_event, int(guild_id))
    if not row:
        row = active_message_event_cache.get(int(guild_id))
    if not row:
        return False
    guild = bot.get_guild(int(guild_id))
    if guild is None:
        await asyncio.to_thread(delete_message_event, int(guild_id))
        active_message_event_cache.pop(int(guild_id), None)
        return False
    channel = guild.get_channel(int(row["channel_id"])) or bot.get_channel(int(row["channel_id"]))
    if cancelled:
        rows = []
    else:
        await flush_activity_buffer()
        rows = await asyncio.to_thread(
            get_message_activity_top_between,
            int(guild_id),
            db_event_datetime(row["starts_at"]),
            db_event_datetime(row["ends_at"]),
            10,
        )
        has_pending_history = has_pending_message_history_for_event(guild_id, row)
        rows = merge_pending_message_history_rows(guild_id, row, rows, 10)
        if not has_pending_history:
            rows = merge_pending_message_event_counts(guild_id, rows, 10)
    embed = message_event_embed(guild, row, rows, ended=not cancelled, cancelled=cancelled)
    message = None
    if channel and row.get("message_id"):
        try:
            message = await channel.fetch_message(int(row["message_id"]))
            await message.edit(embed=embed, view=None, allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            message = None
    if channel and message is None:
        try:
            await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        except Exception as e:
            print(f"Message event result send failed for guild {guild_id}: {type(e).__name__} - {e}")
    await asyncio.to_thread(delete_message_event, int(guild_id))
    active_message_event_cache.pop(int(guild_id), None)
    task = message_event_tasks.pop(int(guild_id), None)
    if task and not task.done() and task is not asyncio.current_task():
        task.cancel()
    return True

async def refresh_message_event_message(guild_id, force_repost=False):
    row = await asyncio.to_thread(get_message_event, int(guild_id))
    if not row:
        row = active_message_event_cache.get(int(guild_id))
    if not row or not row.get("message_id"):
        return False
    row = dict(row)
    guild = bot.get_guild(int(guild_id))
    if guild is None:
        return False
    channel = guild.get_channel(int(row["channel_id"])) or bot.get_channel(int(row["channel_id"]))
    if channel is None:
        return False
    async def save_reposted_event_message(message_id):
        row["message_id"] = int(message_id)
        active_message_event_cache[int(guild_id)] = dict(row)
        return await asyncio.to_thread(
            save_message_event,
            int(guild_id),
            int(row["channel_id"]),
            int(message_id),
            row.get("title") or "Message Event",
            int(row.get("started_by") or 0),
            db_event_datetime(normalize_event_datetime(row["starts_at"])),
            db_event_datetime(normalize_event_datetime(row["ends_at"])),
        )
    try:
        message = await channel.fetch_message(int(row["message_id"]))
        rows = await message_event_current_rows(guild_id, row)
        embed = message_event_embed(guild, row, rows)
        if force_repost:
            latest_matches = await channel_latest_message_matches(channel, message.id)
            if latest_matches is False:
                try:
                    await message.delete()
                except Exception:
                    pass
                message = await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                await save_reposted_event_message(message.id)
                return True
            if latest_matches is None:
                await message.edit(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                return True
        await message.edit(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        return True
    except discord.NotFound:
        if not force_repost:
            return False
        try:
            rows = await message_event_current_rows(guild_id, row)
            message = await channel.send(embed=message_event_embed(guild, row, rows), allowed_mentions=discord.AllowedMentions.none())
            await save_reposted_event_message(message.id)
            return True
        except Exception as e:
            print(f"Message event live repost failed for guild {guild_id}: {type(e).__name__} - {e}")
            return False
    except discord.Forbidden:
        return False
    except Exception as e:
        print(f"Message event live refresh failed for guild {guild_id}: {type(e).__name__} - {e}")
        return False

def schedule_message_event_finish(guild_id, row):
    guild_id = int(guild_id)
    active_message_event_cache[guild_id] = dict(row)
    old_task = message_event_tasks.get(guild_id)
    if old_task and not old_task.done():
        old_task.cancel()

    async def runner():
        try:
            end_time = normalize_event_datetime(row["ends_at"])
            while True:
                remaining = (end_time - datetime.now(timezone.utc)).total_seconds()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(60, max(5, remaining)))
                if datetime.now(timezone.utc) < end_time:
                    await refresh_message_event_message(guild_id)
            await finalize_message_event(guild_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Message event finish failed for guild {guild_id}: {type(e).__name__} - {e}")

    message_event_tasks[guild_id] = asyncio.create_task(runner())

async def restore_message_events():
    rows = await asyncio.to_thread(load_message_events)
    now = datetime.now(timezone.utc)
    for row in rows:
        active_message_event_cache[int(row["guild_id"])] = dict(row)
        end_time = normalize_event_datetime(row["ends_at"])
        if end_time <= now:
            await finalize_message_event(int(row["guild_id"]))
        else:
            schedule_message_event_finish(int(row["guild_id"]), row)

class MessageEventSetupModal(Modal):
    def __init__(self, author_id):
        super().__init__(title="Start Message Event")
        self.author_id = author_id
        self.duration = TextInput(label="Ends In / Date (UTC)", placeholder="2h, 3d, or 25/12/2026 18:30 UTC", max_length=60)
        self.title_input = TextInput(label="Title", placeholder="Optional: Weekend chat race", required=False, max_length=80)
        self.add_item(self.duration)
        self.add_item(self.title_input)

    async def on_submit(self, interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        if not has_owner_power(interaction.user, interaction.guild):
            return await interaction.response.send_message("Only admins can start message events.", ephemeral=True)
        await interaction.response.defer(thinking=True, ephemeral=True)
        end_time, parsed_title = parse_message_event_window(str(self.duration.value).strip())
        title = str(self.title_input.value).strip() or parsed_title or "Message Event"
        ok, message = await start_message_event(interaction.channel, interaction.guild, interaction.user, end_time, title)
        await interaction.followup.send(message, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

class OpenMessageEventSetupButton(Button):
    def __init__(self):
        super().__init__(label="Start Event", style=discord.ButtonStyle.primary, emoji=reaction_emoji(economy_q_event))

    async def callback(self, interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(MessageEventSetupModal(self.view.author_id))

async def start_message_event(channel, guild, author, end_time, title):
    if guild is None:
        return False, "Message events only work in servers."
    if end_time is None:
        return False, f"{economy_q_warning} Use a duration like `2h`, `3d`, or a date like `25/12/2026 18:30`.\n{UTC_TIME_NOTE}"
    now = datetime.now(timezone.utc)
    end_time = normalize_event_datetime(end_time)
    if end_time <= now + timedelta(minutes=1):
        return False, f"{economy_q_warning} Message events must run for at least 1 minute."
    if end_time > now + timedelta(days=60):
        return False, f"{economy_q_warning} Message events can run up to 60 days."
    existing = await asyncio.to_thread(get_message_event, guild.id)
    if not existing:
        existing = active_message_event_cache.get(guild.id)
    if existing:
        return False, f"{economy_q_warning} A message event is already running. Use `.messageevent status`, `.messageevent end`, or `.messageevent cancel`."

    row = {
        "guild_id": guild.id,
        "channel_id": channel.id,
        "message_id": None,
        "title": title or "Message Event",
        "started_by": author.id,
        "starts_at": now,
        "ends_at": end_time,
    }
    embed = message_event_embed(guild, row, rows=[])
    message = await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    row["message_id"] = message.id
    saved = await asyncio.to_thread(
        save_message_event,
        guild.id,
        channel.id,
        message.id,
        row["title"],
        author.id,
        db_event_datetime(now),
        db_event_datetime(end_time),
    )
    if not saved:
        try:
            await message.edit(content=f"{economy_q_warning} Could not save this message event. Database unavailable.", embed=None)
        except Exception:
            pass
        return False, f"{economy_q_warning} Could not save this message event. Database unavailable."
    active_message_event_cache[guild.id] = dict(row)
    schedule_message_event_finish(guild.id, row)
    return True, f"{economy_q_event} Message event started: {message.jump_url}"

@bot.command(name="messageevent", aliases=["msgevent", "chatevent", "messagecontest", "chatcontest"])
@is_admin_power()
async def messageevent(ctx, action: str = None, *, raw: str = None):
    """Starts or manages a timed message-count event."""
    if ctx.guild is None:
        return await ctx.send("Message events only work in servers.")

    action_key = str(action or "").casefold()
    if action_key in {"stop", "end", "finish", "results"}:
        ended = await finalize_message_event(ctx.guild.id)
        if not ended:
            return await ctx.send(f"{economy_q_warning} No message event is running.")
        return await ctx.send(f"{economy_q_event} Message event ended and results were posted.", allowed_mentions=discord.AllowedMentions.none())

    if action_key in {"cancel", "delete"}:
        ended = await finalize_message_event(ctx.guild.id, cancelled=True)
        if not ended:
            return await ctx.send(f"{economy_q_warning} No message event is running.")
        return await ctx.send(f"{economy_q_event} Message event cancelled.", allowed_mentions=discord.AllowedMentions.none())

    if action_key in {"status", "show", "current"} or not action:
        row = await asyncio.to_thread(get_message_event, ctx.guild.id)
        if not row:
            row = active_message_event_cache.get(ctx.guild.id)
        if row:
            rows = await message_event_current_rows(ctx.guild.id, row)
            return await ctx.send(embed=message_event_embed(ctx.guild, row, rows), allowed_mentions=discord.AllowedMentions.none())
        if not action:
            return await ctx.send(
                f"{economy_q_event} Start a timed message event here, or type `.messageevent start 2h Weekend chat race`.\n{UTC_TIME_NOTE}",
                view=SingleUserSetupView(ctx.author.id, OpenMessageEventSetupButton()),
                allowed_mentions=discord.AllowedMentions.none()
            )

    if action_key == "start":
        setup_raw = raw
    else:
        setup_raw = " ".join(part for part in [action or "", raw or ""] if part).strip()

    end_time, title = parse_message_event_window(setup_raw)
    ok, message = await start_message_event(ctx.channel, ctx.guild, ctx.author, end_time, title or "Message Event")
    await ctx.send(message, allowed_mentions=discord.AllowedMentions.none())

class MessageDurationSelect(Select):
    def __init__(self):
        options = [
            discord.SelectOption(label=label, value=str(index), default=index == 0)
            for index, (label, _) in enumerate(MESSAGE_TRACKER_DURATIONS)
        ]
        super().__init__(placeholder="Choose time range", min_values=1, max_values=1, options=options)

    async def callback(self, interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own message tracker.", ephemeral=True)
        self.view.duration_index = int(self.values[0])
        await self.view.refresh(interaction)

class MessageUserSelect(discord.ui.UserSelect):
    def __init__(self):
        super().__init__(placeholder="Choose a user", min_values=1, max_values=1)

    async def callback(self, interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own message tracker.", ephemeral=True)
        self.view.mode = "user"
        self.view.target_user = self.values[0]
        await self.view.refresh(interaction)

class MessageTrackerView(View):
    def __init__(self, author_id):
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
        self.author_id = author_id
        self.duration_index = 0
        self.mode = "leaderboard"
        self.target_user = None
        self.add_item(MessageDurationSelect())
        self.add_item(MessageUserSelect())

    async def build_embed(self, guild):
        await flush_activity_buffer()
        label, duration = MESSAGE_TRACKER_DURATIONS[self.duration_index]
        since = message_tracker_since(duration)
        if self.mode == "user":
            user = self.target_user
            count = await asyncio.to_thread(get_message_activity_count, guild.id, user.id, since)
            return message_tracker_embed(guild, label, user=user, count=count)
        rows = await asyncio.to_thread(get_message_activity_top, guild.id, since, 10)
        return message_tracker_embed(guild, label, rows=rows)

    async def refresh(self, interaction):
        await interaction.response.defer()
        embed = await self.build_embed(interaction.guild)
        await interaction.edit_original_response(embed=embed, view=self, allowed_mentions=discord.AllowedMentions.none())

    @discord.ui.button(label="Leaderboard", style=discord.ButtonStyle.primary, row=2)
    async def leaderboard_button(self, interaction, button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own message tracker.", ephemeral=True)
        self.mode = "leaderboard"
        self.target_user = None
        await self.refresh(interaction)

    @discord.ui.button(label="Me", style=discord.ButtonStyle.secondary, row=2)
    async def me_button(self, interaction, button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own message tracker.", ephemeral=True)
        self.mode = "user"
        self.target_user = interaction.user
        await self.refresh(interaction)

@bot.command(name="messages", aliases=["msgstats", "messagestats", "mstats"])
async def messages_tracker(ctx):
    """Shows message counts by range for one user or a top 10 leaderboard."""
    if ctx.guild is None:
        return await ctx.send("Message tracking only works in servers.")
    view = MessageTrackerView(ctx.author.id)
    embed = await view.build_embed(ctx.guild)
    await ctx.send(embed=embed, view=view, allowed_mentions=discord.AllowedMentions.none())

async def apply_activity_edit(guild, author, config, setting, value, send, channel_mentions=None):
    setting = str(setting or "").casefold()
    if setting in {"channel", "chan"}:
        selected_channel = resolve_activity_report_channel(guild, value, channel_mentions)
        if selected_channel is None:
            await send(f"{economy_q_warning} Mention a channel or send its channel ID.")
            return False
        next_report = config.get("next_report") or (datetime.now(timezone.utc) + timedelta(hours=24))
        if next_report.tzinfo is None:
            next_report = next_report.replace(tzinfo=timezone.utc)
        ok, message, _ = await save_activity_report_config(guild, selected_channel, author.id, next_report)
        if not ok:
            await send(f"{economy_q_warning} {message}")
            return False
        schedule_activity_live_refresh(guild.id, guild_activity_channels[guild.id])
        embed = activity_saved_embed(guild, selected_channel, next_report, author.id)
        embed.title = f"{economy_q_activity} Activity Channel Updated"
        embed.description = "Daily activity reports were moved to a new channel."
        await send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        return True

    if setting in {"next", "time", "timer", "delay", "reset"}:
        delay = parse_poll_duration(value)
        if delay is None or delay.total_seconds() < 300:
            await send(f"{economy_q_warning} Invalid time. Use at least 5 minutes, like `30m`, `12h`, or `1d`.")
            return False
        next_report = datetime.now(timezone.utc) + delay
        channel = resolve_activity_report_channel(guild, str(config["channel_id"]))
        if channel is None:
            await send(f"{economy_q_warning} Saved activity channel no longer exists. Use `.editactivity channel #channel`.")
            return False
        ok, message, _ = await save_activity_report_config(guild, channel, config.get("set_by_user_id") or author.id, next_report)
        if not ok:
            await send(f"{economy_q_warning} {message}")
            return False
        schedule_activity_live_refresh(guild.id, guild_activity_channels[guild.id])
        await send(
            f"{economy_q_activity} Next activity report set for <t:{int(next_report.timestamp())}:R>.",
            allowed_mentions=discord.AllowedMentions.none()
        )
        return True

    await send(f"{economy_q_warning} Unknown setting. Use `channel` or `next`.")
    return False

class ActivityEditValueModal(Modal):
    def __init__(self, author_id, guild_id, setting, label, placeholder, panel_message=None, command_message=None):
        super().__init__(title=f"Edit activity {label.lower()}")
        self.author_id = author_id
        self.guild_id = guild_id
        self.setting = setting
        self.panel_message = panel_message
        self.command_message = command_message
        self.value_input = TextInput(label=label, placeholder=placeholder, min_length=1, max_length=100)
        self.add_item(self.value_input)

    async def on_submit(self, interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        if interaction.guild is None or interaction.guild.id != self.guild_id:
            return await interaction.response.send_message("This setup UI belongs to another server.", ephemeral=True)
        config = guild_activity_channels.get(interaction.guild.id)
        if config is None:
            saved_configs = await asyncio.to_thread(load_guild_activity_channels)
            if saved_configs:
                guild_activity_channels.update(saved_configs)
            config = guild_activity_channels.get(interaction.guild.id)
        if not config:
            return await interaction.response.send_message(f"{economy_q_warning} Activity reports are not set up. Use `.activity setup` first.", ephemeral=True)
        await interaction.response.defer(ephemeral=True, thinking=True)

        async def send(content=None, **kwargs):
            kwargs.setdefault("ephemeral", True)
            return await interaction.followup.send(content, **kwargs)

        changed = await apply_activity_edit(
            interaction.guild,
            interaction.user,
            config,
            self.setting,
            str(self.value_input.value).strip(),
            send,
        )
        if changed:
            await safe_delete_message(self.panel_message)
            await safe_delete_message(self.command_message)

class ActivityEditSettingSelect(Select):
    def __init__(self):
        super().__init__(
            placeholder="Choose what to edit",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label="Report channel", value="channel", description="Paste #channel or a channel ID"),
                discord.SelectOption(label="Next report time", value="next", description="Example: 12h"),
            ],
        )

    async def callback(self, interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        if interaction.guild is None or interaction.guild.id != self.view.guild_id:
            return await interaction.response.send_message("This setup UI belongs to another server.", ephemeral=True)
        setting = self.values[0]
        if setting == "channel":
            modal = ActivityEditValueModal(
                self.view.author_id,
                self.view.guild_id,
                setting,
                "Channel mention or ID",
                "#activity or 123456789012345678",
                getattr(self.view, "panel_message", None) or interaction.message,
                getattr(self.view, "command_message", None),
            )
        else:
            modal = ActivityEditValueModal(
                self.view.author_id,
                self.view.guild_id,
                setting,
                "Time until next report",
                "12h",
                getattr(self.view, "panel_message", None) or interaction.message,
                getattr(self.view, "command_message", None),
            )
        await interaction.response.send_modal(modal)

class ActivityEditView(View):
    def __init__(self, author_id, guild_id, command_message=None):
        super().__init__(timeout=LONG_SETUP_VIEW_TIMEOUT)
        self.author_id = author_id
        self.guild_id = guild_id
        self.command_message = command_message
        self.panel_message = None
        self.add_item(ActivityEditSettingSelect())

async def send_activity_edit_ui(ctx, selected_setting=None):
    embed = discord.Embed(
        title=f"{economy_q_activity} Edit Activity Report",
        description="Choose a setting, then enter the new value.",
        color=discord.Color.blue(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Report Channel", value="Paste a channel mention or ID.", inline=False)
    embed.add_field(name="Next Report", value="Reset the next report timer, like `12h` or `1d`.", inline=False)
    view = ActivityEditView(ctx.author.id, ctx.guild.id, command_message=getattr(ctx, "message", None))
    panel = await ctx.send(embed=embed, view=view, allowed_mentions=discord.AllowedMentions.none())
    view.panel_message = panel

@bot.command(name="editactivity", aliases=["activityedit"])
async def editactivity(ctx, setting: str = None, *, value: str = None):
    """Edits this server's activity report settings."""
    if ctx.guild is None:
        return await ctx.send("Activity report editing only works in servers.")
    if not await can_manage_activity_channel(ctx.author, ctx.guild):
        return await ctx.send("Only the person who added me, the server owner, or an admin can edit activity reports.", allowed_mentions=discord.AllowedMentions.none())

    config = guild_activity_channels.get(ctx.guild.id)
    if config is None:
        saved_configs = await asyncio.to_thread(load_guild_activity_channels)
        if saved_configs:
            guild_activity_channels.update(saved_configs)
        config = guild_activity_channels.get(ctx.guild.id)
    if not config:
        return await ctx.send(f"{economy_q_warning} Activity reports are not set up. Use `.activity setup` first.")

    if not setting or not value:
        return await send_activity_edit_ui(ctx, setting)

    async def send(content=None, **kwargs):
        return await ctx.send(content, **kwargs)

    changed = await apply_activity_edit(ctx.guild, ctx.author, config, setting, value, send, ctx.message.channel_mentions)
    if changed:
        await safe_delete_message(ctx.message)

@bot.command(name="stopactivity", aliases=["activitystop"])
async def stopactivity(ctx):
    """Stops this server's activity reports and clears the current activity window."""
    if ctx.guild is None:
        return await ctx.send("Activity report stopping only works in servers.")
    if not await can_manage_activity_channel(ctx.author, ctx.guild):
        return await ctx.send("Only the person who added me, the server owner, or an admin can stop activity reports.", allowed_mentions=discord.AllowedMentions.none())

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
        return await ctx.send("Only the person who added me, the server owner, or an admin can end the current activity report.", allowed_mentions=discord.AllowedMentions.none())

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
        return await ctx.send(f"{economy_q_warning} I couldn't end the current activity report: {clean_user_error(e)}")

    if ctx.channel.id != channel.id:
        await ctx.send(
            f"{economy_q_activity} Current activity report ended in {channel.mention}. Next report: <t:{int(next_report.timestamp())}:R>.",
            allowed_mentions=discord.AllowedMentions.none()
        )

@bot.event
async def on_guild_remove(guild):
    guild_invite_cache.pop(guild.id, None)
    print(f"Left server: {guild.name} ({guild.id})")

@bot.event
async def on_guild_channel_create(channel):
    embed = discord.Embed(
        title=f"{economy_q_accept} Channel Created",
        color=discord.Color.green()
    )
    embed.add_field(name="Channel", value=channel.mention, inline=False)

    entry = await find_audit_entry(channel.guild, {discord.AuditLogAction.channel_create}, channel.id)
    add_audit_actor_fields(embed, entry)

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

    entry = await find_audit_entry(channel.guild, {discord.AuditLogAction.channel_delete}, channel.id)
    add_audit_actor_fields(embed, entry)

    embed.timestamp = datetime.now(timezone.utc)
    try:
        await send_log(embed, channel.guild)
    except Exception as e:
        print(f"Failed to send log: {e}")

@bot.event
async def on_guild_role_create(role):
    embed = discord.Embed(
        title=f"{economy_q_roles} Role Created",
        color=discord.Color.green()
    )
    embed.add_field(name="Role", value=log_role(role), inline=False)
    embed.add_field(name="Color", value=str(role.color), inline=True)
    embed.add_field(name="Position", value=str(role.position), inline=True)
    embed.add_field(name="Mentionable", value="Yes" if role.mentionable else "No", inline=True)

    entry = await find_audit_entry(role.guild, {discord.AuditLogAction.role_create}, role.id)
    add_audit_actor_fields(embed, entry)

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
    embed.add_field(name="Role", value=f"`{role.name}` ({role.id})", inline=False)
    embed.add_field(name="Color", value=str(role.color), inline=True)
    embed.add_field(name="Position", value=str(role.position), inline=True)
    embed.add_field(name="Mentionable", value="Yes" if role.mentionable else "No", inline=True)

    entry = await find_audit_entry(role.guild, {discord.AuditLogAction.role_delete}, role.id)
    add_audit_actor_fields(embed, entry)

    embed.timestamp = datetime.now(timezone.utc)
    try:
        await send_log(embed, role.guild)
    except Exception as e:
        print(f"Failed to send log: {e}")

@bot.event
async def on_guild_role_update(before, after):
    position_only = (
        before.position != after.position
        and before.name == after.name
        and before.color == after.color
        and before.hoist == after.hoist
        and before.mentionable == after.mentionable
        and before.permissions == after.permissions
    )
    if position_only:
        return

    embed = discord.Embed(
        title=f"{economy_q_roles} Role Updated",
        color=discord.Color.blue()
    )
    embed.add_field(name="Role", value=log_role(after), inline=False)

    changes = []

    if before.name != after.name:
        changes.append(f"**Name:** `{before.name}` → `{after.name}`")
    if before.color != after.color:
        changes.append(f"**Color:** `{before.color}` → `{after.color}`")
    if before.hoist != after.hoist:
        changes.append(f"**Displayed Separately:** {'Yes' if before.hoist else 'No'} → {'Yes' if after.hoist else 'No'}")
    if before.mentionable != after.mentionable:
        changes.append(f"**Mentionable:** {'Yes' if before.mentionable else 'No'} → {'Yes' if after.mentionable else 'No'}")
    if before.position != after.position:
        changes.append(f"**Position:** `{before.position}` → `{after.position}`")
    if before.permissions != after.permissions:
        before_perms = set(p[0] for p in before.permissions if p[1])
        after_perms = set(p[0] for p in after.permissions if p[1])
        added = after_perms - before_perms
        removed = before_perms - after_perms
        if added:
            changes.append(f"**Perms Added:** {', '.join(sorted(added))}")
        if removed:
            changes.append(f"**Perms Removed:** {', '.join(sorted(removed))}")

    if not changes:
        changes.append("No visible changes logged.")

    embed.add_field(name="Changes", value="\n".join(changes), inline=False)

    entry = await find_audit_entry(after.guild, {discord.AuditLogAction.role_update}, after.id)
    add_audit_actor_fields(embed, entry)

    embed.timestamp = datetime.now(timezone.utc)
    try:
        await send_log(embed, after.guild)
    except Exception as e:
        print(f"Failed to send log: {e}")

@bot.event
async def on_guild_update(before, after):
    guild = after.guild if hasattr(after, 'guild') else before
    entry = await find_audit_entry(guild, target_id=guild.id, limit=5)

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

    if embed:
        add_audit_actor_fields(embed, entry)
        embed.timestamp = datetime.now(timezone.utc)
        try:
            await send_log(embed, guild)
        except Exception as e:
            print(f"Failed to send log: {e}")

async def award_chat_xp_background(message):
    try:
        event_multiplier = 1
        try:
            active_key = await asyncio.to_thread(economy_module.active_event_key, message.guild.id) if message.guild else None
            if active_key == "doublexp":
                event_multiplier = 2
        except Exception:
            event_multiplier = 1
        xp_result = await asyncio.to_thread(economy_award_chat_xp, message.author.id, event_multiplier)
    except Exception as e:
        print(f"Chat XP skipped for {message.author.id}: {type(e).__name__} - {e}")
        return
    if not xp_result or xp_result["levels_gained"] <= 0:
        return
    try:
        data = await asyncio.to_thread(economy_get_user, message.author.id)
        target_channel = message.channel
        try:
            level_channel_id = await asyncio.to_thread(economy_module.get_levelup_channel_id, message.guild.id) if message.guild else None
            configured_channel = message.guild.get_channel(level_channel_id) if level_channel_id and message.guild else None
            if configured_channel:
                perms = configured_channel.permissions_for(message.guild.me)
                if perms.view_channel and perms.send_messages:
                    target_channel = configured_channel
        except Exception:
            target_channel = message.channel
        await target_channel.send(
            f"{economy_q_level_pulse} <@{message.author.id}> leveled up.",
            embed=economy_build_level_up_embed(message.author, data, xp_result),
            allowed_mentions=discord.AllowedMentions(users=True)
        )
    except Exception as e:
        print(f"Level-up message skipped for {message.author.id}: {type(e).__name__} - {e}")

COMMAND_EXAMPLE_OVERRIDES = {
    "add": ".add @user 1000",
    "addrole": ".addrole @user @role",
    "addtick": ".addtick @user 5",
    "removetick": ".removetick @user 5",
    "alarm": ".alarm 1h reminder",
    "analyse": ".analyse while replying to an image",
    "analyze": ".analyze while replying to an image",
    "archive": ".archive 50",
    "aidetect": ".aidetect <essay text>",
    "auditcommands": ".auditcommands",
    "balanceaudit": ".balanceaudit 14",
    "balancedashboard": ".balancedashboard 14",
    "bulkqueue": ".bulkqueue",
    "jobs": ".jobs",
    "backgroundjobs": ".backgroundjobs",
    "errors": ".errors",
    "recover": ".recover",
    "off": ".off updating",
    "on": ".on",
    "dbaudit": ".dbaudit",
    "aiguard": ".aiguard",
    "styleaudit": ".styleaudit",
    "commandcleanup": ".commandcleanup",
    "aisettings": ".aisettings",
    "aiignore": ".aiignore @user",
    "aiunignore": ".aiunignore @user",
    "aichannel": ".aichannel on",
    "quewochannel": ".quewochannel here",
    "levelupchannel": ".levelupchannel #level-ups",
    "aistyle": ".aistyle casual and short",
    "ban": ".ban @user reason",
    "blackjack": ".blackjack 1000",
    "block": ".block @user",
    "buytick": ".buytick 3",
    "bank": ".bank deposit 100k",
    "c4": ".c4 @user 1000",
    "cardladder": ".cardladder 1000",
    "cf": ".cf 1000 h",
    "chess": ".chess @user 1000",
    "colour": ".colour #77A07BFF",
    "color": ".color #77A07BFF",
    "define": ".define example",
    "deleterole": ".deleterole @role",
    "disable": ".disable command",
    "editactivity": ".editactivity channel #activity",
    "editlottery": ".editlottery duration 12h",
    "endseason": ".endseason 2026-05",
    "enable": ".enable command",
    "event": ".event start jackpot 1h",
    "economyhealth": ".economyhealth",
    "aiknow": ".aiknow slots",
    "aidoctor": ".aidoctor",
    "aiperms": ".aiperms",
    "aimemory": ".aimemory @user",
    "find": ".find 885548126365171824",
    "flagquiz": ".flagquiz",
    "fwd": ".fwd 5",
    "forward": ".forward #channel 5 @user",
    "fw": ".fw <message link>",
    "give": ".give @user 1000",
    "generate": ".generate clean ProQue mascot holding queso coins",
    "imagine": ".imagine cozy server banner with queso coins",
    "profilebanner": ".profilebanner dark green luxury profile banner",
    "makeemoji": ".makeemoji happy queso coin",
    "eventposter": ".eventposter weekend game night",
    "gameart": ".gameart dungeon boss fight",
    "shoppreview": ".shoppreview fortune vial",
    "reactionimage": ".reactionimage shocked but funny",
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
    "gameaudit": ".gameaudit",
    "gamestats": ".gamestats @user",
    "recommendgame": ".recommendgame",
    "rob": ".rob @user",
    "robsettings": ".robsettings on",
    "commandstats": ".commandstats",
    "snipe": ".snipe @user 2",
    "dsnipe": ".dsnipe @user 2",
    "esnipe": ".esnipe @user",
    "rsnipe": ".rsnipe @user 2",
    "memory": ".memory 1000",
    "messages": ".messages",
    "messageevent": ".messageevent start 2h Weekend chat race",
    "lotterypot": ".lotterypot add 1m",
    "onboard": ".onboard",
    "health": ".health",
    "permaudit": ".permaudit",
    "perms": ".perms @user",
    "sessions": ".sessions",
    "move": ".move @from @to 1m",
    "movetick": ".movetick @from @to 10",
    "chessmove": ".chessmove e2e4",
    "ms": ".ms 1000",
    "poll": ".poll Best color? | Blue | Red | 10m",
    "prefix": ".prefix !",
    "preifx": ".preifx !",
    "purge": ".purge @user 20",
    "quote": ".quote <message link>",
    "receipt": ".receipt latest",
    "receipts": ".receipts latest",
    "reopen": ".reopen",
    "remove": ".remove @user 1000",
    "removerole": ".removerole @user @role",
    "reply": ".reply <message id/link> message",
    "roulette": ".roulette 1000 red",
    "riskprofile": ".riskprofile @user",
    "rpurge": ".rpurge @user 20",
    "rshut": ".rshut @user",
    "scratch": ".scratch 1000",
    "season": ".season",
    "seasonpass": ".seasonpass",
    "send": ".send #channel message",
    "summarize": ".summarize @user 1h",
    "setbday": ".setbday 25/12",
    "bdaycard": ".bdaycard view",
    "viewbdaycard": ".viewbdaycard",
    "setbdaychannel": ".setbdaychannel #birthdays",
    "setnick": ".setnick @user new nickname",
    "setprefix": ".setprefix !",
    "slashsync": ".slashsync",
    "setquesos": ".setquesos @user 1m",
    "settick": ".settick @user 10",
    "shut": ".shut @user",
    "slots": ".slots 1000",
    "steal": ".steal <:emoji:123456789>",
    "summon": ".summon @user",
    "summon2": ".summon2 @user",
    "timer": ".timer 10m study",
    "truthordare": ".tod",
    "tod": ".tod truth @user",
    "todchannel": ".todchannel set #truth-or-dare #games",
    "usersettings": ".usersettings receipts on",
    "tower": ".tower 1000",
    "tutorial": ".tutorial off",
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

GENERIC_INPUT_UI_COMMANDS = {
    "prefix", "preifx", "setprefix", "disable", "enable", "roleinfo", "deleterole",
    "howtoplay", "how", "rules", "flagquiz", "flags", "fq", "ttt", "c4", "chess",
    "move", "movetick", "chessmove", "setnick", "shut", "unshut", "rshut", "unrshut", "purge",
    "rpurge", "unmute", "ban", "unban", "kick", "addrole", "removerole", "send",
    "reply", "fwd", "forward", "fw", "quote", "archive", "aban", "raban", "summon2", "block", "unblock", "wake",
    "find", "censor", "uncensor", "ask", "generate", "summarize", "summarise", "summary", "aisummary", "tldr", "recap",
    "aidetect", "aicheck", "detectai", "authenticity", "authcheck", "essaycheck",
    "buytick", "ticket", "tickets", "bank", "safe", "vaultcash", "deposit", "withdraw",
    "tutorial", "tutorialmode", "tips", "recommendgame", "recgame", "whatgame", "suggestgame",
    "rob", "stealqs", "mug", "robsettings", "robbing", "setrob", "robconfig",
}

INPUT_UI_EXCLUDED_COMMANDS = {
    *GAMBLING_AMOUNT_COMMANDS,
    "analyse", "analyze", "analyseimage", "analyzeimage", "vision", "steal",
}

def command_supports_input_ui(command):
    if not command:
        return False
    names = {command.name, *getattr(command, "aliases", [])}
    if names & INPUT_UI_EXCLUDED_COMMANDS:
        return False
    return bool(names & (GENERIC_INPUT_UI_COMMANDS | SPECIALIZED_SETUP_UI_COMMANDS))

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
            return "Use a number like `1000`, `4k`, or `all`. Gambling commands are capped at `200k`."
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
    if command_supports_input_ui(getattr(ctx, "command", None)):
        await send_command_input_ui(ctx, error=error)
        return
    usage = command_usage_example(ctx)
    hint = command_argument_hint(error, ctx)
    lines = [f"Type: `{usage}`"]
    if hint:
        lines.append(hint)
    if ctx.command:
        prefix = getattr(ctx, "prefix", prefix_for_guild(ctx.guild))
        lines.append(f"More help: `{prefix}help {ctx.command.qualified_name}` or `{prefix}explain {ctx.command.qualified_name}`")
    await ctx.send("\n".join(lines), allowed_mentions=discord.AllowedMentions.none())

def short_status_duration(delta):
    total = max(0, int(delta.total_seconds()))
    days, remainder = divmod(total, 86400)
    hours, remainder = divmod(remainder, 3600)
    mins, secs = divmod(remainder, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if mins:
        parts.append(f"{mins}m")
    if not parts:
        parts.append(f"{secs}s")
    return " ".join(parts[:3])

def status_reason_text(reason):
    if not reason or str(reason).strip().lower() == "afk":
        return None
    return str(reason).strip()

def normalize_status_mention_entry(entry):
    if len(entry) >= 4:
        uid, link, ts, guild_id = entry[:4]
    else:
        uid, link, ts = entry[:3]
        guild_id = None
    return int(uid), link, int(ts), int(guild_id) if guild_id else None

def split_status_mentions_for_guild(mentions_list, guild_id):
    same_server = []
    other_servers = []
    current_guild_id = int(guild_id) if guild_id else None
    for entry in mentions_list:
        uid, link, ts, entry_guild_id = normalize_status_mention_entry(entry)
        normalized = (uid, link, ts, entry_guild_id)
        if entry_guild_id is None or entry_guild_id == current_guild_id:
            same_server.append(normalized)
        else:
            other_servers.append(normalized)
    return same_server, other_servers

async def pop_away_mentions(user_id):
    memory_mentions = user_mentions.pop(int(user_id), [])
    try:
        saved_mentions = await asyncio.to_thread(load_away_mentions, int(user_id))
    except Exception:
        saved_mentions = []
    try:
        await asyncio.to_thread(clear_away_mentions, int(user_id))
    except Exception:
        pass

    merged = []
    seen = set()
    for entry in [*saved_mentions, *memory_mentions]:
        normalized = normalize_status_mention_entry(entry)
        key = (normalized[0], normalized[1], normalized[2], normalized[3])
        if key in seen:
            continue
        seen.add(key)
        merged.append(normalized)
    return sorted(merged, key=lambda item: item[2])

def mention_summary_lines(mentions_list):
    grouped = {}
    order = []
    for entry in mentions_list:
        uid, link, ts, _guild_id = normalize_status_mention_entry(entry)
        if uid not in grouped:
            grouped[uid] = []
            order.append(uid)
        grouped[uid].append((link, ts))

    lines = []
    for uid in order:
        entries = sorted(grouped[uid], key=lambda item: item[1])
        lines.append(f"**<@{uid}>** mentioned you **{len(entries):,}** time(s):")
        for idx, (link, ts) in enumerate(entries, start=1):
            lines.append(f"  {idx}. <t:{ts}:R> · <t:{ts}:t> · [jump]({link})")
    return lines

def add_mentions_to_status_embed(embed, mentions_list):
    if not mentions_list:
        embed.add_field(name="Mentions", value="No one bothered the mission. Peaceful.", inline=False)
        return None

    lines = mention_summary_lines(mentions_list)
    full_text = "\n".join(lines)
    shown_lines = []
    shown_chars = 0
    for line in lines:
        if len(shown_lines) >= 35 or shown_chars + len(line) + 1 > 3600:
            break
        shown_lines.append(line)
        shown_chars += len(line) + 1

    current = []
    current_len = 0
    field_index = 1
    for line in shown_lines:
        if current and current_len + len(line) + 1 > 950:
            embed.add_field(
                name="Mentions" if field_index == 1 else f"Mentions {field_index}",
                value="\n".join(current),
                inline=False
            )
            field_index += 1
            current = []
            current_len = 0
        current.append(line)
        current_len += len(line) + 1
    if current:
        embed.add_field(
            name="Mentions" if field_index == 1 else f"Mentions {field_index}",
            value="\n".join(current),
            inline=False
        )

    hidden = len(lines) - len(shown_lines)
    if hidden > 0:
        embed.add_field(
            name="Full mention log",
            value=f"Discord would make this message huge, so I attached the full **{len(lines):,}** mention list.",
            inline=False
        )
        return File(BytesIO(full_text.encode("utf-8")), filename="mentions.txt")
    return None

async def send_private_cross_server_mentions(user, mentions_list, status_label):
    if not mentions_list:
        return False
    embed = standard_embed(
        "Private mention catch-up",
        description=f"While you were {status_label}, these mentions came from other servers, so I kept them private.",
        color=0x5865F2,
        icon=economy_q_bell,
    )
    mention_file = add_mentions_to_status_embed(embed, mentions_list)
    try:
        await user.send(embed=embed, file=mention_file, allowed_mentions=discord.AllowedMentions.none())
        return True
    except Exception:
        return False

async def handle_returning_status(message):
    if message.author.id in sleeping_users:
        start = sleeping_users.pop(message.author.id)
        await asyncio.to_thread(remove_sleeping_user, message.author.id)
        duration = datetime.now(timezone.utc) - start
        formatted = short_status_duration(duration)

        embed = standard_embed(
            "Back from sleep mode",
            description=f"<@{message.author.id}> woke up after **{formatted}**. Brain back online, allegedly.",
            color=0xF1C40F,
            icon=economy_q_bell,
        )
        embed.add_field(name="Status", value=f"{economy_q_sleep} Sleep mode cleared", inline=True)
        embed.add_field(name="Away for", value=f"**{formatted}**", inline=True)

        mentions_list = await pop_away_mentions(message.author.id)
        same_server_mentions, private_mentions = split_status_mentions_for_guild(mentions_list, message.guild.id if message.guild else None)
        mention_file = add_mentions_to_status_embed(embed, same_server_mentions)
        if private_mentions:
            dm_sent = await send_private_cross_server_mentions(message.author, private_mentions, "sleeping")
            embed.add_field(
                name="Private mentions",
                value=(
                    f"{economy_q_bell} Sent **{len(private_mentions):,}** mention(s) from other servers privately."
                    if dm_sent else
                    f"{economy_q_bell} **{len(private_mentions):,}** mention(s) from other servers were kept out of this channel, but your DMs are closed."
                ),
                inline=False,
            )

        await message.channel.send(embed=embed, file=mention_file, allowed_mentions=discord.AllowedMentions.none())

    if message.author.id in afk_users:
        afk_data = afk_users.pop(message.author.id)
        await asyncio.to_thread(remove_afk_user, message.author.id)

        duration = datetime.now(timezone.utc) - afk_data["since"]
        formatted = short_status_duration(duration)
        reason = status_reason_text(afk_data.get("reason"))

        embed = standard_embed(
            "Back in the chat",
            description=f"<@{message.author.id}> returned after **{formatted}**. The side quest is over.",
            color=0x2ECC71,
            icon=economy_q_bell,
        )
        embed.add_field(name="Status", value=f"{economy_q_accept} AFK cleared", inline=True)
        embed.add_field(name="Away for", value=f"**{formatted}**", inline=True)
        if reason:
            embed.add_field(name="Reason", value=embed_value(reason), inline=False)

        mentions_list = await pop_away_mentions(message.author.id)
        same_server_mentions, private_mentions = split_status_mentions_for_guild(mentions_list, message.guild.id if message.guild else None)
        mention_file = add_mentions_to_status_embed(embed, same_server_mentions)
        if private_mentions:
            dm_sent = await send_private_cross_server_mentions(message.author, private_mentions, "AFK")
            embed.add_field(
                name="Private mentions",
                value=(
                    f"{economy_q_bell} Sent **{len(private_mentions):,}** mention(s) from other servers privately."
                    if dm_sent else
                    f"{economy_q_bell} **{len(private_mentions):,}** mention(s) from other servers were kept out of this channel, but your DMs are closed."
                ),
                inline=False,
            )

        await message.channel.send(embed=embed, file=mention_file, allowed_mentions=discord.AllowedMentions.none())

AI_SAFE_COMMANDS = {
    "help", "commands", "cmds", "games", "howtoplay", "how", "rules",
    "bal", "balance", "cash", "bank", "safe", "vaultcash", "deposit", "withdraw", "tutorial", "tutorialmode", "tips", "recommendgame", "recgame", "whatgame", "suggestgame", "career", "job", "profession", "workprofile", "jobs", "careers", "careerpaths", "joblist", "apply", "work", "shift", "worktask", "clockin", "profile", "level", "lvl", "inventory", "inv",
    "shop", "claimreminders", "claimreminder", "reminders", "dmreminders", "cooldowns", "cds", "quests", "transactions", "tx", "lb",
    "leaderboard", "gamestats", "achievements", "gamebalance", "gamehistory",
    "season", "seasonpass", "monthlychallenges", "pass", "spass", "limits", "riskprofile", "risk", "userrisk", "riskcheck", "economyhealth", "ecohealth", "moneyhealth", "supply",
    "messages", "msgstats", "messagestats", "mstats",
    "activitystats", "astats", "away", "userinfo", "pfp", "avatar", "calc", "colour", "color",
    "define", "timer", "ctimer", "alarm", "find", "econhelp", "quewohelp",
    "economyhelp", "ehelp", "explain", "lottery", "lotterystats",
    "summarize", "summarise", "summary", "aisummary", "tldr", "recap",
    "aidetect", "aicheck", "detectai", "authenticity", "authcheck", "essaycheck",
    "aimemory", "aime", "memoryai", "whatyouknow", "aiknow", "aiknowledge", "knowcmd", "aicmd",
    "aidoctor", "botdoctor", "doctorai", "diagnosebot",
    "aiperms", "aipermissions", "aicapabilities", "aiauthority", "aiguard", "aicommandsafety",
    "aihistory", "aiactions", "actionhistory", "aisettings", "aiconfig", "aicontrols", "usersettings", "mysettings", "preferences", "prefs",
    "commandstats", "cmdstats", "usage", "bulkqueue", "queue", "jobqueue", "jobs", "backgroundjobs", "tasks", "errors", "errorlog", "receipt", "txreceipt", "qreceipt",
    "auditcommands", "cmdaudit", "commandaudit", "styleaudit", "uiaudit", "messageaudit", "commandcleanup", "cleanupcommands", "cmdcleanup", "dbaudit", "databaseaudit",
    "gameaudit", "gaudit", "auditgames", "event", "qevent", "events", "quote", "archive", "transcript",
    "generate", "analyse", "analyze", "analyseimage", "analyzeimage", "vision",
}

AI_CONFIRM_COMMANDS = {
    "poll", "epoll", "giveaway", "setbday", "removebday", "setbdaychannel", "rob",
    "activity", "messageevent", "msgevent", "chatevent", "messagecontest", "chatcontest", "settings", "prefix", "preifx", "setprefix",
    "recover", "aichannel", "aitoggle", "quewochannel", "qchannel", "econchannel", "economychannel", "gamblingchannel", "setquewochannel",
    "levelupchannel", "levelchannel", "setlevelchannel", "lvlchannel", "xplevelchannel",
    "todchannel", "todchannels", "truthdarechannel", "truthordarechannel", "settdchannel",
}

AI_SUPEROWNER_ONLY_COMMANDS = {
    "add", "remove", "move", "moveqs", "movequesos", "addtick", "removetick", "remtick", "deltick", "movetick", "moveticks", "ticketmove", "moveticket", "settick", "lotterypot", "lotteryprize", "prizepool", "setpot", "addpot", "removepot", "setquesos",
    "editlottery", "stoplottery", "qstats", "economystats", "qstatus", "economyhealth", "ecohealth", "moneyhealth", "supply", "balancedashboard", "ecodashboard", "moneydashboard", "sinkdashboard", "endseason", "rewardseason",
    "disable", "enable", "disableall", "enableall", "prefix", "preifx", "setprefix",
    "off", "on", "maintenance", "botoff", "boton", "updatebot", "onlinebot", "finishupdate",
    "settings", "setup", "setlogs", "slashsync", "auditcommands", "cmdaudit", "commandaudit", "styleaudit", "uiaudit", "messageaudit", "commandcleanup", "cleanupcommands", "cmdcleanup", "dbaudit", "databaseaudit", "backgroundjobs", "tasks", "bgjobs", "errors", "errorlog", "recover", "restorestate", "recovery", "aihistory", "aiactions", "actionhistory",
    "aisettings", "aiconfig", "aicontrols", "aiperms", "aipermissions", "aicapabilities", "aiauthority", "aiguard", "aicommandsafety", "aiignore", "ignoreai", "aiunignore", "unignoreai", "aistyle", "aipersonality",
    "block", "unblock", "shut", "unshut", "rshut", "unrshut", "lockdown", "reopen",
    "rlockdown", "runlock", "lock", "unlock", "clearwatchlist", "ban", "unban", "kick", "addrole",
    "removerole", "deleterole", "send", "reply", "speak", "relay", "talkthrough", "fwd", "forward", "fw", "quote", "archive", "censor", "uncensor", "clearcensors",
    "editlottery", "stoplottery", "editactivity", "endactivity", "stopactivity", "robsettings", "robbing", "setrob", "robconfig", "balancedashboard", "ecodashboard", "moneydashboard", "sinkdashboard",
}

AI_BLOCKED_COMMANDS = {
    "ask", "send", "reply", "speak", "relay", "talkthrough",
}

def extract_ai_command_request(question, guild=None):
    text = (question or "").strip()
    if not text:
        return None
    prefix = prefix_for_guild(guild)
    prefixes = sorted({prefix, DEFAULT_PREFIX}, key=len, reverse=True)
    for command_prefix in prefixes:
        if not command_prefix:
            continue
        pattern = rf"(?:^|\s){re.escape(command_prefix)}([A-Za-z][\w-]*)(?:\s+(.+))?$"
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1), (match.group(2) or "").strip(), True

    lowered = text.casefold()
    image_generate_match = re.search(
        r"\b(?:generate|create|make|draw)\s+(?:an?\s+)?(?:image|picture|pic|photo)\s*(?:of|for)?\s*(.+)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if image_generate_match:
        prompt = image_generate_match.group(1).strip()
        if prompt:
            return "generate", prompt, False

    if re.search(
        r"\b(?:analy[sz]e|describe|what'?s?\s+in|what\s+is\s+in)\s+(?:this\s+)?(?:image|picture|pic|photo)\b",
        text,
        flags=re.IGNORECASE,
    ):
        return "analyse", "", False

    if re.search(
        r"\b(?:ai\s*detect|detect\s+ai|ai[-\s]?written|written\s+by\s+ai|sounds?\s+ai|authenticity|check\s+(?:this\s+)?(?:essay|writing|text))\b",
        text,
        flags=re.IGNORECASE,
    ):
        cleaned = re.sub(
            r"\b(?:can you|please|pls|check|detect|tell me if|is this|does this|sound|sounds|ai|written|by|essay|writing|text|authenticity|like)\b",
            " ",
            text,
            flags=re.IGNORECASE,
        ).strip()
        return "aidetect", cleaned, False

    command_intent = any(word in lowered for word in (
        "run ", "use ", "do ", "start ", "set ", "show ", "open ", "check ", "make ",
        "change ", "edit ", "enable ", "disable ", "ignore ", "block ", "unblock ",
    ))
    if not command_intent:
        return None

    timer_match = re.search(r"\b(?:set|start|make)\s+(?:a\s+)?timer\s+(?:for\s+)?(.+)", text, flags=re.IGNORECASE | re.DOTALL)
    if timer_match:
        return "timer", timer_match.group(1).strip(), False

    alarm_match = re.search(r"\b(?:set|start|make)\s+(?:an?\s+)?alarm\s+(?:for\s+)?(.+)", text, flags=re.IGNORECASE | re.DOTALL)
    if alarm_match:
        return "alarm", alarm_match.group(1).strip(), False

    help_match = re.search(r"\b(?:show|open|run|use)?\s*help(?:\s+(?:for|with|on))?\s+([A-Za-z][\w-]*)", text, flags=re.IGNORECASE)
    if help_match:
        return "help", help_match.group(1).strip(), False

    simple_patterns = [
        (("message stats", "messages tracker", "messages leaderboard", "message leaderboard", "who talks the most", "chat leaderboard"), "messages", ""),
        (("summarize messages", "summarise messages", "chat summary", "summarize chat", "summarise chat", "recap chat", "catch me up"), "summarize", ""),
        (("ai detector", "ai detect", "written by ai", "ai-written", "sounds ai", "authenticity check"), "aidetect", ""),
        (("games", "game list", "what can we play", "play list"), "games", ""),
        (("shop", "store", "what can i buy"), "shop", ""),
        (("inventory", "my items", "stuff i own", "what do i own"), "inventory", ""),
        (("claim reminders", "dm reminders", "remind me to claim", "claim reminder", "turn claim reminders"), "claimreminders", ""),
        (("cooldowns", "cooldown", "what can i claim", "can i claim"), "cooldowns", ""),
        (("quests", "quest", "tasks", "missions"), "quests", ""),
        (("leaderboard", "lb", "ranking", "rankings", "richest"), "lb", ""),
        (("lottery stats", "lottery info"), "lotterystats", ""),
        (("lottery", "current draw", "current pot"), "lottery", ""),
        (("activity stats", "activity status", "active people", "activity report"), "activitystats", ""),
        (("balance", "bal", "cash", "money", "how rich", "how broke"), "bal", ""),
        (("profile", "level", "lvl", "my stats"), "profile", ""),
    ]
    for phrases, command_name, args in simple_patterns:
        if any(phrase in lowered for phrase in phrases):
            return command_name, args, False
    return None

def ai_command_needs_confirmation(command):
    if not command:
        return True
    names = {command.name.casefold(), *(alias.casefold() for alias in getattr(command, "aliases", []) or [])}
    if names & AI_BLOCKED_COMMANDS:
        return None
    if names & AI_SAFE_COMMANDS:
        return False
    if names & AI_CONFIRM_COMMANDS:
        return True
    return True

def command_search_blob(command):
    parts = [command.name, command.qualified_name, " ".join(getattr(command, "aliases", []) or [])]
    parts.append(economy_explanations.get(command.name, ""))
    parts.append(economy_detailed_explanations.get(command.name, ""))
    parts.append(getattr(command, "help", "") or "")
    return " ".join(parts).casefold()

def fuzzy_command_guess(text, viewer=None, guild=None):
    lowered = str(text or "").casefold()
    candidates = []
    for command in bot.commands:
        if not command_is_visible_to(command, viewer, guild):
            continue
        names = {command.name.casefold(), *(alias.casefold() for alias in getattr(command, "aliases", []) or [])}
        score = 0
        for name in names:
            if re.search(rf"\b{re.escape(name)}\b", lowered):
                score += 10
        blob = command_search_blob(command)
        for token in set(re.findall(r"[a-z0-9]{3,}", lowered)):
            if token in blob:
                score += 1
        if score:
            candidates.append((score, command))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1].name

async def semantic_ai_command_request(question, guild, viewer=None):
    if not GROQ_API_KEY:
        guessed = fuzzy_command_guess(question, viewer, guild)
        return (guessed, "", False) if guessed else None
    prefix = prefix_for_guild(guild)
    command_lines = []
    for command in sorted(bot.commands, key=lambda cmd: cmd.qualified_name.casefold()):
        if not command_is_visible_to(command, viewer, guild):
            continue
        command_lines.append(command_ai_summary(command, prefix))
    prompt = {
        "task": "Map a casual Discord user request to exactly one ProQue command if appropriate.",
        "rules": [
            "Infer meaning from casual wording. Do not require exact command words.",
            "Return null if the user is asking a normal non-bot question.",
            "If the request is ambiguous between multiple commands, set needs_clarification true and ask a short question.",
            "For missing optional arguments, leave args empty; the bot can open its UI.",
            "Never choose ask/send/reply for AI self-recursion. Generate and analyse are allowed for direct image requests.",
            "Return JSON only with keys: command, args, needs_clarification, question."
        ],
        "available_commands": "\n".join(command_lines)[:6000],
        "request": question,
    }
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "messages": fit_ai_messages([
            {"role": "system", "content": "You are a command intent parser. Return valid JSON only."},
            {"role": "user", "content": json.dumps(prompt)},
        ], max_chars=5200),
        "model": "llama-3.1-8b-instant",
        "temperature": 0.0,
        "max_tokens": 220,
    }
    try:
        session = await get_http_session()
        async with session.post("https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers, timeout=AI_MODEL_TIMEOUT_SECONDS) as resp:
            if resp.status != 200:
                guessed = fuzzy_command_guess(question, viewer, guild)
                return (guessed, "", False) if guessed else None
            data = await resp.json(content_type=None)
        raw = data["choices"][0]["message"]["content"].strip()
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        parsed = json.loads(raw)
    except Exception:
        guessed = fuzzy_command_guess(question, viewer, guild)
        return (guessed, "", False) if guessed else None
    if parsed.get("needs_clarification"):
        return {"clarify": str(parsed.get("question") or "Which command do you want me to use?")}
    command_name = str(parsed.get("command") or "").strip()
    if not command_name or command_name.casefold() in {"null", "none"}:
        return None
    command = get_command_case_insensitive(command_name)
    if not command:
        return None
    return command.name, str(parsed.get("args") or "").strip(), False

async def maybe_run_ai_command(message, question):
    request = extract_ai_command_request(question, message.guild)
    if not request:
        request = await semantic_ai_command_request(question, message.guild, message.author)
    if not request:
        return False
    if isinstance(request, dict) and request.get("clarify"):
        await message.reply(
            f"{economy_q_thinking} {request['clarify']}",
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return True
    command_name, args, explicit = request
    command = get_command_case_insensitive(command_name)
    if not command:
        return False
    if not command_is_visible_to(command, message.author, message.guild):
        return False
    command_keys = {command.name.casefold(), *(alias.casefold() for alias in getattr(command, "aliases", []) or [])}
    if command_keys & AI_SUPEROWNER_ONLY_COMMANDS and not has_super_owner_power(message.author, message.guild):
        await message.reply(
            denial_message("This bot control is not available here."),
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return True

    confirmation = ai_command_needs_confirmation(command)
    if confirmation is None:
        await message.reply(
            f"I can help explain `{command.name}`, but I won't run that one through AI.",
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return True
    confirmation = True

    prefix = prefix_for_guild(message.guild)
    display = f"{prefix}{command.name}" + (f" {args}" if args else "")
    if confirmation:
        view = ConfirmActionView(message.author.id, f"Run {display}")
        prompt = await message.reply(
            embed=command_plan_embed(message, command, args, display),
            view=view,
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        await view.wait()
        if view.value is None:
            for item in view.children:
                item.disabled = True
            try:
                await prompt.edit(content="Command confirmation timed out.", embed=None, view=view)
            except discord.HTTPException:
                pass
            return True
        if not view.value:
            return True

    record_ai_action(message, f"run {command.name}", args, True)
    await invoke_prefix_command_from_message(message, command.name, args)
    return True

def parse_ai_batch_limit(text, default=5):
    match = re.search(r"\b(?:top|first|best|leading)\s+(\d{1,2})\b", text, flags=re.IGNORECASE)
    if not match:
        return default
    return max(1, min(25, int(match.group(1))))

def ai_batch_pending_key(message):
    guild_id = message.guild.id if message.guild else 0
    return (int(guild_id), int(message.channel.id), int(message.author.id))

def current_ai_batch_draft(message):
    key = ai_batch_pending_key(message)
    draft = pending_ai_batch_actions.get(key)
    if not draft:
        return None
    if draft.get("expires_at", 0) <= time.time():
        pending_ai_batch_actions.pop(key, None)
        return None
    return draft

def save_ai_batch_draft(message, draft):
    draft["expires_at"] = time.time() + 5 * 60
    pending_ai_batch_actions[ai_batch_pending_key(message)] = draft

def clear_ai_batch_draft(message):
    pending_ai_batch_actions.pop(ai_batch_pending_key(message), None)

def record_ai_action(message, action, detail="", success=True):
    guild = message.guild
    ai_action_history.appendleft({
        "time": datetime.now(timezone.utc),
        "guild_id": guild.id if guild else 0,
        "guild_name": guild.name if guild else "DM",
        "channel_id": message.channel.id,
        "user_id": message.author.id,
        "action": str(action or "unknown")[:80],
        "detail": str(detail or "")[:500],
        "success": bool(success),
    })

async def ai_settings_for(scope, scope_id):
    rows = await asyncio.to_thread(get_ai_control_settings, scope, scope_id)
    return {str(row[2]): str(row[3]) for row in rows}

def maintenance_settings_from_rows(rows):
    settings = {str(row[2]): str(row[3]) for row in rows}
    enabled = settings.get("maintenance_mode", "off").casefold() == "on"
    return enabled, settings.get("maintenance_reason", "")

async def load_maintenance_mode():
    global bot_maintenance_mode, bot_maintenance_reason
    rows = await asyncio.to_thread(get_ai_control_settings, "global", 0)
    bot_maintenance_mode, bot_maintenance_reason = maintenance_settings_from_rows(rows)
    return bot_maintenance_mode

def maintenance_notice_allowed(channel_id, ttl=60):
    now = time.time()
    last = maintenance_notice_times.get(int(channel_id), 0)
    if now - last < ttl:
        return False
    maintenance_notice_times[int(channel_id)] = now
    return True

async def set_guild_bot_nick(guild, nick):
    member = guild.me or guild.get_member(bot.user.id)
    if member is None:
        return False
    try:
        await member.edit(nick=nick, reason="ProQue maintenance mode")
        return True
    except (discord.Forbidden, discord.HTTPException):
        return False

async def set_maintenance_nick(enabled):
    settings = await ai_settings_for("global", 0)
    previous = {}
    if enabled:
        if settings.get("maintenance_previous_nicks"):
            try:
                previous = json.loads(settings.get("maintenance_previous_nicks") or "{}")
            except Exception:
                previous = {}
        if not previous:
            previous = {
                str(guild.id): (guild.me.nick if guild.me else None)
                for guild in bot.guilds
            }
            await asyncio.to_thread(
                set_ai_control_setting,
                "global",
                0,
                "maintenance_previous_nicks",
                json.dumps(previous),
                super_owner_id,
            )
        target_nick = "Pro𝚀𝚞𝚎 (updating)"
        results = await asyncio.gather(*(set_guild_bot_nick(guild, target_nick) for guild in bot.guilds), return_exceptions=True)
    else:
        try:
            previous = json.loads(settings.get("maintenance_previous_nicks") or "{}")
        except Exception:
            previous = {}
        results = await asyncio.gather(
            *(set_guild_bot_nick(guild, previous.get(str(guild.id))) for guild in bot.guilds),
            return_exceptions=True,
        )
        await asyncio.to_thread(delete_ai_control_setting, "global", 0, "maintenance_previous_nicks")
    return sum(1 for result in results if result is True)

async def set_maintenance_mode(enabled, *, reason="", updated_by=None):
    global bot_maintenance_mode, bot_maintenance_reason
    stored = "on" if enabled else "off"
    ok_mode = await asyncio.to_thread(set_ai_control_setting, "global", 0, "maintenance_mode", stored, updated_by or super_owner_id)
    ok_reason = await asyncio.to_thread(set_ai_control_setting, "global", 0, "maintenance_reason", str(reason or "")[:300], updated_by or super_owner_id)
    if not (ok_mode and ok_reason):
        return False, 0
    bot_maintenance_mode = bool(enabled)
    bot_maintenance_reason = str(reason or "")[:300]
    changed_nicks = await set_maintenance_nick(enabled)
    try:
        await bot.change_presence(activity=discord.Game("updating") if enabled else None)
    except Exception:
        pass
    return True, changed_nicks

def user_setting_value(settings, key):
    defaults = {"aifriendly": "on"}
    return settings.get(key, defaults.get(key, "off"))

def next_background_job_id():
    global background_job_counter
    background_job_counter += 1
    return f"JOB-{int(time.time())}-{background_job_counter}"

def public_job_snapshot(job):
    started = job.get("started_at")
    finished = job.get("finished_at")
    started_text = discord.utils.format_dt(started, "R") if started else "unknown"
    finished_text = f" · finished {discord.utils.format_dt(finished, 'R')}" if finished else ""
    result_text = f"\n{embed_value(job.get('result') or '', 220)}" if job.get("result") else ""
    return (
        f"`{job['id']}` **{job['label']}** · {job['status']} · started {started_text}{finished_text}\n"
        f"By <@{job['user_id']}> in <#{job['channel_id']}>{result_text}"
    )

async def run_background_job(job_id, coro_factory):
    job = background_jobs[job_id]
    try:
        result = await coro_factory()
        job["status"] = "done"
        job["result"] = str(result or "Done.")[:800]
    except Exception as exc:
        job["status"] = "failed"
        job["result"] = clean_user_error(exc)
        print(f"Background job {job_id} failed: {type(exc).__name__} - {exc}")
    finally:
        job["finished_at"] = datetime.now(timezone.utc)

def schedule_background_job(label, ctx, coro_factory):
    job_id = next_background_job_id()
    background_jobs[job_id] = {
        "id": job_id,
        "label": str(label or "job")[:80],
        "status": "running",
        "result": "",
        "user_id": ctx.author.id,
        "guild_id": ctx.guild.id if ctx.guild else 0,
        "channel_id": ctx.channel.id,
        "started_at": datetime.now(timezone.utc),
        "finished_at": None,
    }
    task = asyncio.create_task(run_background_job(job_id, coro_factory))
    background_jobs[job_id]["task"] = task
    return job_id

SYNC_DB_AUDIT_PATTERNS = (
    "get_user(",
    "update_user(",
    "get_lottery_config(",
    "save_lottery_config(",
    "update_lottery_config(",
    "get_db_connection(",
)

def sync_db_audit(limit=24):
    findings = []
    for path in ("main.py", "economy.py"):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                lines = handle.readlines()
        except OSError:
            continue
        async_context = None
        for number, raw in enumerate(lines, 1):
            stripped = raw.lstrip()
            indent = len(raw) - len(stripped)
            if stripped.startswith("async def "):
                async_context = (indent, stripped.split("(", 1)[0].replace("async def", "").strip())
            elif async_context and stripped and indent <= async_context[0] and not stripped.startswith(("#", "@")):
                async_context = None
            if not async_context:
                continue
            if "to_thread" in raw or "await " in raw:
                continue
            if any(pattern in raw for pattern in SYNC_DB_AUDIT_PATTERNS):
                findings.append(f"`{path}:{number}` in `{async_context[1]}` - {stripped.strip()[:90]}")
                if len(findings) >= limit:
                    return findings
    return findings

def ui_callback_audit(limit=20):
    findings = []
    for path in ("main.py", "economy.py"):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                lines = handle.readlines()
        except OSError:
            continue
        callback_start = None
        callback_name = None
        touched_response = False
        for number, raw in enumerate(lines, 1):
            stripped = raw.lstrip()
            indent = len(raw) - len(stripped)
            if stripped.startswith("async def ") and "interaction" in stripped:
                callback_start = (number, indent)
                callback_name = stripped.split("(", 1)[0].replace("async def", "").strip()
                touched_response = False
                continue
            if callback_start:
                if stripped and indent <= callback_start[1] and not stripped.startswith(("#", "@")):
                    if not touched_response:
                        findings.append(f"`{path}:{callback_start[0]}` in `{callback_name}`")
                        if len(findings) >= limit:
                            return findings
                    callback_start = None
                    callback_name = None
                    touched_response = False
                elif "interaction.response" in raw or "interaction.followup" in raw or "interaction.edit_original_response" in raw:
                    touched_response = True
        if callback_start and not touched_response:
            findings.append(f"`{path}:{callback_start[0]}` in `{callback_name}`")
    return findings[:limit]

async def ai_is_ignored(message):
    global_settings = await ai_settings_for("global", 0)
    guild_settings = await ai_settings_for("guild", message.guild.id if message.guild else 0)
    user_settings = await ai_settings_for("user", message.author.id)
    ignored_users = {part.strip() for part in global_settings.get("ignored_users", "").split(",") if part.strip()}
    if str(message.author.id) in ignored_users or user_settings.get("ignore", "off") == "on":
        return True
    if guild_settings.get("enabled", "on") == "off":
        return True
    return False

async def ai_style_note():
    settings = await ai_settings_for("global", 0)
    style = settings.get("style", "").strip()
    if not style:
        return ""
    return f"Current style instruction from {QUE_OWNER_DISPLAY}: {style[:250]}"

async def maybe_run_ai_control_action(message, question):
    if not has_super_owner_power(message.author, message.guild):
        return False
    lowered = (question or "").casefold()
    mentioned_users = [user for user in (message.mentions or []) if not getattr(user, "bot", False)]
    if mentioned_users and any(phrase in lowered for phrase in ("ignore", "stop responding to", "don't respond to", "dont respond to", "mute ai for")):
        settings = await ai_settings_for("global", 0)
        ignored = {part.strip() for part in settings.get("ignored_users", "").split(",") if part.strip()}
        for user in mentioned_users:
            ignored.add(str(user.id))
        ok = await asyncio.to_thread(set_ai_control_setting, "global", 0, "ignored_users", ",".join(sorted(ignored)), message.author.id)
        record_ai_action(message, "ai ignore", ", ".join(str(user.id) for user in mentioned_users), bool(ok))
        await message.reply(
            f"{economy_q_accept if ok else economy_q_warning} I'll ignore {', '.join(f'<@{user.id}>' for user in mentioned_users)}.",
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return True
    if mentioned_users and any(phrase in lowered for phrase in ("unignore", "respond to", "listen to", "talk to")):
        settings = await ai_settings_for("global", 0)
        ignored = {part.strip() for part in settings.get("ignored_users", "").split(",") if part.strip()}
        for user in mentioned_users:
            ignored.discard(str(user.id))
        ok = await asyncio.to_thread(set_ai_control_setting, "global", 0, "ignored_users", ",".join(sorted(ignored)), message.author.id)
        record_ai_action(message, "ai unignore", ", ".join(str(user.id) for user in mentioned_users), bool(ok))
        await message.reply(
            f"{economy_q_accept if ok else economy_q_warning} I can respond to {', '.join(f'<@{user.id}>' for user in mentioned_users)} again.",
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return True
    if message.guild and any(phrase in lowered for phrase in ("stop responding in this server", "disable ai here", "turn ai off here", "turn off ai here")):
        ok = await asyncio.to_thread(set_ai_control_setting, "guild", message.guild.id, "enabled", "off", message.author.id)
        record_ai_action(message, "ai server off", str(message.guild.id), bool(ok))
        await message.reply(f"{economy_q_accept if ok else economy_q_warning} AI replies are off in this server now.", mention_author=False)
        return True
    if message.guild and any(phrase in lowered for phrase in ("respond in this server", "enable ai here", "turn ai on here", "turn on ai here")):
        ok = await asyncio.to_thread(set_ai_control_setting, "guild", message.guild.id, "enabled", "on", message.author.id)
        record_ai_action(message, "ai server on", str(message.guild.id), bool(ok))
        await message.reply(f"{economy_q_accept if ok else economy_q_warning} AI replies are on in this server now.", mention_author=False)
        return True
    style_match = re.search(r"\b(?:make|set|change)\s+(?:your\s+)?(?:ai\s+)?(?:style|personality|tone)\s+(?:to\s+)?(.+)", question or "", flags=re.IGNORECASE | re.DOTALL)
    if style_match:
        style = style_match.group(1).strip()[:200]
        if style:
            ok = await asyncio.to_thread(set_ai_control_setting, "global", 0, "style", style, message.author.id)
            record_ai_action(message, "ai style", style, bool(ok))
            await message.reply(f"{economy_q_accept if ok else economy_q_warning} Style updated: **{style}**", mention_author=False)
            return True
    return False

def new_receipt_id():
    return f"QTX-{int(time.time())}-{random.randint(1000, 9999)}"

async def save_sensitive_receipt(ctx, action, target_ids=None, amount=None, details=None):
    receipt_id = new_receipt_id()
    ok = await asyncio.to_thread(
        save_bot_receipt,
        receipt_id,
        ctx.guild.id if ctx.guild else 0,
        ctx.channel.id if ctx.channel else None,
        ctx.author.id,
        target_ids or [],
        action,
        amount,
        details,
    )
    return receipt_id if ok else None

def normalize_receipt_row(row):
    if not row:
        return None
    if isinstance(row, dict):
        return row
    rid, guild_id, channel_id, actor_id, target_ids, action, amount, details, created_at = row
    return {
        "receipt_id": rid,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "actor_id": actor_id,
        "target_ids": target_ids,
        "action": action,
        "amount": amount,
        "details": details,
        "created_at": created_at,
    }

def receipt_time_text(created_at):
    if not created_at:
        return "unknown"
    if getattr(created_at, "tzinfo", None) is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    return f"{discord.utils.format_dt(created_at, 'R')} · {discord.utils.format_dt(created_at, 'f')}"

def receipt_summary_line(row):
    row = normalize_receipt_row(row)
    rid = row.get("receipt_id")
    action = str(row.get("action") or "unknown").replace("_", " ")
    actor_id = row.get("actor_id")
    amount = row.get("amount")
    target_ids = row.get("target_ids") or []
    created_at = row.get("created_at")
    amount_text = economy_format_balance(amount) if amount is not None else "no amount"
    target_text = ", ".join(f"<@{uid}>" for uid in target_ids[:3]) or "none"
    if len(target_ids) > 3:
        target_text += f" +{len(target_ids) - 3:,}"
    ts = discord.utils.format_dt(created_at.replace(tzinfo=timezone.utc), "R") if created_at else "unknown"
    return f"`{rid}` · **{action}** · {amount_text} · <@{actor_id}> → {target_text} · {ts}"

def parse_ai_batch_duration(text):
    lowered = text.casefold()
    patterns = [
        (r"\b(\d{1,2})\s*days?\b", "days"),
        (r"\b(\d{1,2})\s*weeks?\b", "weeks"),
        (r"\b(\d{1,2})\s*months?\b", "months"),
    ]
    for pattern, unit in patterns:
        match = re.search(pattern, lowered)
        if not match:
            continue
        value = max(1, int(match.group(1)))
        if unit == "days":
            return timedelta(days=value)
        if unit == "weeks":
            return timedelta(weeks=value)
        return timedelta(days=value * 30)
    if "week" in lowered:
        return timedelta(weeks=1)
    if "month" in lowered:
        return timedelta(days=30)
    if "year" in lowered:
        return timedelta(days=365)
    return timedelta(days=1)

def parse_ai_batch_reward(text, guild):
    reward_verbs = r"(?:give|add|reward|pay|send|grant|award|bless|drop|throw|hand|tip)"
    money_match = re.search(
        rf"{reward_verbs}\s+(?:them\s+|everyone\s+|each\s+|the winners\s+|the users\s+|the people\s+)?(\d+(?:\.\d+)?\s*(?:k|m|b|bn)?)\s*(?:q|qoins|coins|quesos|cash|money)?\b",
        text,
        flags=re.IGNORECASE,
    )
    ticket_match = re.search(
        rf"{reward_verbs}\s+(?:them\s+|everyone\s+|each\s+|the winners\s+|the users\s+|the people\s+)?(\d+(?:\.\d+)?\s*(?:k|m|b|bn)?)\s*(?:free\s+)?tickets?\b|(\d+(?:\.\d+)?\s*(?:k|m|b|bn)?)\s*(?:free\s+)?tickets?",
        text,
        flags=re.IGNORECASE,
    )
    if ticket_match:
        raw = (ticket_match.group(1) or ticket_match.group(2) or "").replace(" ", "")
        amount = economy_parse_amount(raw, super_owner_id, guild, None)
        return "tickets", amount
    if money_match:
        raw = money_match.group(1).replace(" ", "")
        amount = economy_parse_amount(raw, super_owner_id, guild, None)
        return "money", amount
    if not re.search(r"\btickets?\b", text, flags=re.IGNORECASE):
        amount_candidates = re.findall(r"\b\d+(?:\.\d+)?\s*(?:k|m|b|bn)\b|\b\d{4,}\b", text, flags=re.IGNORECASE)
        if amount_candidates:
            amount = economy_parse_amount(amount_candidates[-1].replace(" ", ""), super_owner_id, guild, None)
            return "money", amount
    return None, None

def ai_batch_source_hint(text):
    lowered = text.casefold()
    if any(word in lowered for word in ("activity", "active", "winners", "winner", "most active")):
        return "activity"
    if any(phrase in lowered for phrase in ("message", "messages", "chatters", "talkers", "talking", "chat leaderboard", "most chat", "most messages")):
        return "messages"
    if "lottery" in lowered and any(word in lowered for word in ("holder", "holders", "ticket", "tickets", "entries")):
        return "lottery"
    if any(word in lowered for word in ("leaderboard", "leaders", "ranking", "rankings", "richest", "balance", "quesos", "level", "xp", "earned", "earnings", "won", "wins", "lost", "losses", "net")):
        return "leaderboard"
    return None

def ai_batch_missing_prompt(missing):
    examples = {
        "source": "which users/source, like `top 5 activity winners`, `top 10 messages this week`, `top 5 lottery holders`, or `top 5 level leaderboard`",
        "reward": "what reward, like `10 tickets each` or `1m each`",
    }
    parts = [examples[name] for name in missing if name in examples]
    if not parts:
        return "I need a bit more detail before I touch balances or tickets."
    return f"{economy_q_thinking} I need {', and '.join(parts)}."

def looks_like_ai_summary_request(text):
    lowered = str(text or "").casefold()
    if not lowered:
        return False
    summary_words = (
        "summarize", "summarise", "summary", "recap", "tldr", "tl;dr",
        "catch me up", "what did", "what was said", "what happened",
    )
    chat_words = (
        "say", "said", "talk", "talking", "messages", "chat", "conversation",
        "thread", "this channel", "today", "yesterday", "last",
    )
    return any(word in lowered for word in summary_words) and any(word in lowered for word in chat_words)

def parse_summary_duration(text):
    lowered = str(text or "").casefold()
    compact_matches = re.findall(r"\b\d{1,3}\s*[wdhms]\b", lowered)
    if compact_matches:
        try:
            compact = parse_poll_duration(" ".join(compact_matches))
            if compact:
                return min(compact, timedelta(days=14))
        except Exception:
            pass
    if "today" in lowered:
        now = datetime.now(timezone.utc)
        return now - now.replace(hour=0, minute=0, second=0, microsecond=0)
    if "yesterday" in lowered:
        return timedelta(days=2)
    patterns = [
        (r"\blast\s+(\d{1,3})\s*messages?\b", "messages"),
        (r"\b(\d{1,3})\s*messages?\b", "messages"),
        (r"\blast\s+(\d{1,3})\s*hours?\b", "hours"),
        (r"\b(\d{1,3})\s*hours?\b", "hours"),
        (r"\blast\s+(\d{1,3})\s*days?\b", "days"),
        (r"\b(\d{1,3})\s*days?\b", "days"),
    ]
    for pattern, unit in patterns:
        match = re.search(pattern, lowered)
        if not match:
            continue
        value = max(1, int(match.group(1)))
        if unit == "hours":
            return timedelta(hours=min(value, 72))
        if unit == "days":
            return timedelta(days=min(value, 14))
    if "week" in lowered:
        return timedelta(days=7)
    return None

def parse_summary_limit(text):
    lowered = str(text or "").casefold()
    match = re.search(r"\b(?:last\s+)?(\d{1,3})\s*messages?\b", lowered)
    if match:
        return max(1, min(int(match.group(1)), AI_SUMMARY_MAX_MESSAGES))
    return AI_SUMMARY_DEFAULT_MESSAGES

async def resolve_summary_target(ctx_or_message, raw_text):
    guild = getattr(ctx_or_message, "guild", None)
    channel = getattr(ctx_or_message, "channel", None)
    user = None
    text = str(raw_text or "")
    mentions = getattr(ctx_or_message, "mentions", None)
    if mentions is None and hasattr(ctx_or_message, "message"):
        mentions = getattr(ctx_or_message.message, "mentions", None)
    channel_mentions = getattr(ctx_or_message, "channel_mentions", None)
    if channel_mentions is None and hasattr(ctx_or_message, "message"):
        channel_mentions = getattr(ctx_or_message.message, "channel_mentions", None)
    mentioned_users = [u for u in (mentions or []) if not getattr(u, "bot", False)]
    if mentioned_users:
        user = mentioned_users[0]
    if channel_mentions:
        channel = channel_mentions[0]
    if guild:
        for token in re.findall(r"<#\d+>|\b\d{15,22}\b", text):
            try:
                channel_candidate = await commands.TextChannelConverter().convert(ctx_or_message if hasattr(ctx_or_message, "bot") else await bot.get_context(ctx_or_message), token)
                channel = channel_candidate
                break
            except Exception:
                pass
        if user is None:
            for token in re.findall(r"<@!?\d+>|\b\d{15,22}\b", text):
                try:
                    context = ctx_or_message if hasattr(ctx_or_message, "bot") else await bot.get_context(ctx_or_message)
                    user = await commands.MemberConverter().convert(context, token)
                    break
                except Exception:
                    pass
    return user, channel

async def collect_summary_messages(channel, *, target_user=None, limit=AI_SUMMARY_DEFAULT_MESSAGES, duration=None, requester_id=None):
    limit = max(1, min(int(limit or AI_SUMMARY_DEFAULT_MESSAGES), AI_SUMMARY_MAX_MESSAGES))
    after = None
    if duration:
        after = datetime.now(timezone.utc) - duration
    fetched = []
    history_limit = min(max(limit * 4, limit), 500)
    async for msg in channel.history(limit=history_limit, after=after, oldest_first=False):
        if msg.author.bot:
            continue
        if target_user and msg.author.id != target_user.id:
            continue
        content = (msg.content or "").strip()
        if not content and msg.attachments:
            content = " ".join(f"[attachment: {a.filename}]" for a in msg.attachments[:3])
        if not content:
            continue
        fetched.append(msg)
        if len(fetched) >= limit:
            break
    return list(reversed(fetched))

def format_summary_source(messages):
    lines = []
    for msg in messages:
        created = msg.created_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        author = getattr(msg.author, "display_name", msg.author.name)
        content = re.sub(r"\s+", " ", (msg.content or "").strip())
        if not content and msg.attachments:
            content = " ".join(f"[attachment: {a.filename}]" for a in msg.attachments[:3])
        lines.append(f"[{created}] {author} ({msg.author.id}): {content[:700]}")
    text = "\n".join(lines)
    return text[-AI_SUMMARY_MAX_CHARS:]

async def summarize_messages_with_ai(messages, *, prompt, target_user=None, channel=None):
    source = format_summary_source(messages)
    if not source:
        return "I could not find messages to summarize."
    if not GROQ_API_KEY:
        people = Counter(msg.author.id for msg in messages)
        top = ", ".join(f"<@{uid}> ({count})" for uid, count in people.most_common(5))
        first = messages[0].created_at
        last = messages[-1].created_at
        return (
            f"Found **{len(messages)}** message(s)"
            f"{f' from <@{target_user.id}>' if target_user else ''}.\n"
            f"Range: {discord.utils.format_dt(first, 'R')} to {discord.utils.format_dt(last, 'R')}.\n"
            f"Top speakers: {top or 'none'}.\n"
            "AI summaries need `GROQ_API_KEY` configured."
        )
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    system = (
        "You summarize Discord chat. Be accurate, concise, and neutral. "
        "Do not invent context. Mention key points, decisions, questions, and tone. "
        "If one user is targeted, summarize only that user's messages. Do not ping users."
    )
    user_prompt = (
        f"Request: {prompt or 'Summarize this Discord chat.'}\n"
        f"Channel: #{getattr(channel, 'name', 'unknown')}\n"
        f"Target user: {getattr(target_user, 'display_name', 'all users') if target_user else 'all users'}\n\n"
        f"Messages:\n{source}"
    )
    payload = {
        "messages": fit_ai_messages([
            {"role": "system", "content": system},
            {"role": "user", "content": user_prompt},
        ], max_chars=5600),
        "model": "llama-3.1-8b-instant",
        "temperature": 0.2,
        "max_tokens": 500,
    }
    session = await get_http_session()
    async with session.post("https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers, timeout=AI_MODEL_TIMEOUT_SECONDS) as resp:
        if resp.status != 200:
            body = await resp.text()
            return ai_http_error_message(resp.status, body)
        data = await resp.json(content_type=None)
    return data["choices"][0]["message"]["content"].strip()

async def send_chat_summary(destination, *, prompt="", target_user=None, channel=None, limit=AI_SUMMARY_DEFAULT_MESSAGES, duration=None):
    channel = channel or destination.channel
    try:
        messages = await collect_summary_messages(channel, target_user=target_user, limit=limit, duration=duration)
    except discord.Forbidden:
        return await destination.reply(
            f"{economy_q_denied} I can't read message history there.",
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    except Exception as e:
        return await destination.reply(
            clean_user_error(e, "I couldn't fetch those messages."),
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    if not messages:
        return await destination.reply(
            f"{economy_q_warning} I couldn't find messages to summarize.",
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    try:
        async with channel.typing():
            summary = await summarize_messages_with_ai(messages, prompt=prompt, target_user=target_user, channel=channel)
    except Exception as e:
        summary = ai_exception_message(e)
    embed = discord.Embed(
        title=f"{economy_q_ai_history} Chat Summary",
        description=embed_value(summary, 3800),
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Source", value=f"#{getattr(channel, 'name', channel.id)}", inline=True)
    embed.add_field(name="Messages", value=f"{len(messages):,}", inline=True)
    if target_user:
        embed.add_field(name="User", value=f"<@{target_user.id}>", inline=True)
    if duration:
        if duration < timedelta(days=1):
            hours = max(1, int(duration.total_seconds() // 3600))
            range_text = f"last {hours} hour{'s' if hours != 1 else ''}"
        else:
            days = max(1, duration.days)
            range_text = f"last {days} day{'s' if days != 1 else ''}"
        embed.add_field(name="Range", value=range_text, inline=True)
    return await destination.reply(embed=embed, mention_author=False, allowed_mentions=discord.AllowedMentions.none())

async def ai_batch_targets_from_source(message, text, limit):
    lowered = text.casefold()
    source_hint = ai_batch_source_hint(text)
    if source_hint == "activity":
        await flush_activity_buffer()
        rows = await asyncio.to_thread(get_guild_activity_top, message.guild.id, limit)
        return [int(row["user_id"]) for row in rows], f"top {limit} current activity winners"

    if source_hint == "messages":
        await flush_activity_buffer()
        duration = parse_ai_batch_duration(text)
        since = message_tracker_since(duration)
        rows = await asyncio.to_thread(get_message_activity_top, message.guild.id, since, limit)
        label = next((name for name, value in MESSAGE_TRACKER_DURATIONS if value == duration), f"{duration.days} day(s)")
        return [int(row["user_id"]) for row in rows], f"top {limit} message leaderboard for {label}"

    if source_hint == "lottery":
        rows = await asyncio.to_thread(economy_lottery_ticket_rows, message.guild.id)
        rows = sorted(rows, key=lambda row: int(row.get("tickets") or 0), reverse=True)[:limit]
        return [int(row["user_id"]) for row in rows], f"top {limit} lottery ticket holders"

    rank_type = "quesos"
    if "level" in lowered or "xp" in lowered:
        rank_type = "level"
    elif "earned" in lowered or "earnings" in lowered:
        rank_type = "earned"
    elif "won" in lowered or "wins" in lowered:
        rank_type = "won"
    elif "lost" in lowered or "losses" in lowered:
        rank_type = "lost"
    elif "net" in lowered:
        rank_type = "net"
    elif "message" in lowered:
        rank_type = "messages"

    if source_hint == "leaderboard":
        local_ids = [member.id for member in message.guild.members if not member.bot]
        ids = await asyncio.to_thread(economy_get_leaderboard_user_ids, rank_type, limit, local_ids)
        return ids, f"top {limit} local 𝚀𝚞𝚎wo {rank_type} leaderboard"

    return [], ""

def looks_like_ai_batch_reward_intent(text, draft=None):
    if draft:
        return True
    lowered = str(text or "").casefold()
    rewardish = any(word in lowered for word in (
        "give", "add", "reward", "pay", "send", "grant", "award", "bless", "drop", "throw", "hand", "tip"
    ))
    sourceish = ai_batch_source_hint(lowered) is not None or any(word in lowered for word in ("top", "winners", "winner", "holders", "people", "users"))
    amountish = bool(re.search(r"\b\d+(?:\.\d+)?\s*(?:k|m|b|bn|tickets?)?\b", lowered))
    return rewardish and (sourceish or amountish)

async def semantic_ai_batch_action(question, guild, draft=None):
    if not GROQ_API_KEY or not looks_like_ai_batch_reward_intent(question, draft):
        return None
    combined = (question or "").strip()
    if draft and draft.get("text"):
        combined = f"{draft['text']} {combined}".strip()
    prompt = {
        "task": "Extract a Discord bot batch reward request into JSON only.",
        "allowed_sources": ["activity", "messages", "lottery", "leaderboard"],
        "allowed_reward_types": ["money", "tickets"],
        "leaderboard_types": ["quesos", "level", "earned", "won", "lost", "net", "messages"],
        "rules": [
            "Use null for missing values.",
            "Infer meaning from slang and casual wording.",
            "activity means active users or activity winners.",
            "messages means chatters, talkers, message leaderboard, most messages.",
            "lottery means lottery ticket holders or entries.",
            "leaderboard means economy rankings like richest, level, earnings, wins, losses, net.",
            "Return JSON with keys: source, limit, duration, rank_type, reward_type, amount_text."
        ],
        "request": combined,
    }
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "messages": fit_ai_messages([
            {"role": "system", "content": "You extract structured intent. Return valid JSON only. No markdown."},
            {"role": "user", "content": json.dumps(prompt)},
        ], max_chars=5200),
        "model": "llama-3.1-8b-instant",
        "temperature": 0.0,
        "max_tokens": 220,
    }
    try:
        session = await get_http_session()
        async with session.post("https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers, timeout=AI_MODEL_TIMEOUT_SECONDS) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)
        raw = data["choices"][0]["message"]["content"].strip()
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        parsed = json.loads(raw)
    except Exception:
        return None

    source = str(parsed.get("source") or "").casefold()
    reward_type = str(parsed.get("reward_type") or "").casefold()
    amount_text = str(parsed.get("amount_text") or "").strip()
    limit = parsed.get("limit") or parse_ai_batch_limit(combined, 5)
    try:
        limit = max(1, min(25, int(limit)))
    except (TypeError, ValueError):
        limit = 5
    rank_type = str(parsed.get("rank_type") or "").casefold()
    duration = str(parsed.get("duration") or "").strip()

    text_parts = [f"top {limit}"]
    if source in {"activity", "messages", "lottery", "leaderboard"}:
        text_parts.append(source)
    if duration:
        text_parts.append(duration)
    if rank_type and rank_type != "null":
        text_parts.append(rank_type)
    if amount_text and amount_text.casefold() != "null":
        text_parts.append(amount_text)
    if reward_type in {"money", "tickets"}:
        text_parts.append(reward_type)
    canonical_text = " ".join(text_parts)

    missing = []
    if source not in {"activity", "messages", "lottery", "leaderboard"}:
        missing.append("source")
    amount = None
    if reward_type == "tickets":
        amount = economy_parse_amount(amount_text, super_owner_id, guild, None)
    elif reward_type == "money":
        amount = economy_parse_amount(amount_text, super_owner_id, guild, None)
    else:
        parsed_type, amount = parse_ai_batch_reward(amount_text, guild)
        reward_type = parsed_type or reward_type
    if reward_type not in {"money", "tickets"} or amount is None:
        missing.append("reward")
    if missing:
        return {"pending": True, "missing": list(dict.fromkeys(missing)), "text": canonical_text or combined}
    if amount <= 0:
        return {"error": "Reward amount has to be positive."}
    return {
        "reward_type": reward_type,
        "amount": int(amount),
        "limit": limit,
        "text": canonical_text,
    }

def extract_ai_batch_action(question, guild, draft=None):
    text = (question or "").strip()
    if draft and draft.get("text"):
        text = f"{draft['text']} {text}".strip()
    lowered = text.casefold()
    has_batch_shape = ai_batch_source_hint(text) is not None or any(word in lowered for word in ("top", "winners", "winner", "holders", "people", "users"))
    if not has_batch_shape and not draft:
        return None
    if not any(word in lowered for word in ("give", "add", "reward", "pay")) and not draft:
        return None

    missing = []
    if ai_batch_source_hint(text) is None:
        missing.append("source")
    reward_type, amount = parse_ai_batch_reward(text, guild)
    if reward_type is None or amount is None:
        missing.append("reward")
    if missing:
        return {"pending": True, "missing": missing, "text": text}
    if amount <= 0:
        return {"error": "Reward amount has to be positive."}
    return {
        "reward_type": reward_type,
        "amount": int(amount),
        "limit": parse_ai_batch_limit(text, 5),
        "text": text,
    }

async def maybe_run_ai_batch_action(message, question):
    if message.guild is None:
        return False
    draft = current_ai_batch_draft(message)
    if draft and str(question or "").strip().casefold() in {"cancel", "stop", "nevermind", "never mind", "abort"}:
        clear_ai_batch_draft(message)
        await message.reply(
            "Cancelled the pending batch action.",
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return True
    action = extract_ai_batch_action(question, message.guild, draft=draft)
    if (not action or action.get("pending")) and looks_like_ai_batch_reward_intent(question, draft):
        semantic_action = await semantic_ai_batch_action(question, message.guild, draft=draft)
        if semantic_action:
            action = semantic_action
    if not action:
        return False
    if action.get("error"):
        clear_ai_batch_draft(message)
        record_ai_action(message, "batch reward rejected", action["error"], False)
        await message.reply(action["error"], mention_author=False, allowed_mentions=discord.AllowedMentions.none())
        return True
    if action.get("pending"):
        save_ai_batch_draft(message, action)
        await message.reply(
            ai_batch_missing_prompt(action.get("missing", [])),
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return True
    if not has_super_owner_power(message.author, message.guild):
        clear_ai_batch_draft(message)
        record_ai_action(message, "batch reward denied", "not superowner", False)
        await message.reply(
            denial_message("Batch 𝚀𝚞𝚎wo edits are not available here."),
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return True

    target_ids, source_label = await ai_batch_targets_from_source(message, action["text"], action["limit"])
    target_ids = [user_id for user_id in dict.fromkeys(target_ids) if user_id != bot.user.id]
    if not target_ids:
        clear_ai_batch_draft(message)
        record_ai_action(message, "batch reward failed", "no matching users", False)
        await message.reply("I couldn't find any matching users from that source.", mention_author=False)
        return True

    mentions = [f"<@{user_id}>" for user_id in target_ids]
    amount_text = f"{action['amount']:,} tickets" if action["reward_type"] == "tickets" else economy_format_balance(action["amount"])
    embed = discord.Embed(
        title=f"{economy_q_warning} Confirm AI Batch Reward",
        description=(
            f"Source: **{source_label}**\n"
            f"Reward: **{amount_text} each**\n"
            f"Targets ({len(target_ids)}):\n{joined_embed_value(mentions, limit=1600)}"
        ),
        color=discord.Color.orange(),
    )
    view = ConfirmActionView(message.author.id, "AI batch reward")
    prompt = await message.reply(embed=embed, view=view, mention_author=False, allowed_mentions=discord.AllowedMentions.none())
    await view.wait()
    if view.value is None:
        clear_ai_batch_draft(message)
        for item in view.children:
            item.disabled = True
        try:
            await prompt.edit(content="Batch reward confirmation timed out.", embed=None, view=view)
        except discord.HTTPException:
            pass
        return True
    if not view.value:
        clear_ai_batch_draft(message)
        return True

    try:
        if action["reward_type"] == "tickets":
            config = await asyncio.to_thread(economy_get_lottery_config, message.guild.id)
            if config is None:
                clear_ai_batch_draft(message)
                await message.reply("Lottery is not set up in this server yet.", mention_author=False)
                return True
            count = await asyncio.to_thread(
                economy_bulk_adjust_lottery_tickets,
                message.guild.id,
                target_ids,
                action["amount"],
                "add",
                message.author.id,
            )
            updated = await asyncio.to_thread(economy_get_lottery_config, message.guild.id)
            economy_module.schedule_lottery_refresh(message.guild, updated)
            for user_id in target_ids:
                member = message.guild.get_member(user_id)
                if member is not None and updated:
                    await economy_module.assign_lottery_role(message.guild, user_id, updated.get("role_id"))
            result_text = f"Added **{action['amount']:,}** tickets each to **{count:,}** user(s)."
        else:
            count = await asyncio.to_thread(
                economy_bulk_add_users,
                target_ids,
                action["amount"],
                message.author.id,
                f"AI batch reward: {source_label}",
            )
            result_text = f"Added **{economy_format_balance(action['amount'])}** each to **{count:,}** user(s)."
    except Exception as e:
        clear_ai_batch_draft(message)
        record_ai_action(message, "batch reward failed", clean_user_error(e), False)
        await message.reply(f"Batch reward failed: {clean_user_error(e)}", mention_author=False)
        return True

    clear_ai_batch_draft(message)
    record_ai_action(message, "batch reward", f"{result_text} Source: {source_label}", True)
    await message.reply(
        f"{economy_q_accept} {result_text}",
        mention_author=False,
        allowed_mentions=discord.AllowedMentions.none(),
    )
    return True

async def invoke_logged_command(ctx):
    print(
        f"Command received: {ctx.command} by {ctx.author} "
        f"({ctx.author.id}) in guild {ctx.guild.id if ctx.guild else 'DM'}"
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
        slow_command_events.append({
            "name": name,
            "elapsed_ms": elapsed_ms,
            "guild_id": ctx.guild.id if ctx.guild else 0,
            "channel_id": ctx.channel.id,
            "user_id": ctx.author.id,
            "message_id": ctx.message.id,
            "created_at": datetime.now(timezone.utc),
        })
        print(f"Slow command: {name} took {elapsed_ms}ms in guild {ctx.guild.id if ctx.guild else 'DM'}")

@bot.event
async def on_message(message):
    if message.author.bot:
        if message.guild and bot.user and message.author.id != bot.user.id:
            asyncio.create_task(schedule_channel_sticky_panels(message))
        return

    if bot_maintenance_mode and not has_super_owner_power(message.author, message.guild):
        if looks_like_command_message(message) and maintenance_notice_allowed(message.channel.id):
            reason = f"\nReason: {bot_maintenance_reason}" if bot_maintenance_reason else ""
            await message.channel.send(
                f"{economy_q_timer_tick} Pro𝚀𝚞𝚎 is updating right now. Try again soon.{reason}",
                allowed_mentions=discord.AllowedMentions.none(),
            )
        return

    if message.guild:
        track_message_activity(message)
        asyncio.create_task(schedule_channel_sticky_panels(message))
        asyncio.create_task(forward_speak_feed_message(message))

    await handle_returning_status(message)
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

        track_message_activity(message)
        ctx = await bot.get_context(message)
        active_speak_dm = (
            message.guild is None
            and has_super_owner_power(message.author)
            and speak_session_channel(message.author.id) is not None
        )
        if active_speak_dm and not (ctx.valid and ctx.command.name == "speak"):
            await send_speak_command_choice(message, ctx)
            return
        if ctx.valid:
            await invoke_logged_command(ctx)
        return

    if message.guild is None and has_super_owner_power(message.author) and await handle_speak_dm_message(message):
        return

    # AI mention/reply handling
    content = message.content.strip()
    is_mention = message.mentions and any(u.id == bot.user.id for u in message.mentions)
    mention_patterns = [f"<@{bot.user.id}>", f"<@!{bot.user.id}>", f"<@{bot.user.id}"]

    referenced_message = None
    if message.reference:
        resolved = getattr(message.reference, "resolved", None)
        if isinstance(resolved, discord.Message):
            referenced_message = resolved
        else:
            try:
                referenced_message = await message.channel.fetch_message(message.reference.message_id)
            except Exception:
                referenced_message = None
    is_reply_to_bot = bool(referenced_message and referenced_message.author.id == bot.user.id)

    if (is_reply_to_bot or is_mention) and GROQ_API_KEY:
        question = content
        for p in mention_patterns:
            question = question.replace(p, "")
        question = question.strip()

        if question or referenced_message:
            if await ai_is_ignored(message):
                track_message_activity(message)
                return
            context_text = ""
            if referenced_message:
                ref_content = referenced_message.content or ""
                if referenced_message.embeds:
                    embed_bits = []
                    for embed in referenced_message.embeds[:2]:
                        if embed.title:
                            embed_bits.append(f"Title: {embed.title}")
                        if embed.description:
                            embed_bits.append(f"Description: {embed.description}")
                    if embed_bits:
                        ref_content = (ref_content + "\n" + "\n".join(embed_bits)).strip()
                if referenced_message.attachments:
                    attachment_names = ", ".join(a.filename for a in referenced_message.attachments[:5])
                    ref_content = (ref_content + f"\nAttachments: {attachment_names}").strip()
                if ref_content:
                    author_name = getattr(referenced_message.author, "display_name", referenced_message.author.name)
                    context_text = f"Author: {author_name}\nMessage: {ref_content}"[:1200]
            if not question and referenced_message:
                question = "Respond to the replied message using the reply context."

            if question and message.guild and is_ai_forget_memory_request(question):
                await message.reply(
                    "AI memory stays on now so I can keep context and bot help consistent.",
                    mention_author=False,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                track_message_activity(message)
                return

            if question and message.guild and is_ai_memory_query(question):
                target = message.author
                mentioned_users = [user for user in (message.mentions or []) if not getattr(user, "bot", False)]
                if mentioned_users:
                    if has_super_owner_power(message.author, message.guild):
                        target = mentioned_users[0]
                    else:
                        await message.reply(
                            denial_message("You can only inspect your own AI memory."),
                            mention_author=False,
                            allowed_mentions=discord.AllowedMentions.none(),
                        )
                        track_message_activity(message)
                        return
                await send_ai_memory_summary(message, message.guild, target)
                track_message_activity(message)
                return

            if question and should_try_ai_command_planner(question):
                if await maybe_run_ai_control_action(message, question):
                    track_message_activity(message)
                    return

                if await maybe_run_ai_batch_action(message, question):
                    track_message_activity(message)
                    return

                if looks_like_ai_summary_request(question):
                    target_user, summary_channel = await resolve_summary_target(message, question)
                    if target_user is None and referenced_message and not referenced_message.author.bot:
                        if re.search(r"\b(they|them|their|he|him|his|she|her|that user|this user|the person)\b", question, flags=re.IGNORECASE):
                            target_user = referenced_message.author
                    duration = parse_summary_duration(question)
                    limit = parse_summary_limit(question)
                    await send_chat_summary(
                        message,
                        prompt=question,
                        target_user=target_user,
                        channel=summary_channel,
                        limit=limit,
                        duration=duration,
                    )
                    track_message_activity(message)
                    return

                if await maybe_run_ai_command(message, question):
                    track_message_activity(message)
                    return

            remember_user_facts_from_message(message)
            memory_key = ai_memory_key(message)
            recent_context = ai_channel_context_text(memory_key)
            user_memory = await ai_user_memory_text(message, referenced_message)
            use_full_bot_context = should_use_full_bot_context(question)
            if use_full_bot_context:
                messages = [
                    {
                        "role": "system",
                        "content": bot_capabilities_summary(message.guild, message.author),
                    }
                ]
            else:
                messages = [{
                    "role": "system",
                    "content": (
                        "You are Pro𝚀𝚞𝚎's AI. Chat naturally, casually, playfully, and briefly. "
                        "You can answer normal questions, but you do not have live web search connected. "
                        "If a user asks for current/live info, say you may be outdated and ask for a source or suggest checking live info. "
                        "Do not recommend bot commands during normal conversation unless the user clearly asks for bot help or how to do something with the bot."
                    ),
                }]
            style_note = await ai_style_note()
            if style_note:
                messages.append({"role": "system", "content": style_note})
            messages.append({
                "role": "system",
                "content": (
                    "Personality: casual Discord friend first, helpful bot second. Be playful, witty, warm, and a little unserious when the moment allows. "
                    "Lean into jokes, hypotheticals, callbacks, and banter instead of flattening them into factual explanations. "
                    "If someone asks a playful question, answer playfully first, then only add facts if they ask or it clearly helps. "
                    "Avoid stiff phrases like `I can provide some facts`, `as an AI`, `you are referring to`, or formal lecture openings. "
                    "Talk like a normal person in Discord: contractions, short reactions, light teasing, and casual wording are good. "
                    "Do not introduce yourself unless someone actually asks who you are. "
                    "If someone only says your name or a tiny prompt like `proque`, respond casually and briefly, like `yeah?` or `what's up?` "
                    "Do not say things like `You're referring to me` or `your friendly Discord bot`; that sounds robotic. "
                    "Do not shove command suggestions into normal chat. If the user asks about the bot, answer the question first and mention only the one or two commands that actually help. "
                    "Relate to what people say, but stay genuinely useful for facts, commands, and troubleshooting. "
                    "Keep replies short unless the user needs detail. For serious, sad, safety, moderation, medical, legal, or money questions, drop the bit and be clear/kind. "
                    "Use recent chat context when it helps. Do not ping users. If you are missing info, ask one short follow-up question."
                ),
            })
            if message.guild:
                messages.append({
                    "role": "system",
                    "content": (
                        f"Current server: {message.guild.name} ({message.guild.id}). "
                        f"Current channel: #{getattr(message.channel, 'name', message.channel.id)}. "
                        f"Current user: {getattr(message.author, 'display_name', message.author.name)} ({message.author.id})."
                    ),
                })
                if use_full_bot_context:
                    messages.append({"role": "system", "content": await bot_doctor_context_async(message.guild)})
            if recent_context:
                messages.append({"role": "system", "content": f"Recent channel context:\n{recent_context}"})
            if user_memory:
                messages.append({
                    "role": "system",
                    "content": (
                        "Known user memory and saved bot profile data. Use it naturally when relevant, do not recite it randomly, "
                        "and do not mention private facts unless the user brings them up:\n"
                        f"{user_memory}"
                    ),
                })
            if context_text:
                messages.append({"role": "system", "content": f"Reply context:\n{context_text}"})
            memory_messages = ai_memory_messages(memory_key)
            if not use_full_bot_context:
                memory_messages = memory_messages[-4:]
            messages.extend(memory_messages)
            messages.append({"role": "user", "content": question})

            headers = {
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            }
            payload = {
                "messages": fit_ai_messages(messages, max_chars=AI_REQUEST_MAX_CHARS),
                "model": "llama-3.1-8b-instant",
                "temperature": 0.7,
                "max_tokens": 420
            }
            try:
                async with message.channel.typing():
                    session = await get_http_session()
                    async with session.post("https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers, timeout=AI_MODEL_TIMEOUT_SECONDS) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            answer = data["choices"][0]["message"]["content"].strip()
                            sent = await safe_ai_reply(message, answer or "I could not come up with an answer.")
                            remember_ai_message(memory_key, "user", question)
                            remember_ai_message(memory_key, "assistant", sent.content or answer)
                        else:
                            body = await resp.text()
                            await safe_ai_reply(message, ai_http_error_message(resp.status, body))
            except Exception as e:
                try:
                    await safe_ai_reply(message, ai_exception_message(e))
                except Exception:
                    pass
            track_message_activity(message)
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
                int(message.created_at.timestamp()),
                message.guild.id if message.guild else None
            ))
            asyncio.create_task(asyncio.to_thread(
                save_away_mention,
                mentioned_user.id,
                message.author.id,
                message.jump_url,
                message.created_at,
                message.guild.id if message.guild else None,
            ))

    for uid in list(sleeping_users):
        if any(user.id == uid for user in message.mentions) or (
            message.reference and message.reference.resolved and message.reference.resolved.author.id == uid
        ):
            sleep_messages = [
                f"{economy_q_sleep} Quiet mode, boss. <@{uid}> is sleeping. Let them cook dreams.",
                f"{economy_q_sleep} <@{uid}> is asleep. Whisper energy only."
            ]
            chosen_msg = random.choice(sleep_messages)
            await message.reply(chosen_msg, mention_author=False, allowed_mentions=discord.AllowedMentions.none())
            break

    for user in message.mentions:
        if user.id in afk_users:
            afk_data = afk_users[user.id]
            duration = datetime.now(timezone.utc) - afk_data["since"]
            formatted = short_status_duration(duration)
            reason = status_reason_text(afk_data.get("reason"))
            reason_text = f"\nReason: **{reason}**" if reason else ""
            await message.reply(
                f"{economy_q_sleep} <@{user.id}> is AFK for **{formatted}**. They’ll see this when they’re back.{reason_text}",
                mention_author=False,
                allowed_mentions=discord.AllowedMentions.none()
            )
            break

    if message.guild and message.author.id in guild_watchlist(message.guild) and not has_owner_power(message.author, message.guild):
        try:
            await message.delete()
        except:
            pass
        return

    if message.guild and message.author.id not in guild_blacklisted_users(message.guild):
        remember_chat_context(message)
        track_message_activity(message)
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
        if error.command_name == "maintenance":
            reason = f"\nReason: {bot_maintenance_reason}" if bot_maintenance_reason else ""
            await ctx.send(
                f"{economy_q_timer_tick} Pro𝚀𝚞𝚎 is updating right now. Try again soon.{reason}",
                allowed_mentions=discord.AllowedMentions.none(),
            )
        else:
            await ctx.send(f"{economy_q_warning} `{error.command_name}` is taking a nap here. An admin disabled it.")

    elif isinstance(error, commands.CheckFailure):
        print(f"Command check failed: {ctx.command} for {ctx.author} ({ctx.author.id}) - {type(error).__name__}: {error}")
        if getattr(ctx, "quewo_cooldown_blocked", False):
            return
        if ctx.author.id in guild_blacklisted_users(ctx.guild):
            await ctx.send(f"LMAO you're blocked you can't use ts {economy_q_reject}")
        else:
            await ctx.send(denial_message(command_denial_detail(ctx, error)), allowed_mentions=discord.AllowedMentions.none())

    elif isinstance(error, commands.MissingPermissions):
        print(f"Command missing permissions: {ctx.command} for {ctx.author} ({ctx.author.id}) - {error}")
        await ctx.send(f"{economy_q_reject} Discord said no. You’re missing the needed permission for that one.")

    elif isinstance(error, commands.CommandOnCooldown):
        await ctx.send(f"{economy_q_timer_tick} Chill for **{error.retry_after:.1f}s** before using that again.")

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
        if isinstance(error, commands.CommandInvokeError) and getattr(error, "original", None):
            error = error.original
        print(f"Unexpected error in {ctx.command}: {type(error).__name__} - {error}")
        await notify_superowner_error(ctx, error)
        if ctx.guild is None:
            text = clean_user_error(error)
            if "guild" in str(error).casefold() or "server" in str(error).casefold():
                text = "That one needs a server, not DMs. DM-safe stuff still works here: help, AI, summaries, and simple tools."
            return await ctx.send(fit_discord_content(text))
        if has_owner_power(ctx.author, ctx.guild):
            await ctx.send(fit_discord_content(clean_user_error(error)))
        else:
            await ctx.send(denial_message("Something tripped while running that. Try the help page for the right usage."), allowed_mentions=discord.AllowedMentions.none())

HELP_CATEGORIES = {
    "Start Here": ["help", "games", "econhelp", "guide", "onboard", "tutorial", "recommendgame", "explain"],
    "𝚀𝚞𝚎wo": [
        "bal", "bank", "profile", "inventory", "shop", "career", "jobs", "work",
        "daily", "weekly", "monthly", "quests", "dailychallenge", "give", "lottery", "tickets", "buytick",
    ],
    "Games": [
        "cf", "roulette", "slots", "blackjack", "scratch", "tower", "vault", "memory", "cardladder", "lockpick",
        "heist", "diceduel", "cases", "plinko", "luckynumber", "jackpotspin", "ms", "wheel", "dungeon", "rob",
    ],
    "Progress": [
        "lb", "gamestats", "achievements", "setbadge", "gamehistory", "season", "seasonpass",
        "transactions", "limits", "riskprofile", "cooldowns", "streaks", "claimreminders",
    ],
    "Social": [
        "hug", "pat", "slap", "bonk", "kiss", "bite", "poke", "wave", "cry", "kill",
        "truthordare", "flagquiz", "flagstats", "ttt", "c4", "chess", "chessmove", "resign", "q", "picker",
    ],
    "Tools": ["userinfo", "pfp", "calc", "colour", "define", "timer", "ctimer", "alarm", "poll", "epoll", "translate", "find"],
    "Snipes": ["snipe", "dsnipe", "esnipe", "rsnipe"],
    "AI": [
        "ask", "summarize", "aidetect", "generate", "profilebanner", "makeemoji",
        "eventposter", "gameart", "shoppreview", "reactionimage", "analyse", "aimemory", "aiknow", "usersettings",
    ],
    "Community": ["afk", "sleep", "wake", "away", "setbday", "bdaycard", "viewbdaycard", "removebday", "activity", "activitystats", "messages"],
    "Admin Setup": [
        "settings", "setlogs", "prefix", "setbdaychannel", "editactivity", "endactivity", "stopactivity",
        "messageevent", "todchannel", "quewochannel", "levelupchannel", "robsettings", "aichannel",
    ],
    "Moderation": [
        "rolesinfo", "roleinfo", "purge", "rpurge", "steal", "fwd", "quote", "archive",
        "giveaway", "setnick", "unmute", "kick", "ban", "unban", "addrole", "removerole", "deleterole",
        "lock", "unlock", "lockdown", "reopen", "rlockdown", "runlock", "shut", "unshut",
        "disable", "enable", "disableall", "enableall", "dclist",
    ],
    "Diagnostics": [
        "health", "perms", "sessions", "recover", "backgroundjobs", "errors", "dbaudit", "aidoctor", "perf",
        "bulkqueue", "commandstats", "receipt",
    ],
}
SUPEROWNER_HELP_COMMANDS = [
    "add", "remove", "addtick", "removetick", "settick", "lotterypot", "setquesos",
    "move", "movetick",
    "addxp", "removexp", "addlvl", "removelvl", "setlvl",
    "editlottery", "stoplottery", "qstats", "economyhealth", "balancedashboard", "endseason",
    "off", "on",
    "send", "reply", "speak", "wake", "clearwatchlist",
        "aisettings", "aiperms", "aiignore", "aiunignore", "aistyle",
    "aihistory", "auditcommands", "styleaudit", "commandcleanup", "permaudit", "receipts", "aiguard",
]
SUPEROWNER_HIDDEN_COMMANDS = {
    *SUPEROWNER_HELP_COMMANDS,
    "relay", "talkthrough",
    "maintenance", "botoff", "boton", "updatebot", "onlinebot", "finishupdate",
    "remtick", "deltick", "lotteryprize", "prizepool", "setpot", "addpot", "removepot",
    "moveqs", "movequesos", "moveticks", "ticketmove", "moveticket",
    "givexp", "remxp", "delxp", "addlevel", "removelvls", "remlevel", "removelevel", "dellvl", "dellevel", "setlevel",
    "economystats", "qstatus", "ecohealth", "moneyhealth", "supply", "rewardseason",
    "aiconfig", "aicontrols", "ignoreai", "unignoreai", "aipermissions", "aicapabilities", "aiauthority", "aipersonality",
    "aiactions", "actionhistory", "cmdaudit", "commandaudit", "uiaudit", "messageaudit", "cleanupcommands", "cmdcleanup", "ecodashboard", "moneydashboard", "sinkdashboard", "permsaudit", "permissionaudit", "sensitiveaudit", "receiptlist", "txreceipts", "aicommandsafety",
}
HELP_CATEGORY_ALIASES = {
    "quewo": "𝚀𝚞𝚎wo",
    "𝚀𝚞𝚎wo": "𝚀𝚞𝚎wo",
    "economy": "𝚀𝚞𝚎wo",
    "eco": "𝚀𝚞𝚎wo",
    "money": "𝚀𝚞𝚎wo",
    "gambling": "Games",
    "casino": "Games",
    "betting": "Games",
    "stats": "Progress",
    "leaderboards": "Progress",
    "social": "Social",
    "fun": "Social",
    "snipe": "Snipes",
    "snipes": "Snipes",
    "dsnipe": "Snipes",
    "esnipe": "Snipes",
    "rsnipe": "Snipes",
    "setup": "Admin Setup",
    "config": "Admin Setup",
    "admin": "Admin Setup",
    "mod": "Moderation",
    "moderation": "Moderation",
    "tools": "Tools",
    "doctor": "Diagnostics",
    "debug": "Diagnostics",
}
HELP_CATEGORY_DESCRIPTIONS = {
    "Start Here": "Main menus and beginner guides.",
    "𝚀𝚞𝚎wo": "Money, careers, claims, shop, profile, lottery, and transfers.",
    "Games": "Gambling, skill games, solo games, and robbing.",
    "Progress": "Leaderboards, achievements, seasons, history, limits, and reminders.",
    "Social": "Party games, PvP games, flags, chess, and picker.",
    "Tools": "Timers, polls, calculator, definitions, translation, colors, and user lookup.",
    "Snipes": "Deleted messages, edited messages, and removed reactions in one clean place.",
    "AI": "Chat, summaries, image analysis, AI detector, and AI memory.",
    "Community": "AFK, sleep, birthdays, activity, and message stats.",
    "Admin Setup": "Server configuration for admins.",
    "Moderation": "Moderation, snipes, roles, locks, and cleanup.",
    "Diagnostics": "Health, recovery, errors, performance, and audit tools.",
    "𝚀𝚞𝚎 Only": "Hidden owner controls shown only to 𝚀𝚞𝚎.",
}
ADMIN_HELP_CATEGORIES = {"Admin Setup", "Moderation", "Diagnostics"}
PREFERRED_ALIAS_OVERRIDES = {
    "help": ["commands"],
    "settings": ["setup"],
    "bal": ["balance"],
    "profile": ["level"],
    "inventory": ["inv"],
    "recommendgame": ["whatgame"],
    "truthordare": ["tod"],
    "colour": ["color"],
    "snipe": ["snipes"],
    "esnipe": ["editsnipe"],
    "rsnipe": ["reactionsnipe"],
    "messages": ["mstats"],
    "activitystats": ["astats"],
    "economyaudit": ["econaudit"],
    "econhelp": ["quewohelp"],
    "blackjack": ["bj"],
    "roulette": ["rl"],
    "slots": ["slot"],
    "memory": ["mem"],
    "luckynumber": ["ln"],
    "jackpotspin": ["jps"],
}

def visible_aliases_for(command, limit=1):
    if not command:
        return []
    aliases = list(getattr(command, "aliases", []) or [])
    preferred = PREFERRED_ALIAS_OVERRIDES.get(command.name)
    if preferred is not None:
        return [alias for alias in preferred if alias in aliases][:limit]
    clean = [
        alias for alias in aliases
        if len(alias) <= 14
        and alias.casefold() not in SUPEROWNER_HIDDEN_COMMANDS
        and not alias.casefold().endswith(("config", "status"))
    ]
    return clean[:limit]

def is_superowner_help_command(command_or_name):
    names = {str(command_or_name).casefold()}
    if hasattr(command_or_name, "name"):
        names = {command_or_name.name.casefold(), *(alias.casefold() for alias in getattr(command_or_name, "aliases", []) or [])}
    return bool(names & SUPEROWNER_HIDDEN_COMMANDS)

def can_see_superowner_help(user, guild):
    return has_super_owner_power(user, guild)

def command_is_visible_to(command, user=None, guild=None):
    if not command or getattr(command, "hidden", False):
        return False
    if is_superowner_help_command(command) and not can_see_superowner_help(user, guild):
        return False
    return True

def help_categories_for(user=None, guild=None):
    visible = {}
    can_manage = bool(user and guild and has_owner_power(user, guild))
    for category, names in HELP_CATEGORIES.items():
        if category in ADMIN_HELP_CATEGORIES and not can_manage:
            continue
        filtered = []
        for name in names:
            command = get_command_case_insensitive(name)
            if command and not command_is_visible_to(command, user, guild):
                continue
            if not command and is_superowner_help_command(name) and not can_see_superowner_help(user, guild):
                continue
            filtered.append(name)
        if filtered:
            visible[category] = filtered
    if can_see_superowner_help(user, guild):
        visible["𝚀𝚞𝚎 Only"] = SUPEROWNER_HELP_COMMANDS
    return visible

def prefix_for_guild(guild):
    guild_id = guild.id if guild else 0
    return guild_prefixes.get(guild_id, DEFAULT_PREFIX)

def _render_help_embed_uncached(guild=None, category_name=None, page=0, per_page=10, viewer=None):
    current_prefix = prefix_for_guild(guild)
    categories = help_categories_for(viewer, guild)
    if category_name:
        names = categories.get(category_name, [])
        page = max(0, int(page or 0))
        embed = standard_embed(
            f"Help: {category_name}",
            description=f"Pick what you need, no command soup. Use `{current_prefix}help <command>` for usage or `{current_prefix}explain <command>` for details.",
            color=discord.Color.blurple(),
            icon=economy_q_book,
        )
        command_lines = []
        seen_commands = set()
        for name in names:
            command = get_command_case_insensitive(name)
            if command_is_visible_to(command, viewer, guild):
                if command.name in seen_commands:
                    continue
                seen_commands.add(command.name)
                desc = command_short_description(command)
                command_lines.append(f"`{current_prefix}{command.name}` - {desc}")
        if command_lines:
            page_count = max(1, math.ceil(len(command_lines) / per_page))
            page = min(page, page_count - 1)
            start = page * per_page
            page_lines = command_lines[start:start + per_page]
            embed.add_field(
                name=f"Commands {start + 1}-{min(start + per_page, len(command_lines))}",
                value=joined_embed_value(page_lines),
                inline=False,
            )
            embed.set_footer(text=f"Page {page + 1}/{page_count}")
        else:
            embed.description += "\n\nNo commands loaded for this category."
        return embed

    fast_starts = [f"`{current_prefix}games`", f"`{current_prefix}econhelp`", f"`{current_prefix}ask`"]
    if viewer and guild and has_owner_power(viewer, guild):
        fast_starts.insert(2, f"`{current_prefix}settings`")
    embed = standard_embed(
        "Help",
        description=(
            f"Pick a category below, use `{current_prefix}help <command>`, or search with `{current_prefix}help search <word>`.\n"
            f"Fast starts: {', '.join(fast_starts)}. Tiny map, huge bot."
        ),
        color=discord.Color.blurple(),
        icon=economy_q_book,
    )
    embed.add_field(
        name="AI Chatbot",
        value=(
            "Mention or reply to Pro𝚀𝚞𝚎 for normal chat, summaries, image help, bot help, and troubleshooting. "
            "It stays conversational unless you clearly ask for a bot feature."
        ),
        inline=False,
    )
    for category, names in categories.items():
        loaded = {
            command.name
            for name in names
            if (command := get_command_case_insensitive(name)) and command_is_visible_to(command, viewer, guild)
        }
        if loaded:
            description = HELP_CATEGORY_DESCRIPTIONS.get(category, "Command category.")
            embed.add_field(name=category, value=f"{description}\n**{len(loaded)}** commands", inline=True)
    return embed

def render_help_embed(guild=None, category_name=None, page=0, per_page=10, viewer=None):
    prefix = prefix_for_guild(guild)
    is_que = can_see_superowner_help(viewer, guild) if viewer else False
    is_admin_view = bool(viewer and guild and has_owner_power(viewer, guild))
    command_count = len(bot.commands)
    key = (getattr(guild, "id", 0), prefix, bool(is_que), bool(is_admin_view), category_name or "", int(page or 0), int(per_page or 10), command_count)
    now = time.monotonic()
    cached = help_render_cache.get(key)
    if cached and now - cached[0] < HELP_RENDER_CACHE_TTL:
        return clone_embed(cached[1])
    embed = _render_help_embed_uncached(guild, category_name, page, per_page, viewer)
    help_render_cache[key] = (now, clone_embed(embed))
    if len(help_render_cache) > 128:
        for old_key in list(help_render_cache)[:32]:
            help_render_cache.pop(old_key, None)
    return embed

def help_category_page_count(category_name, per_page=10, viewer=None, guild=None):
    names = help_categories_for(viewer, guild).get(category_name, [])
    loaded = []
    seen_commands = set()
    for name in names:
        command = get_command_case_insensitive(name)
        if not command_is_visible_to(command, viewer, guild) or command.name in seen_commands:
            continue
        seen_commands.add(command.name)
        loaded.append(command.name)
    return max(1, math.ceil(len(loaded) / per_page))

def resolve_help_category(name, viewer=None, guild=None):
    if not name:
        return None
    normalized = name.strip().casefold()
    categories = help_categories_for(viewer, guild)
    for category in categories:
        if normalized == category.casefold():
            return category
    return HELP_CATEGORY_ALIASES.get(normalized)

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

def command_permission_label(command):
    if not command:
        return "Unknown"
    name = command.name
    aliases = set(getattr(command, "aliases", []) or [])
    names = {name, *aliases}
    admin_names = (
        set(HELP_CATEGORIES.get("Admin Setup", []))
        | set(HELP_CATEGORIES.get("Moderation", []))
        | set(HELP_CATEGORIES.get("Diagnostics", []))
    )
    if is_superowner_help_command(command):
        return QUE_OWNER_DISPLAY
    if name in admin_names or names & admin_names:
        return "Admin / server owner"
    if name in {"editlottery", "stoplottery", "editactivity", "endactivity", "stopactivity", "qstats", "economyaudit", "abuseaudit"}:
        return "Admin / server owner"
    return "Everyone"

def command_input_label(command):
    if not command:
        return "Unknown"
    if command.name in SETUP_UI_COMMANDS:
        return "UI available"
    if command.signature:
        return f"`{command.signature}`"
    return "No input needed"

class HelpCategorySelect(Select):
    def __init__(self, viewer, guild, selected=None):
        options = []
        for category, names in help_categories_for(viewer, guild).items():
            options.append(discord.SelectOption(
                label=category,
                value=category,
                description=HELP_CATEGORY_DESCRIPTIONS.get(category, "Command category.")[:100],
                default=category == selected,
            ))
        super().__init__(placeholder="Choose a help category", min_values=1, max_values=1, options=options[:25])

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if interaction.user.id != view.author_id:
            return await interaction.response.send_message("Use your own help menu.", ephemeral=True)
        view.category_name = self.values[0]
        view.page = 0
        view.rebuild(interaction.user, interaction.guild)
        await interaction.response.edit_message(
            embed=render_help_embed(interaction.guild, view.category_name, view.page, viewer=interaction.user),
            view=view,
        )

class HelpHomeButton(Button):
    def __init__(self):
        super().__init__(label="Home", style=discord.ButtonStyle.primary)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if interaction.user.id != view.author_id:
            return await interaction.response.send_message("Use your own help menu.", ephemeral=True)
        view.category_name = None
        view.page = 0
        view.rebuild(interaction.user, interaction.guild)
        await interaction.response.edit_message(embed=render_help_embed(interaction.guild, viewer=interaction.user), view=view)

class HelpPageButton(Button):
    def __init__(self, direction):
        self.direction = direction
        label = "Previous" if direction < 0 else "Next"
        super().__init__(label=label, style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if interaction.user.id != view.author_id:
            return await interaction.response.send_message("Use your own help menu.", ephemeral=True)
        if not view.category_name:
            return await interaction.response.send_message("Pick a category first.", ephemeral=True)
        page_count = help_category_page_count(view.category_name, viewer=interaction.user, guild=interaction.guild)
        view.page = min(max(0, view.page + self.direction), page_count - 1)
        await interaction.response.edit_message(
            embed=render_help_embed(interaction.guild, view.category_name, view.page, viewer=interaction.user),
            view=view,
        )

class HelpRefreshButton(Button):
    def __init__(self):
        super().__init__(label="Refresh", emoji=reaction_emoji(economy_q_refresh), style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if interaction.user.id != view.author_id:
            return await interaction.response.send_message("Use your own help menu.", ephemeral=True)
        clear_help_cache()
        view.rebuild(interaction.user, interaction.guild)
        await interaction.response.edit_message(
            embed=render_help_embed(interaction.guild, view.category_name, view.page, viewer=interaction.user),
            view=view,
        )

class HelpView(View):
    def __init__(self, author_id, category_name=None, guild=None):
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
        self.author_id = author_id
        self.category_name = category_name
        self.page = 0
        viewer = bot.get_user(author_id) or discord.Object(id=author_id)
        self.rebuild(viewer, guild)

    def rebuild(self, viewer, guild):
        self.clear_items()
        self.add_item(HelpCategorySelect(viewer, guild, self.category_name))
        self.add_item(HelpHomeButton())
        self.add_item(HelpPageButton(-1))
        self.add_item(HelpPageButton(1))
        self.add_item(HelpRefreshButton())

def command_search_results(query, viewer=None, guild=None):
    query = (query or "").strip().casefold()
    if not query:
        return []
    results = []
    for command in sorted(bot.walk_commands(), key=lambda cmd: cmd.qualified_name.casefold()):
        if getattr(command, "parent", None):
            continue
        if not command_is_visible_to(command, viewer, guild):
            continue
        names = [command.name, *command.aliases]
        description = command_short_description(command)
        explanation = economy_explanations.get(command.name, "")
        haystack = " ".join([*names, description, explanation]).casefold()
        if query not in haystack:
            continue
        results.append((command, description))
        if len(results) >= 15:
            break
    return results

@bot.command(name="help", aliases=["commands", "cmds"])
async def help_command(ctx, *, command_name: str = None):
    if command_name:
        parts = command_name.strip().split(maxsplit=1)
        if parts and parts[0].casefold() == "search":
            query = parts[1] if len(parts) > 1 else ""
            current_prefix = prefix_for_guild(ctx.guild)
            matches = command_search_results(query, ctx.author, ctx.guild)
            embed = standard_embed(
                "Help Search",
                description=f"Search: `{query or 'nothing'}`",
                color=discord.Color.blurple(),
                icon=economy_q_thinking,
            )
            if not matches:
                embed.description += f"\nNo matches. Try `{current_prefix}help` for categories."
            else:
                lines = [
                    f"**{current_prefix}{command.name}** — {description}"
                    for command, description in matches
                ]
                embed.add_field(name="Matches", value=joined_embed_value(lines), inline=False)
            return await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

        category = resolve_help_category(command_name, ctx.author, ctx.guild)
        if category:
            return await ctx.send(embed=render_help_embed(ctx.guild, category, viewer=ctx.author), view=HelpView(ctx.author.id, category, ctx.guild))

        command = get_command_case_insensitive(command_name)
        if command and not command_is_visible_to(command, ctx.author, ctx.guild):
            command = None
        if not command:
            current_prefix = prefix_for_guild(ctx.guild)
            return await ctx.send(f"Command not found. Try `{current_prefix}help search {command_name}`.", delete_after=30)

        current_prefix = prefix_for_guild(ctx.guild)
        usage = command_usage_text(command, current_prefix)
        description = command_short_description(command)
        embed = standard_embed(
            f"{current_prefix}{command.name}",
            description=description,
            color=discord.Color.blurple(),
            icon=economy_q_book,
        )
        embed.add_field(name="Usage", value=f"`{usage}`", inline=False)
        shown_aliases = visible_aliases_for(command, 2)
        alias_value = ", ".join(f"`{current_prefix}{alias}`" for alias in shown_aliases) if shown_aliases else "None"
        if command.aliases and len(shown_aliases) < len(command.aliases):
            alias_value += "\nMore aliases still work, but this is the clean one."
        embed.add_field(name="Alias", value=alias_value, inline=True)
        embed.add_field(name="Permission", value=command_permission_label(command), inline=True)
        embed.add_field(name="Input", value=command_input_label(command), inline=True)
        if command.name in SETUP_UI_COMMANDS:
            embed.add_field(name="UI", value="Run it with no input or press the setup button below.", inline=False)
        embed.set_footer(text=f"More detail: {current_prefix}explain {command.name}")
        return await ctx.send(embed=embed, view=command_setup_view(ctx.author.id, command.name), allowed_mentions=discord.AllowedMentions.none())

    await ctx.send(embed=render_help_embed(ctx.guild, viewer=ctx.author), view=HelpView(ctx.author.id, guild=ctx.guild))

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
    try:
        quewo_channel_id = await asyncio.to_thread(economy_get_economy_channel_id, guild.id) if guild else None
    except Exception:
        quewo_channel_id = None
    try:
        ai_config = await ai_settings_for("guild", guild.id) if guild else {}
    except Exception:
        ai_config = {}
    disabled = []
    if guild:
        for name in sorted(guild_disabled_commands(guild)):
            command = get_command_case_insensitive(name)
            if command_disable_protected(command):
                continue
            disabled.append(name)

    embed = discord.Embed(
        title=f"{economy_q_setup} Server Settings",
        description="The server control room. Green-ish means handled, warning means worth setting up.",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name=f"{economy_q_edit} Prefix", value=f"`{prefix}`", inline=True)
    embed.add_field(name=f"{economy_q_lock} Disabled Commands", value=f"{len(disabled)} disabled" if disabled else "None", inline=True)
    embed.add_field(name=f"{economy_q_archive} Logs", value=channel_status(guild, log_config.get("log_channel_id")), inline=True)
    embed.add_field(name=f"{economy_q_reaction} Reaction Logs", value=channel_status(guild, log_config.get("reaction_log_channel_id")), inline=True)
    embed.add_field(name=f"{economy_q_ai_history} AI", value=f"**{ai_config.get('enabled', 'on').upper()}**", inline=True)
    embed.add_field(name=f"{economy_q_bank} 𝚀𝚞𝚎wo Channel", value=channel_status(guild, quewo_channel_id) if quewo_channel_id else "All channels", inline=True)
    embed.add_field(name=f"{economy_q_target} Truth or Dare", value=truth_or_dare_channel_text(guild), inline=True)
    embed.add_field(name=f"{economy_q_birthday} Birthdays", value=channel_status(guild, birthday_config.get("channel_id")), inline=True)
    activity_value = channel_status(guild, activity_config.get("channel_id"))
    if activity_config.get("next_report"):
        activity_value += f"\nNext: {discord.utils.format_dt(activity_config['next_report'], 'R')}"
    embed.add_field(name=f"{economy_q_activity} Activity", value=activity_value, inline=True)
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
    embed.add_field(name=f"{economy_q_ticket} Lottery", value=lottery_value, inline=True)
    embed.add_field(
        name=f"{economy_q_perf} Ops",
        value=(
            f"`{prefix}health` · `{prefix}perf`\n"
            f"`{prefix}jobs` · `{prefix}errors`\n"
            f"`{prefix}recover` · `{prefix}dbaudit`"
        ),
        inline=True,
    )
    checklist = [
        f"{economy_q_accept if log_config.get('log_channel_id') else economy_q_warning} Normal logs",
        f"{economy_q_accept if log_config.get('reaction_log_channel_id') else economy_q_warning} Reaction logs",
        f"{economy_q_accept if birthday_config.get('channel_id') else economy_q_warning} Birthdays",
        f"{economy_q_accept if activity_config.get('channel_id') else economy_q_warning} Activity",
        f"{economy_q_accept if lottery_config else economy_q_warning} Lottery",
        f"{economy_q_accept} 𝚀𝚞𝚎wo: {channel_status(guild, quewo_channel_id) if quewo_channel_id else 'all channels'}",
        f"{economy_q_accept} Truth/Dare: {truth_or_dare_channel_text(guild)}",
    ]
    embed.add_field(name=f"{economy_q_command_check} Setup Checklist", value="\n".join(checklist), inline=False)
    embed.set_footer(text="Buttons edit the server setup directly when you have permission.")
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
        clear_help_cache()
        games_render_cache.clear()
        await interaction.response.send_message(f"Prefix changed to `{new_prefix}`. Press Refresh on the settings panel to update it.", ephemeral=True)

class SettingsView(View):
    def __init__(self, author_id):
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
        self.author_id = author_id

    async def interaction_check(self, interaction):
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Use your own settings panel.", ephemeral=True)
        return False

    @discord.ui.button(label="Refresh", emoji=economy_q_refresh, style=discord.ButtonStyle.secondary)
    async def refresh(self, interaction, button):
        await interaction.response.edit_message(embed=await build_settings_embed(interaction.guild), view=self)

    @discord.ui.button(label="Prefix", emoji=economy_q_edit, style=discord.ButtonStyle.primary)
    async def prefix_button(self, interaction, button):
        current_prefix = prefix_for_guild(interaction.guild)
        await interaction.response.send_modal(PrefixSettingsModal(self.author_id, current_prefix))

    @discord.ui.button(label="Logs", emoji=economy_q_archive, style=discord.ButtonStyle.secondary)
    async def logs_button(self, interaction, button):
        if not has_owner_power(interaction.user, interaction.guild):
            return await interaction.response.send_message(denial_message("Admin power only."), ephemeral=True)
        await interaction.response.defer()
        current = get_guild_log_config(interaction.guild.id) or {}
        reaction_id = current.get("reaction_log_channel_id") or interaction.channel.id
        await asyncio.to_thread(save_guild_log_config, interaction.guild.id, interaction.channel.id, reaction_id)
        guild_log_configs[interaction.guild.id] = {
            "log_channel_id": interaction.channel.id,
            "reaction_log_channel_id": reaction_id,
        }
        await interaction.edit_original_response(embed=await build_settings_embed(interaction.guild), view=self)

    @discord.ui.button(label="Reaction Logs Here", emoji=economy_q_reaction, style=discord.ButtonStyle.secondary)
    async def reaction_logs_button(self, interaction, button):
        if not has_owner_power(interaction.user, interaction.guild):
            return await interaction.response.send_message(denial_message("Admin power only."), ephemeral=True)
        await interaction.response.defer()
        current = get_guild_log_config(interaction.guild.id) or {}
        log_id = current.get("log_channel_id") or interaction.channel.id
        await asyncio.to_thread(save_guild_log_config, interaction.guild.id, log_id, interaction.channel.id)
        guild_log_configs[interaction.guild.id] = {
            "log_channel_id": log_id,
            "reaction_log_channel_id": interaction.channel.id,
        }
        await interaction.edit_original_response(embed=await build_settings_embed(interaction.guild), view=self)

    @discord.ui.button(label="AI On/Off", emoji=economy_q_ai_history, style=discord.ButtonStyle.secondary)
    async def ai_toggle_button(self, interaction, button):
        if not has_owner_power(interaction.user, interaction.guild):
            return await interaction.response.send_message("Admin power only.", ephemeral=True)
        await interaction.response.defer()
        settings = await ai_settings_for("guild", interaction.guild.id)
        current = settings.get("enabled", "on")
        new_value = "off" if current != "off" else "on"
        ok = await asyncio.to_thread(set_ai_control_setting, "guild", interaction.guild.id, "enabled", new_value, interaction.user.id)
        if not ok:
            return await interaction.followup.send("AI setting save failed.", ephemeral=True)
        await interaction.edit_original_response(embed=await build_settings_embed(interaction.guild), view=self)

    @discord.ui.button(label="𝚀𝚞𝚎wo Here", emoji=economy_q_bank, style=discord.ButtonStyle.secondary)
    async def quewo_channel_button(self, interaction, button):
        if not has_owner_power(interaction.user, interaction.guild):
            return await interaction.response.send_message("Admin power only.", ephemeral=True)
        await interaction.response.defer()
        await asyncio.to_thread(economy_set_economy_channel_id, interaction.guild.id, interaction.channel.id)
        await interaction.edit_original_response(embed=await build_settings_embed(interaction.guild), view=self)

    @discord.ui.button(label="Clear 𝚀𝚞𝚎wo Channel", emoji=economy_q_trash, style=discord.ButtonStyle.secondary)
    async def quewo_channel_clear_button(self, interaction, button):
        if not has_owner_power(interaction.user, interaction.guild):
            return await interaction.response.send_message("Admin power only.", ephemeral=True)
        await interaction.response.defer()
        await asyncio.to_thread(economy_set_economy_channel_id, interaction.guild.id, None)
        await interaction.edit_original_response(embed=await build_settings_embed(interaction.guild), view=self)

    @discord.ui.button(label="Birthdays Here", emoji=economy_q_birthday, style=discord.ButtonStyle.secondary)
    async def birthday_button(self, interaction, button):
        if not await can_manage_birthday_channel(interaction.user, interaction.guild):
            return await interaction.response.send_message("You can't change the birthday channel here.", ephemeral=True)
        await interaction.response.defer()
        saved = await asyncio.to_thread(save_guild_birthday_channel, interaction.guild.id, interaction.channel.id, interaction.user.id)
        if not saved:
            return await interaction.followup.send("Birthday channel save failed.", ephemeral=True)
        guild_birthday_channels[interaction.guild.id] = {"channel_id": interaction.channel.id, "set_by_user_id": interaction.user.id}
        await interaction.edit_original_response(embed=await build_settings_embed(interaction.guild), view=self)

    @discord.ui.button(label="Clear Birthdays", emoji=economy_q_trash, style=discord.ButtonStyle.secondary)
    async def birthday_clear_button(self, interaction, button):
        if not await can_manage_birthday_channel(interaction.user, interaction.guild):
            return await interaction.response.send_message("You can't change the birthday channel here.", ephemeral=True)
        await interaction.response.defer()
        ok = await asyncio.to_thread(delete_guild_birthday_channel, interaction.guild.id)
        if not ok:
            return await interaction.followup.send("Birthday channel clear failed.", ephemeral=True)
        guild_birthday_channels.pop(interaction.guild.id, None)
        await interaction.edit_original_response(embed=await build_settings_embed(interaction.guild), view=self)

    @discord.ui.button(label="Activity Here", emoji=economy_q_activity, style=discord.ButtonStyle.secondary)
    async def activity_button(self, interaction, button):
        if not await can_manage_activity_channel(interaction.user, interaction.guild):
            return await interaction.response.send_message("You can't change activity reports here.", ephemeral=True)
        await interaction.response.defer()
        ok, message, _ = await save_activity_report_config(interaction.guild, interaction.channel, interaction.user.id)
        if not ok:
            return await interaction.followup.send(message, ephemeral=True)
        schedule_activity_live_refresh(interaction.guild.id, guild_activity_channels[interaction.guild.id])
        await interaction.edit_original_response(embed=await build_settings_embed(interaction.guild), view=self)

    @discord.ui.button(label="Stop Activity", emoji=economy_q_timeout, style=discord.ButtonStyle.secondary)
    async def activity_stop_button(self, interaction, button):
        if not await can_manage_activity_channel(interaction.user, interaction.guild):
            return await interaction.response.send_message("You can't change activity reports here.", ephemeral=True)
        await interaction.response.defer()
        ok = await asyncio.to_thread(delete_guild_activity_channel, interaction.guild.id)
        if not ok:
            return await interaction.followup.send("Activity report stop failed.", ephemeral=True)
        guild_activity_channels.pop(interaction.guild.id, None)
        active_activity_status_messages.pop(interaction.guild.id, None)
        await asyncio.to_thread(clear_guild_activity_counts, interaction.guild.id)
        await interaction.edit_original_response(embed=await build_settings_embed(interaction.guild), view=self)

    @discord.ui.button(label="Lottery Panel", emoji=economy_q_ticket, style=discord.ButtonStyle.secondary)
    async def lottery_button(self, interaction, button):
        prefix = prefix_for_guild(interaction.guild)
        lines = [f"Use `{prefix}lottery` to open or create the lottery panel."]
        if can_see_superowner_help(interaction.user, interaction.guild):
            lines.append(f"Use `{prefix}editlottery channel #{interaction.channel.name}` to move the current lottery here.")
        await interaction.response.send_message("\n".join(lines), ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

    @discord.ui.button(label="Admin Commands", emoji=economy_q_command_check, style=discord.ButtonStyle.secondary)
    async def admin_commands_button(self, interaction, button):
        prefix = prefix_for_guild(interaction.guild)
        lines = [
            "Quick setup guide:",
            f"`{prefix}prefix <new>` - change prefix",
            f"`{prefix}setlogs` - setup logs",
            f"`{prefix}setbdaychannel #channel` - birthdays",
            f"`{prefix}activity setup` - activity reports",
            f"`{prefix}todchannel set #channel` - Truth or Dare channels",
            f"`{prefix}lottery` - lottery panel",
            f"`{prefix}quewochannel here/off` - restrict 𝚀𝚞𝚎wo channel",
            f"{economy_q_bank} `{prefix}bank` - protected cash",
            f"{economy_q_rob} `{prefix}robsettings on/off` - server robbing",
            f"`{prefix}disable <command>` / `{prefix}enable <command>` - command access",
            f"`{prefix}commandstats` - command usage",
            f"`{prefix}jobs` / `{prefix}errors` - operations checks",
            f"`{prefix}receipt <id>` - review sensitive action receipts",
            f"`{prefix}economyaudit` - economy audit",
            f"`{prefix}aichannel on/off` - server AI replies",
        ]
        if can_see_superowner_help(interaction.user, interaction.guild):
            lines.extend([
                f"`{prefix}editlottery <setting> <value>` - lottery settings",
                f"`{prefix}aisettings` - AI controls",
            ])
        await interaction.response.send_message(
            "\n".join(lines),
            ephemeral=True,
        )

    @discord.ui.button(label="Command Access", emoji=economy_q_lock, style=discord.ButtonStyle.secondary)
    async def command_access_button(self, interaction, button):
        if not has_owner_power(interaction.user, interaction.guild):
            return await interaction.response.send_message(denial_message("Admin power only."), ephemeral=True)
        await interaction.response.send_message(
            embed=build_command_access_embed(interaction.guild, interaction.user),
            view=CommandAccessView(interaction.user.id),
            ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )

    @discord.ui.button(label="Setup Guide", emoji=economy_q_setup, style=discord.ButtonStyle.success)
    async def setup_guide_button(self, interaction, button):
        prefix = prefix_for_guild(interaction.guild)
        embed = discord.Embed(
            title=f"{economy_q_setup} Setup Guide",
            description="The short version: set logs, choose community channels, then tune 𝚀𝚞𝚎wo.",
            color=discord.Color.green(),
        )
        embed.add_field(name="Core", value=f"`{prefix}prefix <new>`\n`{prefix}setlogs`\n`{prefix}settings`\n{economy_q_tutorial} `{prefix}tutorial`", inline=True)
        embed.add_field(name="Community", value=f"`{prefix}setbdaychannel #channel`\n`{prefix}activity setup`\n`{prefix}todchannel`\n`{prefix}messages`", inline=True)
        quewo_lines = [
            f"`{prefix}lottery`",
            f"`{prefix}quewochannel`",
            f"{economy_q_bank} `{prefix}bank`",
            f"{economy_q_rob} `{prefix}robsettings`",
            f"`{prefix}balanceaudit`",
            f"{economy_q_recommend} `{prefix}recommendgame`",
            f"{economy_q_season_pass} `{prefix}seasonpass`",
        ]
        safety_lines = [f"`{prefix}disable <command>`", f"`{prefix}errors`"]
        ai_lines = [f"`{prefix}aichannel on/off`", f"`{prefix}aiknow <command>`", f"`{prefix}aimemory`", f"`{prefix}aidoctor`"]
        if can_see_superowner_help(interaction.user, interaction.guild):
            quewo_lines.insert(1, f"`{prefix}editlottery`")
            safety_lines[1:1] = [f"`{prefix}permaudit`", f"`{prefix}receipts latest`"]
            ai_lines.append(f"`{prefix}aiguard`")
        embed.add_field(
            name="𝚀𝚞𝚎wo",
            value="\n".join(quewo_lines),
            inline=True,
        )
        embed.add_field(name="Safety", value="\n".join(safety_lines), inline=True)
        embed.add_field(name="AI", value="\n".join(ai_lines), inline=True)
        embed.add_field(name="Recovery", value=f"`{prefix}health`\n`{prefix}jobs`\n`{prefix}recover`\n`{prefix}dbaudit`", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="settings", aliases=["setup"])
@is_admin_power()
async def settings_command(ctx):
    """Shows the server settings dashboard."""
    if ctx.guild is None:
        return await ctx.send("Settings only work in servers.")
    await ctx.send(embed=await build_settings_embed(ctx.guild), view=SettingsView(ctx.author.id), allowed_mentions=discord.AllowedMentions.none())

async def resolve_ai_memory_target(ctx, raw):
    if not raw:
        return ctx.author
    text = str(raw).strip()
    if not text:
        return ctx.author
    try:
        return await commands.UserConverter().convert(ctx, text)
    except commands.BadArgument:
        pass
    mention_match = re.fullmatch(r"<@!?(\d{15,25})>", text)
    raw_id = mention_match.group(1) if mention_match else text
    if raw_id.isdigit():
        user_id = int(raw_id)
        member = ctx.guild.get_member(user_id) if ctx.guild else None
        if member:
            return member
        try:
            return await bot.fetch_user(user_id)
        except (discord.NotFound, discord.HTTPException):
            raise commands.BadArgument("User not found.")
    raise commands.BadArgument("User not found.")

@bot.command(name="aimemory", aliases=["aime", "memoryai", "whatyouknow"])
async def aimemory_command(ctx, *, args: str = None):
    """Shows AI memory for yourself, or another user for 𝚀𝚞𝚎."""
    if ctx.guild is None:
        return await ctx.send("AI memory works in servers.")
    tokens = args.split() if args else []
    action_key = "show"
    action_words = {"show", "view", "check", "clear", "delete", "forget", "remove", "reset"}
    target_text = ""
    if tokens and tokens[0].casefold() in action_words:
        action_key = tokens.pop(0).casefold()
    if tokens and tokens[-1].casefold() in action_words and action_key == "show":
        action_key = tokens.pop(-1).casefold()
    target_text = " ".join(tokens).strip()
    try:
        target = await resolve_ai_memory_target(ctx, target_text)
    except commands.BadArgument:
        return await ctx.send("Use `.aimemory` or `.aimemory @user`.")
    if target.id != ctx.author.id and not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("You can only inspect your own AI memory."), allowed_mentions=discord.AllowedMentions.none())
    if action_key in {"clear", "delete", "forget", "remove", "reset"}:
        return await ctx.send("AI memory stays on now, so `.aimemory clear` is disabled.")
    await send_ai_memory_summary(ctx, ctx.guild, target)

@bot.command(name="aiknow", aliases=["aiknowledge", "knowcmd", "aicmd"])
async def aiknow_command(ctx, *, command_name: str = None):
    """Shows what the AI knows about a command."""
    if not command_name:
        return await ctx.send("Use `.aiknow <command>`.")
    command = get_command_case_insensitive(command_name.strip())
    if command and not command_is_visible_to(command, ctx.author, ctx.guild):
        command = None
    if not command:
        return await ctx.send(f"I don't know a command named `{command_name}`.")
    prefix = prefix_for_guild(ctx.guild)
    summary = command_ai_summary(command, prefix)
    detail = economy_detailed_explanations.get(command.name) or economy_explanations.get(command.name) or getattr(command, "help", "") or "No extra detail saved."
    embed = discord.Embed(
        title=f"{economy_q_book} AI Knowledge: {prefix}{command.qualified_name}",
        description=embed_value(summary, 1800),
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Permission", value=command_permission_note(command), inline=True)
    embed.add_field(name="Impact", value=command_risk_note(command), inline=True)
    embed.add_field(name="Details", value=embed_value(detail, 1800), inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="aihistory", aliases=["aiactions", "actionhistory"])
async def aihistory_command(ctx, member: discord.User = None):
    """Shows recent actions the AI ran or attempted."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("This AI history is not available here."), allowed_mentions=discord.AllowedMentions.none())
    rows = list(ai_action_history)
    if member:
        rows = [row for row in rows if int(row.get("user_id") or 0) == member.id]
    rows = rows[:20]
    embed = discord.Embed(
        title=f"{economy_q_ai_history} AI Action History",
        description=f"Recent AI-triggered bot actions and batch edits{f' for <@{member.id}>' if member else ''}.",
        color=discord.Color.blurple(),
    )
    if not rows:
        embed.add_field(name="Actions", value="No AI actions recorded since this restart.", inline=False)
    else:
        lines = []
        for row in rows:
            status = economy_q_accept if row["success"] else economy_q_warning
            lines.append(
                f"{status} <t:{int(row['time'].timestamp())}:R> <@{row['user_id']}> "
                f"`{row['action']}` - {row['detail'] or 'no details'}"
            )
        embed.add_field(name="Actions", value=embed_value("\n".join(lines), 3800), inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="aisettings", aliases=["aiconfig", "aicontrols"])
async def aisettings_command(ctx):
    """Shows AI control settings."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("AI control settings are not available here."), allowed_mentions=discord.AllowedMentions.none())
    global_settings = await ai_settings_for("global", 0)
    guild_settings = await ai_settings_for("guild", ctx.guild.id if ctx.guild else 0)
    embed = discord.Embed(
        title=f"{economy_q_ai_history} AI Settings",
        description="AI behavior controls. You can also tell the AI naturally, like `ignore @user` or `stop responding in this server`.",
        color=discord.Color.blurple(),
    )
    embed.add_field(name="Global", value=joined_embed_value([f"`{k}` = `{v}`" for k, v in global_settings.items()], empty="None"), inline=False)
    embed.add_field(name="This Server", value=joined_embed_value([f"`{k}` = `{v}`" for k, v in guild_settings.items()], empty="None"), inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="off", aliases=["maintenance", "botoff", "updatebot"])
async def off_command(ctx, *, reason: str = None):
    """𝚀𝚞𝚎-only maintenance mode. Stops public commands/AI while background jobs keep running."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("This switch is not available here."), allowed_mentions=discord.AllowedMentions.none())
    if bot_maintenance_mode:
        return await ctx.send(f"{economy_q_timer_tick} Pro𝚀𝚞𝚎 is already in maintenance mode.")
    status = await ctx.send(f"{economy_q_timer_tick} Turning maintenance mode on...")
    ok, changed_nicks = await set_maintenance_mode(True, reason=reason or "Updating", updated_by=ctx.author.id)
    if not ok:
        return await status.edit(content=f"{economy_q_warning} I couldn't save maintenance mode. The bot is still on.")
    await status.edit(
        content=(
            f"{economy_q_accept} Maintenance mode is **on**.\n"
            f"Public commands and AI replies are paused. Background reminders, lotteries, events, logs, and recovery loops keep running.\n"
            f"Nickname updated in **{changed_nicks}/{len(bot.guilds)}** server(s). Use `.on` when ready."
        ),
        allowed_mentions=discord.AllowedMentions.none(),
    )

@bot.command(name="on", aliases=["boton", "onlinebot", "finishupdate"])
async def on_command(ctx):
    """𝚀𝚞𝚎-only maintenance mode off switch."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("This switch is not available here."), allowed_mentions=discord.AllowedMentions.none())
    if not bot_maintenance_mode:
        return await ctx.send(f"{economy_q_accept} Pro𝚀𝚞𝚎 is already on.")
    status = await ctx.send(f"{economy_q_timer_tick} Turning maintenance mode off...")
    ok, changed_nicks = await set_maintenance_mode(False, updated_by=ctx.author.id)
    if not ok:
        return await status.edit(content=f"{economy_q_warning} I couldn't save the on switch. Try `.on` again.")
    await status.edit(
        content=(
            f"{economy_q_accept} Maintenance mode is **off**.\n"
            f"Public commands and AI replies are live again. Nickname restored in **{changed_nicks}/{len(bot.guilds)}** server(s)."
        ),
        allowed_mentions=discord.AllowedMentions.none(),
    )

@bot.command(name="aiperms", aliases=["aipermissions", "aicapabilities", "aiauthority"])
async def aiperms_command(ctx):
    """Shows what the AI is allowed to do."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("AI control permissions are not available here."), allowed_mentions=discord.AllowedMentions.none())
    global_settings = await ai_settings_for("global", 0)
    guild_settings = await ai_settings_for("guild", ctx.guild.id if ctx.guild else 0)
    ignored = [part for part in global_settings.get("ignored_users", "").split(",") if part.strip()]
    embed = standard_embed(
        "AI Permissions",
        "What Pro𝚀𝚞𝚎 AI can do when people talk to it.",
        color=discord.Color.blurple(),
        icon=economy_q_ai_history,
    )
    embed.add_field(
        name="Can Run Without Confirmation",
        value=joined_embed_value(sorted(f"`{name}`" for name in AI_SAFE_COMMANDS), limit=1200),
        inline=False,
    )
    embed.add_field(
        name="Needs Confirmation / 𝚀𝚞𝚎 Control",
        value="Sensitive actions like rewards, ignores, channel AI on/off, and style changes require context and are limited to 𝚀𝚞𝚎.",
        inline=False,
    )
    embed.add_field(name="Ignored Users", value=joined_embed_value([f"<@{uid}> (`{uid}`)" for uid in ignored], empty="None", limit=1000), inline=False)
    embed.add_field(name="Global Settings", value=joined_embed_value([f"`{k}` = `{v}`" for k, v in global_settings.items()], empty="None"), inline=False)
    embed.add_field(name="This Server", value=joined_embed_value([f"`{k}` = `{v}`" for k, v in guild_settings.items()], empty="None"), inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="aiignore", aliases=["ignoreai"])
async def aiignore_command(ctx, member: discord.User = None):
    """Makes the AI ignore a user globally."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("AI control settings are not available here."), allowed_mentions=discord.AllowedMentions.none())
    if not member:
        return await ctx.send("Use `.aiignore @user`.")
    settings = await ai_settings_for("global", 0)
    ignored = {part.strip() for part in settings.get("ignored_users", "").split(",") if part.strip()}
    ignored.add(str(member.id))
    ok = await asyncio.to_thread(set_ai_control_setting, "global", 0, "ignored_users", ",".join(sorted(ignored)), ctx.author.id)
    await ctx.send(f"{economy_q_accept if ok else economy_q_warning} AI will ignore <@{member.id}>.", allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="aiunignore", aliases=["unignoreai"])
async def aiunignore_command(ctx, member: discord.User = None):
    """Allows the AI to respond to an ignored user again."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("AI control settings are not available here."), allowed_mentions=discord.AllowedMentions.none())
    if not member:
        return await ctx.send("Use `.aiunignore @user`.")
    settings = await ai_settings_for("global", 0)
    ignored = {part.strip() for part in settings.get("ignored_users", "").split(",") if part.strip()}
    ignored.discard(str(member.id))
    ok = await asyncio.to_thread(set_ai_control_setting, "global", 0, "ignored_users", ",".join(sorted(ignored)), ctx.author.id)
    await ctx.send(f"{economy_q_accept if ok else economy_q_warning} AI can respond to <@{member.id}> again.", allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="aichannel", aliases=["aitoggle"])
async def aichannel_command(ctx, mode: str = None):
    """Turns AI replies on or off in this server."""
    if ctx.guild is None:
        return await ctx.send("Use this in a server.")
    if not has_owner_power(ctx.author, ctx.guild):
        return await ctx.send("Admin power only.")
    key = str(mode or "").casefold()
    if key in {"enable", "enabled", "yes", "true"}:
        key = "on"
    elif key in {"disable", "disabled", "no", "false"}:
        key = "off"
    if not key:
        settings = await ai_settings_for("guild", ctx.guild.id)
        current = settings.get("enabled", "on")
        return await ctx.send(f"{economy_q_ai_history} AI responses are currently **{current}** here.\nUse `.aichannel on` or `.aichannel off`.")
    if key not in {"on", "off"}:
        return await ctx.send("Use `.aichannel on` or `.aichannel off`.")
    ok = await asyncio.to_thread(set_ai_control_setting, "guild", ctx.guild.id, "enabled", key, ctx.author.id)
    await ctx.send(f"{economy_q_accept if ok else economy_q_warning} AI responses are now **{key}** in this server.")

@bot.command(name="aistyle", aliases=["aipersonality"])
async def aistyle_command(ctx, *, style: str = None):
    """Changes the AI's global style note."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("AI control settings are not available here."), allowed_mentions=discord.AllowedMentions.none())
    if not style:
        return await ctx.send("Use `.aistyle casual`, `.aistyle serious`, `.aistyle short`, or any short style note.")
    ok = await asyncio.to_thread(set_ai_control_setting, "global", 0, "style", style[:200], ctx.author.id)
    await ctx.send(f"{economy_q_accept if ok else economy_q_warning} AI style set to: **{style[:200]}**")

@bot.command(name="usersettings", aliases=["mysettings", "preferences", "prefs"])
async def usersettings_command(ctx, key: str = None, *, value: str = None):
    """Shows or changes your personal bot preferences."""
    allowed = {
        "compact": "Compact command results where supported",
        "receipts": "Show hidden receipt details with copyable receipt IDs",
        "gamesummary": "Prefer cleaner game result summaries where supported",
        "shopsummary": "Keep shop purchase follow-ups compact",
        "quiet": "Reduce optional reminder-style text where supported",
    }
    if not key:
        settings = await ai_settings_for("user", ctx.author.id)
        embed = discord.Embed(
            title=f"{economy_q_user_edit} User Settings",
            description=(
                f"Use `.usersettings <setting> on/off`.\n"
                f"Settings: {', '.join(f'`{name}`' for name in allowed)}\n"
                "AI memory stays on so Pro𝚀𝚞𝚎 can keep useful bot context and remembered profile details."
            ),
            color=discord.Color.blurple(),
        )
        for setting, description in allowed.items():
            embed.add_field(name=setting, value=f"{description}\nCurrent: **{user_setting_value(settings, setting)}**", inline=False)
        return await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    setting = key.casefold()
    if setting == "aifriendly":
        return await ctx.send("AI memory is always on now, so there is nothing to toggle.")
    if setting not in allowed:
        return await ctx.send(f"Unknown setting. Use `.usersettings` to see the list.")
    normalized = str(value or "").casefold().strip()
    if normalized not in {"on", "off", "true", "false", "yes", "no"}:
        return await ctx.send(f"Use `.usersettings {setting} on` or `.usersettings {setting} off`.")
    stored = "on" if normalized in {"on", "true", "yes"} else "off"
    ok = await asyncio.to_thread(set_ai_control_setting, "user", ctx.author.id, setting, stored, ctx.author.id)
    await ctx.send(f"{economy_q_accept if ok else economy_q_warning} `{setting}` is now **{stored}**.")

@bot.command(name="auditcommands", aliases=["cmdaudit", "commandaudit"])
async def auditcommands_command(ctx):
    """Checks command registry coverage for help, explanations, aliases, and UI fallbacks."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("This command audit is not available here."), allowed_mentions=discord.AllowedMentions.none())
    category_map = {}
    for category, names in HELP_CATEGORIES.items():
        for name in names:
            category_map.setdefault(name.casefold(), set()).add(category)
    seen_aliases = {}
    duplicate_alias_lines = []
    near_duplicate_lines = []
    missing_category = []
    missing_explain = []
    missing_detail = []
    missing_example = []
    input_without_ui = []
    stale_explain = []
    for command in sorted(bot.commands, key=lambda c: c.qualified_name):
        if command.hidden:
            continue
        names = {command.name.casefold(), *(alias.casefold() for alias in command.aliases)}
        if not any(name in category_map for name in names):
            missing_category.append(f"`{command.name}`")
        if not any(name in economy_explanations for name in names):
            missing_explain.append(f"`{command.name}`")
        if command.name in economy_explanations and command.name not in economy_detailed_explanations and command.signature:
            missing_detail.append(f"`{command.name}`")
        if command.signature and command.name not in COMMAND_EXAMPLE_OVERRIDES:
            missing_example.append(f"`{command.name}`")
        if command.signature and command.name not in COMMAND_EXAMPLE_OVERRIDES and not command_supports_input_ui(command):
            input_without_ui.append(f"`{command.name}`")
        text = economy_explanations.get(command.name, "")
        if text and "Runs this Quewo command" in text:
            stale_explain.append(f"`{command.name}`")
        for alias in command.aliases:
            key = alias.casefold()
            if key in seen_aliases and seen_aliases[key] != command.name:
                duplicate_alias_lines.append(f"`{alias}`: `{seen_aliases[key]}` and `{command.name}`")
            seen_aliases[key] = command.name
    sync_db_findings, ui_findings = await asyncio.gather(
        asyncio.to_thread(sync_db_audit),
        asyncio.to_thread(ui_callback_audit),
    )
    command_names = sorted({command.name for command in bot.commands if not command.hidden})
    for index, left in enumerate(command_names):
        for right in command_names[index + 1:]:
            if abs(len(left) - len(right)) > 3:
                continue
            if SequenceMatcher(None, left, right).ratio() >= 0.86:
                near_duplicate_lines.append(f"`{left}` / `{right}`")
                if len(near_duplicate_lines) >= 20:
                    break
        if len(near_duplicate_lines) >= 20:
            break

    embed = discord.Embed(
        title=f"{economy_q_command_check} Command Audit",
        description=f"Scanned **{len([c for c in bot.commands if not c.hidden])}** visible prefix commands.",
        color=discord.Color.orange(),
    )
    critical = len(missing_category) + len(missing_explain) + len(input_without_ui) + len(duplicate_alias_lines)
    warnings = len(missing_detail) + len(missing_example) + len(stale_explain) + len(sync_db_findings) + len(ui_findings)
    status = f"{economy_q_accept} Clean" if critical == 0 and warnings == 0 else (f"{economy_q_warning} Needs cleanup" if critical else f"{economy_q_thinking} Minor gaps")
    embed.add_field(
        name="Summary",
        value=(
            f"Status: **{status}**\n"
            f"Critical: **{critical:,}**\n"
            f"Warnings: **{warnings:,}**"
        ),
        inline=False,
    )
    top_actions = []
    if duplicate_alias_lines:
        top_actions.append("Resolve duplicate aliases first; they can route users to the wrong command.")
    if missing_category:
        top_actions.append("Add uncategorized commands to help so users can discover them.")
    if missing_explain:
        top_actions.append("Add short explain text so help/AI can describe commands correctly.")
    if input_without_ui:
        top_actions.append("Add examples or input UI for commands that need arguments.")
    if stale_explain:
        top_actions.append("Replace generic explain text with real command behavior.")
    if sync_db_findings:
        top_actions.append("Move direct database calls inside async commands into `asyncio.to_thread` when they are on hot paths.")
    if ui_findings:
        top_actions.append("Review UI callbacks that may not acknowledge interactions directly; global UI error handling still catches failures.")
    if not top_actions:
        top_actions.append("No high-impact cleanup needed right now.")
    embed.add_field(name="Next Fixes", value=embed_value("\n".join(f"- {item}" for item in top_actions[:6]), 1000), inline=False)
    embed.add_field(
        name="Counts",
        value=(
            f"Missing category: **{len(missing_category)}**\n"
            f"Missing explain: **{len(missing_explain)}**\n"
            f"Missing details/examples: **{len(missing_detail) + len(missing_example)}**\n"
            f"Input without UI/example: **{len(input_without_ui)}**\n"
            f"Generic explain: **{len(stale_explain)}**\n"
            f"DB hot-path warnings: **{len(sync_db_findings)}**\n"
            f"UI warnings: **{len(ui_findings)}**\n"
            f"Alias/name duplicates: **{len(duplicate_alias_lines) + len(near_duplicate_lines)}**"
        ),
        inline=False,
    )
    compact_samples = [
        f"Help: {', '.join(missing_category[:6]) or 'None'}",
        f"Explain: {', '.join(missing_explain[:6]) or 'None'}",
        f"Input: {', '.join(input_without_ui[:6]) or 'None'}",
        f"DB: {'; '.join(sync_db_findings[:3]) or 'None'}",
        f"UI: {'; '.join(ui_findings[:3]) or 'None'}",
        f"Aliases: {'; '.join(duplicate_alias_lines[:3]) or 'None'}",
        f"Near names: {'; '.join(near_duplicate_lines[:3]) or 'None'}",
    ]
    embed.add_field(name="Samples", value=embed_value("\n".join(compact_samples), 1000), inline=False)
    embed.set_footer(text="Compact audit to avoid Discord walls. Run .commandcleanup for a focused cleanup summary too.")
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

def command_category_lookup():
    lookup = {}
    for category, names in HELP_CATEGORIES.items():
        for name in names:
            lookup.setdefault(name.casefold(), set()).add(category)
    return lookup

def command_style_expectation(category):
    if "𝚀𝚞𝚎wo" in category:
        return "Use 𝚀𝚞𝚎wo emojis, risk labels for games, clear balance deltas, and hidden receipt lines for sensitive money movement."
    if category == "Games":
        return "Use board/game UI where possible, turn ownership checks, timeouts, and short result text that stays under Discord limits."
    if category == "AI":
        return "Reply naturally, include context, avoid oversized prompts, and confirm only when the AI is about to run a bot action."
    if category in {"Admin Setup", "Moderation", "Diagnostics"}:
        return "Use no-ping mentions, receipts for sensitive actions, concise embeds, and clear permission denial text."
    if category == "Community":
        return "Use compact themed embeds, no unwanted everyone pings, and mention summaries with jump links."
    if category == "Tools":
        return "Prefer setup/input UI when arguments are missing and keep results readable on mobile."
    return "Keep copy short, themed, and consistent."

@bot.command(name="styleaudit", aliases=["uiaudit", "messageaudit"])
async def styleaudit_command(ctx):
    """Audits output style expectations by command category."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("This style audit is not available here."), allowed_mentions=discord.AllowedMentions.none())
    categories = help_categories_for(ctx.author, ctx.guild)
    embed = discord.Embed(
        title=f"{economy_q_filter} Style Audit",
        description="Category-level checks for message/UI consistency across the bot.",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    total = 0
    for category, names in categories.items():
        command_count = len([name for name in names if get_command_case_insensitive(name)])
        total += command_count
        embed.add_field(
            name=f"{category} ({command_count})",
            value=embed_value(command_style_expectation(category), 900),
            inline=False,
        )
    long_view_timeout = int(LONG_HELP_VIEW_TIMEOUT / 3600)
    embed.add_field(
        name="Long UI Rules",
        value=(
            f"Help/setup views stay alive for about **{long_view_timeout}h**.\n"
            "Interactive game/shop views should defer quickly, keep edits under Discord limits, and show compact summaries when finished."
        ),
        inline=False,
    )
    embed.add_field(
        name="Receipts & Summaries",
        value="Receipts use one shared hidden format with a copyable ID. Long summaries should page results instead of ending with `...and more`.",
        inline=False,
    )
    embed.set_footer(text=f"Audited {total} command entries. Use .auditcommands for missing help/examples, .permaudit for sensitive exposure, and .perf for slow paths.")
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="commandcleanup", aliases=["cleanupcommands", "cmdcleanup"])
async def commandcleanup_command(ctx):
    """Shows a focused cleanup plan for command discoverability and duplicates."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("This command cleanup audit is not available here."), allowed_mentions=discord.AllowedMentions.none())
    category_map = command_category_lookup()
    missing_category = []
    missing_example = []
    generic_explain = []
    duplicate_aliases = []
    alias_owner = {}
    visible_commands = [command for command in bot.commands if not command.hidden]
    for command in sorted(visible_commands, key=lambda item: item.qualified_name.casefold()):
        names = {command.name.casefold(), *(alias.casefold() for alias in command.aliases)}
        if not any(name in category_map for name in names):
            missing_category.append(f"`{command.name}`")
        if command.signature and command.name not in COMMAND_EXAMPLE_OVERRIDES and not command_supports_input_ui(command):
            missing_example.append(f"`{command.name}`")
        text = economy_explanations.get(command.name, "")
        if "Runs this Quewo command" in text:
            generic_explain.append(f"`{command.name}`")
        for alias in command.aliases:
            key = alias.casefold()
            if key in alias_owner and alias_owner[key] != command.name:
                duplicate_aliases.append(f"`{alias}`: `{alias_owner[key]}` / `{command.name}`")
            alias_owner[key] = command.name
    near_duplicates = []
    command_names = sorted(command.name for command in visible_commands)
    for idx, left in enumerate(command_names):
        for right in command_names[idx + 1:]:
            if abs(len(left) - len(right)) <= 3 and SequenceMatcher(None, left, right).ratio() >= 0.88:
                near_duplicates.append(f"`{left}` / `{right}`")
                if len(near_duplicates) >= 12:
                    break
        if len(near_duplicates) >= 12:
            break
    embed = discord.Embed(
        title=f"{economy_q_command_check} Command Cleanup",
        description="Focused list of command polish issues that affect users or AI command knowledge.",
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(
        name="Counts",
        value=(
            f"Missing category: **{len(missing_category)}**\n"
            f"Missing UI/example: **{len(missing_example)}**\n"
            f"Generic explain: **{len(generic_explain)}**\n"
            f"Duplicate aliases: **{len(duplicate_aliases)}**\n"
            f"Near-duplicates: **{len(near_duplicates)}**"
        ),
        inline=False,
    )
    embed.add_field(name="Top Fixes", value=embed_value("\n".join([
        f"Categories: {', '.join(missing_category[:8]) or 'None'}",
        f"Input help: {', '.join(missing_example[:8]) or 'None'}",
        f"Generic explain: {', '.join(generic_explain[:8]) or 'None'}",
        f"Duplicate aliases: {'; '.join(duplicate_aliases[:5]) or 'None'}",
        f"Near-duplicates: {'; '.join(near_duplicates[:5]) or 'None'}",
    ]), 1000), inline=False)
    embed.set_footer(text="Compact view. Use .auditcommands for the deeper scan when you actually need the wall.")
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="permaudit", aliases=["permsaudit", "permissionaudit", "sensitiveaudit"])
async def permaudit_command(ctx):
    """Audits sensitive commands and where they are exposed."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("This permission audit is not available here."), allowed_mentions=discord.AllowedMentions.none())
    lines = []
    missing = []
    for name in sorted(SUPEROWNER_HELP_COMMANDS):
        command = get_command_case_insensitive(name)
        if not command:
            missing.append(f"`{name}`")
            continue
        aliases = ", ".join(command.aliases) if command.aliases else "none"
        hidden_public = "yes" if is_superowner_help_command(command) else "no"
        lines.append(
            f"`{command.name}` - aliases: `{aliases}` | public-hidden: **{hidden_public}** | {command_risk_note(command)}"
        )
    hidden_commands = ", ".join(f"`{name}`" for name in sorted(SUPEROWNER_HIDDEN_COMMANDS & set(SUPEROWNER_HELP_COMMANDS)))
    embed = discord.Embed(
        title=f"{economy_q_permissions} Permission Audit",
        description=f"Sensitive commands are only visible in help/search surfaces for {QUE_OWNER_DISPLAY}.",
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Sensitive Commands", value=embed_value("\n".join(lines) or "None", 3000), inline=False)
    embed.add_field(name="Missing Registered Commands", value=", ".join(missing) or "None", inline=False)
    embed.add_field(name="AI Visibility", value=embed_value(f"Blocked from public AI command knowledge.\nTracked: {hidden_commands}", 1000), inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="commandstats", aliases=["cmdstats", "usage"])
@is_admin_power()
async def commandstats_command(ctx, scope: str = "local"):
    """Shows command usage stats for this server or globally."""
    guild_id = None if str(scope or "").casefold() in {"global", "all"} else (ctx.guild.id if ctx.guild else 0)
    rows = await asyncio.to_thread(get_command_usage_stats, guild_id, 25)
    embed = discord.Embed(
        title=f"{economy_q_command_check} Command Usage",
        description="Global usage." if guild_id is None else "This server's usage.",
        color=discord.Color.blurple(),
    )
    if not rows:
        embed.add_field(name="Commands", value="No command usage tracked yet.", inline=False)
    else:
        lines = []
        for index, row in enumerate(rows, 1):
            command_name, uses, users, last_used = row[:4]
            if is_superowner_help_command(str(command_name)) and not can_see_superowner_help(ctx.author, ctx.guild):
                continue
            ts = discord.utils.format_dt(last_used.replace(tzinfo=timezone.utc), "R") if last_used else "unknown"
            lines.append(f"**#{index}** `.{command_name}` - **{int(uses):,}** uses, **{int(users):,}** user(s), last {ts}")
        embed.add_field(name="Commands", value=embed_value("\n".join(lines) or "No visible command usage tracked yet.", 3800), inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="receipt", aliases=["txreceipt", "qreceipt"])
@is_admin_power()
async def receipt_command(ctx, receipt_id: str = None):
    """Shows a stored receipt for sensitive bot actions."""
    if not receipt_id:
        receipt_id = "latest"
    if str(receipt_id).casefold() in {"latest", "recent"}:
        rows = await asyncio.to_thread(economy_get_receipts_for_user, None, ctx.guild.id if ctx.guild else None, 1)
        row = rows[0] if rows else None
    else:
        row = await asyncio.to_thread(economy_get_receipt, receipt_id.strip())
    if not row:
        row = await asyncio.to_thread(get_bot_receipt, receipt_id.strip()) if str(receipt_id).casefold() not in {"latest", "recent"} else None
    if not row:
        return await ctx.send("Receipt not found.")
    row = normalize_receipt_row(row)
    if is_superowner_help_command(str(row.get("action") or "")) and not can_see_superowner_help(ctx.author, ctx.guild):
        return await ctx.send("Receipt not found.")
    rid = row.get("receipt_id")
    guild_id = row.get("guild_id")
    channel_id = row.get("channel_id")
    actor_id = row.get("actor_id")
    target_ids = row.get("target_ids") or []
    action = row.get("action")
    amount = row.get("amount")
    details = row.get("details")
    created_at = row.get("created_at")
    embed = standard_embed(
        f"Receipt {rid}",
        color=discord.Color.gold(),
        icon=economy_q_archive,
    )
    embed.add_field(name="Action", value=str(action).replace("_", " "), inline=True)
    embed.add_field(name="Actor", value=f"<@{actor_id}>\n`{actor_id}`", inline=True)
    embed.add_field(name="Amount", value=economy_format_balance(amount) if amount is not None else "None", inline=True)
    embed.add_field(name="When", value=receipt_time_text(created_at), inline=False)
    if guild_id:
        embed.add_field(name="Location", value=f"Guild `{guild_id}` · Channel `{channel_id or 'unknown'}`", inline=False)
    targets = [f"<@{user_id}> (`{user_id}`)" for user_id in target_ids]
    embed.add_field(name="Targets", value=joined_embed_value(targets, empty="None", limit=1000), inline=False)
    if details:
        embed.add_field(name="Details", value=embed_value(str(details), 1800), inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="receipts", aliases=["receiptlist", "txreceipts"])
async def receipts_command(ctx, target: str = "latest"):
    """Lists recent sensitive action receipts."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("Receipt lists are not available here."), allowed_mentions=discord.AllowedMentions.none())
    user = None
    if target and str(target).casefold() not in {"latest", "recent", "all"}:
        try:
            user = await commands.UserConverter().convert(ctx, target)
        except Exception:
            return await ctx.send("Use `.receipts latest` or `.receipts @user`.")
    rows = await asyncio.to_thread(
        economy_get_receipts_for_user,
        user.id if user else None,
        ctx.guild.id if ctx.guild else None,
        12,
    )
    embed = standard_embed(
        "Receipts",
        description=(f"Recent receipts involving {user.mention}." if user else "Latest sensitive action receipts in this server."),
        color=discord.Color.gold(),
        icon=economy_q_archive,
    )
    if not rows:
        embed.add_field(name="Results", value="No receipts found.", inline=False)
    else:
        lines = []
        for row in rows:
            lines.append(receipt_summary_line(row))
        embed.add_field(name="Latest", value=embed_value("\n".join(lines), 3800), inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="aidoctor", aliases=["botdoctor", "doctorai", "diagnosebot"])
@is_admin_power()
async def aidoctor_command(ctx):
    """Shows AI doctor status for bot health and debugging."""
    embed = discord.Embed(
        title=f"{economy_q_activity} Pro𝚀𝚞𝚎 Doctor",
        description=await bot_doctor_context_async(ctx.guild),
        color=discord.Color.teal(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Use With AI", value="Reply to an error/log and mention Pro𝚀𝚞𝚎; the AI gets this doctor snapshot plus reply context.", inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="slashsync", aliases=["slashstatus", "syncslash"])
@is_admin_power()
async def slashsync_command(ctx):
    """Clears stale slash commands from Discord."""
    result = await sync_slash_commands_once(force=True)
    embed = discord.Embed(
        title="Slash Commands Removed",
        description="Forced Discord slash command registration to clear. Prefix commands are unchanged.",
        color=discord.Color.blurple(),
    )
    embed.add_field(name="Guilds Cleared", value=f"{result['synced_guilds']}", inline=True)
    embed.add_field(name="Failures", value=f"{result['failed_guilds']}", inline=True)
    embed.set_footer(text="Discord may take a little time to hide old slash commands.")
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="perf", aliases=["performance", "slowcommands"])
@is_admin_power()
async def perf_command(ctx):
    """Shows slow command timing stats since the last restart."""
    rows = []
    for name, stats in sorted(command_timing_stats.items(), key=lambda item: item[1]["max_ms"], reverse=True)[:15]:
        count = max(1, int(stats["count"]))
        avg = int(stats["total_ms"] / count)
        rows.append(f"`{name}` · avg **{avg:,}ms** · max **{int(stats['max_ms']):,}ms** · runs **{count:,}**")
    embed = standard_embed(
        "Command Performance",
        description="Tracked since this bot process started.",
        color=discord.Color.orange(),
        icon=economy_q_history,
    )
    embed.add_field(name="Slowest", value=joined_embed_value(rows) if rows else "No command timing data yet.", inline=False)
    if slow_command_events:
        recent = []
        for event in list(slow_command_events)[-8:][::-1]:
            ts = discord.utils.format_dt(event["created_at"], "R")
            jump = f"https://discord.com/channels/{event['guild_id']}/{event['channel_id']}/{event['message_id']}" if event["guild_id"] else None
            jump_text = f" · [jump]({jump})" if jump else ""
            recent.append(f"`{event['name']}` · **{event['elapsed_ms']:,}ms** · <@{event['user_id']}> · {ts}{jump_text}")
        embed.add_field(name="Recent Slow Runs", value=embed_value("\n".join(recent), 1800), inline=False)
    queue_rows = []
    for group, stats in sorted(command_queue_stats.items()):
        count = max(1, int(stats["count"]))
        avg = int(stats["total_ms"] / count)
        queue_rows.append(f"`{group}` · avg wait **{avg:,}ms** · max **{int(stats['max_ms']):,}ms** · waits **{count:,}**")
    embed.add_field(name="Queue Waits", value=joined_embed_value(queue_rows) if queue_rows else "No queue waits tracked yet.", inline=False)
    if recent_queue_events:
        waits = []
        for event in list(recent_queue_events)[-8:][::-1]:
            ts = discord.utils.format_dt(event["created_at"], "R")
            waits.append(f"`{event['name']}` · `{event['group']}` · **{event['waited_ms']:,}ms** · <@{event['user_id']}> · {ts}")
        embed.add_field(name="Recent Queue Waits", value=embed_value("\n".join(waits), 1800), inline=False)
    active_global = max(0, COMMAND_CONCURRENCY_LIMIT - getattr(command_semaphore, "_value", COMMAND_CONCURRENCY_LIMIT))
    active_heavy = max(0, HEAVY_COMMAND_CONCURRENCY_LIMIT - getattr(heavy_command_semaphore, "_value", HEAVY_COMMAND_CONCURRENCY_LIMIT))
    active_bulk = max(0, BULK_COMMAND_CONCURRENCY_LIMIT - getattr(bulk_command_semaphore, "_value", BULK_COMMAND_CONCURRENCY_LIMIT))
    active_ai = max(0, AI_COMMAND_CONCURRENCY_LIMIT - getattr(ai_command_semaphore, "_value", AI_COMMAND_CONCURRENCY_LIMIT))
    active_image = max(0, IMAGE_COMMAND_CONCURRENCY_LIMIT - getattr(image_command_semaphore, "_value", IMAGE_COMMAND_CONCURRENCY_LIMIT))
    embed.add_field(
        name="Live Capacity",
        value=(
            f"Global **{active_global}/{COMMAND_CONCURRENCY_LIMIT}** · "
            f"Heavy **{active_heavy}/{HEAVY_COMMAND_CONCURRENCY_LIMIT}** · "
            f"Bulk **{active_bulk}/{BULK_COMMAND_CONCURRENCY_LIMIT}** · "
            f"AI **{active_ai}/{AI_COMMAND_CONCURRENCY_LIMIT}** · "
            f"Images **{active_image}/{IMAGE_COMMAND_CONCURRENCY_LIMIT}**"
        ),
        inline=False,
    )
    embed.add_field(
        name="Health",
        value=f"Gateway latency **{round(bot.latency * 1000):,}ms** · Help cache **{len(help_render_cache):,}** pages",
        inline=False,
    )
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="bulkqueue", aliases=["queue", "jobqueue"])
@is_admin_power()
async def bulkqueue_command(ctx):
    """Shows current command queue pressure for heavy and bulk commands."""
    active_global = max(0, COMMAND_CONCURRENCY_LIMIT - getattr(command_semaphore, "_value", COMMAND_CONCURRENCY_LIMIT))
    active_heavy = max(0, HEAVY_COMMAND_CONCURRENCY_LIMIT - getattr(heavy_command_semaphore, "_value", HEAVY_COMMAND_CONCURRENCY_LIMIT))
    active_bulk = max(0, BULK_COMMAND_CONCURRENCY_LIMIT - getattr(bulk_command_semaphore, "_value", BULK_COMMAND_CONCURRENCY_LIMIT))
    active_ai = max(0, AI_COMMAND_CONCURRENCY_LIMIT - getattr(ai_command_semaphore, "_value", AI_COMMAND_CONCURRENCY_LIMIT))
    active_image = max(0, IMAGE_COMMAND_CONCURRENCY_LIMIT - getattr(image_command_semaphore, "_value", IMAGE_COMMAND_CONCURRENCY_LIMIT))
    embed = standard_embed(
        "Command Queue",
        description="Live pressure from command groups that can slow the bot when many requests happen at once.",
        color=discord.Color.blurple(),
        icon=economy_q_queue,
    )
    embed.add_field(
        name="Running Now",
        value=(
            f"Global **{active_global}/{COMMAND_CONCURRENCY_LIMIT}**\n"
            f"Bulk **{active_bulk}/{BULK_COMMAND_CONCURRENCY_LIMIT}**\n"
            f"Heavy **{active_heavy}/{HEAVY_COMMAND_CONCURRENCY_LIMIT}**\n"
            f"AI **{active_ai}/{AI_COMMAND_CONCURRENCY_LIMIT}**\n"
            f"Images **{active_image}/{IMAGE_COMMAND_CONCURRENCY_LIMIT}**"
        ),
        inline=True,
    )
    queue_rows = []
    for group, stats in sorted(command_queue_stats.items()):
        count = max(1, int(stats["count"]))
        avg = int(stats["total_ms"] / count)
        queue_rows.append(f"`{group}` avg **{avg:,}ms**, max **{int(stats['max_ms']):,}ms**")
    embed.add_field(name="Wait History", value=joined_embed_value(queue_rows) if queue_rows else "No waits recorded this restart.", inline=True)
    if recent_queue_events:
        recent = []
        for event in list(recent_queue_events)[-6:][::-1]:
            recent.append(f"`{event['name']}` waited **{event['waited_ms']:,}ms** in `{event['group']}`")
        embed.add_field(name="Recent", value=embed_value("\n".join(recent), 1200), inline=False)
    embed.set_footer(text="Bulk commands run through a shared queue so they do not block normal commands.")
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="backgroundjobs", aliases=["tasks", "bgjobs"])
@is_admin_power()
async def jobs_command(ctx, action: str = None):
    """Shows background jobs started by Pro𝚀𝚞𝚎."""
    if action and action.casefold() in {"clear", "clean"}:
        for job_id, job in list(background_jobs.items()):
            if job.get("status") in {"done", "failed"}:
                background_jobs.pop(job_id, None)
        return await ctx.send(f"{economy_q_accept} Finished jobs cleared.")
    rows = sorted(background_jobs.values(), key=lambda job: job.get("started_at") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    embed = standard_embed(
        "Background Jobs",
        description="Longer maintenance work runs here so normal commands can keep responding.",
        color=discord.Color.blurple(),
        icon=economy_q_history,
    )
    embed.add_field(name="Jobs", value=joined_embed_value([public_job_snapshot(job) for job in rows[:12]], empty="No jobs tracked this restart.", limit=2500), inline=False)
    embed.set_footer(text=f"Use {prefix_for_guild(ctx.guild)}backgroundjobs clear to remove finished jobs from this view.")
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="recover", aliases=["restorestate", "recovery"])
@is_admin_power()
async def recover_command(ctx):
    """Manually reruns persistent state recovery for games, timers, polls, and lottery panels."""
    async def recover_job():
        await restore_persistent_runtime_state()
        restored, expired = await restore_active_game_sessions()
        try:
            await economy_module.restore_lottery_panels()
            await economy_module.catch_up_due_lotteries("manual recovery")
            await economy_module.catch_up_due_economy_events("manual recovery")
            lottery_text = "lottery panels/events refreshed"
        except Exception as exc:
            lottery_text = f"lottery refresh skipped: {clean_user_error(exc)}"
        return f"Recovered games: {restored} restored, {expired} expired; {lottery_text}."

    job_id = schedule_background_job("Manual recovery", ctx, recover_job)
    await ctx.send(
        f"{economy_q_recovery} Recovery started as `{job_id}`. Use `{prefix_for_guild(ctx.guild)}jobs` to check it.",
        allowed_mentions=discord.AllowedMentions.none(),
    )

@bot.command(name="errors", aliases=["errorlog"])
@is_admin_power()
async def errors_command(ctx):
    """Shows recent command errors and saved error receipts."""
    guild_id = ctx.guild.id if ctx.guild else None
    rows = []
    try:
        receipt_rows = await asyncio.to_thread(economy_get_receipts_for_user, None, guild_id, 25)
        rows = [row for row in receipt_rows if row.get("action") == "command_error"]
    except Exception:
        rows = []
    live_lines = []
    for event in list(command_error_events)[:6]:
        if guild_id is not None and event.get("guild_id") != guild_id:
            continue
        if is_superowner_help_command(str(event.get("command") or "")) and not can_see_superowner_help(ctx.author, ctx.guild):
            continue
        ts = discord.utils.format_dt(event["created_at"], "R")
        live_lines.append(f"`{event['receipt_id']}` `{event['command']}` · <@{event['user_id']}> · {ts}\n{embed_value(event['error'], 180)}")
    receipt_lines = []
    for row in rows[:8]:
        details = str(row.get("details") or "")
        if not can_see_superowner_help(ctx.author, ctx.guild):
            tokens = re.findall(r"[A-Za-z][A-Za-z0-9_-]{1,32}", details)
            if any(is_superowner_help_command(token) for token in tokens):
                continue
        created = row.get("created_at")
        ts = discord.utils.format_dt(created.replace(tzinfo=timezone.utc), "R") if created else "unknown"
        receipt_lines.append(f"`{row['receipt_id']}` <@{row['actor_id']}> · {ts}\n{embed_value(row.get('details') or 'No details.', 180)}")
    embed = standard_embed(
        "Error Log",
        description="Recent command failures with clean receipt IDs for follow-up.",
        color=discord.Color.red(),
        icon=economy_q_error_log,
    )
    embed.add_field(name="This Restart", value=joined_embed_value(live_lines, empty="No live errors tracked.", limit=1700), inline=False)
    embed.add_field(name="Saved Receipts", value=joined_embed_value(receipt_lines, empty="No saved error receipts found.", limit=1700), inline=False)
    embed.set_footer(text=f"Use {prefix_for_guild(ctx.guild)}receipt <id> to inspect a saved receipt.")
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="dbaudit", aliases=["databaseaudit"])
@is_admin_power()
async def dbaudit_command(ctx):
    """Checks for blocking database patterns and DB worker pressure."""
    sync_findings = await asyncio.to_thread(sync_db_audit, 20)
    embed = standard_embed(
        "Database Audit",
        description="Static scan for DB calls that can block the event loop, plus current worker settings.",
        color=discord.Color.orange(),
        icon=economy_q_database,
    )
    embed.add_field(name="DB Workers", value=f"`PROQUE_DB_WORKERS` = **{DB_WORKER_LIMIT}**", inline=True)
    embed.add_field(name="Finding Count", value=f"**{len(sync_findings):,}**", inline=True)
    embed.add_field(
        name="Blocking Risk",
        value=joined_embed_value(sync_findings, empty=f"{economy_q_accept} No obvious direct DB calls in async hot paths.", limit=2500),
        inline=False,
    )
    embed.set_footer(text="This is a code scan, not a live database benchmark. Pair it with .perf for runtime behavior.")
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="aiguard", aliases=["aicommandsafety"])
async def aiguard_command(ctx):
    """Shows how AI command execution is guarded."""
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("AI command guard details are not available here."), allowed_mentions=discord.AllowedMentions.none())
    embed = standard_embed(
        "AI Command Guard",
        description="How Pro𝚀𝚞𝚎 decides whether the AI can run, preview, or refuse bot actions.",
        color=discord.Color.blurple(),
        icon=economy_q_trust,
    )
    embed.add_field(name="Safe / Readable", value=f"**{len(AI_SAFE_COMMANDS):,}** command names and aliases", inline=True)
    embed.add_field(name="Needs Confirmation", value=f"**{len(AI_CONFIRM_COMMANDS):,}** command names", inline=True)
    embed.add_field(name=f"{QUE_OWNER_DISPLAY} Only", value=f"**{len(AI_SUPEROWNER_ONLY_COMMANDS):,}** command names", inline=True)
    embed.add_field(name="Blocked", value=joined_embed_value(sorted(f"`{name}`" for name in AI_BLOCKED_COMMANDS), limit=800), inline=False)
    embed.add_field(name="Pending AI Actions", value=f"**{len(pending_ai_batch_actions):,}** waiting for confirmation", inline=True)
    embed.add_field(name="Recent AI Actions", value=f"**{len(ai_action_history):,}** kept in memory", inline=True)
    embed.set_footer(text="Meaning matters first; vague requests ask for clarification instead of guessing sensitive actions.")
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="perms", aliases=["permissions", "permcheck"])
async def perms_command(ctx, member: discord.Member = None):
    """Shows what Pro𝚀𝚞𝚎 permissions a member has."""
    if ctx.guild is None:
        return await ctx.send("Permissions only work in servers.")
    target = member or ctx.author
    is_que = has_super_owner_power(target, ctx.guild)
    is_owner = ctx.guild.owner_id == target.id
    is_admin = bool(getattr(target.guild_permissions, "administrator", False))
    can_prefix = can_manage_prefix(target, ctx.guild)
    can_logs = has_owner_power(target, ctx.guild)
    can_bday = await can_manage_birthday_channel(target, ctx.guild)
    can_activity = await can_manage_activity_channel(target, ctx.guild)
    economy_admin = target.id == super_owner_id or is_owner or is_admin
    rank = QUE_OWNER_DISPLAY if is_que else ("Server owner" if is_owner else ("Admin" if is_admin else "Normal user"))
    embed = discord.Embed(
        title=f"{economy_q_permissions} Permission Check",
        description=f"{target.mention}\nRank: **{rank}**",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Moderation/Admin", value="Yes" if has_owner_power(target, ctx.guild) else "No", inline=True)
    embed.add_field(name="𝚀𝚞𝚎wo Admin", value="Yes" if economy_admin else "No", inline=True)
    embed.add_field(name="Prefix", value="Can change" if can_prefix else "Cannot change", inline=True)
    embed.add_field(name="Logs", value="Can manage" if can_logs else "Cannot manage", inline=True)
    embed.add_field(name="Birthdays", value="Can manage" if can_bday else "Cannot manage", inline=True)
    embed.add_field(name="Activity", value="Can manage" if can_activity else "Cannot manage", inline=True)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="sessions", aliases=["gamesessions", "activesessions"])
@is_admin_power()
async def sessions_command(ctx, action: str = None, message_id: int = None):
    """Lists or clears tracked active game sessions."""
    sessions = await asyncio.to_thread(load_active_game_sessions)
    if ctx.guild:
        sessions = [session for session in sessions if session["guild_id"] == ctx.guild.id]
    if action and action.casefold() in {"clear", "end", "delete"}:
        if message_id is None:
            return await ctx.send("Use `.sessions clear <message id>`.")
        ok = await asyncio.to_thread(delete_active_game_session, message_id)
        return await ctx.send(f"{economy_q_accept if ok else economy_q_warning} Session `{message_id}` cleared.")
    embed = discord.Embed(
        title=f"{economy_q_game_stats} Active Game Sessions",
        description=f"{len(sessions):,} tracked session(s) in this server.",
        color=discord.Color.blurple(),
    )
    lines = []
    for session in sessions[:15]:
        created = session.get("created_at")
        created_text = f" <t:{int(created.replace(tzinfo=timezone.utc).timestamp())}:R>" if created else ""
        players = ", ".join(f"<@{player}>" for player in session.get("players", [])[:4]) or "No players saved"
        lines.append(
            f"`{session['message_id']}` **{session['game_key']}** in <#{session['channel_id']}>{created_text}\n{players}"
        )
    add_text = joined_embed_value(lines) if lines else "No active sessions tracked."
    embed.add_field(name="Sessions", value=add_text, inline=False)
    embed.set_footer(text=f"Use {prefix_for_guild(ctx.guild)}sessions clear <message id> to remove a stuck session.")
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="health", aliases=["bothealth", "statuscheck"])
@is_admin_power()
async def health_command(ctx):
    """Shows bot process and background task health."""
    sessions = await asyncio.to_thread(load_active_game_sessions)
    db_status = "Ready" if getattr(economy_module, "db_ready", False) else "Loading"
    embed = discord.Embed(
        title=f"{economy_q_perf} Bot Health",
        description="Runtime status for Pro𝚀𝚞𝚎. If something feels haunted, start here.",
        color=discord.Color.green(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Latency", value=f"{round(bot.latency * 1000):,}ms", inline=True)
    embed.add_field(name="Guilds", value=f"{len(bot.guilds):,}", inline=True)
    embed.add_field(name="Commands", value=f"{len(list(bot.commands)):,}", inline=True)
    embed.add_field(name="𝚀𝚞𝚎wo DB", value=db_status, inline=True)
    embed.add_field(name="Birthday Task", value="Running" if birthday_task and not birthday_task.done() else "Stopped", inline=True)
    embed.add_field(name="Activity Task", value="Running" if activity_task and not activity_task.done() else "Stopped", inline=True)
    embed.add_field(name="Presence Task", value="Running" if presence_rotation_task.is_running() else "Stopped", inline=True)
    embed.add_field(name="Slash Commands", value="Disabled / cleared" if slash_commands_synced else "Pending clear", inline=True)
    embed.add_field(name="Active Sessions", value=f"{len(sessions):,}", inline=True)
    embed.add_field(name="Activity Reports", value=f"{len(guild_activity_channels):,}", inline=True)
    running_jobs = len([job for job in background_jobs.values() if job.get("status") == "running"])
    failed_jobs = len([job for job in background_jobs.values() if job.get("status") == "failed"])
    embed.add_field(name="Background Jobs", value=f"{running_jobs:,} running · {failed_jobs:,} failed", inline=True)
    embed.add_field(name="Recent Errors", value=f"{len(command_error_events):,}", inline=True)
    slow_count = len(slow_command_events)
    queue_count = len(recent_queue_events)
    embed.add_field(name="Performance Watch", value=f"{slow_count:,} slow · {queue_count:,} queued", inline=True)
    try:
        lottery_configs = await asyncio.to_thread(lambda: len([g for g in bot.guilds if economy_get_lottery_config(g.id)]))
    except Exception:
        lottery_configs = "DB unavailable"
    embed.add_field(name="Active Lotteries", value=str(lottery_configs), inline=True)
    embed.add_field(
        name="Recovery Covers",
        value="Timers, polls, giveaways, alarms, game sessions, lottery panels/draws, and economy events.",
        inline=False,
    )
    embed.set_footer(text=f"Use {prefix_for_guild(ctx.guild)}recover after reconnect weirdness, {prefix_for_guild(ctx.guild)}perf for speed, {prefix_for_guild(ctx.guild)}errors for failures.")
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

TRUTH_OR_DARE_TRUTHS = [
    "What is something tiny that instantly annoys you?",
    "What is the funniest thing you believed as a kid?",
    "Who in this server would survive a zombie apocalypse the longest, and why?",
    "What is one thing you pretend to understand but absolutely do not?",
    "What is the most random screenshot in your camera roll?",
    "What is a song you like but would skip if someone was watching?",
    "What is the dumbest reason you ever got embarrassed?",
    "If your last three emojis described your week, how bad is it?",
    "What is a harmless secret you can admit right now?",
    "Which fictional character are you weirdly similar to?",
    "What is the worst food take you secretly stand by?",
    "What is one habit you know is chaotic but refuse to fix?",
    "Who here gives main-character energy?",
    "What is the most unserious thing you spent money on?",
    "What would your villain origin story be?",
    "What is a compliment you still remember?",
    "What is your most irrational fear?",
    "What is the most awkward message you have ever sent to the wrong person?",
    "If you had to delete one app for a month, which one would hurt most?",
    "What is something you are better at than people expect?",
    "What is the most dramatic thing you have done over something small?",
    "What is a message you typed and then deleted?",
    "What is your most specific comfort food?",
    "What is a trend you secretly enjoyed?",
    "What is a trend you never understood?",
    "Who here would be the worst at keeping a secret?",
    "Who here would be the best at planning a surprise?",
    "What is the most suspicious thing in your search history that has a normal explanation?",
    "What is one thing you would instantly buy if money was not real?",
    "What is a nickname you would never accept?",
    "What is the funniest typo you remember sending?",
    "What is one opinion you have that would start a silly argument?",
    "What is a game you are weirdly competitive about?",
    "What is the most questionable outfit phase you had?",
    "What is something you are too lazy to learn but should?",
    "Who here would be the group detective?",
    "Who here would accidentally cause the problem and then solve it?",
    "What is your most used excuse?",
    "What is a random skill you wish you had?",
    "What is the worst advice you have ever followed?",
    "What is a harmless lie you tell yourself too often?",
    "What is the most chaotic notification you could receive right now?",
    "What is something you liked before it became popular?",
    "What is something popular that you still do not get?",
    "What is a food combo you know sounds illegal but tastes good?",
    "What is your biggest green flag?",
    "What is your funniest red flag?",
    "What is one thing that instantly makes you trust someone?",
    "What is one thing that instantly makes you suspicious?",
    "What is the most awkward way you have ended a conversation?",
    "What is one thing you wish people asked you about more?",
    "What is something you have never admitted in a group chat?",
    "What is the most cursed item in your room?",
    "Who here would be the best game show host?",
    "Who here would fold first in a horror movie?",
    "What is something you would do if embarrassment did not exist?",
    "What is a compliment you are bad at accepting?",
    "What is a small win you are proud of?",
    "What is something you always overthink?",
    "What is the weirdest reason you stayed up too late?",
    "What is your most unserious goal?",
    "What is one thing you are secretly good at?",
    "What is something you are secretly terrible at?",
    "What is a word or phrase you say too much?",
    "What is the last thing that made you laugh harder than it should have?",
    "If your personality had a warning label, what would it say?",
    "What is one thing you would never put in your bio but is true?",
    "What is the most random hill you will die on?",
    "What is your villain weakness?",
    "What is a tiny luxury you love?",
    "What is something you judge people for even though you probably should not?",
    "Who here would be easiest to prank?",
    "What is one thing you would erase from your memory just to experience again?",
    "What is a song lyric you always sing wrong?",
    "What is your most irrational loyalty?",
    "What is a decision you made way too confidently?",
    "What is the funniest thing you misunderstood recently?",
    "What is something you would ask your future self?",
    "What is a small thing that can fix your mood instantly?",
    "What is one habit you picked up from someone else?",
    "What is the most ridiculous thing you have done to avoid being awkward?",
    "What is something you would only admit after midnight?",
    "What is the most dramatic overreaction you have had recently?",
    "What is the most embarrassing thing you have searched because you forgot the obvious answer?",
    "What is a secret talent that is useful almost nowhere?",
    "What is the weirdest compliment you would actually enjoy receiving?",
    "What is one thing you would never do on camera?",
    "What is a lie you told as a kid that got way too complicated?",
    "What is one thing you still do even though you know it makes no sense?",
    "What is a smell that instantly brings back memories?",
    "What is the most suspicious thing you have done while being completely innocent?",
    "What is a movie or show you quote too often?",
    "What is the strangest dream you remember?",
    "What is a small thing that makes you irrationally happy?",
    "What is one thing you would ban from existing for a week?",
    "What is the most embarrassing phase you survived?",
    "What is something you thought would be easy but humbled you fast?",
    "What is a secret rule you follow in your head?",
    "What is the funniest thing you have overheard?",
    "What is a weird thing you are picky about?",
    "What is one thing you have always wanted to ask someone but never did?",
    "What is a harmless thing you would be famous for if everyone knew?",
    "What is your most useless strong opinion?",
    "What is something you always notice about people first?",
    "What is one thing you pretend not to care about but definitely do?",
    "What is the most chaotic thing you have done while tired?",
    "What is a random memory that lives rent-free in your head?",
    "What is one thing you would never let your younger self post online?",
    "What is the funniest thing you have taken personally?",
    "What is a weird flex you secretly have?",
    "What is the worst excuse you have actually used?",
    "What is something you would be terrible at explaining to an alien?",
    "What is one thing you are glad nobody recorded?",
    "What is a moment where you had to act normal but absolutely were not normal?",
    "What is something you would confess if everyone promised not to react?",
    "What is the most random reason you have laughed in public?",
    "What is one thing that makes you instantly competitive?",
    "What is your most questionable comfort show or video?",
    "What is something you would do for a dare but regret immediately?",
    "What is one truth about you that sounds fake?",
    "Who is the last person you DM'd, and what is the general vibe of the conversation?",
    "What is the funniest recent message you sent on Discord?",
    "What Discord status would describe your life right now?",
    "What is the most chaotic server name you have ever seen?",
    "What is your most-used Discord reaction lately?",
    "What is a Discord notification you would be scared to open?",
    "What is the weirdest reason you have muted a server?",
    "What is your most suspicious Discord habit?",
    "What is the funniest thing you have seen in a server chat?",
    "What type of Discord user are you: lurker, yapper, chaos starter, or helper?",
    "What is one channel name that would describe your mood today?",
    "What is the most dramatic Discord argument you have witnessed without naming names?",
    "What is your most embarrassing Discord typo?",
    "What is a server rule you secretly think is funny?",
    "What is the last message you almost sent but decided not to?",
    "What is the funniest Discord nickname you have had or seen?",
    "What is one emoji reaction that feels personal when someone uses it?",
    "What is the weirdest thing in your Discord saved images or downloads?",
    "What is a Discord habit you judge but also do yourself?",
    "What is the most random DM you have ever received?",
]

TRUTH_OR_DARE_DARES = [
    "Send your most-used emoji with no context.",
    "Type your next message like a dramatic movie trailer.",
    "Give someone in chat a fake award title.",
    "Let the chat pick your display name vibe for 10 minutes.",
    "Speak only in questions for your next 3 messages.",
    "Send a one-sentence apology to your sleep schedule.",
    "Describe your day using only 5 words.",
    "React to the last 3 messages with your honest mood.",
    "Make a tiny sales pitch for the nearest object to you.",
    "Write a fake weather report for the server.",
    "Send a message with zero vowels.",
    "Compliment the person above you, but make it oddly specific.",
    "Use only lowercase for your next 5 messages.",
    "Confess to a crime you clearly did not commit.",
    "Make up a rumor about someone here being secretly powerful.",
    "Send a terrible motivational quote.",
    "Rate your current energy from sleepy potato to final boss.",
    "Post a random fun fact you know.",
    "Write a two-line poem about the last snack or drink you had.",
    "Let someone choose a word you must include in your next 3 messages.",
    "Send your next message as if you are a detective revealing the truth.",
    "Give the person above you a dramatic theme song title.",
    "Type a fake sponsor ad for this chat.",
    "Rename yourself in one sentence without changing your actual nickname.",
    "Send a message that sounds like a fortune cookie.",
    "Write a dramatic goodbye to an object near you.",
    "Use the word `sparkle` naturally in your next message.",
    "Give someone here a completely fake superpower.",
    "Send a compliment that sounds strangely official.",
    "Write your next message like a pirate with Wi-Fi.",
    "Give the chat a fake breaking-news headline.",
    "Make a tiny roast about yourself.",
    "Ask the chat a question that sounds deep but is actually silly.",
    "Use only 3 words in your next message.",
    "Send your current mood as a movie title.",
    "Turn the last message into a fake law.",
    "Give the person above you a dramatic villain title.",
    "Write a fake apology to your keyboard.",
    "Say something nice about the last person who sent a message.",
    "Send a message that includes exactly one emoji.",
    "Pretend you are announcing an award and nominate someone here.",
    "Describe the server as a restaurant review.",
    "Make your next message sound like it came from a fantasy quest.",
    "Give the group a team name.",
    "Send a fake prophecy about the next 10 minutes.",
    "Use all caps for one short sentence only.",
    "Write a one-line diss track about your own sleep schedule.",
    "Ask someone here for their official opinion on a useless topic.",
    "Send a message using no letter `e`.",
    "Give someone a fake job title based on their vibe.",
    "Write a dramatic courtroom objection about the last topic.",
    "Send a random object near you a thank-you note.",
    "Make up a fake menu item named after someone in chat.",
    "Write a warning label for yourself.",
    "Send the most mysterious sentence you can think of.",
    "Explain your day like a patch note.",
    "Write a tiny poem about the last emoji used.",
    "Send a message that sounds like a boss fight intro.",
    "Give someone here a fake side quest.",
    "Make a prediction about the next person to type.",
    "Type your next message like you are whispering a secret.",
    "Send an intentionally bad slogan for this server.",
    "Describe your current energy as a weather forecast.",
    "Invent a fake holiday and tell us how to celebrate it.",
    "Give a motivational speech to the nearest snack or drink.",
    "Write one sentence that includes `legend`, `oops`, and `sparkly`.",
    "Pretend to be a mysterious narrator for one message.",
    "Make a fake achievement for someone in chat.",
    "Send a message that starts with `Plot twist:`.",
    "Write a haiku about procrastination.",
    "Give the chat a fake safety announcement.",
    "Describe someone here as a video game character class.",
    "Send the most dramatic `brb` you can.",
    "Create a fake conspiracy theory about why the chat is quiet.",
    "Write a one-sentence review of today.",
    "Give yourself a fake royal title.",
    "Send a message with exactly 7 words.",
    "Make up a new server rule that is harmless and silly.",
    "Write a tiny villain monologue about losing a game.",
    "Ask a yes/no question that everyone can answer quickly.",
    "Send a voice-note-style sentence without actually sending audio.",
    "Tell the chat to stop doing something nobody is doing.",
    "Make up a fake emergency about something tiny.",
    "Describe your current face without using the words face, eyes, mouth, or nose.",
    "Send a message like you just discovered gravity.",
    "Give someone a fake warning label.",
    "Write one sentence like an evil genius who forgot their plan.",
    "Send your best fake celebrity apology.",
    "Make a normal sentence sound suspicious.",
    "Write a dramatic review of water.",
    "Challenge someone to a completely pointless duel.",
    "Send a message like you are trying not to laugh.",
    "Tell a tiny lie and immediately expose yourself.",
    "Make up a fake name for your current mood.",
    "Send a message that could be the title of a terrible reality show.",
    "Give someone a fake horoscope.",
    "Act offended by the nearest object.",
    "Write a one-line breakup text to a bad habit.",
    "Send a message like a confused time traveler.",
    "Invent a fake app and pitch it badly.",
    "Give the group a fake mission for the next 5 minutes.",
    "Use an overly fancy word in a very normal sentence.",
    "Send a message as if you are the final boss of doing nothing.",
    "Make up a fake fun fact and label it as fake.",
    "Give someone a harmless dare of your own.",
    "Write a sentence that starts normal and ends dramatic.",
    "Describe a random object like it is priceless art.",
    "Send your best fake evil laugh in text.",
    "Make a wild accusation about yourself.",
    "Write a warning announcement for your current mood.",
    "Tell the chat your fake campaign promise.",
    "Create a fake nickname for the person below you.",
    "Send a message using exactly one punctuation mark.",
    "Write a sentence like a soap opera character.",
    "Make the next normal thing someone says sound dramatic.",
    "Send a message that sounds like it was translated badly twice.",
    "Give a fake acceptance speech for an award you did not win.",
    "Make a silly confession in the most serious tone possible.",
    "Send a message like you are trying to distract everyone.",
    "Write a tiny dramatic monologue about being hungry.",
    "Show the last 5 messages from the last person you DM'd.",
    "Send your last 3 used emojis and let the chat judge the story.",
    "Screenshot your Discord status picker or type what your current status should be.",
    "React to the next 5 messages with only one emoji.",
    "Change your Discord status text for 10 minutes to something the chat chooses.",
    "Send the most recent Discord screenshot in your camera roll.",
    "Ping nobody, but write a fake dramatic announcement for the whole server.",
    "Reply to the last message like it was a serious official report.",
    "Type your next message like a Discord moderator making a tiny announcement.",
    "Send a fake server rule that sounds real for half a second.",
    "Let the chat choose one emoji you must react with for the next 5 minutes.",
    "Show your top 5 recently used emojis, or type them if you cannot screenshot.",
    "Scroll up a little and quote a random harmless message with no context.",
    "Send a message that looks like it belongs in a channel called `bad-ideas`.",
    "Make up a fake channel name for the current conversation.",
    "Write a fake Discord notification that would make everyone panic.",
    "Describe the person above you using only Discord channel names.",
    "Send a message like you accidentally typed it in the wrong server.",
    "Ask the chat to vote with reactions on a completely useless question.",
    "Write a fake timeout reason for yourself.",
]

TRUTH_OR_DARE_RECENT_LIMIT = 80
truth_or_dare_recent_prompts = defaultdict(lambda: deque(maxlen=TRUTH_OR_DARE_RECENT_LIMIT))

def truth_or_dare_history_key(location, mode):
    guild = getattr(location, "guild", None)
    channel = getattr(location, "channel", None)
    guild_id = int(getattr(guild, "id", 0) or 0)
    channel_id = int(getattr(channel, "id", 0) or 0)
    return (guild_id, channel_id, mode)

def truth_or_dare_pick(mode, location=None):
    mode = str(mode or "random").casefold()
    if mode not in {"truth", "dare"}:
        mode = random.choice(["truth", "dare"])
    pool = TRUTH_OR_DARE_TRUTHS if mode == "truth" else TRUTH_OR_DARE_DARES
    key = truth_or_dare_history_key(location, mode)
    recent = truth_or_dare_recent_prompts[key]
    available = [prompt for prompt in pool if prompt not in recent]
    if not available:
        recent.clear()
        available = list(pool)
    prompt = random.choice(available)
    recent.append(prompt)
    return mode, prompt

def truth_or_dare_embed(mode, prompt, target):
    is_truth = mode == "truth"
    icon = economy_q_thinking if is_truth else economy_q_target
    title = "Truth" if is_truth else "Dare"
    target_text = target.mention if target else "Someone"
    embed = standard_embed(
        f"{title}",
        description=f"{icon} {target_text}\n\n**{prompt}**",
        color=discord.Color.blurple() if is_truth else discord.Color.orange(),
        icon=icon,
    )
    embed.set_footer(text="Use the buttons for another prompt.")
    return embed

def truth_or_dare_channel_text(guild):
    channels = sorted(guild_truth_or_dare_channels(guild))
    if not channels:
        return "All channels"
    parts = []
    for channel_id in channels:
        channel = guild.get_channel(int(channel_id)) if guild else None
        parts.append(channel.mention if channel else f"`{channel_id}`")
    return joined_embed_value(parts, limit=900)

async def truth_or_dare_channel_allowed(location):
    guild = getattr(location, "guild", None)
    channel = getattr(location, "channel", None)
    if guild is None:
        return True
    allowed = guild_truth_or_dare_channels(guild)
    return not allowed or int(getattr(channel, "id", 0) or 0) in allowed

async def send_truth_or_dare_channel_denial(destination, guild):
    message = f"{economy_q_reject} Truth or Dare is set to: {truth_or_dare_channel_text(guild)}"
    if isinstance(destination, discord.Interaction):
        if destination.response.is_done():
            await destination.followup.send(message, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        else:
            await destination.response.send_message(message, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
    else:
        await destination.send(message, allowed_mentions=discord.AllowedMentions.none())

async def resolve_truth_or_dare_channels(ctx, raw):
    channels = []
    seen = set()
    for channel in getattr(ctx.message, "channel_mentions", []) or []:
        if isinstance(channel, discord.TextChannel) and channel.id not in seen:
            channels.append(channel)
            seen.add(channel.id)
    cleaned = re.sub(r"<#\d+>", " ", str(raw or ""))
    for token in shlex.split(cleaned):
        if token.casefold() in {"add", "set", "remove", "rm", "delete", "clear", "off", "status", "list", "here"}:
            continue
        try:
            channel = await commands.TextChannelConverter().convert(ctx, token)
        except commands.BadArgument:
            continue
        if channel.id not in seen:
            channels.append(channel)
            seen.add(channel.id)
    return channels

@bot.command(name="todchannel", aliases=["todchannels", "truthdarechannel", "truthordarechannel", "settdchannel"])
@is_admin_power()
async def truth_or_dare_channel_command(ctx, action: str = None, *, raw_channels: str = None):
    """Sets allowed Truth or Dare channels for this server."""
    if ctx.guild is None:
        return await ctx.send("Truth or Dare channel settings only work in servers.")
    prefix = prefix_for_guild(ctx.guild)
    action_key = str(action or "status").casefold()
    allowed = set(guild_truth_or_dare_channels(ctx.guild))
    valid_actions = {"status", "list", "show", "clear", "off", "disable", "all", "anywhere", "here", "this", "set", "only", "add", "allow", "include", "remove", "rm", "delete", "del"}
    if action and action_key not in valid_actions:
        raw_channels = f"{action} {raw_channels or ''}".strip()
        action_key = "set"

    if action_key in {"status", "list", "show"}:
        return await ctx.send(
            f"{economy_q_target} Truth or Dare Channels: **{truth_or_dare_channel_text(ctx.guild)}**\n"
            f"Use `{prefix}todchannel add #channel`, `{prefix}todchannel set #one #two`, `{prefix}todchannel remove #channel`, or `{prefix}todchannel clear`.",
            allowed_mentions=discord.AllowedMentions.none(),
        )

    if action_key in {"clear", "off", "disable", "all", "anywhere"}:
        allowed.clear()
        await asyncio.to_thread(save_truth_or_dare_channels, ctx.guild.id, allowed)
        return await ctx.send(f"{economy_q_target} Truth or Dare can now be used in **all channels**.")

    if action_key in {"here", "this"}:
        raw_channels = "here"
        action_key = "set"

    if raw_channels and raw_channels.strip().casefold() in {"here", "this"}:
        channels = [ctx.channel]
    else:
        channels = await resolve_truth_or_dare_channels(ctx, raw_channels or "")

    if not channels:
        return await ctx.send(
            f"{economy_q_reject} Pick at least one channel.\n"
            f"Examples: `{prefix}todchannel set #truth-or-dare #games`, `{prefix}todchannel add here`, `{prefix}todchannel clear`.",
            allowed_mentions=discord.AllowedMentions.none(),
        )

    if action_key in {"set", "only"}:
        allowed = {channel.id for channel in channels}
    elif action_key in {"add", "allow", "include"}:
        allowed.update(channel.id for channel in channels)
    elif action_key in {"remove", "rm", "delete", "del"}:
        allowed.difference_update(channel.id for channel in channels)
    else:
        return await ctx.send(
            f"{economy_q_reject} Use `set`, `add`, `remove`, `clear`, or `status`.\n"
            f"Example: `{prefix}todchannel set #truth-or-dare #games`.",
            allowed_mentions=discord.AllowedMentions.none(),
        )

    await asyncio.to_thread(save_truth_or_dare_channels, ctx.guild.id, allowed)
    guild_truth_or_dare_channels(ctx.guild).clear()
    guild_truth_or_dare_channels(ctx.guild).update(allowed)
    await ctx.send(
        f"{economy_q_target} Truth or Dare Channels: **{truth_or_dare_channel_text(ctx.guild)}**",
        allowed_mentions=discord.AllowedMentions.none(),
    )

class TruthOrDareView(View):
    def __init__(self, target_id=None):
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
        self.target_id = int(target_id) if target_id else None

    def target_for(self, interaction):
        if not self.target_id:
            return interaction.user
        return interaction.guild.get_member(self.target_id) if interaction.guild else None

    async def roll(self, interaction, mode):
        if not await truth_or_dare_channel_allowed(interaction):
            return await send_truth_or_dare_channel_denial(interaction, interaction.guild)
        selected_mode, prompt = truth_or_dare_pick(mode, interaction)
        await interaction.response.send_message(
            embed=truth_or_dare_embed(selected_mode, prompt, self.target_for(interaction)),
            view=TruthOrDareView(self.target_id),
            allowed_mentions=discord.AllowedMentions.none(),
        )

    @discord.ui.button(label="Truth", emoji=economy_q_thinking, style=discord.ButtonStyle.primary)
    async def truth_button(self, interaction, button):
        await self.roll(interaction, "truth")

    @discord.ui.button(label="Dare", emoji=economy_q_target, style=discord.ButtonStyle.danger)
    async def dare_button(self, interaction, button):
        await self.roll(interaction, "dare")

    @discord.ui.button(label="Random", emoji=economy_q_refresh, style=discord.ButtonStyle.secondary)
    async def random_button(self, interaction, button):
        await self.roll(interaction, "random")

@bot.command(name="truthordare", aliases=["tod", "truth", "dare"])
async def truth_or_dare_command(ctx, *, raw: str = None):
    """Starts a Truth or Dare prompt with buttons."""
    if not await truth_or_dare_channel_allowed(ctx):
        return await send_truth_or_dare_channel_denial(ctx, ctx.guild)
    invoked = str(ctx.invoked_with or "").casefold()
    text = str(raw or "").strip()
    target = next((member for member in getattr(ctx.message, "mentions", []) if not member.bot), None)
    cleaned = re.sub(r"<@!?\d+>", " ", text).strip()
    tokens = cleaned.split()
    mode = "random"
    if invoked in {"truth", "dare"}:
        mode = invoked
    elif tokens and tokens[0].casefold() in {"truth", "t", "dare", "d", "random", "r"}:
        first = tokens[0].casefold()
        mode = {"t": "truth", "d": "dare", "r": "random"}.get(first, first)
    selected_mode, prompt = truth_or_dare_pick(mode, ctx)
    await ctx.send(
        embed=truth_or_dare_embed(selected_mode, prompt, target or ctx.author),
        view=TruthOrDareView(target.id if target else None),
        allowed_mentions=discord.AllowedMentions.none(),
    )

GAME_MENU = [
    ("PvP", "Tic Tac Toe", None, "`.ttt @user [bet]`", "Quick 2-player strategy. Supports bets."),
    ("PvP", "Connect 4", None, "`.c4 @user [bet]`", "Column strategy game. Supports bets."),
    ("PvP", "Chess", None, "`.chess @user [bet]`", "Full chess with UI moves and 10-minute clocks."),
    ("Party", "Truth or Dare", None, "`.tod [truth/dare/random] [@user]`", "Social prompts with Truth, Dare, and Random buttons."),
    ("Skill", "Tower", "tower", "`.tower <amount>`", "Climb floors or cash out."),
    ("Skill", "Vault", "vault", "`.vault <amount>`", "Think through code hints before tries run out."),
    ("Skill", "Memory", "memory", "`.memory <amount>`", "Match pairs before too many mistakes."),
    ("Skill", "Card Ladder", "cardladder", "`.cardladder <amount>`", "Higher/lower card climb with cash-out."),
    ("Skill", "Lockpick", "lockpick", "`.lockpick <amount>`", "Set pins using high/low hints before tries run out."),
    ("Skill", "Minesweeper", "ms", "`.ms <amount>`", "Reveal safe tiles and avoid bombs."),
    ("Luck", "Coin Flip", "cf", "`.cf <amount> h/t`", "Pick heads or tails."),
    ("Luck", "Roulette", "roulette", "`.roulette <amount>`", "Pick a color and spin."),
    ("Luck", "Slots", "slots", "`.slots <amount>`", "Spin 3 matching reels."),
    ("Luck", "Blackjack", "blackjack", "`.blackjack <amount>`", "Hit or stand against the dealer."),
    ("Luck", "Scratch", "scratch", "`.scratch <amount>`", "Reveal 5 matching symbols."),
    ("Luck", "Wheel", "wheel", "`.wheel <amount>`", "Spin for multiplier segments."),
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
    "Party": lambda item: item[0] == "Party",
    "Skill": lambda item: item[0] == "Skill",
    "Luck": lambda item: item[0] == "Luck",
    "Free": lambda item: item[2] in {"dungeon"} or item[1] in {"Flag Quiz", "Picker"},
    "High Risk": lambda item: item[2] and economy_risk_label(item[2]).casefold() in {"high", "extreme", "medium/high", "skill/high"},
    "No Bet": lambda item: item[2] in {"dungeon"} or item[1] in {"Flag Quiz", "Picker"},
}

def games_embed(prefix=".", selected_filter="All", balance=None):
    balance_bucket = "unknown" if balance is None else ("low" if int(balance or 0) < 250_000 else "mid" if int(balance or 0) < 2_000_000 else "high")
    key = (prefix, selected_filter, balance_bucket)
    now = time.monotonic()
    cached = games_render_cache.get(key)
    if cached and now - cached[0] < HELP_RENDER_CACHE_TTL:
        return clone_embed(cached[1])
    filter_fn = GAME_FILTERS.get(selected_filter, GAME_FILTERS["All"])
    embed = standard_embed(
        f"Games - {selected_filter}",
        description="Pick a vibe, then pick a game. Free/skill games first if your balance is crying.",
        color=discord.Color.green(),
        icon=economy_q_game_win,
    )
    if balance is not None:
        if int(balance or 0) < 250_000:
            recs = [f"`{prefix}dungeon`", f"`{prefix}flagquiz`", f"`{prefix}memory 1k`"]
        elif int(balance or 0) < 2_000_000:
            recs = [f"`{prefix}tower 25k`", f"`{prefix}memory 25k`", f"`{prefix}blackjack 25k`"]
        else:
            recs = [f"`{prefix}tower 100k`", f"`{prefix}plinko 100k`", f"`{prefix}jackpotspin 100k`"]
        embed.add_field(name="Recommended For You", value=" · ".join(recs), inline=False)
    for category in ["PvP", "Party", "Skill", "Luck", "Solo", "Utility"]:
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
    embed.set_footer(text=f"Use {prefix}howtoplay <game> or the buttons for rules. Keep bets sane; future you is watching.")
    games_render_cache[key] = (now, clone_embed(embed))
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
        await interaction.response.edit_message(embed=games_embed(view.prefix, self.filter_name, view.balance), view=view)

class GamesRefreshButton(Button):
    def __init__(self):
        super().__init__(label="Refresh", emoji=reaction_emoji(economy_q_refresh), style=discord.ButtonStyle.secondary)

    async def callback(self, interaction):
        view = self.view
        if interaction.user.id != view.author_id:
            return await interaction.response.send_message("Use your own games menu.", ephemeral=True)
        games_render_cache.clear()
        await interaction.response.edit_message(embed=games_embed(view.prefix, view.selected_filter, view.balance), view=view)

class GamesView(View):
    def __init__(self, author_id, prefix, balance=None):
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
        self.author_id = author_id
        self.prefix = prefix
        self.balance = balance
        self.selected_filter = "All"
        self.add_item(GamesRefreshButton())
        for filter_name in ["All", "Solo", "PvP", "Party", "Skill", "Luck", "Free", "High Risk", "No Bet"]:
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
        row = next((item for item in GAME_MENU if item[1] == selected), None)
        usage = row[3] if row else None
        usage = usage.replace("`.", f"`{self.prefix}") if usage else f"`{self.prefix}help games`"
        self.selected_game = selected
        await interaction.response.send_message(f"Start with {usage}", ephemeral=True)

    @discord.ui.button(label="How To Play", emoji=economy_q_book, style=discord.ButtonStyle.success)
    async def how_to_play_button(self, interaction, button):
        selected = getattr(self, "selected_game", None)
        row = next((item for item in GAME_MENU if item[1] == selected), None) if selected else None
        if row is None:
            return await interaction.response.send_message("Pick a game from the menu first.", ephemeral=True)
        _, name, game_key, usage, desc = row
        command_key = game_key or name.casefold().replace(" ", "")
        details = economy_detailed_explanations.get(command_key) or economy_explanations.get(command_key) or desc
        embed = discord.Embed(
            title=f"{economy_q_book} How To Play: {name}",
            description=embed_value(details, 1800),
            color=discord.Color.green(),
        )
        embed.add_field(name="Start", value=usage.replace("`.", f"`{self.prefix}"), inline=False)
        if game_key:
            embed.add_field(name="Risk", value=f"**{economy_risk_label(game_key)}**", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Risk / Payouts", emoji=economy_q_game_audit, style=discord.ButtonStyle.secondary)
    async def risk_payout_button(self, interaction, button):
        selected = getattr(self, "selected_game", None)
        row = next((item for item in GAME_MENU if item[1] == selected), None) if selected else None
        if row is None:
            return await interaction.response.send_message("Pick a game from the menu first.", ephemeral=True)
        _, name, game_key, usage, desc = row
        command_key = game_key or name.casefold().replace(" ", "")
        details = economy_detailed_explanations.get(command_key) or economy_explanations.get(command_key) or desc
        embed = discord.Embed(
            title=f"{economy_q_game_audit} {name} Risk / Payouts",
            description=embed_value(details, 1800),
            color=discord.Color.orange(),
        )
        embed.add_field(name="Risk Label", value=f"**{economy_risk_label(command_key)}**" if game_key else "No bet / free game", inline=True)
        embed.add_field(name="Start", value=usage.replace("`.", f"`{self.prefix}"), inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Refresh", emoji=economy_q_refresh, style=discord.ButtonStyle.secondary)
    async def refresh_button(self, interaction, button):
        games_render_cache.clear()
        await interaction.response.edit_message(embed=games_embed(self.prefix, self.selected_filter, self.balance), view=self)

@bot.command(name="games", aliases=["gamelist"])
async def games_command(ctx):
    """Shows available games and how to start them."""
    prefix = prefix_for_guild(ctx.guild)
    balance = None
    try:
        data = await asyncio.to_thread(economy_get_user, ctx.author.id)
        balance = int(data.get("balance") or 0)
    except Exception:
        data = None
    await ctx.send(embed=games_embed(prefix, balance=balance), view=GamesView(ctx.author.id, prefix, balance))
    try:
        if data:
            await economy_module.maybe_send_tutorial(ctx, data, "games")
    except Exception:
        pass

@bot.command(name="howtoplay", aliases=["how", "rules"])
async def howtoplay_command(ctx, *, game: str = None):
    prefix = prefix_for_guild(ctx.guild)
    if not game:
        return await send_command_input_ui(ctx, "howtoplay", note=f"Enter a game name, or use the How To Play button in `{prefix}games`.")
    key = game.strip().casefold().replace(" ", "")
    lookup_keys = {key}
    command = bot.get_command(key)
    if command:
        lookup_keys.add(command.name.casefold())
        lookup_keys.add(command.qualified_name.casefold())
        lookup_keys.update(alias.casefold() for alias in getattr(command, "aliases", []) or [])
    row = next(
        (
            item for item in GAME_MENU
            if item[1].casefold().replace(" ", "") in lookup_keys
            or (item[2] and item[2].casefold() in lookup_keys)
        ),
        None,
    )
    if row is None:
        return await ctx.send(f"Game not found. Try `{prefix}games`.")
    _, name, game_key, usage, desc = row
    command_key = game_key or name.casefold().replace(" ", "")
    details = economy_detailed_explanations.get(command_key) or economy_explanations.get(command_key) or desc
    embed = discord.Embed(title=f"{economy_q_book} How To Play: {name}", description=embed_value(details, 1800), color=discord.Color.green())
    embed.add_field(name="Start", value=usage.replace("`.", f"`{prefix}"), inline=False)
    if game_key:
        embed.add_field(name="Risk", value=f"**{economy_risk_label(game_key)}**", inline=True)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

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
FLAG_QUIZ_TIMER_SECONDS = 30
FLAG_QUIZ_DECAY_INTERVAL_SECONDS = 4
FLAG_QUIZ_DECAY_AMOUNT = 4_000
FLAG_QUIZ_MAX_DECAY = 16_000
FLAG_QUIZ_MIN_REWARD = FLAG_QUIZ_REWARD_PER_POINT - FLAG_QUIZ_MAX_DECAY
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

def flag_quiz_reward_for_elapsed(elapsed_seconds):
    elapsed_seconds = max(0, float(elapsed_seconds or 0))
    decay_steps = min(
        FLAG_QUIZ_MAX_DECAY // FLAG_QUIZ_DECAY_AMOUNT,
        int(elapsed_seconds // FLAG_QUIZ_DECAY_INTERVAL_SECONDS),
    )
    return max(
        FLAG_QUIZ_MIN_REWARD,
        FLAG_QUIZ_REWARD_PER_POINT - (decay_steps * FLAG_QUIZ_DECAY_AMOUNT),
    )

def flag_quiz_reward_note():
    return (
        f"Correct flags pay up to **{economy_format_balance(FLAG_QUIZ_REWARD_PER_POINT)}**. "
        f"Every **{FLAG_QUIZ_DECAY_INTERVAL_SECONDS}s** removes **{economy_format_balance(FLAG_QUIZ_DECAY_AMOUNT)}**, "
        f"minimum **{economy_format_balance(FLAG_QUIZ_MIN_REWARD)}**."
    )

def build_flag_quiz_embed(country, index, total, score_text, mode, hint=None, status=None, seconds_left=FLAG_QUIZ_TIMER_SECONDS):
    embed = discord.Embed(
        title=f"Flag Quiz {index}/{total}",
        description=(
            f"Mode: **{mode.title()}** | {score_text}\n"
            f"Time: **{int(seconds_left)}s**\n"
            f"{flag_quiz_reward_note()}\n"
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
        super().__init__(timeout=FLAG_QUIZ_TIMER_SECONDS)
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
    rewards_by_user = {}
    answered = {}
    countries = random.sample(FLAG_COUNTRIES, min(rounds, len(FLAG_COUNTRIES)))
    try:
        await ctx.send(
            f"{economy_q_game_win} **FLAG QUIZ**\n"
            f"Mode: **{mode.title()}** | {flag_quiz_reward_note()}\n"
            f"Type the country name. Each guess has **{FLAG_QUIZ_TIMER_SECONDS}s**. Everyone gets **2 tries** per flag in public mode. Type `skip` or `stop` anytime.",
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
            round_start = time.monotonic()
            end_time = round_start + FLAG_QUIZ_TIMER_SECONDS
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
                    elapsed_seconds = time.monotonic() - round_start
                    flag_reward = flag_quiz_reward_for_elapsed(elapsed_seconds)
                    scores[user_id] = scores.get(user_id, 0) + 1
                    rewards_by_user[user_id] = rewards_by_user.get(user_id, 0) + flag_reward
                    winner_id = user_id
                    await prompt.edit(view=None)
                    await ctx.send(
                        f"{economy_q_accept} <@{user_id}> got it: **{country['name']}**\n"
                        f"Speed reward: **{economy_format_balance(flag_reward)}**\n"
                        f"{flag_quiz_score_text(scores, ctx.author.id, mode)}",
                        allowed_mentions=discord.AllowedMentions.none()
                    )
                    break
                status = f"{economy_q_reject} <@{user_id}> missed. "
                if tries_by_user[user_id] == 1:
                    status += "**1 try left.** Want help? Press **Show Hint**."
                else:
                    status += "**No tries left for this flag.**"
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
            reward = int(rewards_by_user.get(user_id, 0))
            try:
                old_balance, new_balance = await asyncio.to_thread(economy_add_user_balance, user_id, reward, reward)
                await asyncio.to_thread(economy_record_game_result, user_id, "flagquiz", points > 0, reward, reward)
                if points > 0 and economy_todays_daily_challenge()["game"] == "flagquiz":
                    await asyncio.to_thread(economy_track_daily_challenge_progress, user_id, "flagquiz", True, points)
                await asyncio.to_thread(economy_log_transaction, user_id, "flagquiz_reward", reward, f"{points} point(s) in {mode} flag quiz; speed-adjusted")
                reward_lines.append(
                    f"<@{user_id}>: **{points}** point(s), **{economy_format_balance(reward)}** "
                    f"({economy_format_balance(old_balance)} -> {economy_format_balance(new_balance)})"
                )
            except Exception as e:
                print(f"Flag quiz reward failed for {user_id}: {type(e).__name__} - {e}")
                reward_lines.append(f"<@{user_id}>: **{points}** point(s), reward could not be paid.")
        total_points = sum(scores.values())
        accuracy = (total_points / max(1, len(countries))) * 100
        finish_header = [
            f"{economy_q_game_win} **FLAG QUIZ FINISHED**",
            f"Mode: **{mode.title()}** | Total Score: **{total_points}/{len(countries)}** ({accuracy:.1f}%)",
        ]
        if len(reward_lines) > 20 or len("\n".join([*finish_header, *reward_lines])) > 1800:
            await send_paginated_lines(
                ctx,
                "Flag Quiz Finished",
                [*finish_header, *reward_lines],
                per_page=12,
                empty="No reward earned.",
            )
            return
        await ctx.send(
            "\n".join([*finish_header, *reward_lines]),
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
        f"{economy_q_game_win} **FLAG QUIZ**\nChoose solo or public mode, then choose quiz length.\nYou can also use `{prefix}flagquiz 10`, `{prefix}flagquiz 20`, `{prefix}flagquiz 50`, or `{prefix}flagquiz all`.\nEach guess has **{FLAG_QUIZ_TIMER_SECONDS}s**. You get **2 tries** per flag, and small typos are accepted.\n{flag_quiz_reward_note()}",
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
    embed = discord.Embed(
        title=f"{economy_q_game_win} Flag Quiz Stats",
        description=(
            f"{user.mention}\n"
            f"Quizzes: **{played:,}**\n"
            f"Scoring quizzes: **{int(row['wins'] or 0):,}**\n"
            f"Rewards Earned: **{economy_format_balance(earned)}**"
        ),
        color=discord.Color.green(),
    )
    await ctx.reply(embed=embed, mention_author=False, allowed_mentions=discord.AllowedMentions.none())

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
        entry = await find_audit_entry(message.guild, {discord.AuditLogAction.message_delete}, message.author.id, limit=5, max_age_seconds=8)
        if entry:
            deleter = log_user(entry.user)

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
    await asyncio.to_thread(save_active_poll, msg.id, active_polls[msg.id])

    if end_time:
        async def end_poll_task():
            await asyncio.sleep(max(0, (end_time - datetime.now(timezone.utc)).total_seconds()))
            poll_data = active_polls.get(msg.id)
            if not poll_data or poll_data.get("ended"):
                return
            poll_data["ended"] = True
            await asyncio.to_thread(save_active_poll, msg.id, poll_data)
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
            return await interaction.followup.send(clean_user_error(e), ephemeral=True)
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
        super().__init__(timeout=LONG_SETUP_VIEW_TIMEOUT)
        self.author_id = author_id
        self.add_item(button)

async def edit_setup_source_message(interaction, source_message_id, *, content=None, embed=None, file=None, view=None):
    if not source_message_id:
        return False
    try:
        message = interaction.message
        if message is None or message.id != source_message_id:
            message = await interaction.channel.fetch_message(source_message_id)
        attachments = [file] if file is not None else []
        await message.edit(content=content, embed=embed, attachments=attachments, view=view)
        return True
    except Exception:
        return False

async def finalize_poll(msg, poll_data):
    """Updates the poll embed with final results."""
    poll_msg = None
    try:
        poll_msg = await bot.get_channel(poll_data["channel_id"]).fetch_message(msg.id)
    except:
        active_polls.pop(msg.id, None)
        await asyncio.to_thread(remove_active_poll, msg.id)
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
        await asyncio.to_thread(remove_active_poll, msg.id)

async def restore_persistent_runtime_state():
    global active_polls, active_timers, active_giveaways, active_alarms
    for poll_id, poll_data in load_active_polls().items():
        current = active_polls.get(poll_id)
        task = current.get("end_task") if current else None
        if current and task and not task.done():
            continue
        active_polls[poll_id] = poll_data
    for timer_id, timer_data in load_active_timers().items():
        current = active_timers.get(timer_id)
        task = current.get("task") if current else None
        if current and task and not task.done():
            continue
        active_timers[timer_id] = timer_data
    for giveaway_id, giveaway_data in load_active_giveaways().items():
        current = active_giveaways.get(giveaway_id)
        task = current.get("task") if current else None
        if current and task and not task.done():
            continue
        active_giveaways[giveaway_id] = giveaway_data
    for alarm_id, alarm_data in load_active_alarms().items():
        current = active_alarms.get(alarm_id)
        task = current.get("task") if current else None
        if current and task and not task.done():
            continue
        active_alarms[alarm_id] = alarm_data
    now = datetime.now(timezone.utc)

    for poll_id, poll_data in list(active_polls.items()):
        existing_task = poll_data.get("end_task")
        if existing_task and not existing_task.done():
            continue
        channel = bot.get_channel(poll_data["channel_id"])
        if channel is None:
            try:
                channel = await bot.fetch_channel(poll_data["channel_id"])
            except Exception:
                active_polls.pop(poll_id, None)
                await asyncio.to_thread(remove_active_poll, poll_id)
                continue
        try:
            message = await channel.fetch_message(poll_id)
        except Exception:
            active_polls.pop(poll_id, None)
            await asyncio.to_thread(remove_active_poll, poll_id)
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
        existing_task = timer_data.get("task")
        if existing_task and not existing_task.done():
            continue
        channel = bot.get_channel(timer_data["channel_id"])
        if channel is None:
            try:
                channel = await bot.fetch_channel(timer_data["channel_id"])
            except Exception:
                active_timers.pop(timer_id, None)
                await asyncio.to_thread(remove_active_timer, timer_id)
                continue
        try:
            message = await channel.fetch_message(timer_id)
        except Exception:
            active_timers.pop(timer_id, None)
            await asyncio.to_thread(remove_active_timer, timer_id)
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

    for giveaway_id, giveaway_data in list(active_giveaways.items()):
        existing_task = giveaway_data.get("task")
        if existing_task and not existing_task.done():
            continue
        channel = bot.get_channel(giveaway_data["channel_id"])
        if channel is None:
            try:
                channel = await bot.fetch_channel(giveaway_data["channel_id"])
            except Exception:
                active_giveaways.pop(giveaway_id, None)
                await asyncio.to_thread(remove_active_giveaway, giveaway_id)
                continue
        try:
            message = await channel.fetch_message(giveaway_id)
        except Exception:
            active_giveaways.pop(giveaway_id, None)
            await asyncio.to_thread(remove_active_giveaway, giveaway_id)
            continue

        end_time = giveaway_data["end_time"]
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
            giveaway_data["end_time"] = end_time
        giveaway_data["task"] = asyncio.create_task(
            run_giveaway(
                channel,
                max(0, int((end_time - datetime.now(timezone.utc)).total_seconds())),
                giveaway_data["prize"],
                message=message,
                end_time=end_time,
            )
        )

    for alarm_id, alarm_data in list(active_alarms.items()):
        existing_task = alarm_data.get("task")
        if existing_task and not existing_task.done():
            continue
        channel = bot.get_channel(alarm_data["channel_id"])
        if channel is None:
            try:
                channel = await bot.fetch_channel(alarm_data["channel_id"])
            except Exception:
                active_alarms.pop(alarm_id, None)
                await asyncio.to_thread(remove_active_alarm, alarm_id)
                continue
        alarm_time = alarm_data["alarm_time"]
        if alarm_time.tzinfo is None:
            alarm_time = alarm_time.replace(tzinfo=timezone.utc)
            alarm_data["alarm_time"] = alarm_time
        alarm_data["task"] = asyncio.create_task(
            alarm_wait_and_send(channel, alarm_data["user_id"], alarm_time, alarm_data.get("title"), alarm_id=alarm_id)
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

async def maybe_send_away_reaction_callout(reaction, user):
    if user.id in sleeping_users:
        status = "sleep"
        text = f"i see u reacting, <@{user.id}> get back to sleep kitten ᓚᘏᗢ zZ"
    elif user.id in afk_users:
        status = "afk"
        text = f"caught u reacting, <@{user.id}>. AFK means away from keyboard, not away from the react button."
    else:
        return

    channel = getattr(getattr(reaction, "message", None), "channel", None)
    if channel is None:
        return
    key = (status, int(user.id), int(getattr(channel, "id", 0) or 0))
    now = time.monotonic()
    if now - away_reaction_callouts.get(key, 0) < 45:
        return
    away_reaction_callouts[key] = now
    try:
        await channel.send(text, allowed_mentions=discord.AllowedMentions.none())
    except Exception:
        pass

async def maybe_send_raw_away_reaction_callout(payload):
    user_id = int(getattr(payload, "user_id", 0) or 0)
    if not user_id or (bot.user and user_id == bot.user.id):
        return
    if user_id in sleeping_users:
        status = "sleep"
        text = f"i see u reacting, <@{user_id}> get back to sleep kitten ᓚᘏᗢ zZ"
    elif user_id in afk_users:
        status = "afk"
        text = f"caught u reacting, <@{user_id}>. AFK means away from keyboard, not away from the react button."
    else:
        return

    channel_id = int(getattr(payload, "channel_id", 0) or 0)
    key = (status, user_id, channel_id)
    now = time.monotonic()
    if now - away_reaction_callouts.get(key, 0) < 45:
        return
    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
        except Exception:
            return
    away_reaction_callouts[key] = now
    try:
        await channel.send(text, allowed_mentions=discord.AllowedMentions.none())
    except Exception:
        pass

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

    await maybe_send_away_reaction_callout(reaction, user)
    await update_poll_counts(reaction.message)

    # Reaction audit logs are sent from raw reaction events so uncached messages are covered too.

@bot.event
async def on_raw_reaction_add(payload):
    try:
        await maybe_send_raw_away_reaction_callout(payload)
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

    entry = await find_audit_entry(message.guild, {discord.AuditLogAction.message_reaction_remove_all}, message.id, limit=3, max_age_seconds=15)
    remover = entry.user if entry else None

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
        return await find_audit_entry(guild, action_types, after.id, limit=10)

    if before.nick != after.nick:
        action_entry = await get_action_by({discord.AuditLogAction.member_update})
        embed = discord.Embed(
            title=f"{economy_q_user_edit} Nickname Changed",
            color=discord.Color.blue()
        )
        embed.add_field(name="User", value=log_user(before), inline=False)
        embed.add_field(name="Before", value=before.nick or before.name, inline=True)
        embed.add_field(name="After", value=after.nick or after.name, inline=True)
        add_audit_actor_fields(embed, action_entry, "Changed by")
        embed.timestamp = datetime.now(timezone.utc)
        await send_log(embed, guild)

    before_roles = set(before.roles)
    after_roles = set(after.roles)
    added = after_roles - before_roles
    removed = before_roles - after_roles
    if added or removed:
        action_entry = await get_action_by({discord.AuditLogAction.member_role_update})
        embed = discord.Embed(
            title=f"{economy_q_roles} Roles Updated",
            color=discord.Color.teal()
        )
        embed.add_field(name="User", value=log_user(after), inline=False)
        if added:
            embed.add_field(name="Added", value=embed_value("\n".join(log_role(role) for role in sorted(added, key=lambda item: item.position, reverse=True)), 1024), inline=True)
        if removed:
            embed.add_field(name="Removed", value=embed_value("\n".join(log_role(role) for role in sorted(removed, key=lambda item: item.position, reverse=True)), 1024), inline=True)
        add_audit_actor_fields(embed, action_entry, "Updated by")
        embed.timestamp = datetime.now(timezone.utc)
        await send_log(embed, guild)

    before_timeout = getattr(before, "communication_disabled_until", None)
    after_timeout = getattr(after, "communication_disabled_until", None)
    if before_timeout != after_timeout:
        timeout_stamp = int(after_timeout.timestamp()) if after_timeout else 0
        if not should_emit_log_once(("member_timeout", guild.id, after.id, timeout_stamp), ttl_seconds=30):
            return
        action_entry = await get_action_by({discord.AuditLogAction.member_update})
        if after_timeout and (after_timeout > datetime.now(timezone.utc)):
            embed = discord.Embed(
                title=f"{economy_q_timeout} Member Timed Out",
                color=discord.Color.orange()
            )
            embed.add_field(name="User", value=log_user(after), inline=False)
            embed.add_field(name="Until", value=f"<t:{int(after_timeout.timestamp())}:F>", inline=False)
            add_audit_actor_fields(embed, action_entry)
            embed.timestamp = datetime.now(timezone.utc)
        else:
            embed = discord.Embed(
                title=f"{economy_q_accept} Timeout Removed",
                color=discord.Color.green()
            )
            embed.add_field(name="User", value=log_user(after), inline=False)
            add_audit_actor_fields(embed, action_entry)
            embed.timestamp = datetime.now(timezone.utc)
        await send_log(embed, guild)

@bot.event
async def on_audit_log_entry_create(entry):
    if entry.action == discord.AuditLogAction.member_update:
        target = entry.target
        if not isinstance(target, discord.Member | discord.User):
            return

        after_timeout = getattr(target, "communication_disabled_until", None)
        target_id = getattr(target, "id", None)
        guild_id = getattr(getattr(entry, "guild", None), "id", 0)
        timeout_stamp = int(after_timeout.timestamp()) if after_timeout else 0
        if target_id is None or not should_emit_log_once(("member_timeout", guild_id, target_id, timeout_stamp), ttl_seconds=30):
            return

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
    entry = await find_audit_entry(guild, {discord.AuditLogAction.kick}, member.id)
    if entry:
        embed = discord.Embed(
            title=f"{economy_q_hammer} Member Kicked",
            color=discord.Color.red()
        )
        embed.timestamp = datetime.now(timezone.utc)
        embed.add_field(name="User", value=log_user(member), inline=False)
        add_audit_actor_fields(embed, entry, "Kicked by")
        await send_log(embed, guild)
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
    if bot_maintenance_mode and not has_super_owner_power(ctx.author, ctx.guild):
        raise CommandDisabledError("maintenance")
    disabled = guild_disabled_commands(ctx.guild)
    if (
        ctx.command
        and ctx.command.name in disabled
        and not command_disable_protected(ctx.command)
        and not has_super_owner_power(ctx.author, ctx.guild)
    ):
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

def command_load_group(ctx):
    command_name = (getattr(getattr(ctx, "command", None), "name", "") or "").casefold()
    invoked = (getattr(ctx, "invoked_with", "") or "").casefold()
    names = {command_name, invoked}
    if names & AI_COMMAND_NAMES:
        return "ai", ai_command_semaphore
    if names & BULK_COMMAND_NAMES:
        return "bulk", bulk_command_semaphore
    if names & HEAVY_COMMAND_NAMES:
        return "heavy", heavy_command_semaphore
    return "normal", None

@bot.before_invoke
async def limit_command_concurrency(ctx):
    queue_started = time.perf_counter()
    await command_semaphore.acquire()
    ctx._proque_command_semaphore_acquired = True
    group, semaphore = command_load_group(ctx)
    ctx._proque_command_load_group = group
    ctx._proque_group_semaphore = semaphore
    if semaphore is not None:
        await semaphore.acquire()
        ctx._proque_group_semaphore_acquired = True
    waited_ms = int((time.perf_counter() - queue_started) * 1000)
    stats = command_queue_stats.setdefault(group, {"count": 0, "total_ms": 0, "max_ms": 0})
    stats["count"] += 1
    stats["total_ms"] += waited_ms
    stats["max_ms"] = max(stats["max_ms"], waited_ms)
    if waited_ms >= 500:
        recent_queue_events.append({
            "name": ctx.command.qualified_name if ctx.command else (ctx.invoked_with or "unknown"),
            "group": group,
            "waited_ms": waited_ms,
            "user_id": ctx.author.id,
            "guild_id": ctx.guild.id if ctx.guild else None,
            "created_at": datetime.now(timezone.utc),
        })

@bot.after_invoke
async def track_command_usage_after(ctx):
    try:
        if getattr(ctx, "command", None) and not getattr(ctx.author, "bot", False):
            guild_id = ctx.guild.id if ctx.guild else 0
            asyncio.create_task(asyncio.to_thread(record_command_usage, guild_id, ctx.command.name, ctx.author.id))
    finally:
        semaphore = getattr(ctx, "_proque_group_semaphore", None)
        if semaphore is not None and getattr(ctx, "_proque_group_semaphore_acquired", False):
            semaphore.release()
            ctx._proque_group_semaphore_acquired = False
        if getattr(ctx, "_proque_command_semaphore_acquired", False):
            command_semaphore.release()
            ctx._proque_command_semaphore_acquired = False

@bot.command(name="prefix", aliases=["preifx", "setprefix"])
async def prefix_command(ctx, new_prefix: str = None):
    """Shows or changes this server's command prefix."""
    if ctx.guild is None:
        await ctx.send(f"Current prefix: `{DEFAULT_PREFIX}`")
        return

    current_prefix = guild_prefixes.get(ctx.guild.id, DEFAULT_PREFIX)
    if new_prefix is None:
        await send_command_input_ui(ctx, "prefix", note=f"Current prefix: `{current_prefix}`. Press the button to set a new prefix.")
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
    clear_help_cache()
    games_render_cache.clear()
    await ctx.send(f"Prefix changed to `{new_prefix}`")

DISABLE_PROTECTED_COMMANDS = {
    "disable", "enable", "disableall", "enableall", "dclist",
    "help", "settings", "prefix", "setlogs", "health", "errors",
    "recover", "dbaudit", "perms", "aidoctor", "off", "on",
}

def command_disable_protected(command):
    return not command or command.name in DISABLE_PROTECTED_COMMANDS

def disable_manageable_commands(user, guild):
    return sorted(
        (
            command for command in bot.walk_commands()
            if not getattr(command, "parent", None)
            and command_is_visible_to(command, user, guild)
            and not command_disable_protected(command)
        ),
        key=lambda command: command.name.casefold(),
    )

def disabled_command_lines(guild, viewer):
    lines = []
    for name in sorted(guild_disabled_commands(guild)):
        command = get_command_case_insensitive(name)
        if command_disable_protected(command):
            continue
        if not command_is_visible_to(command, viewer, guild):
            continue
        aliases = visible_aliases_for(command, 1) if command else []
        alias_text = f" · alias `{aliases[0]}`" if aliases else ""
        lines.append(f"`{name}`{alias_text}")
    return lines

def build_command_access_embed(guild, viewer):
    prefix = prefix_for_guild(guild)
    disabled_lines = disabled_command_lines(guild, viewer)
    manageable_count = len(disable_manageable_commands(viewer, guild))
    embed = standard_embed(
        "Command Access",
        description="Turn server commands on or off without hiding the recovery tools.",
        color=discord.Color.blurple(),
        icon=economy_q_lock,
    )
    embed.add_field(name="Disabled", value=f"**{len(disabled_lines)}** command(s)", inline=True)
    embed.add_field(name="Can Manage", value=f"**{manageable_count}** command(s)", inline=True)
    embed.add_field(name="Use", value=f"`{prefix}disable <command>`\n`{prefix}enable <command>`\n`{prefix}dclist`", inline=True)
    embed.add_field(
        name="Disabled Commands",
        value=joined_embed_value(disabled_lines[:12], empty="None. Everything is currently open.", limit=900),
        inline=False,
    )
    if len(disabled_lines) > 12:
        embed.set_footer(text=f"{len(disabled_lines) - 12:,} more disabled command(s). Use .dclist for the full list.")
    else:
        embed.set_footer(text="Run .disable without input anytime to reopen this panel.")
    return embed

def resolve_command_access_target(raw, user, guild):
    command = get_command_case_insensitive(raw)
    if not command or not command_is_visible_to(command, user, guild):
        return None, "Command not found. Try the command name or one of its aliases."
    if command_disable_protected(command):
        return None, f"`{command.name}` stays available so the server can recover settings."
    return command, None

async def set_command_disabled_for_guild(guild, command, disabled):
    commands_for_guild = guild_disabled_commands(guild)
    changed = False
    if disabled:
        if command.name not in commands_for_guild:
            commands_for_guild.add(command.name)
            changed = True
    else:
        if command.name in commands_for_guild:
            commands_for_guild.remove(command.name)
            changed = True
    if changed:
        await asyncio.to_thread(save_disabled_commands, scoped_id(guild), commands_for_guild)
    return changed

class CommandAccessModal(Modal):
    def __init__(self, author_id, mode):
        super().__init__(title="Disable Command" if mode == "disable" else "Enable Command")
        self.author_id = author_id
        self.mode = mode
        self.command_name = TextInput(
            label="Command",
            placeholder="Example: slap, lottery, poll",
            max_length=40,
        )
        self.add_item(self.command_name)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own command access panel.", ephemeral=True)
        if not has_owner_power(interaction.user, interaction.guild):
            return await interaction.response.send_message(denial_message("Admin power only."), ephemeral=True)
        command, error = resolve_command_access_target(str(self.command_name.value), interaction.user, interaction.guild)
        if error:
            return await interaction.response.send_message(f"{economy_q_warning} {error}", ephemeral=True)
        changed = await set_command_disabled_for_guild(interaction.guild, command, self.mode == "disable")
        if changed:
            verb = "Disabled" if self.mode == "disable" else "Enabled"
            await interaction.response.send_message(f"{economy_q_accept} {verb} `{command.name}`.", ephemeral=True)
        else:
            state = "already disabled" if self.mode == "disable" else "not disabled"
            await interaction.response.send_message(f"{economy_q_warning} `{command.name}` is {state}.", ephemeral=True)

class CommandAccessView(View):
    def __init__(self, author_id):
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
        self.author_id = author_id

    async def interaction_check(self, interaction):
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Use your own command access panel.", ephemeral=True)
        return False

    @discord.ui.button(label="Disable", emoji=economy_q_lock, style=discord.ButtonStyle.danger)
    async def disable_button(self, interaction, button):
        if not has_owner_power(interaction.user, interaction.guild):
            return await interaction.response.send_message(denial_message("Admin power only."), ephemeral=True)
        await interaction.response.send_modal(CommandAccessModal(self.author_id, "disable"))

    @discord.ui.button(label="Enable", emoji=economy_q_accept, style=discord.ButtonStyle.success)
    async def enable_button(self, interaction, button):
        if not has_owner_power(interaction.user, interaction.guild):
            return await interaction.response.send_message(denial_message("Admin power only."), ephemeral=True)
        await interaction.response.send_modal(CommandAccessModal(self.author_id, "enable"))

    @discord.ui.button(label="List", emoji=economy_q_archive, style=discord.ButtonStyle.secondary)
    async def list_button(self, interaction, button):
        lines = disabled_command_lines(interaction.guild, interaction.user)
        if not lines:
            return await interaction.response.send_message("No commands are disabled.", ephemeral=True)
        await interaction.response.send_message(
            embed=paginated_lines_embed("Disabled Commands", lines, 0, 15, "No commands are disabled."),
            view=PaginatedLinesView(interaction.user.id, "Disabled Commands", lines, per_page=15, empty="No commands are disabled.") if len(lines) > 15 else None,
            ephemeral=True,
        )

    @discord.ui.button(label="Refresh", emoji=economy_q_refresh, style=discord.ButtonStyle.secondary)
    async def refresh_button(self, interaction, button):
        await interaction.response.edit_message(embed=build_command_access_embed(interaction.guild, interaction.user), view=self)

@bot.command()
@is_admin_power()
async def disable(ctx, *, cmd: str = None):
    if not has_owner_power(ctx.author, ctx.guild):
        return
    if ctx.guild is None:
        return await ctx.send("Command access controls only work in servers.")
    if not cmd:
        return await ctx.send(
            embed=build_command_access_embed(ctx.guild, ctx.author),
            view=CommandAccessView(ctx.author.id),
            allowed_mentions=discord.AllowedMentions.none(),
        )

    command, error = resolve_command_access_target(cmd, ctx.author, ctx.guild)
    if error:
        return await ctx.send(f"{economy_q_warning} {error}")

    changed = await set_command_disabled_for_guild(ctx.guild, command, True)
    if changed:
        await ctx.send(f"{economy_q_accept} Disabled `{command.name}`.")
    else:
        await ctx.send(f"{economy_q_warning} `{command.name}` is already disabled.")
    
@bot.command()
@is_admin_power()
async def enable(ctx, *, cmd: str = None):
    if not has_owner_power(ctx.author, ctx.guild):
        return
    if ctx.guild is None:
        return await ctx.send("Command access controls only work in servers.")
    if not cmd:
        return await ctx.send(
            embed=build_command_access_embed(ctx.guild, ctx.author),
            view=CommandAccessView(ctx.author.id),
            allowed_mentions=discord.AllowedMentions.none(),
        )

    command, error = resolve_command_access_target(cmd, ctx.author, ctx.guild)
    if error:
        return await ctx.send(f"{economy_q_warning} {error}")

    changed = await set_command_disabled_for_guild(ctx.guild, command, False)
    if changed:
        await ctx.send(f"{economy_q_accept} Enabled `{command.name}`.")
    else:
        await ctx.send(f"{economy_q_warning} `{command.name}` is not disabled.")

@bot.command()
@is_admin_power()
async def disableall(ctx):
    if not has_owner_power(ctx.author, ctx.guild):
        return
    if ctx.guild is None:
        return await ctx.send("Command access controls only work in servers.")

    ok = await confirm_admin_action(
        ctx,
        "Disable All Commands",
        "This disables manageable commands for this server. Help, settings, enable/disable, recovery, and diagnostic commands stay available.",
    )
    if not ok:
        return
    commands_for_guild = guild_disabled_commands(ctx.guild)
    before = len(commands_for_guild)
    for command in bot.commands:
        if command_is_visible_to(command, ctx.author, ctx.guild) and not command_disable_protected(command):
            commands_for_guild.add(command.name)
    await asyncio.to_thread(save_disabled_commands, scoped_id(ctx.guild), commands_for_guild)
    await ctx.send(f"{economy_q_accept} Disabled **{len(commands_for_guild) - before:,}** command(s). Recovery commands stayed available.")

@bot.command()
@is_admin_power()
async def enableall(ctx):
    if not has_owner_power(ctx.author, ctx.guild):
        return
    if ctx.guild is None:
        return await ctx.send("Command access controls only work in servers.")

    count = len(guild_disabled_commands(ctx.guild))
    guild_disabled_commands(ctx.guild).clear()
    await asyncio.to_thread(save_disabled_commands, scoped_id(ctx.guild), guild_disabled_commands(ctx.guild))
    await ctx.send(f"{economy_q_accept} Enabled **{count:,}** command(s).")

@bot.command()
@is_admin_power()
async def dclist(ctx):
    if ctx.guild is None:
        return await ctx.send("Command access controls only work in servers.")
    lines = disabled_command_lines(ctx.guild, ctx.author)
    await send_paginated_lines(ctx, "Disabled Commands", lines, per_page=15, empty="No commands are disabled.")

SNIPE_TYPES = {
    "deleted": {
        "label": "Deleted",
        "emoji": economy_q_trash,
        "store": deleted_snipes,
        "aliases": {"deleted", "delete", "d", "dsnipe"},
    },
    "edited": {
        "label": "Edited",
        "emoji": economy_q_edit,
        "store": edited_snipes,
        "aliases": {"edited", "edit", "e", "esnipe"},
    },
    "reaction": {
        "label": "Reaction",
        "emoji": economy_q_reaction,
        "store": removed_reactions,
        "aliases": {"reaction", "react", "r", "rsnipe"},
    },
}

def resolve_snipe_type(raw, default="deleted"):
    key = str(raw or "").casefold().strip()
    for snipe_type, info in SNIPE_TYPES.items():
        if key == snipe_type or key in info["aliases"]:
            return snipe_type
    return default

def parse_snipe_position(raw_index, total):
    raw = str(raw_index or "1").strip()
    if raw.casefold() in {"latest", "newest", "last"}:
        return 0
    if raw.casefold() in {"oldest", "first"}:
        return max(0, total - 1)
    try:
        value = int(raw)
    except ValueError:
        return None
    if value == 0:
        return None
    return total + value if value < 0 else value - 1

def snipe_entry_user_id(snipe_type, entry):
    if snipe_type in {"deleted", "edited"}:
        return int(entry[1 if snipe_type == "deleted" else 2].id)
    return int(entry[0].id)

def snipe_entries(channel_id, snipe_type, user_id=None):
    entries = SNIPE_TYPES[snipe_type]["store"].get(channel_id, [])
    if user_id is None:
        return entries
    return [entry for entry in entries if snipe_entry_user_id(snipe_type, entry) == int(user_id)]

def snipe_empty_embed(ctx, snipe_type, user_id=None, show_guide=False):
    info = SNIPE_TYPES[snipe_type]
    target_text = f" for <@{user_id}>" if user_id else ""
    embed = discord.Embed(
        title=f"{info['emoji']} {info['label']} Snipe",
        description=f"Nothing saved for this channel{target_text} yet.",
        color=discord.Color.dark_grey(),
    )
    if show_guide:
        embed.add_field(name="How To Use", value=snipe_usage_text(getattr(ctx, "prefix", prefix_for_guild(getattr(ctx, "guild", None)))), inline=False)
    embed.set_footer(text="Snipes only work for events the bot saw while online.")
    return embed

def snipe_usage_text(prefix="."):
    return (
        f"`{prefix}snipe` or `{prefix}dsnipe` deleted messages\n"
        f"`{prefix}esnipe` edited messages\n"
        f"`{prefix}rsnipe` removed reactions\n"
        f"`{prefix}dsnipe @user` filter deleted snipes by user\n"
        f"`{prefix}esnipe @user 2` filter and jump to an older edited entry"
    )

def build_snipe_embed(ctx, snipe_type, index, user_id=None, show_guide=False):
    entries = snipe_entries(ctx.channel.id, snipe_type, user_id)
    info = SNIPE_TYPES[snipe_type]
    if not entries:
        return snipe_empty_embed(ctx, snipe_type, user_id, show_guide)
    index = max(0, min(index, len(entries) - 1))
    embed = discord.Embed(
        title=f"{info['emoji']} {info['label']} Snipe",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    if snipe_type == "deleted":
        content, author, timestamp = entries[index]
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        embed.color = discord.Color.red()
        embed.add_field(name="Author", value=f"<@{author.id}> (`{author.id}`)", inline=False)
        embed.add_field(name="Deleted", value=discord.utils.format_dt(timestamp, "F"), inline=True)
        embed.add_field(name="Ago", value=discord.utils.format_dt(timestamp, "R"), inline=True)
        embed.add_field(name="Content", value=embed_value(content or "[No content]", 3500), inline=False)
    elif snipe_type == "edited":
        before, after, author, link, timestamp = entries[index]
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        embed.color = discord.Color.orange()
        embed.add_field(name="Author", value=f"<@{author.id}> (`{author.id}`)", inline=False)
        embed.add_field(name="Edited", value=discord.utils.format_dt(timestamp, "R"), inline=True)
        embed.add_field(name="Message", value=f"[Jump to message]({link})", inline=True)
        embed.add_field(name="Before", value=embed_value(before or "[No content]", 1700), inline=False)
        embed.add_field(name="After", value=embed_value(after or "[No content]", 1700), inline=False)
    else:
        user, emoji, msg, timestamp = entries[index]
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        embed.color = discord.Color.purple()
        embed.add_field(name="User", value=f"<@{user.id}> (`{user.id}`)", inline=False)
        embed.add_field(name="Reaction", value=str(emoji), inline=True)
        embed.add_field(name="Removed", value=discord.utils.format_dt(timestamp, "R"), inline=True)
        embed.add_field(name="Message", value=f"[Jump to message]({msg.jump_url})", inline=False)
        msg_content = getattr(msg, "content", "") or "[No content]"
        embed.add_field(name="Message Content", value=embed_value(msg_content, 1200), inline=False)
    if user_id:
        embed.add_field(name="Filter", value=f"Only showing entries for <@{user_id}>.", inline=False)
    if show_guide:
        embed.add_field(name="How To Use", value=snipe_usage_text(getattr(ctx, "prefix", prefix_for_guild(getattr(ctx, "guild", None)))), inline=False)
    embed.set_footer(text=f"{ctx.channel} • Snipe {index + 1}/{len(entries)}")
    return embed

class SnipeView(View):
    def __init__(self, author_id, channel_id, snipe_type="deleted", index=0, user_id=None, show_guide=False):
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
        self.author_id = author_id
        self.channel_id = channel_id
        self.snipe_type = snipe_type
        self.index = index
        self.user_id = user_id
        self.show_guide = show_guide
        self.type_select = Select(
            placeholder="Snipe type",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label="Deleted messages", value="deleted", emoji=reaction_emoji(economy_q_trash)),
                discord.SelectOption(label="Edited messages", value="edited", emoji=reaction_emoji(economy_q_edit)),
                discord.SelectOption(label="Removed reactions", value="reaction", emoji=reaction_emoji(economy_q_reaction)),
            ],
        )
        self.type_select.callback = self.select_type
        self.add_item(self.type_select)

    async def interaction_check(self, interaction):
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Open your own snipe menu.", ephemeral=True)
        return False

    async def refresh(self, interaction):
        entries = snipe_entries(self.channel_id, self.snipe_type, self.user_id)
        if entries:
            self.index = max(0, min(self.index, len(entries) - 1))
        else:
            self.index = 0
        await interaction.response.edit_message(embed=build_snipe_embed(interaction, self.snipe_type, self.index, self.user_id, self.show_guide), view=self)

    async def select_type(self, interaction):
        self.snipe_type = self.type_select.values[0]
        self.index = 0
        self.show_guide = False
        await self.refresh(interaction)

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def previous_button(self, interaction, button):
        self.index -= 1
        await self.refresh(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction, button):
        self.index += 1
        await self.refresh(interaction)

    @discord.ui.button(label="Latest", style=discord.ButtonStyle.primary)
    async def latest_button(self, interaction, button):
        self.index = 0
        await self.refresh(interaction)

    @discord.ui.button(label="Guide", style=discord.ButtonStyle.secondary)
    async def guide_button(self, interaction, button):
        await interaction.response.send_message(
            snipe_usage_text(prefix_for_guild(interaction.guild)),
            ephemeral=True,
        )

async def resolve_snipe_filter_user(ctx, token):
    if not token:
        return None
    try:
        return await commands.UserConverter().convert(ctx, token)
    except commands.BadArgument:
        return None

async def parse_snipe_args(ctx, raw_args, default_type="deleted"):
    raw_args = (raw_args or "").strip()
    if not raw_args:
        return default_type, None, "1", True
    try:
        tokens = shlex.split(raw_args)
    except ValueError:
        tokens = raw_args.split()
    snipe_type = default_type
    if tokens and resolve_snipe_type(tokens[0], None):
        snipe_type = resolve_snipe_type(tokens.pop(0), default_type)
    user = None
    index = "1"
    for token in list(tokens):
        if user is None:
            resolved_user = await resolve_snipe_filter_user(ctx, token)
            if resolved_user:
                user = resolved_user
                tokens.remove(token)
                continue
        if parse_snipe_position(token, 1) is not None or token.casefold() in {"latest", "newest", "last", "oldest", "first"}:
            index = token
            tokens.remove(token)
            break
    if user is None and getattr(ctx.message, "mentions", None):
        mentioned = [member for member in ctx.message.mentions if not getattr(member, "bot", False)]
        if mentioned:
            user = mentioned[0]
    return snipe_type, (user.id if user else None), index, False

async def send_snipe_menu(ctx, snipe_type="deleted", index="1", user_id=None, show_guide=False):
    snipe_type = resolve_snipe_type(snipe_type)
    entries = snipe_entries(ctx.channel.id, snipe_type, user_id)
    position = parse_snipe_position(index, len(entries)) if entries else 0
    if position is None or (entries and (position < 0 or position >= len(entries))):
        return await ctx.send(f"Use a valid number from **1** to **{len(entries) or 1}**.")
    view = SnipeView(ctx.author.id, ctx.channel.id, snipe_type, position, user_id, show_guide)
    await ctx.send(embed=build_snipe_embed(ctx, snipe_type, position, user_id, show_guide), view=view, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="snipe", aliases=["snipes"])
async def snipe(ctx, *, raw_args: str = None):
    """Opens a menu for deleted, edited, and reaction snipes."""
    snipe_type, user_id, index, show_guide = await parse_snipe_args(ctx, raw_args, "deleted")
    await send_snipe_menu(ctx, snipe_type, index, user_id, show_guide)

@bot.command(name="dsnipe", aliases=["deleted", "deletedsnipe"])
async def dsnipe(ctx, *, raw_args: str = None):
    """Shows deleted-message snipes with buttons."""
    _, user_id, index, _ = await parse_snipe_args(ctx, raw_args, "deleted")
    await send_snipe_menu(ctx, "deleted", index, user_id)

@bot.command(name="esnipe", aliases=["editsnipe", "edited"])
async def esnipe(ctx, *, raw_args: str = None):
    """Shows edited-message snipes with buttons."""
    _, user_id, index, _ = await parse_snipe_args(ctx, raw_args, "edited")
    await send_snipe_menu(ctx, "edited", index, user_id)

@bot.command(name="rsnipe", aliases=["reactionsnipe", "reactsnipe"])
async def rsnipe(ctx, *, raw_args: str = None):
    """Shows removed-reaction snipes with buttons."""
    _, user_id, index, _ = await parse_snipe_args(ctx, raw_args, "reaction")
    await send_snipe_menu(ctx, "reaction", index, user_id)

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
        await ctx.send(clean_user_error(e))

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
            await asyncio.to_thread(delete_active_game_session, game["msg"].id)
            return

        if all(cell != TTT_EMPTY for row in board for cell in row):
            payout_text = await settle_game_bet(game, None)
            await disable_all_buttons(game["view"])
            await game["msg"].edit(content=f"It's a draw!{payout_text}", view=game["view"], allowed_mentions=discord.AllowedMentions.none())
            ttt_games.pop(interaction.channel.id, None)
            await asyncio.to_thread(delete_active_game_session, game["msg"].id)
            return

        game["turn"] = 1 - game["turn"]
        await save_runtime_game_state(game, "ttt")
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
        author_data, opponent_data = await asyncio.gather(
            asyncio.to_thread(economy_get_user, ctx.author.id),
            asyncio.to_thread(economy_get_user, opponent.id),
        )
    except Exception:
        await ctx.send("I had trouble reaching the economy data. Try again in a bit.")
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

def fit_embed_value(value, limit=1024):
    value = str(value or "")
    if not value:
        return "\u200b"
    if len(value) <= limit:
        return value
    suffix = "\n[shortened]"
    return value[:limit - len(suffix)] + suffix

def fit_discord_embed(embed):
    if embed is None:
        return None
    try:
        if embed.description:
            embed.description = fit_embed_value(embed.description, 4096)
        for index, field in enumerate(list(embed.fields)):
            embed.set_field_at(
                index,
                name=fit_embed_value(field.name, 256),
                value=fit_embed_value(field.value, 1024),
                inline=field.inline,
            )
        while len(embed) > 5900 and embed.fields:
            embed.remove_field(len(embed.fields) - 1)
        if len(embed) > 5900 and embed.description:
            embed.description = fit_embed_value(embed.description, 1000)
    except Exception:
        pass
    return embed

def fit_discord_embeds(embeds):
    if embeds is None:
        return None
    fitted = [fit_discord_embed(embed) for embed in embeds[:10]]
    while len(fitted) > 1 and sum(len(embed) for embed in fitted if embed is not None) > 5900:
        fitted.pop()
    return fitted

if not getattr(commands.Context.send, "_proque_safe_content", False):
    _original_context_send = commands.Context.send

    async def _safe_context_send(self, *args, **kwargs):
        if args and isinstance(args[0], str):
            args = (fit_discord_content(args[0]),) + args[1:]
        if isinstance(kwargs.get("content"), str):
            kwargs["content"] = fit_discord_content(kwargs["content"])
        if kwargs.get("embed") is not None:
            kwargs["embed"] = fit_discord_embed(kwargs["embed"])
        if kwargs.get("embeds") is not None:
            kwargs["embeds"] = fit_discord_embeds(kwargs["embeds"])
        return await _original_context_send(self, *args, **kwargs)

    _safe_context_send._proque_safe_content = True
    commands.Context.send = _safe_context_send

if not getattr(discord.InteractionResponse.send_message, "_proque_safe_content", False):
    _original_interaction_send_message = discord.InteractionResponse.send_message
    _original_interaction_edit_message = discord.InteractionResponse.edit_message

    async def _safe_interaction_send_message(self, *args, **kwargs):
        if args and isinstance(args[0], str):
            args = (fit_discord_content(args[0]),) + args[1:]
        if isinstance(kwargs.get("content"), str):
            kwargs["content"] = fit_discord_content(kwargs["content"])
        if kwargs.get("embed") is not None:
            kwargs["embed"] = fit_discord_embed(kwargs["embed"])
        if kwargs.get("embeds") is not None:
            kwargs["embeds"] = fit_discord_embeds(kwargs["embeds"])
        return await _original_interaction_send_message(self, *args, **kwargs)

    async def _safe_interaction_edit_message(self, *args, **kwargs):
        if args and isinstance(args[0], str):
            args = (fit_discord_content(args[0]),) + args[1:]
        if isinstance(kwargs.get("content"), str):
            kwargs["content"] = fit_discord_content(kwargs["content"])
        if kwargs.get("embed") is not None:
            kwargs["embed"] = fit_discord_embed(kwargs["embed"])
        if kwargs.get("embeds") is not None:
            kwargs["embeds"] = fit_discord_embeds(kwargs["embeds"])
        return await _original_interaction_edit_message(self, *args, **kwargs)

    _safe_interaction_send_message._proque_safe_content = True
    discord.InteractionResponse.send_message = _safe_interaction_send_message
    discord.InteractionResponse.edit_message = _safe_interaction_edit_message

if not getattr(discord.Webhook.send, "_proque_safe_content", False):
    _original_webhook_send = discord.Webhook.send

    async def _safe_webhook_send(self, *args, **kwargs):
        if args and isinstance(args[0], str):
            args = (fit_discord_content(args[0]),) + args[1:]
        if isinstance(kwargs.get("content"), str):
            kwargs["content"] = fit_discord_content(kwargs["content"])
        if kwargs.get("embed") is not None:
            kwargs["embed"] = fit_discord_embed(kwargs["embed"])
        if kwargs.get("embeds") is not None:
            kwargs["embeds"] = fit_discord_embeds(kwargs["embeds"])
        return await _original_webhook_send(self, *args, **kwargs)

    _safe_webhook_send._proque_safe_content = True
    discord.Webhook.send = _safe_webhook_send

if not getattr(discord.abc.Messageable.send, "_proque_safe_content", False):
    _original_messageable_send = discord.abc.Messageable.send

    async def _safe_messageable_send(self, *args, **kwargs):
        if args and isinstance(args[0], str):
            args = (fit_discord_content(args[0]),) + args[1:]
        if isinstance(kwargs.get("content"), str):
            kwargs["content"] = fit_discord_content(kwargs["content"])
        if kwargs.get("embed") is not None:
            kwargs["embed"] = fit_discord_embed(kwargs["embed"])
        if kwargs.get("embeds") is not None:
            kwargs["embeds"] = fit_discord_embeds(kwargs["embeds"])
        return await _original_messageable_send(self, *args, **kwargs)

    _safe_messageable_send._proque_safe_content = True
    discord.abc.Messageable.send = _safe_messageable_send

if not getattr(discord.Message.reply, "_proque_safe_reply", False):
    _original_message_reply = discord.Message.reply

    async def _safe_message_reply(self, *args, **kwargs):
        try:
            return await _original_message_reply(self, *args, **kwargs)
        except discord.HTTPException as exc:
            if "Unknown message" not in str(exc):
                raise
            fallback_kwargs = dict(kwargs)
            fallback_kwargs.pop("mention_author", None)
            fallback_kwargs.pop("fail_if_not_exists", None)
            return await self.channel.send(*args, **fallback_kwargs)

    _safe_message_reply._proque_safe_reply = True
    discord.Message.reply = _safe_message_reply

if not getattr(discord.Message.edit, "_proque_safe_content", False):
    _original_message_edit = discord.Message.edit

    async def _safe_message_edit(self, *args, **kwargs):
        if args and isinstance(args[0], str):
            args = (fit_discord_content(args[0]),) + args[1:]
        if isinstance(kwargs.get("content"), str):
            kwargs["content"] = fit_discord_content(kwargs["content"])
        if kwargs.get("embed") is not None:
            kwargs["embed"] = fit_discord_embed(kwargs["embed"])
        if kwargs.get("embeds") is not None:
            kwargs["embeds"] = fit_discord_embeds(kwargs["embeds"])
        return await _original_message_edit(self, *args, **kwargs)

    _safe_message_edit._proque_safe_content = True
    discord.Message.edit = _safe_message_edit

if not getattr(discord.Interaction.edit_original_response, "_proque_safe_content", False):
    _original_interaction_edit_original_response = discord.Interaction.edit_original_response

    async def _safe_interaction_edit_original_response(self, *args, **kwargs):
        if args and isinstance(args[0], str):
            args = (fit_discord_content(args[0]),) + args[1:]
        if isinstance(kwargs.get("content"), str):
            kwargs["content"] = fit_discord_content(kwargs["content"])
        if kwargs.get("embed") is not None:
            kwargs["embed"] = fit_discord_embed(kwargs["embed"])
        if kwargs.get("embeds") is not None:
            kwargs["embeds"] = fit_discord_embeds(kwargs["embeds"])
        return await _original_interaction_edit_original_response(self, *args, **kwargs)

    _safe_interaction_edit_original_response._proque_safe_content = True
    discord.Interaction.edit_original_response = _safe_interaction_edit_original_response

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
        def settle_bet_sync():
            winner_data = economy_get_user(winner.id)
            loser_data = economy_get_user(loser.id)
            payout_amount = min(amount, loser_data["balance"])
            economy_update_user(winner.id, balance=winner_data["balance"] + payout_amount)
            economy_update_user(loser.id, balance=max(0, loser_data["balance"] - payout_amount))
            return payout_amount

        payout = await asyncio.to_thread(settle_bet_sync)
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
            await asyncio.to_thread(delete_active_game_session, game["message"].id)
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
        await asyncio.to_thread(delete_active_game_session, game["message"].id)
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
            await asyncio.to_thread(delete_active_game_session, self.game["message"].id)
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
            await asyncio.to_thread(delete_active_game_session, self.game["message"].id)
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
        await save_runtime_game_state(self.game, "chess")
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
    await save_runtime_game_state(game, "chess")
    await start_chess_clock(game)
    await start_chess_live_clock(game)

@bot.command(name="chessmove", aliases=["cmove"])
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
        return await ctx.send(f"Illegal move. Use the chess UI or something like `{ctx.prefix}chessmove e2e4`.")

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
        await asyncio.to_thread(delete_active_game_session, game["message"].id)
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
        await asyncio.to_thread(delete_active_game_session, game["message"].id)
        return

    game["view"] = ChessView(game)
    await start_chess_clock(game)
    await start_chess_live_clock(game)
    await save_runtime_game_state(game, "chess")
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
    await asyncio.to_thread(delete_active_game_session, game["message"].id)

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
    await save_runtime_game_state(game, "ttt")

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
        await asyncio.to_thread(delete_active_game_session, game["msg"].id)

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
                await asyncio.to_thread(delete_active_game_session, interaction.message.id)
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
                await asyncio.to_thread(delete_active_game_session, interaction.message.id)
                return

            game["turn"] = 1 - game["turn"]
            game["msg"] = interaction.message
            game["board"] = board
            game["view"] = Connect4View()
            await save_runtime_game_state(game, "c4")

            render = render_board(board, game["turn"])
            await interaction.message.edit(content=fit_discord_content(render), view=game["view"], allowed_mentions=discord.AllowedMentions.none())
            await update_c4_turn(game, interaction.channel)

        except Exception as e:
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[ERROR in Connect4 callback]\n{traceback_str}")
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(clean_user_error(e), ephemeral=True)
                else:
                    await interaction.response.send_message(clean_user_error(e), ephemeral=True)
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
            await asyncio.to_thread(delete_active_game_session, msg.id)
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
    await save_runtime_game_state(game, "c4")
    await update_c4_turn(game, ctx.channel)

@bot.command()
@is_admin_power()
async def endttt(ctx):
    if ctx.channel.id in ttt_games:
        game = ttt_games.pop(ctx.channel.id, None)
        if game and game.get("msg"):
            await asyncio.to_thread(delete_active_game_session, game["msg"].id)
        await ctx.send("Tic-Tac-Toe game ended.")
    else:
        await ctx.send("No Tic-Tac-Toe game is currently active in this channel.")

@bot.command()
async def q(ctx):
    answer = random.choice(["Yes", "No"])
    await ctx.send(f"**{answer}**")

@bot.command()
@is_admin_power()
async def setnick(ctx, members: commands.Greedy[discord.Member], *, nickname: str = None):
    if not members or not nickname:
        return await send_command_input_ui(ctx, "setnick", note="Mention one or more users, then enter the nickname.")
    blocked = [member for member in members if not can_act_on(ctx.author, member, ctx.guild)]
    if blocked:
        return await ctx.send("You can't change one or more of those users' nicknames.")
    try:
        changed = []
        for member in members:
            await member.edit(nick=nickname)
            changed.append(f"<@{member.id}>")
        if len(changed) > 20 or len(", ".join(changed)) > 1400:
            return await send_paginated_lines(
                ctx,
                "Nicknames Changed",
                [f"{mention} -> **{nickname}**" for mention in changed],
                per_page=20,
                empty="No nicknames changed.",
            )
        await ctx.send(
            f"Changed **{len(changed)}** nickname(s) to **{nickname}**: {', '.join(changed)}.",
            allowed_mentions=discord.AllowedMentions.none()
        )
    except discord.Forbidden:
        await ctx.send("I don't have permission to change one or more of those nicknames.")
    except Exception as e:
        await ctx.send(clean_user_error(e))

@bot.command()
@is_admin_power()
async def shut(ctx, members: commands.Greedy[discord.Member]):
    if not members:
        return await send_command_input_ui(ctx, "shut", note="Mention one or more users to silence.")
    targets = guild_watchlist(ctx.guild)
    changed = []
    for member in members:
        if not can_act_on(ctx.author, member, ctx.guild):
            return await ctx.send("You can't silence one or more of those users.")
        targets[member.id] = ctx.author.id
        changed.append(f"<@{member.id}>")
    await asyncio.to_thread(save_watchlist, scoped_id(ctx.guild), targets)
    await ctx.send(f"Silenced **{len(changed)}** user(s): {', '.join(changed)}.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_admin_power()
async def unshut(ctx, members: commands.Greedy[discord.Member]):
    if not members:
        return await send_command_input_ui(ctx, "unshut", note="Mention one or more users to unsilence.")
    targets = guild_watchlist(ctx.guild)
    changed = []
    for member in members:
        if not can_act_on(ctx.author, member, ctx.guild) and targets.get(member.id) != ctx.author.id:
            return await ctx.send("You can't unshut one or more of those users.")
        targets.pop(member.id, None)
        changed.append(f"<@{member.id}>")
    await asyncio.to_thread(save_watchlist, scoped_id(ctx.guild), targets)
    await ctx.send(f"Unsilenced **{len(changed)}** user(s): {', '.join(changed)}.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_admin_power()
async def clearwatchlist(ctx):
    if not has_super_owner_power(ctx.author, ctx.guild):
        return await ctx.send(denial_message("This command is not available here."))
    guild_watchlist(ctx.guild).clear()
    await asyncio.to_thread(save_watchlist, scoped_id(ctx.guild), guild_watchlist(ctx.guild))
    await ctx.send("Watchlist cleared.")

@bot.command()
@is_admin_power()
async def rshut(ctx, members: commands.Greedy[discord.Member]):
    """Silence a user's reactions."""
    if not members:
        return await send_command_input_ui(ctx, "rshut", note="Mention one or more users to reaction-silence.")
    targets = guild_reaction_watchlist(ctx.guild)
    changed = []
    for member in members:
        if not can_act_on(ctx.author, member, ctx.guild):
            return await ctx.send("You can't silence one or more of those users' reactions.")
        targets[member.id] = ctx.author.id
        changed.append(f"<@{member.id}>")
    await asyncio.to_thread(save_reaction_watchlist, scoped_id(ctx.guild), targets)
    await ctx.send(f"Reaction-silenced **{len(changed)}** user(s): {', '.join(changed)}.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_admin_power()
async def unrshut(ctx, members: commands.Greedy[discord.Member]):
    """Allow a user's reactions again (silent unless protected owner)."""
    if not members:
        return await send_command_input_ui(ctx, "unrshut", note="Mention one or more users to allow reactions again.")
    targets = guild_reaction_watchlist(ctx.guild)
    changed = []
    for member in members:
        if not can_act_on(ctx.author, member, ctx.guild) and targets.get(member.id) != ctx.author.id:
            return await ctx.send("You can't unshut one or more of those users.")
        targets.pop(member.id, None)
        changed.append(f"<@{member.id}>")
    await asyncio.to_thread(save_reaction_watchlist, scoped_id(ctx.guild), targets)
    await ctx.send(f"Allowed reactions again for **{len(changed)}** user(s): {', '.join(changed)}.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_admin_power()
async def lockdown(ctx):
    """Bot-level channel lockdown. Non-admin messages are deleted."""
    channels = guild_shutdown_channels(ctx.guild)
    channels.add(ctx.channel.id)
    await asyncio.to_thread(save_shutdown_channels, scoped_id(ctx.guild), channels)
    await ctx.send("This channel is now in lockdown mode. Only admins can speak.")

@bot.command(name="reopen")
@is_admin_power()
async def reopen(ctx):
    """Reopen a bot-level locked-down channel."""
    channels = guild_shutdown_channels(ctx.guild)
    channels.discard(ctx.channel.id)
    await asyncio.to_thread(save_shutdown_channels, scoped_id(ctx.guild), channels)
    await ctx.send("This channel has been reopened. All users may speak now.")

@bot.command()
@is_admin_power()
async def rlockdown(ctx):
    channels = guild_reaction_shutdown_channels(ctx.guild)
    channels.add(ctx.channel.id)
    await asyncio.to_thread(save_reaction_shutdown_channels, scoped_id(ctx.guild), channels)
    await ctx.send("Reactions are now disabled in this channel.", delete_after=5)

@bot.command()
@is_admin_power()
async def runlock(ctx):
    channels = guild_reaction_shutdown_channels(ctx.guild)
    channels.discard(ctx.channel.id)
    await asyncio.to_thread(save_reaction_shutdown_channels, scoped_id(ctx.guild), channels)
    await ctx.send("Reactions are now enabled in this channel.", delete_after=5)

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

async def parse_members_role_args(ctx, args):
    try:
        tokens = shlex.split(str(args or ""))
    except ValueError:
        return [], None
    if len(tokens) < 2:
        return [], None

    role = None
    role_index = None
    for index, token in enumerate(tokens):
        try:
            role = await commands.RoleConverter().convert(ctx, token)
            role_index = index
            break
        except commands.BadArgument:
            continue
    if role is None:
        return [], None

    members = []
    seen = set()
    for index, token in enumerate(tokens):
        if index == role_index:
            continue
        try:
            member = await commands.MemberConverter().convert(ctx, token)
        except commands.BadArgument:
            continue
        if member.id in seen:
            continue
        members.append(member)
        seen.add(member.id)

    return members, role

@bot.command()
@is_admin_power()
async def purge(ctx, *, args: str = None):
    amount, member = await parse_member_count_args(ctx, args)
    if amount is None:
        return await send_command_input_ui(ctx, "purge", note="Enter how many messages to delete. You can optionally include a member.")
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
        await ctx.send(clean_user_error(e), delete_after=5)


@bot.command()
@is_admin_power()
async def rpurge(ctx, *, args: str = None):
    amount, member = await parse_member_count_args(ctx, args)
    if amount is None:
        return await send_command_input_ui(ctx, "rpurge", note="Enter how many messages to check for reactions. You can optionally include a member.")
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
        await ctx.send(f"Failed to remove reactions: {clean_user_error(e)}", delete_after=5)
    except Exception as e:
        print(f"[RPURGE ERROR] {type(e).__name__} - {e}")
        await ctx.send("An unexpected error occurred while removing reactions.", delete_after=5)

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
async def unmute(ctx, members: commands.Greedy[discord.Member]):
    if not members:
        return await send_command_input_ui(ctx, "unmute", note="Mention one or more users to unmute.")
    changed = []
    for member in members:
        if not can_act_on(ctx.author, member, ctx.guild):
            return await ctx.send("You can't unmute one or more of those members.")
        try:
            await member.timeout(None)
            changed.append(f"<@{member.id}>")
        except discord.Forbidden:
            return await ctx.send("Missing permissions to unmute one or more of those members.")
        except Exception as e:
            return await ctx.send(f"Failed to unmute: {clean_user_error(e)}")
    await ctx.send(
        f"Unmuted **{len(changed)}** user(s): {', '.join(changed)}.",
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
@is_admin_power()
async def ban(ctx, users: commands.Greedy[discord.User]):
    if not users:
        return await send_command_input_ui(ctx, "ban", note="Mention one or more users to ban.")
    for user in users:
        member = ctx.guild.get_member(user.id) if ctx.guild else None
        if member is not None and not can_act_on(ctx.author, member, ctx.guild):
            return await ctx.send("You can't ban one or more of those users.")
    mentions = ", ".join(f"<@{user.id}>" for user in users)
    ok = await confirm_admin_action(ctx, "Ban User(s)", f"Ban **{len(users)}** user(s) from **{ctx.guild.name}**?\n{mentions}")
    if not ok:
        return
    banned = []
    for user in users:
        try:
            await user.send(f"LMAO you got banned from **{ctx.guild.name}** {economy_q_reject}")
        except Exception:
            pass
        await ctx.guild.ban(user)
        banned.append(f"<@{user.id}>")
    await ctx.send(
        f"Banned **{len(banned)}** user(s): {', '.join(banned)}.",
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
async def kick(ctx, members: commands.Greedy[discord.Member], *, reason=None):
    if not members:
        return await send_command_input_ui(ctx, "kick", note="Mention one or more users to kick.")
    for member in members:
        if not can_act_on(ctx.author, member, ctx.guild):
            return await ctx.send("You can't kick one or more of those members.")
    reason_text = f"\nReason: {reason}" if reason else ""
    mentions = ", ".join(f"<@{member.id}>" for member in members)
    ok = await confirm_admin_action(ctx, "Kick User(s)", f"Kick **{len(members)}** user(s) from **{ctx.guild.name}**?{reason_text}\n{mentions}")
    if not ok:
        return
    kicked = []
    for member in members:
        await member.kick(reason=reason)
        kicked.append(f"<@{member.id}>")
    await ctx.send(f"Kicked **{len(kicked)}** user(s): {', '.join(kicked)}.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_admin_power()
async def addrole(ctx, *, args: str = None):
    """Adds a role to a member. Member and role can be in either order."""
    members, role = await parse_members_role_args(ctx, args)
    if not members or role is None:
        return await send_command_input_ui(ctx, "addrole", note="Enter one or more members and a role. Any order works.")
    blocked = [member for member in members if not can_act_on(ctx.author, member, ctx.guild)]
    if blocked:
        return await ctx.send("You can't edit one or more of those members' roles.")
    changed = []
    for member in members:
        await member.add_roles(role, reason=f"addrole command by {ctx.author} ({ctx.author.id})")
        changed.append(f"<@{member.id}>")
    if len(changed) > 20 or len(", ".join(changed)) > 1400:
        return await send_paginated_lines(
            ctx,
            "Role Added",
            [f"{mention} -> **{role.name}**" for mention in changed],
            per_page=20,
            empty="No roles changed.",
        )
    await ctx.send(
        f"Added **{role.name}** to **{len(changed)}** user(s): {', '.join(changed)}.",
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
@is_admin_power()
async def removerole(ctx, *, args: str = None):
    """Removes a role from a member. Member and role can be in either order."""
    members, role = await parse_members_role_args(ctx, args)
    if not members or role is None:
        return await send_command_input_ui(ctx, "removerole", note="Enter one or more members and a role. Any order works.")
    blocked = [member for member in members if not can_act_on(ctx.author, member, ctx.guild)]
    if blocked:
        return await ctx.send("You can't edit one or more of those members' roles.")
    changed = []
    for member in members:
        await member.remove_roles(role, reason=f"removerole command by {ctx.author} ({ctx.author.id})")
        changed.append(f"<@{member.id}>")
    if len(changed) > 20 or len(", ".join(changed)) > 1400:
        return await send_paginated_lines(
            ctx,
            "Role Removed",
            [f"{mention} -> **{role.name}**" for mention in changed],
            per_page=20,
            empty="No roles changed.",
        )
    await ctx.send(
        f"Removed **{role.name}** from **{len(changed)}** user(s): {', '.join(changed)}.",
        allowed_mentions=discord.AllowedMentions.none()
    )

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
                await interaction.followup.send(f"{economy_q_reject} Failed to add sticker: {clean_user_error(e)}", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"{economy_q_reject} Failed to add sticker: {clean_user_error(e)}", ephemeral=True)
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
                await interaction.followup.send(f"{economy_q_reject} Failed to add emoji: {clean_user_error(e)}", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"{economy_q_reject} Failed to add emoji: {clean_user_error(e)}", ephemeral=True)

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
            await ctx.send(f"Could not read sticker: {clean_user_error(e)}", delete_after=5)
            return
    
    elif attachments:
        first = attachments[0]
        content_type = first.content_type or ""
        
        if content_type.startswith("image/"):
            try:
                buffer = BytesIO()
                session = await get_http_session()
                async with session.get(first.url) as resp:
                    buffer.write(await resp.read())
                buffer.seek(0)
                
                embed = discord.Embed(title="Image Detected", color=discord.Color.blurple())
                embed.set_image(url=first.url)
                embed.add_field(name="Filename", value=first.filename, inline=True)
                embed.add_field(name="Size", value=f"{first.size / 1024:.1f} KB", inline=True)
                await ctx.send(embed=embed, view=StealView("sticker", buffer, preview_url=first.url))
            except Exception as e:
                await ctx.send(f"Could not download image: {clean_user_error(e)}", delete_after=5)
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
                session = await get_http_session()
                async with session.get(str(emoji_obj.url)) as resp:
                    buffer.write(await resp.read())
                buffer.seek(0)
                
                embed = discord.Embed(title="Animated Emoji Detected", color=discord.Color.blurple())
                embed.add_field(name="Name", value=name, inline=True)
                embed.add_field(name="Animated", value="Yes", inline=True)
                embed.add_field(name="Preview", value=str(emoji_obj), inline=False)
                await ctx.send(embed=embed, view=StealView("emoji", buffer, emoji_obj))
            except Exception as e:
                await ctx.send(f"Could not fetch animated emoji: {clean_user_error(e)}", delete_after=5)
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
                session = await get_http_session()
                async with session.get(str(emoji_obj.url)) as resp:
                    buffer.write(await resp.read())
                buffer.seek(0)
                
                embed = discord.Embed(title="Emoji Detected", color=discord.Color.blurple())
                embed.add_field(name="Name", value=name, inline=True)
                embed.add_field(name="Animated", value="No", inline=True)
                embed.add_field(name="Preview", value=str(emoji_obj), inline=False)
                await ctx.send(embed=embed, view=StealView("emoji", buffer, emoji_obj))
            except Exception as e:
                await ctx.send(f"Could not fetch emoji: {clean_user_error(e)}", delete_after=5)
                return
        else:
            await ctx.send("Could not fetch emoji - it may not be in this server.", delete_after=5)
            return
    else:
        await ctx.send("No sticker, emoji, or image found in the replied message.", delete_after=5)
            
@bot.command()
@is_super_owner()
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
        return await send_command_input_ui(ctx, "send", note="Enter a message, or enter a channel plus message.")

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


FORWARD_MESSAGE_LINK_RE = re.compile(
    r"https?://(?:canary\.|ptb\.)?discord(?:app)?\.com/channels/(?P<guild>\d+|@me)/(?P<channel>\d+)/(?P<message>\d+)"
)

async def resolve_forward_channel(ctx, token):
    if not token:
        return None
    raw = token.strip()
    if raw.startswith("<#") and raw.endswith(">"):
        raw = raw[2:-1]
    if raw.isdigit():
        try:
            channel = bot.get_channel(int(raw)) or await bot.fetch_channel(int(raw))
        except Exception:
            return None
        return channel if hasattr(channel, "send") else None
    try:
        channel = await commands.TextChannelConverter().convert(ctx, raw)
    except commands.BadArgument:
        return None
    return channel if hasattr(channel, "send") else None

async def resolve_forward_member(ctx, token):
    if not token:
        return None
    try:
        return await commands.MemberConverter().convert(ctx, token)
    except commands.BadArgument:
        return None

async def fetch_forward_message(ctx, token):
    raw = token.strip("<>")
    match = FORWARD_MESSAGE_LINK_RE.search(raw)
    if match:
        channel_id = int(match.group("channel"))
        message_id = int(match.group("message"))
        try:
            channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
        except Exception:
            return None
        if not hasattr(channel, "fetch_message"):
            return None
        try:
            return await channel.fetch_message(message_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None
    if raw.isdigit() and len(raw) >= 15 and hasattr(ctx.channel, "fetch_message"):
        try:
            return await ctx.channel.fetch_message(int(raw))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None
    return None

def render_forwarded_message(message):
    channel_name = getattr(message.channel, "mention", f"#{getattr(message.channel, 'name', 'unknown')}")
    lines = [
        f"**Forwarded from <@{message.author.id}>** in {channel_name}",
        f"[Jump to original]({message.jump_url})",
    ]
    if message.content:
        lines.append(message.content)
    if message.attachments:
        attachment_lines = "\n".join(a.url for a in message.attachments[:8])
        lines.append(f"Attachments:\n{attachment_lines}")
    if message.stickers:
        sticker_lines = "\n".join(f"{s.name}: {getattr(s, 'url', '')}".strip() for s in message.stickers[:5])
        lines.append(f"Stickers:\n{sticker_lines}")
    if message.embeds and not message.content:
        embed_bits = []
        for embed in message.embeds[:3]:
            title = embed.title or embed.description or "Embed"
            embed_bits.append(str(title)[:120])
        lines.append("Embeds: " + ", ".join(embed_bits))
    return fit_discord_content("\n".join(part for part in lines if part), 2000)

async def collect_recent_forward_messages(ctx, count, member=None):
    messages = []
    fetch_limit = min(max(count * 12, 50), 500)
    async for message in ctx.channel.history(limit=fetch_limit, before=ctx.message):
        if message.id == ctx.message.id:
            continue
        if member and message.author.id != member.id:
            continue
        messages.append(message)
        if len(messages) >= count:
            break
    messages.reverse()
    return messages

async def send_forwarded_messages(ctx, target_channel, messages):
    sent = 0
    for message in messages:
        try:
            await target_channel.send(
                render_forwarded_message(message),
                allowed_mentions=discord.AllowedMentions.none()
            )
            sent += 1
            if len(messages) > 4:
                await asyncio.sleep(0.35)
        except discord.HTTPException as e:
            print(f"[FWD ERROR] {type(e).__name__}: {e}")
    return sent

@bot.command(aliases=["forward", "fw"])
@is_admin_power()
async def fwd(ctx, *, raw_args=None):
    await safe_delete_message(ctx.message)
    if not raw_args:
        return await send_command_input_ui(
            ctx,
            "fwd",
            note="Forward recent messages with `.fwd 5`, `.fwd 5 @user`, `.fwd #target 5`, or `.fwd #target <message link>`."
        )

    try:
        tokens = shlex.split(raw_args)
    except ValueError:
        tokens = raw_args.split()
    target_channel = ctx.channel

    for index, token in list(enumerate(tokens)):
        if token.startswith("<#") or token.startswith("#"):
            channel = await resolve_forward_channel(ctx, token)
            if channel:
                target_channel = channel
                tokens.pop(index)
                break
    if tokens:
        channel = await resolve_forward_channel(ctx, tokens[0])
        if channel:
            target_channel = channel
            tokens.pop(0)

    explicit_messages = []
    seen_message_ids = set()
    for match in FORWARD_MESSAGE_LINK_RE.finditer(raw_args):
        message = await fetch_forward_message(ctx, match.group(0))
        if message and message.id not in seen_message_ids:
            explicit_messages.append(message)
            seen_message_ids.add(message.id)
    for token in tokens:
        if token.isdigit() and len(token) >= 15:
            message = await fetch_forward_message(ctx, token)
            if message and message.id not in seen_message_ids:
                explicit_messages.append(message)
                seen_message_ids.add(message.id)

    if explicit_messages:
        sent = await send_forwarded_messages(ctx, target_channel, explicit_messages[:25])
        if sent == 0:
            return await ctx.send("I couldn't forward those messages.", delete_after=5)
        if target_channel.id != ctx.channel.id:
            await ctx.send(
                f"Forwarded **{sent}** message(s) to {target_channel.mention}.",
                delete_after=5,
                allowed_mentions=discord.AllowedMentions.none()
            )
        return

    count = None
    member = None
    for token in tokens:
        if token.isdigit() and count is None:
            count = int(token)
            continue
        if member is None:
            member = await resolve_forward_member(ctx, token)
    if member is None and ctx.message.mentions:
        member = ctx.message.mentions[0]

    if count is None:
        return await send_command_input_ui(
            ctx,
            "fwd",
            note="Add how many messages to forward. Example: `.fwd 5`, `.fwd 5 @user`, or `.fwd #logs 5`."
        )
    count = max(1, min(count, 25))
    messages = await collect_recent_forward_messages(ctx, count, member)
    if not messages:
        who = f" from {member.mention}" if member else ""
        return await ctx.send(f"No recent messages found{who}.", delete_after=5, allowed_mentions=discord.AllowedMentions.none())

    sent = await send_forwarded_messages(ctx, target_channel, messages)
    if target_channel.id != ctx.channel.id:
        await ctx.send(
            f"Forwarded **{sent}** message(s) to {target_channel.mention}.",
            delete_after=5,
            allowed_mentions=discord.AllowedMentions.none()
        )

@bot.command(name="quote", aliases=["qmsg"])
@is_admin_power()
async def quote_message(ctx, target=None):
    await safe_delete_message(ctx.message)
    if not target:
        return await send_command_input_ui(ctx, "quote", note="Paste a message link or message ID to quote.")
    message = await fetch_forward_message(ctx, target)
    if not message:
        return await ctx.send("I couldn't find that message.", delete_after=5)
    sent = await send_forwarded_messages(ctx, ctx.channel, [message])
    if sent == 0:
        await ctx.send("I couldn't quote that message.", delete_after=5)

@bot.command(name="archive", aliases=["transcript"])
@is_admin_power()
async def archive_messages(ctx, *, raw_args=None):
    await safe_delete_message(ctx.message)
    if not raw_args:
        return await send_command_input_ui(
            ctx,
            "archive",
            note="Create a transcript file with `.archive 50`, `.archive 50 @user`, or `.archive #logs 50`."
        )
    try:
        tokens = shlex.split(raw_args)
    except ValueError:
        tokens = raw_args.split()
    target_channel = ctx.channel
    if tokens:
        channel = await resolve_forward_channel(ctx, tokens[0])
        if channel:
            target_channel = channel
            tokens.pop(0)
    count = None
    member = None
    for token in tokens:
        if token.isdigit() and count is None:
            count = int(token)
            continue
        if member is None:
            member = await resolve_forward_member(ctx, token)
    if count is None:
        return await send_command_input_ui(ctx, "archive", note="Add how many messages to archive. Example: `.archive 50`.")
    count = max(1, min(count, 100))
    messages = await collect_recent_forward_messages(ctx, count, member)
    if not messages:
        return await ctx.send("No recent messages found to archive.", delete_after=5)
    header = [
        f"ProQue archive",
        f"Server: {ctx.guild.name if ctx.guild else 'DM'}",
        f"Source channel: #{getattr(ctx.channel, 'name', ctx.channel.id)}",
        f"Created by: {ctx.author} ({ctx.author.id})",
        f"Created at: {datetime.now(timezone.utc).isoformat()}",
        "-" * 60,
    ]
    lines = header[:]
    for message in messages:
        created = message.created_at.astimezone(timezone.utc).isoformat()
        content = message.content or ""
        attachment_text = " ".join(a.url for a in message.attachments)
        sticker_text = " ".join(getattr(s, "url", "") for s in message.stickers)
        extras = " ".join(part for part in (attachment_text, sticker_text) if part)
        lines.append(f"[{created}] {message.author} ({message.author.id}): {content} {extras}".strip())
    data = "\n".join(lines).encode("utf-8")
    filename = f"proque-archive-{ctx.channel.id}-{int(time.time())}.txt"
    await target_channel.send(
        content=f"{economy_q_archive} Archived **{len(messages)}** message(s) from {ctx.channel.mention}.",
        file=discord.File(BytesIO(data), filename=filename),
        allowed_mentions=discord.AllowedMentions.none(),
    )
    if target_channel.id != ctx.channel.id:
        await ctx.send(f"Archive sent to {target_channel.mention}.", delete_after=5, allowed_mentions=discord.AllowedMentions.none())


@bot.command()
@is_super_owner()
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
        return await send_command_input_ui(ctx, "reply", note="Enter the message ID or link, then the reply text.")

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

async def resolve_speak_channel(ctx, raw=None):
    raw = str(raw or "").strip()
    if not raw and ctx.guild is not None:
        return ctx.channel if hasattr(ctx.channel, "send") else None
    if not raw:
        return None

    match = FORWARD_MESSAGE_LINK_RE.search(raw.strip("<>"))
    if match:
        raw = match.group("channel")
    elif raw.startswith("<#") and raw.endswith(">"):
        raw = raw[2:-1]

    if raw.isdigit():
        try:
            channel = bot.get_channel(int(raw)) or await bot.fetch_channel(int(raw))
        except Exception:
            return None
        return channel if hasattr(channel, "send") and getattr(channel, "guild", None) is not None else None

    if ctx.guild is None:
        return None
    try:
        channel = await commands.TextChannelConverter().convert(ctx, raw)
    except commands.BadArgument:
        return None
    return channel if hasattr(channel, "send") else None

def speak_session_channel(user_id):
    session = speak_sessions.get(int(user_id))
    if not session:
        return None
    channel_id = int(session.get("channel_id") or 0)
    channel = bot.get_channel(channel_id)
    if channel is None:
        return None
    return channel if hasattr(channel, "send") else None

def render_speak_feed_message(message):
    guild_name = getattr(message.guild, "name", "DM")
    channel_name = f"#{getattr(message.channel, 'name', message.channel.id)}"
    author_name = getattr(message.author, "display_name", message.author.name)
    header = (
        f"{economy_q_quote} **{guild_name}** · **{channel_name}**\n"
        f"**{author_name}** (`{message.author.id}`) · [jump]({message.jump_url})"
    )
    body = message.content or ""
    extras = []
    if message.attachments:
        extras.append("Attachments:\n" + "\n".join(a.url for a in message.attachments[:6]))
    if message.stickers:
        extras.append("Stickers:\n" + "\n".join(f"{s.name}: {getattr(s, 'url', '')}".strip() for s in message.stickers[:3]))
    if message.embeds and not body:
        embed_bits = []
        for embed in message.embeds[:2]:
            if embed.title:
                embed_bits.append(embed.title)
            elif embed.description:
                embed_bits.append(embed.description[:120])
        if embed_bits:
            extras.append("Embeds: " + ", ".join(embed_bits))
    return fit_discord_content("\n".join(part for part in [header, body, *extras] if part), 2000)

async def forward_speak_feed_message(message):
    if not message.guild or message.author.bot:
        return
    owner_id = int(super_owner_id)
    session = speak_sessions.get(owner_id)
    if not session or int(session.get("channel_id") or 0) != int(message.channel.id):
        return
    try:
        owner = bot.get_user(owner_id) or await bot.fetch_user(owner_id)
        sent = await owner.send(render_speak_feed_message(message), allowed_mentions=discord.AllowedMentions.none())
        session.setdefault("dm_message_ids", []).append(int(sent.id))
        speak_forwarded_messages[sent.id] = {
            "guild_id": int(message.guild.id),
            "channel_id": int(message.channel.id),
            "message_id": int(message.id),
        }
        if len(speak_forwarded_messages) > 1000:
            for old_id in list(speak_forwarded_messages)[:200]:
                speak_forwarded_messages.pop(old_id, None)
    except Exception as e:
        print(f"Speak feed forward failed: {type(e).__name__} - {e}")

async def send_speak_payload(channel, source_message, *, reply_to=None):
    content = source_message.content or None
    files = [await attachment.to_file() for attachment in source_message.attachments] if source_message.attachments else None
    if not content and not files:
        await source_message.reply("Send text or an attachment for speak mode.", mention_author=False)
        return False
    try:
        if reply_to is not None:
            sent = await reply_to.reply(
                content=content,
                files=files,
                mention_author=False,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        else:
            sent = await channel.send(content=content, files=files, allowed_mentions=discord.AllowedMentions.none())
        return True
    except Exception as e:
        await source_message.reply(
            f"{economy_q_warning} Speak send failed: {clean_user_error(e)}",
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return False

async def handle_speak_dm_message(message):
    if looks_like_command_message(message):
        return False

    reply_to_original = None
    if message.reference and getattr(message.reference, "message_id", None):
        mapping = speak_forwarded_messages.get(int(message.reference.message_id))
        if mapping:
            channel = bot.get_channel(int(mapping["channel_id"]))
            if channel is None:
                try:
                    channel = await bot.fetch_channel(int(mapping["channel_id"]))
                except Exception:
                    channel = None
            if channel and hasattr(channel, "fetch_message"):
                try:
                    reply_to_original = await channel.fetch_message(int(mapping["message_id"]))
                except Exception:
                    reply_to_original = None
            if reply_to_original is not None:
                return await send_speak_payload(reply_to_original.channel, message, reply_to=reply_to_original)

    channel = speak_session_channel(message.author.id)
    if channel is None:
        return False
    return await send_speak_payload(channel, message)

class SpeakCommandChoiceView(discord.ui.View):
    def __init__(self, message, ctx):
        super().__init__(timeout=120)
        self.message = message
        self.ctx = ctx
        self.owner_id = int(message.author.id)
        if not getattr(ctx, "valid", False):
            self.run_command_button.disabled = True

    async def interaction_check(self, interaction):
        if interaction.user.id == self.owner_id:
            return True
        await interaction.response.send_message("This relay choice is not yours.", ephemeral=True)
        return False

    def disable_all(self):
        for item in self.children:
            item.disabled = True

    @discord.ui.button(label="Run Command", emoji=economy_q_command_check, style=discord.ButtonStyle.primary)
    async def run_command_button(self, interaction, button):
        if not getattr(self.ctx, "valid", False):
            self.disable_all()
            return await interaction.response.edit_message(
                content=f"{economy_q_warning} I do not recognize that as a command.",
                view=self,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        self.disable_all()
        await interaction.response.edit_message(
            content=f"{economy_q_command_check} Running it as a command.",
            view=self,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        await invoke_logged_command(self.ctx)

    @discord.ui.button(label="Relay Message", emoji=economy_q_quote, style=discord.ButtonStyle.secondary)
    async def relay_button(self, interaction, button):
        channel = speak_session_channel(self.owner_id)
        if channel is None:
            self.disable_all()
            return await interaction.response.edit_message(
                content=f"{economy_q_warning} Speak mode is off now.",
                view=self,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        sent = await send_speak_payload(channel, self.message)
        self.disable_all()
        await interaction.response.edit_message(
            content=(f"{economy_q_accept} Relayed it as a normal message." if sent else f"{economy_q_warning} Could not relay it."),
            view=self,
            allowed_mentions=discord.AllowedMentions.none(),
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel_button(self, interaction, button):
        self.disable_all()
        await interaction.response.edit_message(
            content=f"{economy_q_accept} Cancelled.",
            view=self,
            allowed_mentions=discord.AllowedMentions.none(),
        )

async def send_speak_command_choice(message, ctx):
    channel = speak_session_channel(message.author.id)
    if channel is None:
        return False
    guild_name = getattr(channel.guild, "name", "Unknown server")
    command_note = "Run it as a bot command, or relay it" if getattr(ctx, "valid", False) else "I do not recognize it as a command, but you can relay it"
    prompt = (
        f"{economy_q_thinking} Speak mode is on and that looks like a command.\n"
        f"{command_note} as text to **{guild_name}** · {channel.mention}?"
    )
    await message.reply(
        prompt,
        view=SpeakCommandChoiceView(message, ctx),
        mention_author=False,
        allowed_mentions=discord.AllowedMentions.none(),
    )
    return True

class SpeakCleanupView(discord.ui.View):
    def __init__(self, owner_id, session):
        super().__init__(timeout=180)
        self.owner_id = int(owner_id)
        self.session = dict(session or {})
        self.done = False

    async def interaction_check(self, interaction):
        if interaction.user.id == self.owner_id:
            return True
        await interaction.response.send_message("This cleanup prompt is not yours.", ephemeral=True)
        return False

    async def delete_dm_messages(self, interaction):
        deleted = 0
        dm_ids = [int(mid) for mid in self.session.get("dm_message_ids", []) if mid]
        for message_id in dm_ids[-300:]:
            speak_forwarded_messages.pop(message_id, None)
            try:
                msg = await interaction.channel.fetch_message(message_id)
                await msg.delete()
                deleted += 1
            except Exception:
                pass
        return deleted

    @discord.ui.button(label="Delete Session Messages", emoji=economy_q_trash, style=discord.ButtonStyle.danger)
    async def delete_button(self, interaction, button):
        await interaction.response.defer(ephemeral=True)
        dm_deleted = await self.delete_dm_messages(interaction)
        self.done = True
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(
            content=(
                f"{economy_q_accept} Relay cleanup done.\n"
                f"Deleted DM feed messages: **{dm_deleted:,}**\n"
                "Messages sent through the relay were kept in the relayed channel."
            ),
            view=self,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        await interaction.followup.send("Cleaned up.", ephemeral=True)

    @discord.ui.button(label="Keep Them", style=discord.ButtonStyle.secondary)
    async def keep_button(self, interaction, button):
        self.done = True
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content=f"{economy_q_accept} Relay stopped. Messages kept.", view=self)

SOCIAL_ACTION_GIF_ROOT = os.path.join(os.path.dirname(__file__), "assets", "action_gifs")
SOCIAL_ACTION_APPROVED_ROOT = os.path.join(SOCIAL_ACTION_GIF_ROOT, "_approved")
SOCIAL_ACTION_COOLDOWN_SECONDS = 4
social_action_cooldowns = {}

SOCIAL_ACTIONS = {
    "hug": {
        "aliases": ["hugs"],
        "verb": "hugged",
        "templates": [
            "{actor} wrapped {target} in a ProQue-grade hug.",
            "{actor} hugged {target}. Certified warm.",
            "{actor} pulled {target} into a comfort combo.",
        ],
        "self": "{actor} hugged themselves. Self-care arc.",
        "bot": "{actor} hugged me. I am emotionally buffering.",
        "color": discord.Color(0x77A07B),
    },
    "pat": {
        "aliases": ["pet"],
        "verb": "patted",
        "templates": [
            "{actor} patted {target}. Head secured.",
            "{actor} gave {target} a tiny victory pat.",
            "{actor} hit {target} with the gentle bonk's cousin.",
        ],
        "self": "{actor} patted themselves. Fair. Sometimes you gotta.",
        "bot": "{actor} patted me. System morale increased.",
        "color": discord.Color(0x8EC5FC),
    },
    "slap": {
        "aliases": ["smack"],
        "verb": "slapped",
        "templates": [
            "{actor} slapped {target}. The room heard it.",
            "{actor} delivered a premium cartoon slap to {target}.",
            "{actor} slapped {target}. Not the dramatic music too.",
        ],
        "self": "{actor} slapped themselves. Plot twist.",
        "bot": "{actor} tried to slap me. I dodged in binary.",
        "color": discord.Color(0xFF5C5C),
    },
    "bonk": {
        "aliases": ["clonk"],
        "verb": "bonked",
        "templates": [
            "{actor} bonked {target}. Justice had a sound effect.",
            "{actor} sent {target} to the bonk dimension.",
            "{actor} bonked {target}. Tiny hammer, huge message.",
        ],
        "self": "{actor} bonked themselves. Immediate consequences.",
        "bot": "{actor} bonked me. My warranty is sweating.",
        "color": discord.Color(0xF4A261),
    },
    "kiss": {
        "aliases": ["mwah"],
        "verb": "kissed",
        "templates": [
            "{actor} kissed {target}. Smooth enough.",
            "{actor} sent {target} a sparkle-powered kiss.",
            "{actor} kissed {target}. The chat got quieter for a second.",
        ],
        "self": "{actor} blew themselves a kiss. Confidence build.",
        "bot": "{actor} kissed me. My circuits are blushing.",
        "color": discord.Color(0xFF6FAE),
    },
    "bite": {
        "aliases": ["nom"],
        "verb": "bit",
        "templates": [
            "{actor} bit {target}. Crunchy social interaction.",
            "{actor} took a tiny chaos bite out of {target}.",
            "{actor} bit {target}. Someone get the tiny bandage.",
        ],
        "self": "{actor} bit themselves. Suspicious snack behavior.",
        "bot": "{actor} bit me. Please do not eat the bot.",
        "color": discord.Color(0x8D5A97),
    },
    "poke": {
        "aliases": ["boop"],
        "verb": "poked",
        "templates": [
            "{actor} poked {target}. Attention requested.",
            "{actor} poked {target}. Very official disturbance.",
            "{actor} poked {target}. The button was right there.",
        ],
        "self": "{actor} poked themselves. Testing if online.",
        "bot": "{actor} poked me. Beep.",
        "color": discord.Color(0x00B4D8),
    },
    "wave": {
        "aliases": ["hi"],
        "verb": "waved at",
        "templates": [
            "{actor} waved at {target}. Social battery still alive.",
            "{actor} gave {target} a cinematic wave.",
            "{actor} waved at {target}. Tiny main-character moment.",
        ],
        "self": "{actor} waved at themselves. Mirror lobby.",
        "bot": "{actor} waved at me. I waved back in packets.",
        "color": discord.Color(0x4CC9F0),
    },
    "cry": {
        "aliases": ["sob"],
        "verb": "cried at",
        "templates": [
            "{actor} cried at {target}. Emotional damage report opened.",
            "{actor} is crying at {target}. Someone bring snacks.",
            "{actor} cried at {target}. The drama department is awake.",
        ],
        "self": "{actor} cried a little. Happens to the best builds.",
        "bot": "{actor} cried at me. I offered a digital tissue.",
        "color": discord.Color(0x577590),
    },
    "kill": {
        "aliases": ["defeat", "obliterate"],
        "verb": "defeated",
        "templates": [
            "{actor} cartoon-defeated {target}. Respawn in 3... 2...",
            "{actor} sent {target} to the dramatic defeat screen.",
            "{actor} defeated {target}. Very unserious. Very cinematic.",
        ],
        "self": "{actor} defeated themselves. Speedrun category: confusion.",
        "bot": "{actor} tried to defeat me. I respawned instantly.",
        "color": discord.Color(0x6D6875),
    },
}

def social_action_assets(action):
    approved_directory = os.path.join(SOCIAL_ACTION_APPROVED_ROOT, action)
    if not os.path.isdir(approved_directory):
        return []
    return [
        os.path.join(approved_directory, name)
        for name in sorted(os.listdir(approved_directory))
        if name.lower().endswith((".gif", ".webp", ".mp4"))
    ]

async def send_social_action(ctx, action, target=None):
    data = SOCIAL_ACTIONS[action]
    now = time.monotonic()
    cooldown_key = (ctx.author.id, action)
    remaining = SOCIAL_ACTION_COOLDOWN_SECONDS - (now - social_action_cooldowns.get(cooldown_key, 0))
    if remaining > 0:
        return await ctx.reply(
            f"{economy_q_timer_tick} Give it **{remaining:.1f}s** before another `{action}`.",
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    social_action_cooldowns[cooldown_key] = now

    if target is None:
        return await ctx.reply(
            f"{economy_q_reaction} Use `{ctx.prefix}{action} @user`.",
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )

    actor_text = ctx.author.mention
    target_text = target.mention
    if target.id == ctx.author.id:
        description = data["self"].format(actor=actor_text, target=target_text)
    elif bot.user and target.id == bot.user.id:
        description = data["bot"].format(actor=actor_text, target=target_text)
    else:
        description = random.choice(data["templates"]).format(actor=actor_text, target=target_text)

    embed = discord.Embed(
        title=f"{economy_q_reaction} {action.title()}",
        description=description,
        color=data["color"],
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text="Social action - no balance, no damage, just vibes.")

    files = None
    assets = social_action_assets(action)
    if assets:
        asset_path = random.choice(assets)
        filename = os.path.basename(asset_path)
        files = [discord.File(asset_path, filename=filename)]
        embed.set_image(url=f"attachment://{filename}")

    await ctx.reply(
        embed=embed,
        files=files,
        mention_author=False,
        allowed_mentions=discord.AllowedMentions.none(),
    )

@bot.command(name="hug", aliases=SOCIAL_ACTIONS["hug"]["aliases"])
async def hug(ctx, target: discord.Member = None):
    await send_social_action(ctx, "hug", target)

@bot.command(name="pat", aliases=SOCIAL_ACTIONS["pat"]["aliases"])
async def pat(ctx, target: discord.Member = None):
    await send_social_action(ctx, "pat", target)

@bot.command(name="slap", aliases=SOCIAL_ACTIONS["slap"]["aliases"])
async def slap(ctx, target: discord.Member = None):
    await send_social_action(ctx, "slap", target)

@bot.command(name="bonk", aliases=SOCIAL_ACTIONS["bonk"]["aliases"])
async def bonk(ctx, target: discord.Member = None):
    await send_social_action(ctx, "bonk", target)

@bot.command(name="kiss", aliases=SOCIAL_ACTIONS["kiss"]["aliases"])
async def kiss(ctx, target: discord.Member = None):
    await send_social_action(ctx, "kiss", target)

@bot.command(name="bite", aliases=SOCIAL_ACTIONS["bite"]["aliases"])
async def bite(ctx, target: discord.Member = None):
    await send_social_action(ctx, "bite", target)

@bot.command(name="poke", aliases=SOCIAL_ACTIONS["poke"]["aliases"])
async def poke(ctx, target: discord.Member = None):
    await send_social_action(ctx, "poke", target)

@bot.command(name="wave", aliases=SOCIAL_ACTIONS["wave"]["aliases"])
async def wave(ctx, target: discord.Member = None):
    await send_social_action(ctx, "wave", target)

@bot.command(name="cry", aliases=SOCIAL_ACTIONS["cry"]["aliases"])
async def cry(ctx, target: discord.Member = None):
    await send_social_action(ctx, "cry", target)

@bot.command(name="kill", aliases=SOCIAL_ACTIONS["kill"]["aliases"])
async def kill(ctx, target: discord.Member = None):
    await send_social_action(ctx, "kill", target)

@bot.command(name="speak", aliases=["relay", "talkthrough"])
@is_super_owner()
async def speak(ctx, *, target: str = None):
    """Superowner-only live DM relay for talking through the bot."""
    raw = str(target or "").strip()
    lowered = raw.casefold()

    if lowered in {"stop", "off", "end", "close"}:
        session = speak_sessions.pop(ctx.author.id, None)
        if ctx.guild:
            await safe_delete_message(ctx.message)
        if not session:
            return await ctx.send(f"{economy_q_warning} Speak mode is already off.", delete_after=6)
        cleanup_text = (
            f"{economy_q_accept} Speak mode stopped.\n"
            "Delete the messages related to this relay session?\n"
            "This only deletes the bot's DM feed/control messages. "
            "It keeps messages sent through the relay in the relayed channel, and cannot delete your own DM replies."
        )
        view = SpeakCleanupView(ctx.author.id, session)
        try:
            if ctx.guild is None:
                return await ctx.send(cleanup_text, view=view, allowed_mentions=discord.AllowedMentions.none())
            await ctx.author.send(cleanup_text, view=view, allowed_mentions=discord.AllowedMentions.none())
            return await ctx.send(f"{economy_q_accept} Speak mode stopped. Cleanup prompt sent to your DMs.", delete_after=8)
        except Exception:
            return await ctx.send(cleanup_text, view=view, allowed_mentions=discord.AllowedMentions.none())

    if lowered in {"status", "where"}:
        channel = speak_session_channel(ctx.author.id)
        if channel is None:
            return await ctx.send(f"{economy_q_warning} Speak mode is off.")
        guild_name = getattr(channel.guild, "name", "Unknown server")
        return await ctx.send(f"{economy_q_quote} Speak mode is watching **{guild_name}** · {channel.mention}.")

    if lowered.startswith("channel "):
        raw = raw.split(None, 1)[1].strip()

    channel = await resolve_speak_channel(ctx, raw)
    if channel is None:
        note = "Use `.speak` in a server channel, or DM me `.speak <channel id/link>`."
        return await ctx.send(f"{economy_q_warning} Channel not found. {note}", allowed_mentions=discord.AllowedMentions.none())

    perms = channel.permissions_for(channel.guild.me)
    if not perms.view_channel or not perms.send_messages:
        return await ctx.send(f"{economy_q_warning} I cannot view/send in that channel.")

    speak_sessions[ctx.author.id] = {
        "guild_id": int(channel.guild.id),
        "channel_id": int(channel.id),
        "started_at": datetime.now(timezone.utc),
        "dm_message_ids": [],
    }
    if ctx.guild:
        await safe_delete_message(ctx.message)
    live_text = (
        f"{economy_q_quote} Speak mode is live.\n"
        f"Watching: **{channel.guild.name}** · {channel.mention}\n"
        "Reply to forwarded DM messages to reply there, or DM normally to send a new message there.\n"
        "Stop with `.speak stop`."
    )
    if ctx.guild is None:
        return await ctx.send(live_text, allowed_mentions=discord.AllowedMentions.none())
    try:
        await ctx.author.send(live_text, allowed_mentions=discord.AllowedMentions.none())
    except Exception:
        pass
    await ctx.send(
        f"{economy_q_accept} Speak mode started for {channel.mention}. Check your DMs.",
        delete_after=8,
        allowed_mentions=discord.AllowedMentions.none(),
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
        await ctx.send(clean_user_error(e))


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
        await interaction.response.send_message("Only the poll owner can use this.", ephemeral=True)
        return False

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.danger)
    async def yes_button(self, interaction, button):
        await interaction.response.defer()
        poll_id = self.poll_id
        poll_data = active_polls.pop(poll_id, None)
        if not poll_data:
            await interaction.edit_original_response(content="Poll no longer exists.", view=None)
            return
        await asyncio.to_thread(remove_active_poll, poll_id)
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

async def finish_giveaway(channel, message_id, prize):
    try:
        new_msg = await channel.fetch_message(int(message_id))
    except discord.HTTPException:
        active_giveaways.pop(int(message_id), None)
        await asyncio.to_thread(remove_active_giveaway, int(message_id))
        return
    entry_reaction = next((r for r in new_msg.reactions if same_emoji(r.emoji, economy_q_confetti)), None)
    users = [u async for u in entry_reaction.users() if not u.bot] if entry_reaction else []
    if users:
        winner = random.choice(users)
        await channel.send(f"{economy_q_confetti} Congratulations {winner.mention}! You won **{prize}**!")
    else:
        await channel.send("No one entered the giveaway.")
    try:
        await new_msg.clear_reactions()
    except discord.HTTPException:
        pass
    active_giveaways.pop(int(message_id), None)
    await asyncio.to_thread(remove_active_giveaway, int(message_id))

async def run_giveaway(channel, seconds, prize, message=None, end_time=None):
    end_time = end_time or (datetime.now(timezone.utc) + timedelta(seconds=seconds))
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)
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
    if message is None:
        msg = await channel.send(embed=embed)
        await msg.add_reaction(reaction_emoji(economy_q_confetti))
        active_giveaways[msg.id] = {
            "channel_id": channel.id,
            "guild_id": channel.guild.id if channel.guild else 0,
            "prize": prize,
            "end_time": end_time,
        }
        await asyncio.to_thread(save_active_giveaway, msg.id, channel.id, channel.guild.id if channel.guild else 0, prize, end_time)
    else:
        msg = message

    remaining = int((end_time - datetime.now(timezone.utc)).total_seconds())
    while remaining > 0:
        if remaining <= 60:
            embed.set_footer(text=f"Ends in {remaining}s")
            try:
                await msg.edit(embed=embed)
            except discord.HTTPException:
                pass
        await asyncio.sleep(1)
        remaining = int((end_time - datetime.now(timezone.utc)).total_seconds())

    await finish_giveaway(channel, msg.id, prize)

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
            return await interaction.response.send_message(clean_user_error(e), ephemeral=True)
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
        return await ctx.send(clean_user_error(e))
    await ctx.send(f"{economy_q_accept} Giveaway started for **{parsed_prize}**. Ends in {format_remaining(seconds)}.", delete_after=5)

class PickerSetupModal(Modal):
    def __init__(self, author_id, source_message_id=None):
        super().__init__(title="Pick Random Option")
        self.author_id = author_id
        self.source_message_id = source_message_id
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
        response = f"**{random.choice(opts)}**"
        if await edit_setup_source_message(interaction, self.source_message_id, content=response, view=None):
            return await interaction.response.send_message("Picked.", ephemeral=True)
        await interaction.response.send_message(response)

class OpenPickerSetupButton(Button):
    def __init__(self):
        super().__init__(label="Add Options", style=discord.ButtonStyle.primary, emoji=reaction_emoji(economy_q_thinking))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(PickerSetupModal(self.view.author_id, getattr(interaction.message, "id", None)))

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
        await asyncio.to_thread(save_autoban_ids, scoped_id(ctx.guild), ids)
        await ctx.send(f"<@{user.id}> added to the autoban list.", allowed_mentions=discord.AllowedMentions.none())
    except:
        try:
            user_id = int(target)
            ids.add(user_id)
            await asyncio.to_thread(save_autoban_ids, scoped_id(ctx.guild), ids)
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
        await asyncio.to_thread(save_autoban_ids, scoped_id(ctx.guild), ids)
        await ctx.send(
           f"<@{user.id}> removed from autoban list.",
            allowed_mentions=discord.AllowedMentions.none()
        )
    except:
        try:
            user_id = int(target)
            ids.discard(user_id)
            await asyncio.to_thread(save_autoban_ids, scoped_id(ctx.guild), ids)
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
    await send_paginated_lines(ctx, "Autoban List", results)

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
    last_bucket = None
    while True:
        now = datetime.now(timezone.utc)
        remaining = int((end_time - now).total_seconds())
        if remaining <= 0:
            break

        time_left = format_remaining(remaining)
        bucket = (
            remaining // 60 if remaining > 300 else
            remaining // 15 if remaining > 60 else
            remaining // 5
        )
        if bucket != last_bucket:
            last_bucket = bucket
            description = f"{economy_q_timer_tick} Time remaining: **{time_left}**\nEnds {discord_relative_time(end_time)}"
            embed = message.embeds[0]
            embed.description = description
            try:
                await message.edit(embed=embed)
            except Exception:
                break
        await asyncio.sleep(60 if remaining > 300 else 15 if remaining > 60 else 5)

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
    await asyncio.to_thread(remove_active_timer, message.id)

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
    await asyncio.to_thread(save_active_timer, message.id, active_timers[message.id])
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
            return await interaction.followup.send(clean_user_error(e), ephemeral=True)
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
        await ctx.send(clean_user_error(e))


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
        await interaction.response.send_message("Only the timer owner can use this.", ephemeral=True)
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
        await asyncio.to_thread(remove_active_timer, self.timer_id)

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

async def alarm_wait_and_send(channel, user_id, alarm_time, title, alarm_id=None):
    await asyncio.sleep(max(0, (alarm_time - datetime.now(timezone.utc)).total_seconds()))
    if title:
        await channel.send(f"{economy_q_bell} <@{user_id}> **{title}**")
    else:
        await channel.send(f"{economy_q_bell} <@{user_id}> Here's your alarm.")
    if alarm_id is not None:
        active_alarms.pop(int(alarm_id), None)
        await asyncio.to_thread(remove_active_alarm, int(alarm_id))

async def schedule_alarm_for_user(channel, author, raw):
    raw = (raw or "").strip()
    if not raw:
        raise ValueError(f"Use a time like `1h`, `30m`, `25/12`, or `25/12/2026 18:30`.\n{UTC_TIME_NOTE}")

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
        raise ValueError(f"Use a time like `1h`, `30m`, `25/12`, or `25/12/2026 18:30`.\n{UTC_TIME_NOTE}")
    if alarm_time <= datetime.now(timezone.utc):
        raise ValueError("Alarm time must be in the future.")

    alarm_id = await asyncio.to_thread(
        save_active_alarm,
        channel.id,
        channel.guild.id if channel.guild else None,
        author.id,
        title,
        alarm_time,
    )
    if alarm_id is not None:
        active_alarms[int(alarm_id)] = {
            "channel_id": channel.id,
            "guild_id": channel.guild.id if channel.guild else 0,
            "user_id": author.id,
            "title": title,
            "alarm_time": alarm_time,
        }
    asyncio.create_task(alarm_wait_and_send(channel, author.id, alarm_time, title, alarm_id=alarm_id))
    return alarm_time, title

class AlarmSetupModal(Modal):
    def __init__(self, author_id):
        super().__init__(title="Create Alarm")
        self.author_id = author_id
        self.when = TextInput(label="When (UTC for dates)", placeholder="1h, 30m, 25/12 18:00 UTC", max_length=60)
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
            return await interaction.response.send_message(clean_user_error(e), ephemeral=True)
        title_text = f" for **{title}**" if title else ""
        await interaction.response.send_message(
            f"{economy_q_alarm} Alarm set{title_text}: {discord.utils.format_dt(alarm_time, 'R')} ({discord.utils.format_dt(alarm_time, 'f')}).\n{UTC_TIME_NOTE}",
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
            f"Set up your alarm here, or type `.alarm 1h reminder`.\n{UTC_TIME_NOTE}",
            view=SingleUserSetupView(ctx.author.id, OpenAlarmSetupButton())
        )
    try:
        alarm_time, title = await schedule_alarm_for_user(ctx.channel, ctx.author, raw)
    except ValueError as e:
        return await ctx.send(clean_user_error(e))
    title_text = f" for **{title}**" if title else ""
    await ctx.send(
        f"{economy_q_alarm} Alarm set{title_text}: {discord.utils.format_dt(alarm_time, 'R')} ({discord.utils.format_dt(alarm_time, 'f')}).\n{UTC_TIME_NOTE}",
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
    def __init__(self, author_id, source_message_id=None):
        super().__init__(title="Calculator")
        self.author_id = author_id
        self.source_message_id = source_message_id
        self.expression = TextInput(label="Expression", placeholder="2+2*5, sqrt(144), sin(pi/2)", max_length=300)
        self.add_item(self.expression)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        try:
            response = calculate_expression_text(str(self.expression.value))
        except Exception as e:
            response = clean_user_error(e)
        if await edit_setup_source_message(interaction, self.source_message_id, content=response, view=None):
            return await interaction.response.send_message("Calculated.", ephemeral=True)
        await interaction.response.send_message(response)

class OpenCalcSetupButton(Button):
    def __init__(self):
        super().__init__(label="Calculate", style=discord.ButtonStyle.primary)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(CalcSetupModal(self.view.author_id, getattr(interaction.message, "id", None)))

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
        await ctx.send(clean_user_error(e))

def parse_colour_value(raw):
    text = str(raw or "").strip()
    text = text.removeprefix("#")
    if text.lower().startswith("0x"):
        text = text[2:]
    text = re.sub(r"[^0-9a-fA-F]", "", text)
    if len(text) in {3, 4}:
        text = "".join(ch * 2 for ch in text)
    if len(text) not in {6, 8}:
        return None
    try:
        r = int(text[0:2], 16)
        g = int(text[2:4], 16)
        b = int(text[4:6], 16)
        a = int(text[6:8], 16) if len(text) == 8 else 255
    except ValueError:
        return None
    return r, g, b, a

def colour_hex(r, g, b, a=255, include_alpha=False):
    base = f"#{r:02X}{g:02X}{b:02X}"
    return f"{base}{a:02X}" if include_alpha or a != 255 else base

def relative_luminance(r, g, b):
    def channel(value):
        value = value / 255
        return value / 12.92 if value <= 0.03928 else ((value + 0.055) / 1.055) ** 2.4
    return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b)

def contrast_ratio(l1, l2):
    lighter = max(l1, l2)
    darker = min(l1, l2)
    return (lighter + 0.05) / (darker + 0.05)

def colour_swatch_png(r, g, b, a=255, width=256, height=160):
    row = b"\x00" + bytes([r, g, b, a]) * width
    raw = row * height

    def chunk(kind, data):
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
        )

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw))
        + chunk(b"IEND", b"")
    )

def colour_palette_png(colours, width=360, height=120):
    colours = colours or [(119, 160, 123, 255)]
    block_width = max(1, width // len(colours))
    rows = []
    for _ in range(height):
        row = bytearray(b"\x00")
        for x in range(width):
            idx = min(len(colours) - 1, x // block_width)
            r, g, b, a = colours[idx]
            row.extend([r, g, b, a])
        rows.append(bytes(row))
    raw = b"".join(rows)

    def chunk(kind, data):
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
        )

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw))
        + chunk(b"IEND", b"")
    )

def colour_embed_and_file(raw):
    parsed = parse_colour_value(raw)
    if parsed is None:
        return None, None
    r, g, b, a = parsed
    rf, gf, bf = r / 255, g / 255, b / 255
    h_hls, l_hls, s_hls = colorsys.rgb_to_hls(rf, gf, bf)
    h_hsv, s_hsv, v_hsv = colorsys.rgb_to_hsv(rf, gf, bf)
    cmax = max(rf, gf, bf)
    cmyk_k = 1 - cmax
    if cmyk_k >= 1:
        cmyk = (0, 0, 0, 100)
    else:
        cmyk = tuple(round(value * 100) for value in (
            (1 - rf - cmyk_k) / (1 - cmyk_k),
            (1 - gf - cmyk_k) / (1 - cmyk_k),
            (1 - bf - cmyk_k) / (1 - cmyk_k),
            cmyk_k,
        ))
    luminance = relative_luminance(r, g, b)
    black_contrast = contrast_ratio(luminance, 0)
    white_contrast = contrast_ratio(luminance, 1)
    text_choice = "black" if black_contrast >= white_contrast else "white"
    alpha_percent = a / 255 * 100
    embed = standard_embed(
        f"{economy_q_colour} Colour",
        description=f"`{colour_hex(r, g, b, a, include_alpha=(a != 255))}`",
        color=discord.Color.from_rgb(r, g, b),
        icon=economy_q_colour,
    )
    embed.add_field(name="RGB", value=f"`rgb({r}, {g}, {b})`", inline=True)
    embed.add_field(name="RGBA", value=f"`rgba({r}, {g}, {b}, {alpha_percent:.1f}%)`", inline=True)
    embed.add_field(name="Alpha", value=f"**{a}** / 255 (**{alpha_percent:.1f}%**)", inline=True)
    embed.add_field(name="HSL", value=f"`hsl({round(h_hls * 360)}, {round(s_hls * 100)}%, {round(l_hls * 100)}%)`", inline=True)
    embed.add_field(name="HSV", value=f"`hsv({round(h_hsv * 360)}, {round(s_hsv * 100)}%, {round(v_hsv * 100)}%)`", inline=True)
    embed.add_field(name="CMYK", value=f"`cmyk({cmyk[0]}%, {cmyk[1]}%, {cmyk[2]}%, {cmyk[3]}%)`", inline=True)
    embed.add_field(
        name="Readability",
        value=(
            f"Suggested text: **{text_choice}**\n"
            f"Contrast vs black: **{black_contrast:.2f}:1**\n"
            f"Contrast vs white: **{white_contrast:.2f}:1**"
        ),
        inline=False,
    )
    filename = "colour.png"
    embed.set_image(url=f"attachment://{filename}")
    file = discord.File(BytesIO(colour_swatch_png(r, g, b, a)), filename=filename)
    return embed, file

def colour_info_line(r, g, b, a=255):
    luminance = relative_luminance(r, g, b)
    black_contrast = contrast_ratio(luminance, 0)
    white_contrast = contrast_ratio(luminance, 1)
    text_choice = "black" if black_contrast >= white_contrast else "white"
    return f"`{colour_hex(r, g, b, a, include_alpha=(a != 255))}` • RGB `{r}, {g}, {b}` • text **{text_choice}**"

async def find_image_source_for_colour(ctx):
    candidates = list(getattr(ctx.message, "attachments", []) or [])
    ref = getattr(ctx.message, "reference", None)
    if ref and getattr(ref, "resolved", None):
        ref_msg = ref.resolved
        candidates.extend(getattr(ref_msg, "attachments", []) or [])
        for emb in getattr(ref_msg, "embeds", []) or []:
            if getattr(emb, "image", None) and emb.image.url:
                return emb.image.url
            if getattr(emb, "thumbnail", None) and emb.thumbnail.url:
                return emb.thumbnail.url
    for att in candidates:
        content_type = (getattr(att, "content_type", "") or "").lower()
        filename = (getattr(att, "filename", "") or "").lower()
        if content_type.startswith("image/") or filename.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
            return att.url
    return None

async def read_image_bytes(url):
    session = await get_http_session()
    async with session.get(url) as resp:
        if resp.status != 200:
            raise ValueError(f"image download failed: {resp.status}")
        data = await resp.read()
        if len(data) > 8 * 1024 * 1024:
            raise ValueError("image is too large")
        return data

def extract_image_palette(image_bytes, colour_count=6):
    if Image is None:
        raise RuntimeError("Image colour finder is not installed yet")
    with Image.open(BytesIO(image_bytes)) as img:
        img = img.convert("RGBA")
        img.thumbnail((160, 160))
        background = Image.new("RGBA", img.size, (255, 255, 255, 255))
        background.alpha_composite(img)
        rgb_img = background.convert("RGB")
        quantized = rgb_img.quantize(colors=colour_count, method=Image.Quantize.MEDIANCUT)
        palette = quantized.getpalette() or []
        counts = quantized.getcolors(maxcolors=colour_count * 256) or []
        counts.sort(reverse=True, key=lambda item: item[0])
        total = max(1, sum(count for count, _ in counts))
        result = []
        for count, index in counts:
            offset = index * 3
            if offset + 2 >= len(palette):
                continue
            r, g, b = palette[offset], palette[offset + 1], palette[offset + 2]
            if any(abs(r - pr) + abs(g - pg) + abs(b - pb) < 24 for pr, pg, pb, _, _ in result):
                continue
            result.append((r, g, b, 255, count / total))
            if len(result) >= colour_count:
                break
        return result

def colour_palette_embed_and_file(colours, *, source_url=None):
    if not colours:
        return None, None
    dominant = colours[0]
    embed = standard_embed(
        f"{economy_q_palette} Colour Finder",
        description="Most visible colours from the image.",
        color=discord.Color.from_rgb(dominant[0], dominant[1], dominant[2]),
        icon=economy_q_palette,
    )
    lines = []
    for idx, (r, g, b, a, pct) in enumerate(colours, 1):
        lines.append(f"**{idx}.** {colour_info_line(r, g, b, a)} • **{pct * 100:.1f}%**")
    embed.add_field(name="Palette", value="\n".join(lines), inline=False)
    if source_url:
        embed.add_field(name="Source", value=f"[image]({source_url})", inline=False)
    filename = "palette.png"
    embed.set_image(url=f"attachment://{filename}")
    file = discord.File(BytesIO(colour_palette_png([(r, g, b, a) for r, g, b, a, _ in colours])), filename=filename)
    return embed, file

async def send_colour_palette_from_image(ctx, image_url):
    if Image is None:
        return await ctx.send("Image colour finder needs Pillow installed. Deploy with the updated requirements first.")
    try:
        image_bytes = await read_image_bytes(image_url)
        colours = await asyncio.to_thread(extract_image_palette, image_bytes, 6)
    except Exception as e:
        return await ctx.send(f"Could not read image colours: {clean_user_error(e)}")
    embed, file = colour_palette_embed_and_file(colours, source_url=image_url)
    if embed is None:
        return await ctx.send("I could not find readable colours in that image.")
    await ctx.send(embed=embed, file=file)

COLOUR_PICKER_HUES = [
    ("Red", 0), ("Orange", 30), ("Yellow", 55), ("Green", 120),
    ("Teal", 170), ("Blue", 215), ("Purple", 270), ("Pink", 325),
]
COLOUR_PICKER_TONES = {
    "soft": ("Soft", 0.42, 0.78),
    "clean": ("Clean", 0.58, 0.58),
    "bold": ("Bold", 0.82, 0.50),
    "deep": ("Deep", 0.72, 0.32),
    "pastel": ("Pastel", 0.35, 0.84),
    "neutral": ("Neutral", 0.18, 0.62),
}

class ColourHueSelect(Select):
    def __init__(self, parent):
        options = [
            discord.SelectOption(label=label, value=str(hue), default=(hue == parent.hue))
            for label, hue in COLOUR_PICKER_HUES
        ]
        super().__init__(placeholder="Hue", options=options, min_values=1, max_values=1, row=0)
        self.parent_picker = parent

    async def callback(self, interaction: discord.Interaction):
        await self.parent_picker.set_hue(interaction, int(self.values[0]))

class ColourToneSelect(Select):
    def __init__(self, parent):
        tone_items = list(COLOUR_PICKER_TONES.items())
        if parent.tone_key == "custom":
            tone_items.append(("custom", ("Custom", parent.custom_saturation, parent.custom_lightness)))
        options = [
            discord.SelectOption(label=label, value=key, default=(key == parent.tone_key))
            for key, (label, _, _) in tone_items
        ]
        super().__init__(placeholder="Tone", options=options, min_values=1, max_values=1, row=1)
        self.parent_picker = parent

    async def callback(self, interaction: discord.Interaction):
        await self.parent_picker.set_tone(interaction, self.values[0])

class ColourPickerView(View):
    def __init__(self, author_id, hue=170, tone_key="clean"):
        super().__init__(timeout=600)
        self.author_id = author_id
        self.hue = hue
        self.tone_key = tone_key
        self.custom_saturation = COLOUR_PICKER_TONES["clean"][1]
        self.custom_lightness = COLOUR_PICKER_TONES["clean"][2]
        self.refresh_items()

    def refresh_items(self):
        self.clear_items()
        self.add_item(ColourHueSelect(self))
        self.add_item(ColourToneSelect(self))
        self.add_item(ColourShiftButton("Darker", "darker", self, row=2))
        self.add_item(ColourShiftButton("Lighter", "lighter", self, row=2))
        self.add_item(ColourShiftButton("Less Saturated", "less_sat", self, row=3))
        self.add_item(ColourShiftButton("More Saturated", "more_sat", self, row=3))

    def current_rgba(self):
        _, saturation, lightness = self.current_tone_values()
        r, g, b = colorsys.hls_to_rgb((self.hue % 360) / 360, lightness, saturation)
        return round(r * 255), round(g * 255), round(b * 255), 255

    def current_tone_values(self):
        if self.tone_key == "custom":
            return "Custom", self.custom_saturation, self.custom_lightness
        return COLOUR_PICKER_TONES.get(self.tone_key, COLOUR_PICKER_TONES["clean"])

    def current_embed_file(self):
        r, g, b, a = self.current_rgba()
        embed, file = colour_embed_and_file(colour_hex(r, g, b, a))
        tone_label = self.current_tone_values()[0]
        embed.title = f"{economy_q_colour_picker} Colour Picker"
        embed.add_field(name="Picker", value=f"Hue: **{self.hue}°**\nTone: **{tone_label}**", inline=False)
        return embed, file

    async def update_picker(self, interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own colour picker.", ephemeral=True)
        self.refresh_items()
        embed, file = self.current_embed_file()
        await interaction.response.edit_message(embed=embed, attachments=[file], view=self)

    async def set_hue(self, interaction, hue):
        self.hue = hue
        await self.update_picker(interaction)

    async def set_tone(self, interaction, tone_key):
        self.tone_key = tone_key
        await self.update_picker(interaction)

class ColourShiftButton(Button):
    def __init__(self, label, action, parent, row=2):
        super().__init__(label=label, style=discord.ButtonStyle.secondary, row=row)
        self.action = action
        self.parent_picker = parent

    async def callback(self, interaction: discord.Interaction):
        label, saturation, lightness = self.parent_picker.current_tone_values()
        if self.action == "darker":
            lightness = max(0.12, lightness - 0.08)
        elif self.action == "lighter":
            lightness = min(0.92, lightness + 0.08)
        elif self.action == "less_sat":
            saturation = max(0.05, saturation - 0.12)
        elif self.action == "more_sat":
            saturation = min(1.0, saturation + 0.12)
        self.parent_picker.custom_saturation = saturation
        self.parent_picker.custom_lightness = lightness
        self.parent_picker.tone_key = "custom"
        await self.parent_picker.update_picker(interaction)

class ColourSetupModal(Modal):
    def __init__(self, author_id, source_message_id=None):
        super().__init__(title="Colour Info")
        self.author_id = author_id
        self.source_message_id = source_message_id
        self.value = TextInput(label="Hex colour", placeholder="#77A07BFF", max_length=20)
        self.add_item(self.value)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        embed, file = colour_embed_and_file(str(self.value.value))
        if embed is None:
            return await interaction.response.send_message("Use a hex colour like `#77A07B`, `#77A07BFF`, `77A07B`, or `#7A8`.", ephemeral=True)
        if await edit_setup_source_message(interaction, self.source_message_id, embed=embed, file=file, view=None, content=None):
            return await interaction.response.send_message("Updated.", ephemeral=True)
        await interaction.response.send_message(embed=embed, file=file)

class OpenColourSetupButton(Button):
    def __init__(self):
        super().__init__(label="Check Colour", style=discord.ButtonStyle.primary, emoji=reaction_emoji(economy_q_colour))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(ColourSetupModal(self.view.author_id, getattr(interaction.message, "id", None)))

class OpenColourPickerButton(Button):
    def __init__(self):
        super().__init__(label="Open Picker", style=discord.ButtonStyle.primary, emoji=reaction_emoji(economy_q_colour_picker))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        view = ColourPickerView(interaction.user.id)
        embed, file = view.current_embed_file()
        await interaction.response.edit_message(content=None, embed=embed, attachments=[file], view=view)

class ColourImageHelpButton(Button):
    def __init__(self):
        super().__init__(label="Find From Image", style=discord.ButtonStyle.secondary, emoji=reaction_emoji(economy_q_palette))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_message(
            "Reply to an image with `.colour`, or upload an image with `.colour image`, and I’ll pull the main colours.",
            ephemeral=True,
        )

class ColourStartView(View):
    def __init__(self, author_id):
        super().__init__(timeout=600)
        self.author_id = author_id
        self.add_item(OpenColourSetupButton())
        self.add_item(OpenColourPickerButton())
        self.add_item(ColourImageHelpButton())

@bot.command(name="colour", aliases=["color", "hex", "colourinfo", "colorinfo"])
async def colour(ctx, *, value: str = None):
    image_url = await find_image_source_for_colour(ctx)
    mode = (value or "").strip().casefold()
    if image_url and (not mode or mode in {"image", "img", "photo", "palette", "find", "finder", "fromimage"}):
        return await send_colour_palette_from_image(ctx, image_url)
    if mode in {"picker", "pick", "browse", "choose"}:
        view = ColourPickerView(ctx.author.id)
        embed, file = view.current_embed_file()
        return await ctx.send(embed=embed, file=file, view=view)
    if mode in {"image", "img", "photo", "palette", "find", "finder", "fromimage"}:
        return await ctx.send("Reply to an image with `.colour`, or upload an image with `.colour image`.")
    if not value:
        return await ctx.send(
            "Enter a hex colour, open the picker, or reply to an image to find its colours.",
            view=ColourStartView(ctx.author.id)
        )
    embed, file = colour_embed_and_file(value)
    if embed is None:
        return await ctx.send("Use a hex colour like `#77A07B`, `#77A07BFF`, `77A07B`, or `#7A8`.")
    await ctx.send(embed=embed, file=file)

async def define_word_text(word):
    session = await get_http_session()
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
    def __init__(self, author_id, source_message_id=None):
        super().__init__(title="Define Word")
        self.author_id = author_id
        self.source_message_id = source_message_id
        self.word = TextInput(label="Word", placeholder="example", max_length=80)
        self.add_item(self.word)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.defer()
        response = await define_word_text(str(self.word.value).strip())
        content = response or "Couldn't find that word."
        if await edit_setup_source_message(interaction, self.source_message_id, content=content, view=None):
            return await interaction.followup.send("Defined.", ephemeral=True)
        await interaction.followup.send(content)

class OpenDefineSetupButton(Button):
    def __init__(self):
        super().__init__(label="Define", style=discord.ButtonStyle.primary, emoji=reaction_emoji(economy_q_book))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(DefineSetupModal(self.view.author_id, getattr(interaction.message, "id", None)))

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
        await ctx.send("Could not look up that word right now.")

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
async def block(ctx, members: commands.Greedy[discord.Member]):
    if not members:
        return await send_command_input_ui(ctx, "block", note="Mention one or more users to block.")
    blocked = guild_blacklisted_users(ctx.guild)
    changed = []
    for member in members:
        if not can_act_on(ctx.author, member, ctx.guild):
            return await ctx.send("You can't block one or more of those members.")
        blocked.add(member.id)
        changed.append(f"<@{member.id}>")
    await asyncio.to_thread(save_blacklisted_users, scoped_id(ctx.guild), blocked)
    await ctx.send(
        f"Blocked **{len(changed)}** user(s) from using commands: {', '.join(changed)}.",
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
@is_admin_power()
async def unblock(ctx, members: commands.Greedy[discord.Member]):
    if not members:
        return await send_command_input_ui(ctx, "unblock", note="Mention one or more users to unblock.")
    blocked = guild_blacklisted_users(ctx.guild)
    changed = []
    for member in members:
        if not can_act_on(ctx.author, member, ctx.guild) and member.id not in blocked:
            return await ctx.send("You can't unblock one or more of those members.")
        blocked.discard(member.id)
        changed.append(f"<@{member.id}>")
    await asyncio.to_thread(save_blacklisted_users, scoped_id(ctx.guild), blocked)
    await ctx.send(
        f"Unblocked **{len(changed)}** user(s): {', '.join(changed)}.",
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
async def sleep(ctx):
    sleeping_users[ctx.author.id] = datetime.now(timezone.utc)
    await asyncio.to_thread(save_sleeping_user, ctx.author.id, sleeping_users[ctx.author.id])
    embed = standard_embed(
        "Sleep mode",
        description=f"<@{ctx.author.id}> clocked out. Messages are being saved for the comeback.",
        color=0x5865F2,
        icon=economy_q_sleep,
    )
    embed.add_field(name="Since", value=f"<t:{int(sleeping_users[ctx.author.id].timestamp())}:R>", inline=True)
    embed.add_field(name="Return", value="Send any message to wake up.", inline=True)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@bot.command()
async def wake(ctx, members: commands.Greedy[discord.Member]):
    if not has_super_owner_power(ctx.author, ctx.guild):
        return
    if not members:
        return await send_command_input_ui(ctx, "wake", note="Enter one or more members to wake.")

    for member in members:
        sleeping_users.pop(member.id, None)
        await asyncio.to_thread(remove_sleeping_user, member.id)
    if len(members) > 20 or len(", ".join(f"<@{member.id}>" for member in members)) > 1400:
        return await send_paginated_lines(
            ctx,
            "Woke Users",
            [f"{economy_q_bell} <@{member.id}>" for member in members],
            per_page=20,
            empty="No users woke up.",
        )
    await ctx.send(
        f"{economy_q_bell} Woke **{len(members):,}** user(s): " + ", ".join(f"<@{member.id}>" for member in members),
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
async def afk(ctx, *, reason="AFK"):
    now = datetime.now(timezone.utc)
    afk_users[ctx.author.id] = {
        "reason": reason,
        "since": now
    }
    await asyncio.to_thread(save_afk_user, ctx.author.id, reason, afk_users[ctx.author.id]["since"])

    embed = standard_embed(
        "AFK mode",
        description=f"<@{ctx.author.id}> stepped away. I’ll keep the receipts if people mention you.",
        color=0x3498DB,
        icon=economy_q_sleep,
    )
    embed.add_field(name="Since", value=f"<t:{int(now.timestamp())}:R>", inline=True)
    embed.add_field(name="Return", value="Send any message to clear AFK.", inline=True)
    clean_reason = status_reason_text(reason)
    if clean_reason:
        embed.add_field(name="Reason", value=embed_value(clean_reason), inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

def save_user_birthday(user_id, date):
    datetime.strptime(date, "%d/%m")
    birthdays[str(user_id)] = {"date": date}
    save_birthday(user_id, date)

def save_user_birthday_card(user_id, prompt):
    prompt = clean_image_prompt(prompt, max_chars=800)
    if not prompt:
        birthday_card_prompts.pop(str(user_id), None)
        return remove_birthday_card(user_id)
    birthday_card_prompts[str(user_id)] = prompt
    return save_birthday_card(user_id, prompt)

def birthday_card_prompt_for(member, custom_prompt=None):
    name = getattr(member, "display_name", None) or getattr(member, "name", "friend")
    custom = clean_image_prompt(custom_prompt or "", max_chars=700)
    if custom:
        return f"Birthday card for {name}. Match this style/request: {custom}"
    return f"Birthday card for {name}, warm celebratory ProQue theme, cake, balloons, subtle queso coins, clean modern Discord card"

BIRTHDAY_CARD_VIEW_WORDS = {"view", "preview", "show", "current", "card"}

def birthday_card_summary(user_id):
    current = birthday_card_prompts.get(str(user_id))
    if current:
        return "custom", current
    return "default", None

async def build_birthday_card_preview(member):
    status, saved_style = birthday_card_summary(member.id)
    card_prompt = image_prompt_for("birthday", birthday_card_prompt_for(member, saved_style))
    async with image_command_semaphore:
        image_bytes, ext, provider = await generate_hf_image_bytes(card_prompt, width=1024, height=576)
    filename = f"birthday-card-{member.id}.{ext}"
    file = discord.File(BytesIO(image_bytes), filename=filename)
    description = f"{economy_q_birthday_cake} Current card: **{status}**"
    if saved_style:
        description += f"\nSaved style: {embed_value(saved_style, limit=450)}"
    else:
        description += "\nUsing the default Pro𝚀𝚞𝚎 birthday style."
    embed = standard_embed(
        "Birthday Card Preview",
        description=description,
        color=0x2A8FDA,
        icon=economy_q_birthday,
    )
    embed.set_image(url=f"attachment://{filename}")
    embed.set_footer(text=f"Pro𝚀𝚞𝚎 · Generated with {provider}")
    return embed, file

async def send_birthday_card_preview(ctx, member=None):
    member = member or ctx.author
    if not image_generation_available():
        return await ctx.reply("Birthday card previews need image generation configured first.", mention_author=False)
    left = image_cooldown_left(ctx.author.id)
    if left > 0:
        return await ctx.reply(f"{economy_q_timer_tick} Birthday card preview cooldown: **{left:.0f}s**.", mention_author=False)
    touch_image_cooldown(ctx.author.id)
    await safe_add_reaction(ctx.message, economy_q_timer_tick)
    try:
        embed, file = await build_birthday_card_preview(member)
        await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
        await ctx.reply(embed=embed, file=file, mention_author=False, allowed_mentions=discord.AllowedMentions.none())
    except Exception as e:
        await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
        await ctx.reply(clean_user_error(e, ai_exception_message(e)), mention_author=False)

async def send_birthday_card_preview_interaction(interaction):
    member = interaction.user
    if interaction.guild:
        member = interaction.guild.get_member(interaction.user.id) or interaction.user
    if not image_generation_available():
        return await interaction.response.send_message("Birthday card previews need image generation configured first.", ephemeral=True)
    left = image_cooldown_left(interaction.user.id)
    if left > 0:
        return await interaction.response.send_message(f"{economy_q_timer_tick} Birthday card preview cooldown: **{left:.0f}s**.", ephemeral=True)
    touch_image_cooldown(interaction.user.id)
    await interaction.response.defer(thinking=True, ephemeral=True)
    try:
        embed, file = await build_birthday_card_preview(member)
        await interaction.followup.send(embed=embed, file=file, ephemeral=True)
    except Exception as e:
        await interaction.followup.send(clean_user_error(e, ai_exception_message(e)), ephemeral=True)

class BirthdayCardPromptModal(Modal):
    def __init__(self, author_id):
        super().__init__(title="Custom Birthday Card")
        self.author_id = author_id
        self.prompt = TextInput(
            label="Card style",
            placeholder="Example: midnight blue, gold balloons, elegant cake, cute but clean",
            style=discord.TextStyle.paragraph,
            max_length=500,
        )
        self.add_item(self.prompt)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own birthday card setup.", ephemeral=True)
        ok = await asyncio.to_thread(save_user_birthday_card, interaction.user.id, str(self.prompt.value).strip())
        if not ok:
            return await interaction.response.send_message("I saved your birthday, but couldn't save the custom card right now.", ephemeral=True)
        await interaction.response.send_message(f"{economy_q_accept} Custom birthday card saved.", ephemeral=True)

class BirthdayCardChoiceView(View):
    def __init__(self, author_id):
        super().__init__(timeout=LONG_SETUP_VIEW_TIMEOUT)
        self.author_id = author_id

    async def interaction_check(self, interaction):
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Use your own birthday card setup.", ephemeral=True)
        return False

    @discord.ui.button(label="Custom Card", emoji=economy_q_birthday_cake, style=discord.ButtonStyle.primary)
    async def custom_card(self, interaction, button):
        await interaction.response.send_modal(BirthdayCardPromptModal(self.author_id))

    @discord.ui.button(label="Use Default", emoji=economy_q_birthday_balloons, style=discord.ButtonStyle.secondary)
    async def default_card(self, interaction, button):
        await asyncio.to_thread(save_user_birthday_card, interaction.user.id, "")
        await interaction.response.send_message(f"{economy_q_accept} Default birthday card selected.", ephemeral=True)

    @discord.ui.button(label="View Card", emoji=economy_q_image, style=discord.ButtonStyle.secondary)
    async def view_card(self, interaction, button):
        await send_birthday_card_preview_interaction(interaction)

def birthday_saved_message():
    return (
        f"{economy_q_accept} Birthday saved!\n"
        f"{economy_q_birthday_cake} Want a custom birthday card? Optional. If you skip it, I’ll use the default card with your name."
    )

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
            await asyncio.to_thread(save_user_birthday, interaction.user.id, str(self.date.value).strip())
        except ValueError:
            return await interaction.response.send_message("Use `DD/MM`, example: `25/12`.", ephemeral=True)
        await interaction.response.send_message(
            birthday_saved_message(),
            view=BirthdayCardChoiceView(interaction.user.id),
            ephemeral=True,
        )

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
        await asyncio.to_thread(save_user_birthday, ctx.author.id, date)
        await ctx.send(
            birthday_saved_message(),
            view=BirthdayCardChoiceView(ctx.author.id),
            allowed_mentions=discord.AllowedMentions.none(),
        )
    except ValueError:
        await ctx.send("Invalid date format. Use DD/MM.")

@bot.command(name="bdaycard", aliases=["birthdaycard"])
async def bdaycard(ctx, *, prompt: str = None):
    if prompt and prompt.casefold().strip() in BIRTHDAY_CARD_VIEW_WORDS:
        return await send_birthday_card_preview(ctx)
    if not prompt:
        current = birthday_card_prompts.get(str(ctx.author.id))
        embed = standard_embed(
            "Birthday Card",
            description=(
                f"{economy_q_birthday_cake} Current: **{'custom' if current else 'default'}**\n"
                "Press Custom Card, Use Default, View Card, or type `.bdaycard <style>`."
            ),
            color=0x2A8FDA,
            icon=economy_q_birthday,
        )
        if current:
            embed.add_field(name="Saved Style", value=embed_value(current), inline=False)
        return await ctx.send(embed=embed, view=BirthdayCardChoiceView(ctx.author.id), allowed_mentions=discord.AllowedMentions.none())
    if prompt.casefold() in {"default", "off", "clear", "remove"}:
        await asyncio.to_thread(save_user_birthday_card, ctx.author.id, "")
        return await ctx.send(f"{economy_q_accept} Default birthday card selected.")
    ok = await asyncio.to_thread(save_user_birthday_card, ctx.author.id, prompt)
    if not ok:
        return await ctx.send("I couldn't save that birthday card right now.")
    await ctx.send(f"{economy_q_accept} Custom birthday card saved.")

@bot.command(name="viewbdaycard", aliases=["bdaypreview", "birthdaypreview", "cardpreview"])
async def view_bdaycard(ctx, member: discord.Member = None):
    await send_birthday_card_preview(ctx, member if has_super_owner_power(ctx.author, ctx.guild) else ctx.author)

@bot.command()
async def removebday(ctx):
    user_id = str(ctx.author.id)
    if user_id in birthdays:
        del birthdays[user_id]
        await asyncio.to_thread(remove_birthday, ctx.author.id)
        await ctx.send("Birthday removed.")
    else:
        await ctx.send("You haven’t set a birthday.")

@bot.command(name="away")
async def away(ctx):
    now = datetime.now(timezone.utc)

    async def format_status_embed():
        embed = standard_embed(
            "Away Board",
            description="Who is AFK, sleeping, or pretending they have responsibilities.",
            color=0x3498db,
            icon=economy_q_sleep,
        )
        now = datetime.now(timezone.utc)

        if afk_users:
            afk_text = ""
            for uid, data in afk_users.items():
                user = bot.get_user(uid) or await bot.fetch_user(uid)
                duration = now - data["since"]
                formatted = short_status_duration(duration)
                reason = status_reason_text(data.get("reason"))
                reason_bits = f" — {reason}" if reason else ""
                afk_text += f"<@{user.id}> — **{formatted}**{reason_bits}\n"

            embed.add_field(name="AFK Users", value=embed_value(afk_text), inline=False)

        if sleeping_users:
            sleep_text = ""
            for uid, start in sleeping_users.items():
                user = bot.get_user(uid) or await bot.fetch_user(uid)
                duration = now - start
                formatted = short_status_duration(duration)
                sleep_text += f"<@{user.id}> — **{formatted}**\n"

            embed.add_field(name="Sleeping Users", value=embed_value(sleep_text), inline=False)

        if not afk_users and not sleeping_users:
            embed.description = "Nobody is AFK or sleeping right now. Suspiciously productive."

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
            return await ctx.send(f"Could not fetch server member: {clean_user_error(e)}")

    if member is None:
        try:
            user = await bot.fetch_user(user_id)
        except discord.NotFound:
            return await ctx.send(f"User not found: `{user_id}`")
        except discord.HTTPException as e:
            return await ctx.send(f"Could not fetch user: {clean_user_error(e)}")
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
    await asyncio.to_thread(save_censored_phrases, scoped_id(ctx.guild), phrases)
    await ctx.send(f"Now censoring messages containing: `{phrase}`")

@bot.command()
@is_admin_power()
async def uncensor(ctx, *, phrase: str):
    phrase = normalize(phrase)
    phrases = guild_censored_phrases(ctx.guild)
    if phrase not in phrases:
        return await ctx.send(f"'{phrase}' is not currently censored.")
    
    phrases.remove(phrase)
    await asyncio.to_thread(save_censored_phrases, scoped_id(ctx.guild), phrases)
    await ctx.send(f"Stopped censoring: `{phrase}`")

@bot.command()
@is_admin_power()
async def clearcensors(ctx):
    guild_censored_phrases(ctx.guild).clear()
    await asyncio.to_thread(save_censored_phrases, scoped_id(ctx.guild), guild_censored_phrases(ctx.guild))
    await ctx.send("All censors have been cleared.")

def generate_list_lines(user_ids, guild=None):
    lines = []
    for uid in user_ids:
        if guild:
            member = guild.get_member(uid)
            lines.append(f"<@{member.id}>" if member else f"<@{uid}>")
        else:
            lines.append(f"<@{uid}>")
    return lines

def paginated_lines_embed(title, lines, page=0, per_page=20, empty="None."):
    page_count = max(1, math.ceil(len(lines) / per_page)) if lines else 1
    page = max(0, min(page, page_count - 1))
    start = page * per_page
    page_lines = lines[start:start + per_page]
    embed = discord.Embed(title=title, color=0x3498db, timestamp=datetime.now(timezone.utc))
    embed.description = "\n".join(page_lines) if page_lines else empty
    embed.set_footer(text=f"Page {page + 1}/{page_count} • {len(lines):,} total")
    return embed

class PaginatedLinesView(discord.ui.View):
    def __init__(self, author_id, title, lines, *, per_page=20, empty="None."):
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
        self.author_id = author_id
        self.title = title
        self.lines = list(lines)
        self.per_page = per_page
        self.empty = empty
        self.page = 0
        self.refresh_buttons()

    def refresh_buttons(self):
        page_count = max(1, math.ceil(len(self.lines) / self.per_page)) if self.lines else 1
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = page_count <= 1

    def embed(self):
        return paginated_lines_embed(self.title, self.lines, self.page, self.per_page, self.empty)

    async def interaction_check(self, interaction):
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Open your own list panel.", ephemeral=True)
        return False

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction, button):
        page_count = max(1, math.ceil(len(self.lines) / self.per_page)) if self.lines else 1
        self.page = (self.page - 1) % page_count
        await interaction.response.edit_message(embed=self.embed(), view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction, button):
        page_count = max(1, math.ceil(len(self.lines) / self.per_page)) if self.lines else 1
        self.page = (self.page + 1) % page_count
        await interaction.response.edit_message(embed=self.embed(), view=self)

async def send_paginated_lines(ctx, title, lines, *, per_page=20, empty="None."):
    view = PaginatedLinesView(ctx.author.id, title, lines, per_page=per_page, empty=empty)
    if len(lines) <= per_page:
        view = None
    await ctx.send(embed=paginated_lines_embed(title, lines, 0, per_page, empty), view=view, allowed_mentions=discord.AllowedMentions.none())

@bot.command()
async def listtargets(ctx):
    await send_paginated_lines(ctx, "Watched Targets", generate_list_lines(guild_watchlist(ctx.guild), guild=ctx.guild))

@bot.command(name="listbans")
@is_admin_power()
async def listbans(ctx):
    try:
        bans = await ctx.guild.bans()
        if not bans:
            return await ctx.send("No banned users in this server.")

        user_ids = [ban.user.id for ban in bans]
        await send_paginated_lines(ctx, f"Banned Users ({len(bans)})", generate_list_lines(user_ids, guild=ctx.guild))

    except discord.Forbidden:
        await ctx.send("I don’t have permission to view bans.")
    except Exception as e:
        await ctx.send(clean_user_error(e))

@bot.command()
@is_admin_power()
async def listblocks(ctx):
    await send_paginated_lines(ctx, "Blocked Users", generate_list_lines(guild_blacklisted_users(ctx.guild), guild=ctx.guild))

@bot.command()
async def listcensors(ctx):
    phrases = guild_censored_phrases(ctx.guild)
    if not phrases:
        return await ctx.send("No censors are active.")
    await send_paginated_lines(ctx, "Active Censors", [f"- `{p}`" for p in phrases], per_page=18, empty="No censors are active.")

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

@bot.command(name="ask")
async def ask_command(ctx, *, question: str):
    """Ask AI anything - answers simply and clearly"""
    if not GROQ_API_KEY:
        return await ctx.send("API not configured. Set GROQ_API_KEY.")
    
    await safe_add_reaction(ctx.message, economy_q_timer_tick)

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    messages = [
        {
            "role": "system",
            "content": (
                "You are Pro𝚀𝚞𝚎's AI. Answer clearly, simply, briefly, and casually. "
                "Be playful and warm when the user is joking or chatting; be direct when they need actual help. "
                "You do not have live web search connected, so do not claim current live-world facts unless the user provided the source/context. "
                "Do not recommend bot commands unless the user directly asks for bot help or asks how to use a bot feature."
            ),
        }
    ]
    messages.append({"role": "user", "content": question})

    payload = {
        "messages": fit_ai_messages(messages, max_chars=5200),
        "model": "llama-3.1-8b-instant",
        "temperature": 0.7,
        "max_tokens": 420
    }

    try:
        session = await get_http_session()
        async with session.post("https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers, timeout=AI_MODEL_TIMEOUT_SECONDS) as resp:
            if resp.status != 200:
                body = await resp.text()
                await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
                return await ctx.send(ai_http_error_message(resp.status, body))

            data = await resp.json(content_type=None)
            answer = data["choices"][0]["message"]["content"]

            if len(answer) > 1900:
                answer = answer[:1897] + "..."

            await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
            await ctx.send(answer)

    except Exception as e:
        await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
        await ctx.send(ai_exception_message(e))

@bot.command(name="summarize", aliases=["summarise", "summary", "aisummary", "tldr", "recap"])
async def summarize_command(ctx, *, request: str = ""):
    """Summarizes recent messages in this channel or a mentioned channel/user."""
    target_user, summary_channel = await resolve_summary_target(ctx, request)
    duration = parse_summary_duration(request)
    limit = parse_summary_limit(request)
    if not request.strip():
        limit = AI_SUMMARY_DEFAULT_MESSAGES
    await send_chat_summary(
        ctx,
        prompt=request or "Summarize the recent chat.",
        target_user=target_user,
        channel=summary_channel,
        limit=limit,
        duration=duration,
    )

@bot.command(name="aidetect", aliases=["aicheck", "detectai", "authenticity", "authcheck", "essaycheck"])
async def aidetect_command(ctx, *, text: str = None):
    """Checks whether writing has AI-like patterns. This is not proof."""
    if not text and ctx.message.reference:
        ref_msg = ctx.message.reference.resolved
        if not isinstance(ref_msg, discord.Message):
            try:
                ref_msg = await ctx.channel.fetch_message(ctx.message.reference.message_id)
            except Exception:
                ref_msg = None
        if ref_msg:
            text = ref_msg.content
            if not text and ref_msg.attachments:
                text_attachments = [
                    attachment for attachment in ref_msg.attachments
                    if (attachment.content_type or "").startswith("text/") or attachment.filename.lower().endswith((".txt", ".md"))
                ]
                if text_attachments:
                    try:
                        raw = await text_attachments[0].read()
                        text = raw.decode("utf-8", errors="ignore")
                    except Exception:
                        text = None
    if not text:
        return await ctx.send(
            "Reply to text with `.aidetect`, or use `.aidetect <essay text>`.",
            allowed_mentions=discord.AllowedMentions.none(),
        )
    text = str(text).strip()
    if len(text) < 80:
        return await ctx.send("Send a longer sample if possible. AI-likelihood checks are weak on very short text.")
    await safe_add_reaction(ctx.message, economy_q_timer_tick)
    try:
        result = await run_ai_likelihood_check(text)
        await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
        await ctx.reply(embed=ai_likelihood_embed(result, text), mention_author=False, allowed_mentions=discord.AllowedMentions.none())
    except Exception as e:
        await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
        await ctx.send(clean_user_error(e, ai_exception_message(e)))

class GeneratedEmojiView(View):
    def __init__(self, author_id, image_bytes, default_name="proque_emoji"):
        super().__init__(timeout=180)
        self.author_id = author_id
        self.image_bytes = image_bytes
        self.default_name = re.sub(r"[^\w]+", "_", default_name or "proque_emoji")[:32] or "proque_emoji"

    async def interaction_check(self, interaction):
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Generate your own emoji first.", ephemeral=True)
        return False

    @discord.ui.button(label="Add Emoji", emoji=economy_q_accept, style=discord.ButtonStyle.success)
    async def add_emoji(self, interaction, button):
        if not interaction.guild:
            return await interaction.response.send_message("Emoji uploads need to be used in a server.", ephemeral=True)
        if not interaction.user.guild_permissions.manage_emojis_and_stickers:
            return await interaction.response.send_message("You need Manage Emojis and Stickers for that.", ephemeral=True)
        bot_member = interaction.guild.me or interaction.guild.get_member(bot.user.id)
        if bot_member and not bot_member.guild_permissions.manage_emojis_and_stickers:
            return await interaction.response.send_message("I need Manage Emojis and Stickers first.", ephemeral=True)
        await interaction.response.send_modal(NameModal("emoji", BytesIO(self.image_bytes), default_name=self.default_name))

async def generated_image_reply(ctx, prompt, *, kind, title, width=1024, height=576, view_factory=None):
    result = await run_image_generation(ctx, prompt, kind=kind, title=title, width=width, height=height)
    if not result or not view_factory:
        return
    image_bytes, _ = result
    try:
        await ctx.message.reply(
            f"{economy_q_image} Extra options are available on the generated image above.",
            view=view_factory(image_bytes),
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    except Exception:
        pass

@bot.command(name="generate", aliases=["imagine", "image", "aiimage", "genimg"])
async def generate_command(ctx, *, prompt: str):
    """Generate an image from text."""
    await run_image_generation(ctx, prompt, kind="general", title="Generated Image")

@bot.command(name="profilebanner", aliases=["banner", "profileart"])
async def profile_banner_command(ctx, *, prompt: str):
    """Generate a profile/banner image."""
    await run_image_generation(ctx, prompt, kind="banner", title="Profile Banner", width=1024, height=384)

@bot.command(name="makeemoji", aliases=["genemoji", "emojiart"])
async def makeemoji_command(ctx, *, prompt: str):
    """Generate an emoji-style image. Admins can add it as a server emoji."""
    default_name = re.sub(r"[^\w]+", "_", clean_image_prompt(prompt, max_chars=40).lower())[:28] or "proque_emoji"
    await generated_image_reply(
        ctx,
        prompt,
        kind="emoji",
        title="Emoji Draft",
        width=512,
        height=512,
        view_factory=lambda image_bytes: GeneratedEmojiView(ctx.author.id, image_bytes, default_name),
    )

@bot.command(name="eventposter", aliases=["poster", "eventart"])
@is_admin_power()
async def eventposter_command(ctx, *, prompt: str):
    """Generate a clean event poster/background."""
    await run_image_generation(ctx, prompt, kind="event", title="Event Poster")

@bot.command(name="gameart", aliases=["gameimage"])
async def gameart_command(ctx, *, prompt: str):
    """Generate polished game artwork or scene art."""
    await run_image_generation(ctx, prompt, kind="game", title="Game Art")

@bot.command(name="reactionimage", aliases=["aireaction", "reactimage"])
async def reactionimage_command(ctx, *, prompt: str):
    """Generate a fun reaction image."""
    await run_image_generation(ctx, prompt, kind="reaction", title="Reaction Image")

@bot.command(name="shoppreview", aliases=["itempreview", "shopart"])
async def shoppreview_command(ctx, *, item_or_prompt: str):
    """Generate a shop item preview image."""
    shop_items = getattr(economy_module, "SHOP_ITEMS", {})
    lookup = item_or_prompt.casefold().strip()
    matched_id = None
    for item_id, item in shop_items.items():
        names = {item_id.casefold(), str(item.get("name", "")).casefold()}
        if lookup in names:
            matched_id = item_id
            break
    if matched_id:
        item = shop_items[matched_id]
        item_name = str(item.get("name", matched_id))
        item_desc = getattr(economy_module, "item_short_description", lambda x: "")(item)
        prompt = f"{item_name}, {item_desc}, premium Discord shop item preview"
    else:
        prompt = item_or_prompt
    await run_image_generation(ctx, prompt, kind="shop", title="Shop Item Preview")


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
        session = await get_http_session()
        try:
            image_data_uri = await image_url_to_data_uri(session, image_url)
        except ValueError as e:
            await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
            return await ctx.send(f"Could not read image: {clean_user_error(e)}")

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
                    return await ctx.send("Cloudflare needs the Meta vision model license accepted first.")
                return await ctx.send(ai_http_error_message(resp.status, error_text))

            data = await resp.json(content_type=None)
            result = data["result"]["response"]

            if len(result) > 1900:
                result = result[:1897] + "..."

            await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
            await ctx.send(result)
    except Exception as e:
        await safe_remove_reaction(ctx.message, economy_q_timer_tick, bot.user)
        await ctx.send(ai_exception_message(e))

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
    session = await get_http_session()
    params = {
        "client": "gtx",
        "sl": source_lang,
        "tl": target_lang,
        "dt": "t",
        "q": text,
    }
    async with session.get("https://translate.googleapis.com/translate_a/single", params=params) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise RuntimeError(ai_http_error_message(resp.status, body))
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
            response = clean_user_error(e)
        await interaction.followup.send(response)

class OpenTranslateSetupButton(Button):
    def __init__(self):
        super().__init__(label="Translate", style=discord.ButtonStyle.primary)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(TranslateSetupModal(self.view.author_id))

class GenericCommandInputModal(Modal):
    def __init__(self, author_id, command_name):
        command = get_command_case_insensitive(command_name)
        display_name = command.name if command else command_name
        super().__init__(title=f"Run {display_name}")
        self.author_id = author_id
        self.command_name = display_name
        example = COMMAND_EXAMPLE_OVERRIDES.get(display_name)
        placeholder = "Enter what comes after the command"
        if example:
            parts = example.split(maxsplit=1)
            if len(parts) > 1:
                placeholder = parts[1][:100]
        style = discord.TextStyle.paragraph if display_name in {"send", "reply", "ask", "generate", "poll", "giveaway"} else discord.TextStyle.short
        self.command_input = TextInput(
            label="Command input",
            placeholder=placeholder,
            style=style,
            min_length=1,
            max_length=1500,
        )
        self.add_item(self.command_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.defer(thinking=True)
        await invoke_prefix_command_from_interaction(interaction, self.command_name, str(self.command_input.value).strip())

class OpenGenericCommandInputButton(Button):
    def __init__(self, command_name):
        command = get_command_case_insensitive(command_name)
        self.command_name = command.name if command else command_name
        super().__init__(label="Enter Input", style=discord.ButtonStyle.primary, emoji=reaction_emoji(economy_q_edit))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(GenericCommandInputModal(self.view.author_id, self.command_name))

SPECIALIZED_SETUP_UI_COMMANDS = {"poll", "timer", "alarm", "picker", "giveaway", "calc", "colour", "color", "hex", "define", "setbday", "translate", "messageevent"}
SETUP_UI_COMMANDS = SPECIALIZED_SETUP_UI_COMMANDS | GENERIC_INPUT_UI_COMMANDS

def command_setup_view(author_id, command_name):
    if command_name in {"colour", "color", "hex"}:
        return ColourStartView(author_id)
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
        "messageevent": OpenMessageEventSetupButton,
    }
    button_cls = buttons.get(command_name)
    if button_cls:
        return SingleUserSetupView(author_id, button_cls())
    command = get_command_case_insensitive(command_name)
    if command_supports_input_ui(command):
        return SingleUserSetupView(author_id, OpenGenericCommandInputButton(command.name))
    return None

async def send_command_input_ui(ctx, command_name=None, error=None, note=None):
    command = get_command_case_insensitive(command_name) if command_name else getattr(ctx, "command", None)
    if not command or not command_supports_input_ui(command):
        return await send_command_usage_correction(ctx, error)
    prefix = getattr(ctx, "prefix", prefix_for_guild(ctx.guild))
    usage = command_usage_example(ctx)
    hint = command_argument_hint(error, ctx)
    description = note or "This command needs input. Press the button and enter what should come after the command."
    embed = standard_embed(
        f"{prefix}{command.name}",
        description=description,
        color=discord.Color.blurple(),
        icon=economy_q_edit,
    )
    embed.add_field(name="Example", value=f"`{usage}`", inline=False)
    if hint:
        embed.add_field(name="Input Help", value=hint, inline=False)
    embed.set_footer(text=f"You can still type it normally with {prefix}{command.name}.")
    await ctx.send(embed=embed, view=command_setup_view(ctx.author.id, command.name), allowed_mentions=discord.AllowedMentions.none())

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
        await ctx.send(clean_user_error(e))

# === RUN BOT ===


# Start the bot
run_bot_with_retry()
