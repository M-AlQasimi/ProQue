import discord
from discord.ext import commands, tasks
from flask import Flask
from threading import Thread
from datetime import datetime, timezone
from discord.ui import Button, View, Select
from io import BytesIO
from discord import File, Emoji, StickerItem, app_commands, Interaction, Embed
import re
import random
import datetime
import os
import aiohttp
import asyncio
import logging
import json
import pytz
last_message_time = 0

bday_file = "birthdays.json"
AFK_FILE = "afk_users.json"
SLEEP_FILE = "sleeping_users.json"
MODS_FILE = "mods.json"
OWNERS_FILE = "owners.json"

def load_ids(filename):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return set(json.load(f))
    return set()

def save_ids(filename, id_set):
    with open(filename, "w") as f:
        json.dump(list(id_set), f)

def load_dict(filename):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f)
    return {}

def save_dict(filename, data):
    with open(filename, "w") as f:
        json.dump(data, f)

try:
    with open(bday_file, "r") as f:
        birthdays = json.load(f)
except FileNotFoundError:
    birthdays = {}

raw_afk = load_dict(AFK_FILE)
afk_users = {
    int(uid): {
        "reason": data["reason"],
        "since": datetime.datetime.fromisoformat(data["since"])
    } for uid, data in raw_afk.items()
}

raw_sleeping = load_dict(SLEEP_FILE)
sleeping_users = {
    int(uid): datetime.datetime.fromisoformat(time_str)
    for uid, time_str in raw_sleeping.items()
}

class MyBot(commands.Bot):
    async def setup_hook(self):
        await self.tree.sync()
        print("Slash commands synced.")
        
intents = discord.Intents.all()
bot = MyBot(command_prefix='.', intents=intents)
print(f"Bot is starting with intents: {bot.intents}")

log_channel_id = 1394806479881769100
rlog_channel_id = 1394806602502115470
bday_channel_id = 1364346683709718619
super_owner_id = 885548126365171824  
mods = load_ids(MODS_FILE)
owners = load_ids(OWNERS_FILE)

autoban_ids = set()
blacklisted_users = set()
reaction_shut = set()
shutdown_channels = set()
disabled_commands = set()
watchlist = {}
sleeping_users = {}
afk_users = {}
c4_games = {}
ttt_games = {}
edited_snipes = {}
deleted_snipes = {}
removed_reactions = {}
active_timers = {}
active_polls = {}
user_mentions = {}

app = Flask('')

class CommandDisabledError(commands.CheckFailure):
    def __init__(self, command_name):
        self.command_name = command_name

@app.route('/')
def home():
    return "I'm alive", 200

async def send_log(embed):
    try:
        channel = bot.get_channel(log_channel_id)
        if channel is None:
            try:
                channel = await bot.fetch_channel(log_channel_id)
            except Exception as e:
                print(f"Could not fetch channel: {e}")
                return
        await channel.send(embed=embed)
    except Exception as e:
        print(f"Failed to send log: {e}")

async def send_rlog(embed):
    channel = bot.get_channel(rlog_channel_id)
    if channel:
        await channel.send(embed=embed)
    else:
        print("Reaction log channel not found.")

async def safe_send(destination, *args, **kwargs):
    global last_message_time
    now = asyncio.get_event_loop().time()
    wait_time = 2 - (now - last_message_time)
    if wait_time > 0:
        await asyncio.sleep(wait_time)
    last_message_time = asyncio.get_event_loop().time()
    return await destination.send(*args, **kwargs)

def run():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run)
    t.start()

def is_owner():
    async def predicate(ctx):
        return ctx.author.id == super_owner_id or ctx.author.id in owners
    return commands.check(predicate)

def is_mod():
    async def predicate(ctx):
        return ctx.author.id == super_owner_id or ctx.author.id in mods
    return commands.check(predicate)

def is_owner_or_mod():
    async def predicate(ctx):
        return ctx.author.id in owners or ctx.author.id == super_owner_id or ctx.author.id in mods
    return commands.check(predicate)

@tasks.loop(minutes=4)
async def keep_alive_task():
    print("Heartbeat")

@bot.event
async def on_ready():
    print(f'ProQue is online as {bot.user}')
    if not keep_alive_task.is_running():
        keep_alive_task.start()
    asyncio.create_task(birthday_check_loop())
    print("Bot ready, waiting to sync slash commands...")

async def query_ai(question):
    try:
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {os.environ['HUGGINGFACE_API_KEY']}"}
            payload = {"inputs": question}
            async with session.post(
                "https://api-inference.huggingface.co/models/togethercomputer/RedPajama-INCITE-Chat-3B-v1",
                headers=headers,
                json=payload
            ) as resp:
                data = await resp.json()
                print(f"[DEBUG] HuggingFace response: {data}")
                return data[0]["generated_text"].strip() if isinstance(data, list) else str(data)
    except Exception as e:
        print(f"[ERROR] AI query failed: {e}")
        return "Sorry, I couldn't get an answer."

async def birthday_check_loop():
    await bot.wait_until_ready()
    already_sent = set()

    while not bot.is_closed():
        now = datetime.datetime.now(datetime.timezone.utc)
        today_str = now.strftime("%d/%m")

        if now.hour == 0 and now.minute == 0:
            for user_id, data in birthdays.items():
                if data["date"] == today_str and (user_id, now.date()) not in already_sent:
                    channel = bot.get_channel(bday_channel_id)
                    if channel:
                        user = await bot.fetch_user(int(user_id))
                        await channel.send(f"@everyone it's {user.mention}'s birthday today! 🎉🎂")
                        already_sent.add((user_id, now.date()))

        await asyncio.sleep(60)

@bot.event
async def on_member_join(member):
    embed = discord.Embed(
        title="Member Joined",
        color=discord.Color.green()
    )
    embed.add_field(name="User", value=f"{member} ({member.id})", inline=False)
    embed.timestamp = datetime.datetime.now(datetime.timezone.utc)
    print(f"Sending log for {member} ({member.id})")
    await send_log(embed)

@bot.event
async def on_member_ban(guild, user):
    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.ban):
        if entry.target.id == user.id:
            embed = discord.Embed(
                title="🔨 Member Banned",
                color=discord.Color.red()
            )
            embed.add_field(name="User", value=f"{user} ({user.id})", inline=False)
            embed.add_field(name="Banned by", value=f"{entry.user} ({entry.user.id})", inline=False)
            embed.add_field(name="Reason", value=entry.reason or "No reason provided", inline=False)
            embed.timestamp = datetime.datetime.now(timezone.utc)
            print("Sending log:", embed.title)
            try:
                await send_log(embed)
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
            embed.add_field(name="User", value=f"{user} ({user.id})", inline=False)
            embed.add_field(name="Unbanned by", value=f"{entry.user} ({entry.user.id})", inline=False)
            embed.add_field(name="Reason", value=entry.reason or "No reason provided", inline=False)
            embed.timestamp = datetime.datetime.now(timezone.utc)
            print("Sending log:", embed.title)
            try:
                await send_log(embed)
            except Exception as e:
                print(f"Failed to send log: {e}")
            return

@bot.event
async def on_guild_join(guild):
    print(f"Joined server: {guild.name} ({guild.id})")

@bot.event
async def on_guild_remove(guild):
    print(f"Left server: {guild.name} ({guild.id})")

@bot.event
async def on_guild_channel_create(channel):
    embed = discord.Embed(
        title="📥 Channel Created",
        color=discord.Color.green()
    )
    embed.add_field(name="Channel", value=channel.mention, inline=False)

    async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_create):
        if entry.target.id == channel.id:
            embed.add_field(name="By", value=f"{entry.user} ({entry.user.id})", inline=False)
            break

    embed.timestamp = datetime.datetime.now(timezone.utc)
    print("Sending log:", embed.title)
    try:
        await send_log(embed)
    except Exception as e:
        print(f"Failed to send log: {e}")

@bot.event
async def on_guild_channel_delete(channel):
    embed = discord.Embed(
        title="🗑️ Channel Deleted",
        color=discord.Color.red()
    )
    embed.add_field(name="Channel", value=channel.name, inline=False)

    async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_delete):
        if entry.target.id == channel.id:
            embed.add_field(name="By", value=f"{entry.user} ({entry.user.id})", inline=False)
            break

    embed.timestamp = datetime.datetime.now(timezone.utc)
    print("Sending log:", embed.title)
    try:
        await send_log(embed)
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
            embed.add_field(name="By", value=f"{entry.user} ({entry.user.id})", inline=False)
            break

    embed.timestamp = datetime.datetime.now(timezone.utc)
    print("Sending log:", embed.title)
    try:
        await send_log(embed)
    except Exception as e:
        print(f"Failed to send log: {e}")

@bot.event
async def on_guild_role_delete(role):
    embed = discord.Embed(
        title="🗑️ Role Deleted",
        color=discord.Color.red()
    )
    embed.add_field(name="Role", value=role.name, inline=False)

    async for entry in role.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_delete):
        if entry.target.id == role.id:
            embed.add_field(name="By", value=f"{entry.user} ({entry.user.id})", inline=False)
            break

    embed.timestamp = datetime.datetime.now(timezone.utc)
    print("Sending log:", embed.title)
    try:
        await send_log(embed)
    except Exception as e:
        print(f"Failed to send log: {e}")

@bot.event
async def on_guild_role_update(before, after):
    embed = discord.Embed(
        title="🎨 Role Updated",
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
            embed.add_field(name="By", value=f"{entry.user} ({entry.user.id})", inline=False)
            break

    embed.timestamp = datetime.datetime.now(timezone.utc)
    print("Sending log:", embed.title)
    try:
        await send_log(embed)
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
            title="📝 Server Name Changed",
            description=f"**Before:** {before.name}\n**After:** {after.name}",
            color=discord.Color.orange()
        )

    elif before.icon != after.icon:
        embed = discord.Embed(
            title="🖼️ Server Icon Changed",
            color=discord.Color.orange()
        )
        if before.icon:
            embed.set_thumbnail(url=before.icon.url)
        if after.icon:
            embed.set_image(url=after.icon.url)

    elif before.verification_level != after.verification_level:
        embed = discord.Embed(
            title="🔒 Verification Level Changed",
            description=f"**Before:** {before.verification_level.name}\n**After:** {after.verification_level.name}",
            color=discord.Color.orange()
        )

    if embed and entry:
        embed.add_field(name="By", value=f"{entry.user} ({entry.user.id})", inline=False)
        embed.timestamp = datetime.datetime.now(timezone.utc)
        print("Sending log:", embed.title)
        try:
            await send_log(embed)
        except Exception as e:
            print(f"Failed to send log: {e}")

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    content_lower = message.content.lower().strip()
    if content_lower.startswith("pq"):
        print(f"[DEBUG] PQ trigger detected: {message.content}")
        question = message.content[2:].strip()
        if not question:
            question = ""

        async with message.channel.typing():
            answer = await query_ai(question)
            await message.channel.send(answer)

    if message.channel.id in shutdown_channels and message.author.id not in owners:
        try:
            await message.delete()
        except:
            pass
        return

    for mentioned_user in message.mentions:
        if mentioned_user.id in afk_users or mentioned_user.id in sleeping_users:
            if mentioned_user.id not in user_mentions:
                user_mentions[mentioned_user.id] = []
            user_mentions[mentioned_user.id].append((
                message.author.id,
                message.jump_url,
                int(message.created_at.replace(tzinfo=datetime.timezone.utc).timestamp())
            ))

    if message.author.id in sleeping_users:
        start = sleeping_users.pop(message.author.id)
        save_dict(SLEEP_FILE, {str(uid): dt.isoformat() for uid, dt in sleeping_users.items()})

        duration = datetime.datetime.now(timezone.utc) - start
        days, remainder = divmod(int(duration.total_seconds()), 86400)
        hours, remainder = divmod(remainder, 3600)
        mins = remainder // 60
        formatted = " ".join([f"{days}d" if days else "", f"{hours}h" if hours else "", f"{mins}m" if mins or not (days or hours) else ""]).strip()

        embed = discord.Embed(
            title=f"Good morning, {message.author.display_name} 🌅",
            description=f"You were sleeping for **{formatted}**.",
            color=0xF1C40F,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )

        mentions_list = user_mentions.pop(message.author.id, [])
        if mentions_list:
            embed.add_field(name="Mentions received", value=f"You received **{len(mentions_list)}** mentions:", inline=False)
            for uid, link, ts in mentions_list:
                embed.add_field(
                    name=f"{bot.get_user(uid) or await bot.fetch_user(uid)}",
                    value=f"<t:{ts}:R> — [Click to view message]({link})",
                    inline=True
                )

        await message.channel.send(embed=embed)

    for uid in sleeping_users:
        if any(user.id == uid for user in message.mentions) or (
            message.reference and message.reference.resolved and message.reference.resolved.author.id == uid
        ):
            user = bot.get_user(uid)
            if not user:
                try:
                    user = await bot.fetch_user(uid)
                    await asyncio.sleep(1)
                except:
                    user = None
            if user:
                await message.channel.send(
                    f"<@{user.id}> is sleeping. 💤",
                    allowed_mentions=discord.AllowedMentions.none()
                )
                break

    for user in message.mentions:
        if user.id in afk_users:
            afk_data = afk_users[user.id]
            duration = datetime.datetime.now(datetime.timezone.utc) - afk_data["since"]
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
        save_dict(AFK_FILE, {str(uid): {"reason": data["reason"], "since": data["since"].isoformat()} for uid, data in afk_users.items()})

        duration = datetime.datetime.now(datetime.timezone.utc) - afk_data["since"]
        days, remainder = divmod(int(duration.total_seconds()), 86400)
        hours, remainder = divmod(remainder, 3600)
        mins = remainder // 60
        formatted = " ".join([f"{days}d" if days else "", f"{hours}h" if hours else "", f"{mins}m" if mins or not (days or hours) else ""]).strip()

        reason = afk_data['reason']
        reason_text = f": **{reason}**" if reason.lower() != "afk" else ""

        embed = discord.Embed(
            title=f"Welcome back, {message.author.display_name}",
            description=f"You were AFK for **{formatted}**{reason_text}",
            color=0x2ECC71,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )

        mentions_list = user_mentions.pop(message.author.id, [])
        if mentions_list:
            embed.add_field(name="Mentions received", value=f"You received **{len(mentions_list)}** mentions:", inline=False)
            for uid, link, ts in mentions_list:
                embed.add_field(
                    name=f"{bot.get_user(uid) or await bot.fetch_user(uid)}",
                    value=f"<t:{ts}:R> — [Click to view message]({link})",
                    inline=True
                )

        await message.channel.send(embed=embed)

    if message.author.id in watchlist and message.author.id != super_owner_id:
        try:
            await message.delete()
        except:
            pass

    await bot.process_commands(message)

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return

    elif isinstance(error, CommandDisabledError):
        await ctx.send(f"**{error.command_name}** is disabled.")

    elif isinstance(error, commands.CheckFailure):
        if ctx.author.id in blacklisted_users:
            await ctx.send("LMAO you're blocked you can't use ts 😭✌🏻")
        else:
            await ctx.send("You can't use that heh")

    elif isinstance(error, commands.MissingPermissions):
        await ctx.send("You don’t have permission to do that.")

    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("Missing required argument.")

    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid input. Check your arguments.")

    else:
        print(f"Unexpected error in {ctx.command}: {type(error).__name__} - {error}")
        if ctx.author.id in owners:
            await ctx.send("Error.")
        else:
            await ctx.send("You can't use that heh")

@bot.event
async def on_message_delete(message):
    if not message.content and not message.attachments:
        return

    content = message.content or ""
    deleter = "Unknown"

    if message.guild:
        async for entry in message.guild.audit_logs(limit=5, action=discord.AuditLogAction.message_delete):
            if entry.target.id == message.author.id and (datetime.datetime.now(timezone.utc) - entry.created_at).total_seconds() < 5:
                deleter = f"{entry.user} ({entry.user.id})"
                break

    embed = discord.Embed(
        title="🗑️ Message Deleted",
        color=discord.Color.red()
    )
    embed.add_field(name="User", value=f"{message.author} ({message.author.id})", inline=False)
    embed.add_field(name="Deleted by", value=deleter, inline=False)
    embed.add_field(name="Channel", value=message.channel.mention, inline=False)
    embed.timestamp = datetime.datetime.now(timezone.utc)

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
    deleted_snipes[message.channel.id] = deleted_snipes[message.channel.id][:10]

    if (message.mention_everyone or message.mentions) and not message.author.bot:
        try:
            mentions = []
            if message.mention_everyone:
                mentions.append("@everyone or @here")
            if message.mentions:
                mentions.extend(m.mention for m in message.mentions)

            ghost_embed = discord.Embed(
                title="⚠️ Ghost Ping Detected!",
                description=f"**Author:** {message.author.mention} (`{message.author.id}`)\n"
                            f"**Channel:** {message.channel.mention}\n"
                            f"**Mentions:** {' '.join(mentions)}\n"
                            f"**Message:** {message.content or '*[No content]*'}",
                color=discord.Color.red()
            )
            ghost_embed.set_footer(text="Ghost Ping Log")
            ghost_embed.timestamp = message.created_at

            print("Sending log:", ghost_embed.title)
            await send_log(ghost_embed)
        except Exception as e:
            print(f"[Ghost Ping Log Error] {e}")

    print("Sending log:", embed.title)
    try:
        await send_log(embed)
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
        log_entries.append(f"**{i}.** {msg.author} ({msg.author.id}): {content}")
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
            title=f"🧹 Bulk Messages Deleted (Part {i}/{len(chunks)})",
            description="\n".join(chunk),
            color=discord.Color.red()
        )
        embed.add_field(name="Channel", value=messages[0].channel.mention, inline=False)
        embed.timestamp = datetime.datetime.now(timezone.utc)
        try:
            await send_log(embed)
        except Exception as e:
            print(f"Failed to send log part {i}: {e}")

    if attachments:
        attach_chunks = [attachments[i:i+5] for i in range(0, len(attachments), 5)]
        for i, group in enumerate(attach_chunks, 1):
            embed = discord.Embed(
                title=f"📎 Attachments from Purged Messages (Part {i}/{len(attach_chunks)})",
                color=discord.Color.orange()
            )
            for name, url in group:
                embed.add_field(name=name, value=url, inline=False)
            embed.add_field(name="Channel", value=messages[0].channel.mention, inline=False)
            embed.timestamp = datetime.datetime.now(timezone.utc)
            try:
                await send_log(embed)
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
            datetime.datetime.now(datetime.timezone.utc)
        ))
        edited_snipes[before.channel.id] = edited_snipes[before.channel.id][:10]

        embed = discord.Embed(
            title="✏️ Message Edited",
            color=discord.Color.orange()
        )
        embed.add_field(name="Author", value=f"{before.author} ({before.author.id})", inline=False)
        embed.add_field(name="Before", value=before.content, inline=False)
        embed.add_field(name="After", value=after.content, inline=False)
        embed.add_field(name="Message", value=f"[Jump to Message]({before.jump_url})", inline=False)
        embed.add_field(name="Channel", value=before.channel.mention, inline=False)
        embed.timestamp = datetime.datetime.now(datetime.timezone.utc)

        print("Sending log:", embed.title)
        try:
            await send_log(embed)
        except Exception as e:
            print(f"Failed to send log: {e}")

async def update_poll_counts(message):
    if message.id not in active_polls:
        return
    
    poll_data = active_polls[message.id]
    
    yes_count = 0
    no_count = 0
    for reaction in message.reactions:
        if str(reaction.emoji) == "✔️":
            yes_count = reaction.count - 1
        elif str(reaction.emoji) == "✖️":
            no_count = reaction.count - 1

    embed = message.embeds[0] if message.embeds else discord.Embed(title=poll_data["question"])
    embed.set_field_at(0, name="Yes", value=str(yes_count), inline=True)
    embed.set_field_at(1, name="No", value=str(no_count), inline=True)
    await message.edit(embed=embed)


@bot.event
async def on_reaction_remove(reaction, user):
    if user.bot and user.id != super_owner_id:
        return

    await update_poll_counts(reaction.message)
    
    msg = reaction.message
    entry = (user, reaction.emoji, msg, datetime.datetime.now(timezone.utc).replace(tzinfo=timezone.utc))
    removed_reactions.setdefault(msg.channel.id, []).insert(0, entry)
    removed_reactions[msg.channel.id] = removed_reactions[msg.channel.id][:10]

    embed = discord.Embed(
        title="🗑️ Reaction Removed",
        color=discord.Color.red()
    )
    embed.add_field(name="User", value=f"{user} ({user.id})", inline=False)
    embed.add_field(name="Emoji", value=str(reaction.emoji), inline=True)
    embed.add_field(name="Message", value=f"[Jump to Message]({msg.jump_url})", inline=False)
    embed.add_field(name="Channel", value=msg.channel.mention, inline=False)
    embed.timestamp = datetime.datetime.now(timezone.utc)

    print("Sending log:", embed.title)
    try:
        await send_rlog(embed)
    except Exception as e:
        print(f"Failed to send log: {e}")

@bot.event
async def on_reaction_add(reaction, user):
    if user.bot and user.id != super_owner_id:
        return
   
    await update_poll_counts(reaction.message)

    
    msg = reaction.message

    embed = discord.Embed(
        title="➕ Reaction Added",
        color=discord.Color.green()
    )
    embed.add_field(name="User", value=f"{user} ({user.id})", inline=False)
    embed.add_field(name="Emoji", value=str(reaction.emoji), inline=True)
    embed.add_field(name="Message", value=f"[Jump to Message]({msg.jump_url})", inline=False)
    embed.add_field(name="Channel", value=msg.channel.mention, inline=False)
    embed.timestamp = datetime.datetime.now(timezone.utc)

    print("Sending log:", embed.title)
    try:
        await send_rlog(embed)
    except Exception as e:
        print(f"Failed to send log: {e}")

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
        title="🗑️ All Reactions Removed",
        description=f"All reactions were removed from a [message]({message.jump_url}) in {channel.mention}.",
        color=discord.Color.red()
    )
    if remover:
        embed.add_field(name="By", value=f"{remover} ({remover.id})", inline=False)
    else:
        embed.add_field(name="By", value="Unknown", inline=False)
    
    embed.set_footer(text=f"Message ID: {message.id}")
    embed.timestamp = datetime.datetime.utcnow()

    print("Sending log: All reactions removed")
    try:
        await send_rlog(embed)
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
                return f"{entry.user} ({entry.user.id})"
        return None

    if before.nick != after.nick:
        action_by = await get_action_by({discord.AuditLogAction.member_update})
        embed = discord.Embed(
            title="📝 Nickname Changed",
            color=discord.Color.blue()
        )
        embed.add_field(name="User", value=f"{before} ({before.id})", inline=False)
        embed.add_field(name="Before", value=before.nick or before.name, inline=True)
        embed.add_field(name="After", value=after.nick or after.name, inline=True)
        if action_by:
            embed.add_field(name="Changed by", value=action_by, inline=False)
        embed.timestamp = datetime.datetime.now(datetime.timezone.utc)
        await send_log(embed)

    before_roles = set(before.roles)
    after_roles = set(after.roles)
    added = after_roles - before_roles
    removed = before_roles - after_roles
    if added or removed:
        action_by = await get_action_by({discord.AuditLogAction.member_role_update})
        embed = discord.Embed(
            title="🎭 Roles Updated",
            color=discord.Color.teal()
        )
        embed.add_field(name="User", value=f"{after} ({after.id})", inline=False)
        if added:
            embed.add_field(name="Added", value=", ".join(role.name for role in added), inline=True)
        if removed:
            embed.add_field(name="Removed", value=", ".join(role.name for role in removed), inline=True)
        if action_by:
            embed.add_field(name="Updated by", value=action_by, inline=False)
        embed.timestamp = datetime.datetime.now(datetime.timezone.utc)
        await send_log(embed)

    before_timeout = getattr(before, "communication_disabled_until", None)
    after_timeout = getattr(after, "communication_disabled_until", None)
    if before_timeout != after_timeout:
        action_by = await get_action_by({discord.AuditLogAction.member_update})
        if after_timeout and (after_timeout > datetime.datetime.now(datetime.timezone.utc)):
            embed = discord.Embed(
                title="⏳ Member Timed Out",
                color=discord.Color.orange()
            )
            embed.add_field(name="User", value=f"{after} ({after.id})", inline=False)
            embed.add_field(name="Until", value=f"<t:{int(after_timeout.timestamp())}:F>", inline=False)
            if action_by:
                embed.add_field(name="By", value=action_by, inline=False)
            embed.timestamp = datetime.datetime.now(datetime.timezone.utc)
        else:
            embed = discord.Embed(
                title="✔️ Timeout Removed",
                color=discord.Color.green()
            )
            embed.add_field(name="User", value=f"{after} ({after.id})", inline=False)
            if action_by:
                embed.add_field(name="By", value=action_by, inline=False)
            embed.timestamp = datetime.datetime.now(datetime.timezone.utc)
        await send_log(embed)

@bot.event
async def on_audit_log_entry_create(entry):
    if entry.action == discord.AuditLogAction.member_update:
        target = entry.target
        if not isinstance(target, discord.Member | discord.User):
            return

        after_timeout = getattr(target, "communication_disabled_until", None)

        if after_timeout and after_timeout.timestamp() > datetime.datetime.now(timezone.utc).timestamp():
            embed = discord.Embed(
                title="⏳ Member Timed Out",
                color=discord.Color.orange()
            )
            embed.add_field(name="User", value=f"{target} ({target.id})", inline=False)
            embed.add_field(name="Until", value=f"<t:{int(after_timeout.timestamp())}:F>", inline=False)
            embed.add_field(name="By", value=f"{entry.user} ({entry.user.id})", inline=False)
            embed.timestamp = datetime.datetime.now(timezone.utc)
            try:
                await send_log(embed)
            except Exception as e:
                print(f"Failed to send timeout log: {e}")

        elif after_timeout is None:
            embed = discord.Embed(
                title="✔️ Timeout Removed",
                color=discord.Color.green()
            )
            embed.add_field(name="User", value=f"{target} ({target.id})", inline=False)
            embed.add_field(name="By", value=f"{entry.user} ({entry.user.id})", inline=False)
            embed.timestamp = datetime.datetime.now(timezone.utc)
            try:
                await send_log(embed)
            except Exception as e:
                print(f"Failed to send timeout removal log: {e}")

@bot.event
async def on_user_update(before, after):
    if before.name != after.name:
        embed = discord.Embed(title="📝 Username Changed", color=discord.Color.blue())
        embed.add_field(name="User", value=f"{after.mention} ({after.id})", inline=False)
        embed.add_field(name="Before", value=before.name, inline=True)
        embed.add_field(name="After", value=after.name, inline=True)
        embed.timestamp = datetime.datetime.now(timezone.utc)
        print("Sending log:", embed.title)
        try:
            await send_log(embed)
        except Exception as e:
            print(f"Failed to send log: {e}")

    if before.discriminator != after.discriminator:
        embed = discord.Embed(title="Discriminator Changed", color=discord.Color.purple())
        embed.add_field(name="User", value=f"{after.mention} ({after.id})", inline=False)
        embed.add_field(name="Before", value=before.discriminator, inline=True)
        embed.add_field(name="After", value=after.discriminator, inline=True)
        embed.timestamp = datetime.datetime.now(timezone.utc)
        print("Sending log:", embed.title)
        try:
            await send_log(embed)
        except Exception as e:
            print(f"Failed to send log: {e}")

    if before.avatar != after.avatar:
        embed = discord.Embed(title="Avatar Changed", color=discord.Color.gold())
        embed.add_field(name="User", value=f"{after.mention} ({after.id})", inline=False)
        embed.set_thumbnail(url=before.avatar.url if before.avatar else discord.Embed.Empty)
        embed.set_image(url=after.avatar.url if after.avatar else discord.Embed.Empty)
        embed.timestamp = datetime.datetime.now(timezone.utc)
        print("Sending log:", embed.title)
        try:
            await send_log(embed)
        except Exception as e:
            print(f"Failed to send log: {e}")

@bot.event
async def on_member_remove(member):
    guild = member.guild
    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.kick):
        if entry.target.id == member.id:
            embed = discord.Embed(
                title="🔨 Member Kicked",
                color=discord.Color.red()
            )
            embed.timestamp = datetime.datetime.now(timezone.utc)
            embed.add_field(name="User", value=f"{member} ({member.id})", inline=False)
            embed.add_field(name="Kicked by", value=f"{entry.user} ({entry.user.id})", inline=False)
            embed.add_field(name="Reason", value=entry.reason or "No reason provided", inline=False)
            print("Sending log:", embed.title)
            try:
                await send_log(embed)
            except Exception as e:
                print(f"Failed to send log: {e}")
            return

    embed = discord.Embed(
        title="Member Left",
        color=discord.Color.orange()
    )
    embed.timestamp = datetime.datetime.now(timezone.utc)
    embed.add_field(name="User", value=f"{member} ({member.id})", inline=False)
    print("Sending log:", embed.title)
    try:
        await send_log(embed)
    except Exception as e:
        print(f"Failed to send log: {e}")
        
@bot.event
async def on_voice_state_update(member, before, after):
    changes = []
    if not before.channel and after.channel:
        changes.append(f"🔊 **Joined voice:** {after.channel.mention}")
    elif before.channel and not after.channel:
        changes.append(f"**Left voice:** {before.channel.name}")
    elif before.channel != after.channel:
        changes.append(f"➡️ **Moved voice:** {before.channel.name} → {after.channel.name}")

    if before.self_mute != after.self_mute:
        changes.append(f"{'🔇 Muted' if after.self_mute else '🔊 Unmuted'}")

    if before.self_deaf != after.self_deaf:
        changes.append(f"{'Deafened' if after.self_deaf else '👂 Undeafened'}")

    if changes:
        embed = discord.Embed(
            title="🎙️ Voice State Changed",
            description="\n".join(changes),
            color=discord.Color.blurple()
        )
        embed.set_author(name=f"{member} ({member.id})", icon_url=member.display_avatar.url)
        embed.timestamp = datetime.datetime.now(timezone.utc)
        print("Sending log:", embed.title)
        try:
            await send_log(embed)
        except Exception as e:
            print(f"Failed to send log: {e}")

@bot.check
async def globally_block_disabled(ctx):
    if ctx.command and ctx.command.name in disabled_commands and ctx.author.id != super_owner_id:
        raise CommandDisabledError(ctx.command.name)
    return True

@bot.check
async def block_blacklisted(ctx):
    return ctx.author.id not in blacklisted_users

@bot.command()
@is_owner_or_mod()
async def disable(ctx, cmd: str):
    if ctx.author.id != super_owner_id:
        return

    command = bot.get_command(cmd)
    if not command:
        await ctx.send("Command not found.")
        return

    disabled_commands.add(command.name)
    await ctx.send(f"Disabled **{command.name}**")
    
@bot.command()
@is_owner_or_mod()
async def enable(ctx, cmd: str):
    if ctx.author.id != super_owner_id:
        return

    command = bot.get_command(cmd)
    if not command:
        await ctx.send("Command not found.")
        return

    if command.name in disabled_commands:
        disabled_commands.remove(command.name)
        await ctx.send(f"Enabled **{command.name}**")
    else:
        await ctx.send(f"**{command.name}** is not disabled.")

@bot.command()
@is_owner_or_mod()
async def disableall(ctx):
    if ctx.author.id != super_owner_id:
        return

    for command in bot.commands:
        if command.name != "enableall":
            disabled_commands.add(command.name)
    await ctx.send("Disabled **all commands**")

@bot.command()
@is_owner_or_mod()
async def enableall(ctx):
    if ctx.author.id != super_owner_id:
        return

    disabled_commands.clear()
    await ctx.send("Enabled **all commands**")

@bot.command()
@is_owner_or_mod()
async def dclist(ctx):
    if not disabled_commands:
        await ctx.send("No commands are disabled.")
    else:
        formatted = "\n".join(f"**{name}**" for name in disabled_commands)
        await ctx.send(f"Disabled Commands:\n{formatted}")
        
@bot.command()
async def dsnipe(ctx, index: str = "1"):
    try:
        if index.startswith("-"):
            count = int(index[1:])
            messages = deleted_snipes.get(ctx.channel.id, [])
            if not messages or count < 1:
                return await ctx.send("Nothing to snipe.")
            selected = messages[:count]
            response = ""
            for i, (content, author, timestamp) in enumerate(selected, 1):
                unix_time = int(timestamp.replace(tzinfo=datetime.timezone.utc).timestamp())
                response += f"#{i} - Deleted by <@{author.id}> at <t:{unix_time}:f>:\n{content}\n\n"
            await ctx.send(response[:2000], allowed_mentions=discord.AllowedMentions.none())
        else:
            n = int(index) - 1
            messages = deleted_snipes.get(ctx.channel.id, [])
            if not messages or n >= len(messages) or n < 0:
                return await ctx.send("Nothing to snipe.")
            content, author, timestamp = messages[n]
            unix_time = int(timestamp.replace(tzinfo=datetime.timezone.utc).timestamp())
            await ctx.send(
                f"Deleted by <@{author.id}> at <t:{unix_time}:f>:\n{content}",
                allowed_mentions=discord.AllowedMentions.none()
            )
    except:
        await ctx.send("Invalid index. Use a number like `.dsnipe 3` or `.dsnipe -3`.")

@bot.command()
async def esnipe(ctx, index: str = "1"):
    try:
        messages = edited_snipes.get(ctx.channel.id, [])
        if index.startswith("-"):
            count = int(index[1:])
            if not messages or count < 1:
                return await ctx.send("Nothing to snipe.")
            selected = messages[:count]
            response = ""
            for i, (before, after, author, link, timestamp) in enumerate(selected, 1):
                unix_time = int(timestamp.timestamp())
                response += (
                    f"#{i} - Edited by <@{author.id}> at <t:{unix_time}:f>:\n"
                    f"**Before:** {before}\n**After:** {after}\n[Jump to message]({link})\n\n"
                )
            await ctx.send(response[:2000], allowed_mentions=discord.AllowedMentions.none())
        else:
            n = int(index) - 1
            if not messages or n >= len(messages) or n < 0:
                return await ctx.send("Nothing to snipe.")
            before, after, author, link, timestamp = messages[n]
            unix_time = int(timestamp.timestamp())
            await ctx.send(
                f"Edited by <@{author.id}> at <t:{unix_time}:f>:\n**Before:** {before}\n**After:** {after}\n[Jump to message]({link})",
                allowed_mentions=discord.AllowedMentions.none()
            )
    except:
        await ctx.send("Invalid index. Use a number like `.esnipe 2` or `.esnipe -3`.")

@bot.command()
async def rsnipe(ctx, index: str = "1"):
    try:
        logs = removed_reactions.get(ctx.channel.id, [])
        if index.startswith("-"):
            count = int(index[1:])
            if not logs or count < 1:
                return await ctx.send("Nothing to snipe.")
            selected = logs[:count]
            response = ""
            for i, (user, emoji, msg, timestamp) in enumerate(selected, 1):
                unix_time = int(timestamp.replace(tzinfo=datetime.timezone.utc).timestamp())
                response += (
                    f"#{i} - <@{user.id}> removed {emoji} from [this message]({msg.jump_url}) at <t:{unix_time}:f>.\n\n"
                )
            await ctx.send(response[:2000], allowed_mentions=discord.AllowedMentions.none())
        else:
            n = int(index) - 1
            if not logs or n >= len(logs) or n < 0:
                return await ctx.send("Nothing to snipe.")
            user, emoji, msg, timestamp = logs[n]
            unix_time = int(timestamp.replace(tzinfo=datetime.timezone.utc).timestamp())
            await ctx.send(
               f"<@{user.id}> removed {emoji} from [this message]({msg.jump_url}) at <t:{unix_time}:f>.",
                allowed_mentions=discord.AllowedMentions.none()
            )
    except:
        await ctx.send("Invalid index. Use a number like `.rsnipe 2` or `.rsnipe -3`.")

@bot.command()
@is_owner_or_mod()
@commands.has_permissions(manage_emojis=True)
async def steal(ctx):
    if not ctx.message.reference:
        return await ctx.send("Reply to a message containing a sticker or custom emoji.")

    ref = await ctx.channel.fetch_message(ctx.message.reference.message_id)
    item_url = None
    item_name = "stolen"
    is_emoji = False

    emoji_match = re.search(r"<(a)?:\w+:(\d+)>", ref.content)
    if emoji_match:
        emoji_id = emoji_match.group(2)
        is_emoji = True
        item_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.{'gif' if emoji_match.group(1) else 'png'}"
        item_name = re.search(r":(\w+):", ref.content).group(1)

    elif ref.stickers:
        sticker = ref.stickers[0]
        item_url = sticker.url
        item_name = sticker.name

    else:
        return await ctx.send("No emoji or sticker found in the replied message.")

    class StealView(View):
        def __init__(self):
            super().__init__(timeout=15)
            self.response_sent = False

        @button(label="Add as Emoji", style=discord.ButtonStyle.primary)
        async def add_emoji(self, interaction: discord.Interaction, button: Button):
            if self.response_sent:
                return
            self.response_sent = True
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(item_url) as resp:
                        if resp.status != 200:
                            return await interaction.response.send_message("Failed to fetch image.", ephemeral=True)
                        img_bytes = await resp.read()
                        await ctx.guild.create_custom_emoji(name=item_name[:32], image=img_bytes)
                        await interaction.response.send_message("✔️ Emoji added!")
            except discord.Forbidden:
                await interaction.response.send_message("I don't have permission to add emojis.", ephemeral=True)
            except Exception as e:
                await interaction.response.send_message(f"Failed: {e}", ephemeral=True)

        @button(label="Add as Sticker", style=discord.ButtonStyle.success)
        async def add_sticker(self, interaction: discord.Interaction, button: Button):
            if self.response_sent:
                return
            self.response_sent = True
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(item_url) as resp:
                        if resp.status != 200:
                            return await interaction.response.send_message("Failed to fetch sticker.", ephemeral=True)
                        img_bytes = await resp.read()
                        image_file = File(BytesIO(img_bytes), filename="sticker.png")
                        await ctx.guild.create_sticker(
                            name=item_name[:30],
                            description="stolen sticker",
                            emoji="👍",
                            file=image_file
                        )
                        await interaction.response.send_message("✔️ Sticker added!")
            except discord.Forbidden:
                await interaction.response.send_message("I don't have permission to add stickers.", ephemeral=True)
            except Exception as e:
                await interaction.response.send_message(f"Failed: {e}", ephemeral=True)

    await ctx.send("Choose how you want to save this:", view=StealView())

@bot.command()
@is_owner_or_mod()
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
            lines.append("**🔒 Roles with Power:**")
            lines.extend(powerful_roles)

        if bot_roles:
            lines.append("**🤖 Bot Roles (No Power):**")
            lines.extend(bot_roles)

        if no_power_roles:
            lines.append("**➖ Custom Roles (No Power):**")
            lines.extend(no_power_roles)

        if other_roles:
            lines.append("**📦 Other Roles:**")
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
@is_owner_or_mod()
async def roleinfo(ctx, role: discord.Role):
    members = [member.mention for member in role.members]
    is_admin = role.permissions.administrator

    if is_admin:
        perms_text = "**Admin Permissions: ✔️**"
    else:
        perms = [name.replace('_', ' ').title() for name, value in role.permissions if value]
        perms_text = "**Permissions:**\n" + ", ".join(perms) if perms else "No special permissions."

    member_list = ", ".join(members) if members else "No members have this role."
    embed = discord.Embed(title=f"Role Info: {role.name}", color=role.color)
    embed.add_field(name="Members", value=member_list, inline=False)
    embed.add_field(name="Permissions", value=perms_text, inline=False)
    await ctx.send(embed=embed)

@bot.command()
@is_owner()
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
        response += f"✔️ Deleted roles: {', '.join(deleted)}\n"
    if failed:
        response += f"✖️ Failed to delete: {', '.join(failed)}"

    await ctx.send(response or "No roles processed.")

@bot.command()
@is_owner_or_mod()
async def test(ctx):
    await ctx.send("I'm alive heh")

@bot.command()
async def testlog(ctx):
    embed = discord.Embed(title="Test Log", description="This is a test log.", color=discord.Color.green())
    try:
        await send_log(embed)
        print("DEBUG: testlog command used")
    except Exception as e:
        print(f"Failed to send test log: {e}")
        await ctx.send("Failed to send test log.")

@bot.command()
async def userinfo(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed = discord.Embed(title="User Info", color=0x3498db)
    embed.set_thumbnail(url=member.avatar.url)
    embed.add_field(name="Username", value=str(member), inline=True)
    embed.add_field(name="ID", value=member.id, inline=True)
    embed.add_field(
        name="Joined Server",
        value=member.joined_at.strftime("%d %b %Y • %H:%M UTC"),
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
    await ctx.send(member.avatar.url)

class TicTacToeButton(Button):
    def __init__(self, row, col):
        super().__init__(style=discord.ButtonStyle.secondary, label='⬜', row=row)
        self.row = row
        self.col = col

    async def callback(self, interaction: discord.Interaction):
        game = ttt_games.get(interaction.channel.id)
        if not game or interaction.user != game["players"][game["turn"]]:
            return await interaction.response.send_message("Not your turn.", ephemeral=True)

        board = game["board"]
        if board[self.row][self.col] != '⬜':
            return await interaction.response.send_message("That spot is taken.", ephemeral=True)

        mark = '✖️' if game["turn"] == 0 else '🔘'
        board[self.row][self.col] = mark
        self.label = mark
        self.disabled = True

        await interaction.response.edit_message(view=game["view"])

        if game["timeout_task"]:
            game["timeout_task"].cancel()

        winner = check_winner(board)
        if winner:
            await game["msg"].edit(
                content=f"🎉 <@{interaction.user.id}> wins!",
                view=game["view"],
                allowed_mentions=discord.AllowedMentions.none()
            )
            await disable_all_buttons(game["view"])
            del ttt_games[interaction.channel.id]
            return

        if all(cell != '⬜' for row in board for cell in row):
            await game["msg"].edit(content="It's a draw!", view=game["view"])
            await disable_all_buttons(game["view"])
            del ttt_games[interaction.channel.id]
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
        if board[i][0] == board[i][1] == board[i][2] != '⬜':
            return True
        if board[0][i] == board[1][i] == board[2][i] != '⬜':
            return True
    if board[0][0] == board[1][1] == board[2][2] != '⬜':
        return True
    if board[0][2] == board[1][1] == board[2][0] != '⬜':
        return True
    return False

async def disable_all_buttons(view):
    for item in view.children:
        item.disabled = True

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
        await interaction.response.edit_message(content="✔️ Challenge accepted!", view=None)

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.danger)
    async def decline(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.opponent:
            return await interaction.response.send_message("You're not the challenged player.", ephemeral=True)
        self.declined = True
        self.stop()
        await interaction.response.edit_message(content="✖️ Challenge declined.", view=None)

    async def on_timeout(self):
        if not self.accepted and not self.declined:
            await self.ctx.send(
                f"<@{self.opponent.id}> didn't respond in time. Game canceled.",
                allowed_mentions=discord.AllowedMentions.none()
            )

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
        allowed_mentions=discord.AllowedMentions.none()
    )
    await view.wait()

    if not view.accepted:
        return

    board = [['⬜'] * 3 for _ in range(3)]
    game_view = TicTacToeView()
    msg = await ctx.send("Game started!", view=game_view)
    game = {
        "players": [ctx.author, opponent],
        "turn": 0,
        "board": board,
        "view": game_view,
        "msg": msg,
        "timeout_task": None
    }
    ttt_games[ctx.channel.id] = game

    await update_turn(game, ctx.channel)

async def update_turn(game, channel):
    current = game["players"][game["turn"]]
    time_left = 30

    async def countdown():
        nonlocal time_left
        while time_left > 0:
            await game["msg"].edit(
                content=f"<@{current.id}>, it's your turn! ({time_left}s)",
                view=game["view"],
                allowed_mentions=discord.AllowedMentions.none()
            )
            await asyncio.sleep(1)
            time_left -= 1

        await game["msg"].edit(
            content=f"⏱️ <@{current.id}> took too long. Game over!",
            view=game["view"],
            allowed_mentions=discord.AllowedMentions.none()
        )
        await disable_all_buttons(game["view"])
        del ttt_games[channel.id]

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
                    piece = "⚫" if game["turn"] == 0 else "⚪"
                    board[row][self.col] = piece
                    break
            else:
                return await interaction.response.send_message("Column full.", ephemeral=True)

            if game["timeout_task"]:
                game["timeout_task"].cancel()

            if check_c4_winner(board, piece):
                render = render_board(board, game["turn"])
                await interaction.message.edit(
                    content=f"{render}\n\n🎉 <@{interaction.user.id}> wins!",
                    view=Connect4View()
                )
                del c4_games[interaction.channel.id]
                return

            if all(cell != " " for row in board for cell in row):
                render = render_board(board, game["turn"])
                await interaction.message.edit(
                    content=f"{render}\n\nIt's a draw!",
                    view=Connect4View()
                )
                del c4_games[interaction.channel.id]
                return

            game["turn"] = 1 - game["turn"]
            game["msg"] = interaction.message
            game["board"] = board
            game["view"] = Connect4View()

            render = render_board(board, game["turn"])
            await interaction.message.edit(content=render, view=game["view"])
            await update_c4_turn(game, interaction.channel)

        except Exception as e:
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[ERROR in Connect4 callback]\n{traceback_str}")
            try:
                await interaction.response.send_message(f"Error:\n```{e}```", ephemeral=True)
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
    msg = game["msg"]
    board = game["board"]
    time_left = 30

    async def countdown():
        nonlocal time_left
        try:
            while time_left > 0:
                await msg.edit(
                    content=f"{render_board(board, game['turn'])}\n\n<@{current.id}>, it's your turn! ({time_left}s)",
                    view=game["view"]
                )
                await asyncio.sleep(1)
                time_left -= 1

            await msg.edit(
                content=f"{render_board(board, game['turn'])}\n\n⏱️ <@{current.id}> took too long. Game over!",
                view=game["view"]
            )
            del c4_games[channel.id]
        except Exception as e:
            print("Error in countdown:", e)

    game["timeout_task"] = asyncio.create_task(countdown())

def render_board(board, turn):
    bg = "◻️" if turn == 0 else "◾"
    rendered = ""
    for row in board:
        for cell in row:
            rendered += cell if cell in ("⚫", "⚪") else bg
        rendered += "\n"
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
        allowed_mentions=discord.AllowedMentions(users=[opponent.id])
    )
    await view.wait()

    if not view.accepted:
        return

    board = [[" "] * 7 for _ in range(6)]
    render = render_board(board, 0)
    game_view = Connect4View()
    msg = await ctx.send(render, view=game_view)

    game = {
        "players": [ctx.author, opponent],
        "turn": 0,
        "board": board,
        "view": game_view,
        "msg": msg,
        "timeout_task": None
    }
    c4_games[ctx.channel.id] = game
    await update_c4_turn(game, ctx.channel)

@bot.command()
@is_owner_or_mod()
async def endttt(ctx):
    if ctx.channel.id in ttt_games:
        del ttt_games[ctx.channel.id]
        await ctx.send("Tic-Tac-Toe game ended.")
    else:
        await ctx.send("No Tic-Tac-Toe game is currently active in this channel.")

@bot.command()
async def q(ctx):
    answer = random.choice(["Yes", "No"])
    await ctx.send(f"**{answer}**")

@bot.command()
@is_owner_or_mod()
async def setnick(ctx, member: discord.Member, *, nickname: str):
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
@is_owner()
async def shut(ctx, member: discord.Member):
    print(f"shut command used by {ctx.author} on {member}")
    if member.id == super_owner_id:
        return
    watchlist[member.id] = ctx.author.id
    await ctx.send(f"<@{member.id}> has been silenced.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_owner()
async def unshut(ctx, member: discord.Member):
    if member.id in owners:
        if watchlist.get(member.id) == super_owner_id and ctx.author.id != super_owner_id:
            return await ctx.send("Only 𝚀𝚞𝚎 can stop watching that owner.")
    watchlist.pop(member.id, None)

@bot.command()
@is_owner()
async def clearwatchlist(ctx):
    if ctx.author.id != super_owner_id:
        return await ctx.send("Only 𝚀𝚞𝚎 can clear the watchlist.")
    watchlist.clear()
    await ctx.send("Watchlist cleared.")

@bot.command()
@is_owner()
async def shutdown(ctx):
    shutdown_channels.add(ctx.channel.id)
    await ctx.send("This channel is now in shutdown mode. Only owners can speak.")

@bot.command()
@is_owner()
async def reopen(ctx):
    shutdown_channels.discard(ctx.channel.id)
    await ctx.send("This channel has been reopened. All users may speak now.")

@bot.command()
async def addowner(ctx, *users: discord.User):
    global owners
    if ctx.author.id != super_owner_id:
        return await ctx.send("Only 𝚀𝚞𝚎 can add owners.")
    
    added = []
    already = []
    for user in users:
        if user.id in owners:
            already.append(user)
        else:
            owners.add(user.id)
            added.append(user)
    if added:
        save_ids(OWNERS_FILE, owners)
    
    if len(added) == 1:
        await ctx.send(f"{added[0].mention} has been added as an owner.")
    elif len(added) > 1:
        mentions = ", ".join(u.mention for u in added)
        await ctx.send(f"{mentions} have been added as owners.")

    for user in already:
        await ctx.send(f"{user.mention} is already an owner.")

@bot.command()
async def removeowner(ctx, user: discord.User):
    global owners
    if ctx.author.id != super_owner_id:
        return await ctx.send("Only 𝚀𝚞𝚎 can remove owners.")
    
    if user.id in owners:
        owners.remove(user.id)
        save_ids(OWNERS_FILE, owners)
        await ctx.send(f"{user.mention} has been removed from owners.")
    else:
        await ctx.send("{user.mention} is not an owner.")

@bot.command()
async def clearowners(ctx):
    global owners
    if ctx.author.id != super_owner_id:
        return await ctx.send("Only 𝚀𝚞𝚎 can do that.")
    owners.clear()
    owners.add(super_owner_id)
    save_ids(OWNERS_FILE, owners)
    await ctx.send("Cleared all owners.")

@bot.command()
async def listowners(ctx):
    if not owners:
        return await ctx.send("No owners found.")
    
    owner_mentions = [f"<@{uid}>" for uid in owners]
    await ctx.send("Owners:\n" + "\n".join(owner_mentions), allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_owner()
async def addmod(ctx, *users: discord.User):
    global mods
    added = []
    already = []
    for user in users:
        if user.id in mods:
            already.append(user)
        else:
            mods.add(user.id)
            added.append(user)
    if added:
        save_ids(MODS_FILE, mods)
    
    if len(added) == 1:
        await ctx.send(f"{added[0].mention} has been added as a mod.")
    elif len(added) > 1:
        mentions = ", ".join(u.mention for u in added)
        await ctx.send(f"{mentions} have been added as mods.")

    for user in already:
        await ctx.send(f"{user.mention} is already a mod.")

@bot.command()
@is_owner()
async def removemod(ctx, user: discord.User):
    global mods
    if user.id in mods:
        mods.remove(user.id)
        save_ids(MODS_FILE, mods)
        await ctx.send(f"{user.mention} has been removed from mods.")
    else:
        await ctx.send("{user.mention} is not a mod.")

@bot.command()
async def listmods(ctx):
    if not mods:
        return await ctx.send("No mods found.")
    mod_mentions = [f"<@{uid}>" for uid in mods]
    await ctx.send("Mods:\n" + "\n".join(mod_mentions), allowed_mentions=discord.AllowedMentions.none())

class OwnerModManagement(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="addowner", description="Add one or multiple owners")
    async def addowner(self, interaction: discord.Interaction, users: str):
        if interaction.user.id != super_owner_id:
            return await interaction.response.send_message("Only 𝚀𝚞𝚎 can add owners.", ephemeral=True)

        user_ids = []
        for part in users.split():
            if part.isdigit():
                user_ids.append(int(part))
            else:
                if part.startswith('<@') and part.endswith('>'):
                    part = part.replace('<@!', '').replace('<@', '').replace('>', '')
                    if part.isdigit():
                        user_ids.append(int(part))

        added = []
        already = []
        for uid in user_ids:
            user = self.bot.get_user(uid)
            if not user:
                continue
            if uid in owners:
                already.append(user)
            else:
                owners.add(uid)
                added.append(user)

        if added:
            save_ids(OWNERS_FILE, owners)

        messages = []
        if len(added) == 1:
            messages.append(f"{added[0].mention} has been added as an owner.")
        elif len(added) > 1:
            mentions = ", ".join(u.mention for u in added)
            messages.append(f"{mentions} have been added as owners.")
        for user in already:
            messages.append(f"{user.mention} is already an owner.")

        await interaction.response.send_message("\n".join(messages) or "No valid users specified.", ephemeral=True)

    @app_commands.command(name="addmod", description="Add one or multiple mods")
    @commands.check_any(commands.is_owner(), commands.has_permissions(administrator=True))
    async def addmod(self, interaction: discord.Interaction, users: str):
        if interaction.user.id != super_owner_id and interaction.user.id not in owners:
            return await interaction.response.send_message("Only owners and 𝚀𝚞𝚎 can add mods.", ephemeral=True)

        user_ids = []
        for part in users.split():
            if part.isdigit():
                user_ids.append(int(part))
            else:
                if part.startswith('<@') and part.endswith('>'):
                    part = part.replace('<@!', '').replace('<@', '').replace('>', '')
                    if part.isdigit():
                        user_ids.append(int(part))

        added = []
        already = []
        for uid in user_ids:
            user = self.bot.get_user(uid)
            if not user:
                continue
            if uid in mods:
                already.append(user)
            else:
                mods.add(uid)
                added.append(user)

        if added:
            save_ids(MODS_FILE, mods)

        messages = []
        if len(added) == 1:
            messages.append(f"{added[0].mention} has been added as a mod.")
        elif len(added) > 1:
            mentions = ", ".join(u.mention for u in added)
            messages.append(f"{mentions} have been added as mods.")
        for user in already:
            messages.append(f"{user.mention} is already a mod.")

        await interaction.response.send_message("\n".join(messages) or "No valid users specified.", ephemeral=True)

async def setup(bot):
    await bot.add_cog(OwnerModManagement(bot))

@bot.command()
async def listtargets(ctx):
    names = []
    for uid in watchlist:
        member = ctx.guild.get_member(uid)
        if member:
            names.append(f"<@{member.id}> (**{member.name}**)")
    await ctx.send(
        "Targets:\n" + ("\n".join(names) if names else "No targets being watched."),
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
@is_owner_or_mod()
async def purge(ctx, amount: int, member: discord.Member = None):
    await ctx.message.delete()
    try:
        if member is None:
            await ctx.channel.purge(limit=amount)
        else:
            def check(m):
                return m.author == member

            deleted = []
            async for message in ctx.channel.history(limit=1000):
                if check(message):
                    deleted.append(message)
                    if len(deleted) == amount:
                        break

            if not deleted:
                return await ctx.send("No messages found to delete.")

            await ctx.channel.delete_messages(deleted)
    except discord.Forbidden:
        await ctx.send("I don’t have permission to delete messages.")
    except discord.HTTPException as e:
        await ctx.send(f"Error: {type(e).__name__} - {e}")

@bot.command(name="lock")
@is_owner()
async def lock_channel(ctx):
    overwrite = ctx.channel.overwrites_for(ctx.guild.default_role)
    overwrite.send_messages = False
    await ctx.channel.set_permissions(ctx.guild.default_role, overwrite=overwrite)
    await ctx.send("Channel locked.")

@bot.command()
@is_owner()
async def unlock(ctx):
    overwrite = ctx.channel.overwrites_for(ctx.guild.default_role)
    overwrite.send_messages = True
    await ctx.channel.set_permissions(ctx.guild.default_role, overwrite=overwrite)
    await ctx.send("Channel unlocked.")

@bot.command()
@is_owner_or_mod()
async def mute(ctx, member: discord.Member, duration: str):
    import datetime

    time_units = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    try:
        seconds = int(duration[:-1]) * time_units.get(duration[-1], 0)
        if seconds <= 0:
            return await ctx.send("Invalid duration.")
    except:
        return await ctx.send("Invalid duration format. Use like `10m`, `1h`, etc.")

    try:
        until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=seconds)
        await member.timeout(until)
        await ctx.send(
            f"{member.mention} has been muted for {duration}.",
            allowed_mentions=discord.AllowedMentions.none()
        )
    except Exception as e:
        await ctx.send(f"Failed to mute: {e}")

@bot.command()
@is_owner_or_mod()
async def unmute(ctx, member: discord.Member):
    try:
        await member.timeout(None)
        await ctx.send(
            f"{member.id} has been unmuted.",
            allowed_mentions=discord.AllowedMentions.none()
        )
    except discord.Forbidden:
        await ctx.send("Missing permissions to unmute this member.")
    except Exception as e:
        await ctx.send(f"Failed to unmute: {e}")

@bot.command()
@is_owner()
async def ban(ctx, user: discord.User, *, reason=None):
    await ctx.guild.ban(user, reason=reason)
    await ctx.send(
        f"<@{user.id}> has been banned.",
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
@is_owner()
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

@bot.command(name="listbans")
@is_owner_or_mod()
async def listbans(ctx):
    try:
        bans = await ctx.guild.bans()
        if not bans:
            return await ctx.send("No banned users in this server.")

        lines = []
        for ban in bans:
            user = ban.user
            lines.append(f"<@{user.id}> **{user.name}#{user.discriminator}** (ID: {user.id})")

        msg = "\n".join(lines)
        if len(msg) > 2000:
            await ctx.send(f"Too many banned users to show ({len(bans)} total).")
        else:
            await ctx.send(f"**Banned Users:**\n{msg}", allowed_mentions=discord.AllowedMentions.none())
    except discord.Forbidden:
        await ctx.send("I don’t have permission to view bans.")
    except Exception as e:
        await ctx.send(f"Error: {type(e).__name__} - {e}")

@bot.command()
@is_owner_or_mod()
async def kick(ctx, member: discord.Member, *, reason=None):
    await member.kick(reason=reason)
    await ctx.send(f"<@{member.id}> has been kicked.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_owner_or_mod()
async def addrole(ctx, member: discord.Member, role: discord.Role):
    await member.add_roles(role)
    await ctx.send(f"Added **{role.name}** to <@{member.id}>.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_owner_or_mod()
async def removerole(ctx, member: discord.Member, role: discord.Role):
    await member.remove_roles(role)
    await ctx.send(f"Removed **{role.name}** from <@{member.id}>.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
@is_owner()
async def speak(ctx, *, msg):
    await ctx.message.delete()
    await ctx.send(msg)

    if ctx.author.id != super_owner_id:
        print(f"[SPEAK LOG] {ctx.author} ({ctx.author.id}) used .speak")

@bot.command()
@is_owner()
async def reply(ctx, message_id: int, *, text: str):
    try:
        await ctx.message.delete()
        msg = await ctx.channel.fetch_message(message_id)
        await msg.reply(text)

        if ctx.author.id != super_owner_id:
            print(f"[REPLY LOG] {ctx.author} ({ctx.author.id}) used .reply")
    except discord.NotFound:
        await ctx.send("Message not found.")
    except discord.HTTPException as e:
        await ctx.send("Failed to reply to the message.")
        print(f"[REPLY ERROR] {type(e).__name__}: {e}")

@bot.command()
async def poll(ctx, *, args):
    if "|" in args:
        question_part, time_part = args.split("|", 1)
        question = question_part.strip()
        time_str = time_part.strip()
    else:
        question = args.strip()
        time_str = None

    end_time = None
    if time_str:
        try:
            d = h = m = 0
            matches = re.findall(r"(\d+)\s*(d|h|m)", time_str.lower())
            for value, unit in matches:
                if unit == "d":
                    d = int(value)
                elif unit == "h":
                    h = int(value)
                elif unit == "m":
                    m = int(value)
            delta = datetime.timedelta(days=d, hours=h, minutes=m)
            end_time = datetime.datetime.now(timezone.utc) + delta
        except:
            end_time = None

    embed = discord.Embed(
        title=question,
        color=discord.Color.blue()
    )
    embed.add_field(name="Yes", value="0", inline=True)
    embed.add_field(name="No", value="0", inline=True)
    embed.set_footer(text="Poll 📊")
    if end_time:
        embed.timestamp = end_time

    msg = await ctx.send(embed=embed)
    await msg.add_reaction("✔️")
    await msg.add_reaction("✖️")

    active_polls[msg.id] = {"question": question, "channel_id": ctx.channel.id, "author_id": ctx.author.id}

    if end_time:
        await asyncio.sleep((end_time - datetime.datetime.now(timezone.utc)).total_seconds())
        try:
            poll_msg = await ctx.channel.fetch_message(msg.id)
        except:
            return

        yes_count = 0
        no_count = 0
        for reaction in poll_msg.reactions:
            if str(reaction.emoji) == "✔️":
                yes_count = reaction.count - 1
            elif str(reaction.emoji) == "✖️":
                no_count = reaction.count - 1

        final_embed = discord.Embed(
            title=question,
            color=discord.Color.green()
        )
        final_embed.add_field(name="Yes", value=str(yes_count), inline=True)
        final_embed.add_field(name="No", value=str(no_count), inline=True)
        final_embed.set_footer(text="Poll Ended 📊")
        final_embed.timestamp = end_time

        await poll_msg.edit(embed=final_embed)
        author = ctx.author
        await poll_msg.reply(f"{author.mention} your poll has ended!", mention_author=True)

class EndPollSelectView(View):
    def __init__(self, ctx, polls_list):
        super().__init__(timeout=60)
        self.ctx = ctx
        self.polls_list = polls_list
        self.message = None

        options = [
            discord.SelectOption(label=f"{data['question'][:50]}...", value=str(msg_id))
            for msg_id, data in polls_list
        ]
        self.add_item(EndPollSelect(options))


class EndPollSelect(Select):
    def __init__(self, options):
        super().__init__(placeholder="Select a poll to end...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        poll_id = int(self.values[0])
        if poll_id not in active_polls:
            await interaction.response.send_message("Poll no longer exists.", ephemeral=True)
            return

        poll_data = active_polls.pop(poll_id)
        channel = bot.get_channel(poll_data["channel_id"])
        if not channel:
            await interaction.response.send_message("Poll channel not found.", ephemeral=True)
            return

        try:
            poll_msg = await channel.fetch_message(poll_id)
        except:
            await interaction.response.send_message("Poll message not found.", ephemeral=True)
            return

        yes_count = 0
        no_count = 0
        for reaction in poll_msg.reactions:
            if str(reaction.emoji) == "✔️":
                yes_count = reaction.count - 1
            elif str(reaction.emoji) == "✖️":
                no_count = reaction.count - 1

        final_embed = discord.Embed(
            title=poll_data["question"],
            color=discord.Color.green()
        )
        final_embed.add_field(name="Yes", value=str(yes_count), inline=True)
        final_embed.add_field(name="No", value=str(no_count), inline=True)
        final_embed.set_footer(text="Poll Ended 📊")
        final_embed.timestamp = datetime.datetime.now(datetime.timezone.utc)

        await poll_msg.edit(embed=final_embed)

        author = await bot.fetch_user(poll_data["author_id"])
        await poll_msg.reply(f"{author.mention} your poll has ended!", mention_author=True)

        await interaction.response.send_message("Poll ended successfully.", ephemeral=True)


@bot.command()
async def epoll(ctx):
    if ctx.author.id == super_owner_id:
        polls_list = list(active_polls.items())
    else:
        polls_list = [(msg_id, data) for msg_id, data in active_polls.items() if data["author_id"] == ctx.author.id]

    if not polls_list:
        return await ctx.send("No active polls found.")

    view = EndPollSelectView(ctx, polls_list)
    sent_msg = await ctx.send("Select a poll to end:", view=view)
    view.message = sent_msg

@bot.command()
@is_owner_or_mod()
async def giveaway(ctx, time: str, *, prize: str):
    match = re.match(r"(\d+)([smhdw])", time)
    if not match:
        return await ctx.send("Invalid time format. Use s/m/h/d/w.")
    amount, unit = int(match[1]), match[2]
    unit_map = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400, 'w': 604800}
    seconds = amount * unit_map[unit]
    embed = discord.Embed(title="Giveaway! 🎁", description=f"Prize: **{prize}**\nReact with 🎉 to enter!", color=0x00ff00)
    embed.set_footer(text=f"Ends in {time}")
    msg = await ctx.send(embed=embed)
    await msg.add_reaction("🎉")
    end_time = datetime.datetime.now(timezone.utc) + datetime.timedelta(seconds=seconds)
    while seconds > 0:
        if seconds <= 60:
            embed.set_footer(text=f"Ends in {seconds}s")
            await msg.edit(embed=embed)
        await asyncio.sleep(1)
        seconds = int((end_time - datetime.datetime.now(timezone.utc)).total_seconds())
    new_msg = await ctx.channel.fetch_message(msg.id)
    users = [u async for u in new_msg.reactions[0].users() if not u.bot]
    if users:
        winner = random.choice(users)
        await ctx.send(
            f"Congratulations {winner.mention} 🎉! You won **{prize}**!",
        )
    else:
        await ctx.send("No one entered the giveaway.")
    await msg.clear_reactions()

@bot.command()
async def picker(ctx, *, options):
    opts = [o.strip() for o in options.split(",") if o.strip()]
    if not opts:
        return await ctx.send("Please provide options separated by commas.")
    choice = random.choice(opts)
    await ctx.send(f"**{choice}**")

@bot.command()
@is_owner()
async def aban(ctx, target):
    try:
        user = await commands.UserConverter().convert(ctx, target)
        autoban_ids.add(user.id)
        await ctx.send(f"**{user.name}** added to the autoban list.")
    except:
        try:
            user_id = int(target)
            autoban_ids.add(user_id)
            await ctx.send(f"User ID `{user_id}` added to the autoban list.")
        except:
            await ctx.send("Invalid user or ID.")

@bot.command()
@is_owner()
async def raban(ctx, target):
    try:
        user = await commands.UserConverter().convert(ctx, target)
        autoban_ids.discard(user.id)
        await ctx.send(
           f"<@{user.id}> (**{user.name}**) removed from autoban list.",
            allowed_mentions=discord.AllowedMentions.none()
        )
    except:
        try:
            user_id = int(target)
            autoban_ids.discard(user_id)
            await ctx.send(f"User ID `{user_id}` removed from autoban list.")
        except:
            await ctx.send("Invalid user or ID.")

@bot.command()
@is_owner_or_mod()
async def abanlist(ctx):
    if not autoban_ids:
        return await ctx.send("No autobanned users.")
    results = []
    for uid in autoban_ids:
        member = ctx.guild.get_member(uid)
        if member:
            results.append(f"<@{uid}> (**{member.name}**)")
        else:
            results.append(f"User ID: {uid}")
    await ctx.send("Autoban List:\n" + "\n".join(results), allowed_mentions=discord.AllowedMentions.none())

@bot.event
async def on_member_join(member):
    if member.id in autoban_ids:
        try:
            await member.ban(reason="Autoban")
        except:
            pass

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

async def timer_countdown(ctx, message, end_time, time_str, title, owner_id):
    while True:
        now = datetime.datetime.now(datetime.timezone.utc)
        remaining = int((end_time - now).total_seconds())
        if remaining <= 0:
            break

        time_left = format_remaining(remaining)
        description = f"⏳ Time remaining:\n```{time_left}```"
        embed = message.embeds[0]
        embed.description = description
        try:
            await message.edit(embed=embed)
        except Exception:
            break
        await asyncio.sleep(1)

    if title and title != "Timer":
        embed.description = f"⏰ Time's up!\n```{title}``` timer has ended"
    else:
        embed.description = "⏰ Time's up!"

    embed.set_footer(text=f"Ended at:")
    embed.timestamp = datetime.datetime.now(datetime.timezone.utc)
    try:
        await message.edit(embed=embed)
    except Exception:
        pass

    active_timers.pop(message.id, None)

    user = ctx.guild.get_member(owner_id) or ctx.author
    if title:
        await ctx.send(f"⏰ {user.mention} Your **{title}** timer for **{time_str}** is over!")
    else:
        await ctx.send(f"⏰ {user.mention} Your timer for **{time_str}** is over!")

@bot.command()
async def timer(ctx, *, args: str):
    match = re.match(r'(.+?)(?:\s+[\"“”\'‘’](.+?)[\"“”\'‘’])?$', args)
    if not match:
        return await ctx.send("Invalid format. Use `.timer 10m` or `.timer 10m \"Title here\"`")

    time_str = match.group(1).strip()
    title = match.group(2)

    seconds = parse_time_string(time_str)
    if seconds is None or seconds <= 0:
        return await ctx.send("Invalid time format. Use `1h 20m`, `30s`, `2d 5h`, etc. Supported units: s, m, h, d.")

    title_display = title if title else "Timer"
    end_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=seconds)

    embed = Embed(
        title=title_display,
        description=f"⏳ Time remaining:\n```{time_str}```",
        color=0x00ff00
    )
    embed.set_footer(text=f"Ends at:")
    embed.timestamp = end_time
    message = await ctx.send(embed=embed)

    task = asyncio.create_task(timer_countdown(ctx, message, end_time, time_str, title, ctx.author.id))

    active_timers[message.id] = {
        "owner_id": ctx.author.id,
        "message": message,
        "title": title,
        "time_str": time_str,
        "end_time": end_time,
        "task": task
    }


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
        return interaction.user.id == self.ctx.author.id or interaction.user.id == super_owner_id

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.danger)
    async def yes_button(self, interaction, button):
        timer_task = self.timer_data.get("task")
        if timer_task and not timer_task.done():
            timer_task.cancel()

        embed = self.timer_data["message"].embeds[0]
        now = datetime.datetime.now(datetime.timezone.utc)
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

        if self.parent_view and self.parent_view.message:
            try:
                self.parent_view.disable_all_items()
                await self.parent_view.message.edit(
                    content=f"Timer cancelled with `{remaining_text}` left: [Timer]({self.timer_data['message'].jump_url})",
                    view=None
                )
            except:
                pass

        await interaction.response.edit_message(
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
            remaining = int((data["end_time"] - datetime.datetime.now(datetime.timezone.utc)).total_seconds())
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
            ephemeral=True
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
            remaining = int((data["end_time"] - datetime.datetime.now(datetime.timezone.utc)).total_seconds())
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

@bot.command()
async def alarm(ctx, date: str):
    try:
        alarm_time = datetime.datetime.strptime(date, "%d/%m/%Y")
    except ValueError:
        return await ctx.send("Invalid date format. Use DD/MM/YYYY.")
    
    now = datetime.datetime.now(timezone.utc)
    if alarm_time <= now:
        return await ctx.send("Date must be in the future.")
    
    delta = (alarm_time - now).total_seconds()
    await ctx.send(f"⏰ Alarm set for {date}, {ctx.author.mention}. I'll ping you then.")
    await asyncio.sleep(delta)
    await ctx.send(f"🔔 {ctx.author.mention} It's **{date}**! Here's your alarm.")

@bot.command()
async def define(ctx, *, word: str):
    async with aiohttp.ClientSession() as session:
        url = f"https://api.dictionaryapi.dev/api/v2/entries/en/{word}"
        async with session.get(url) as resp:
            if resp.status != 200:
                return await ctx.send("Couldn't find that word.")
            data = await resp.json()
            try:
                definitions = []
                for meaning in data[0]["meanings"]:
                    part_of_speech = meaning["partOfSpeech"]
                    for d in meaning["definitions"]:
                        definition = d["definition"]
                        definitions.append(f"**({part_of_speech})** {definition}")
                unique_defs = list(dict.fromkeys(definitions))
                response = f"📖 **Definition of `{word}`:**\n" + "\n".join(unique_defs[:3])
                await ctx.send(response)
            except:
                await ctx.send("Error.")

@bot.command()
@is_owner()
async def summon(ctx, *, message: str = "h-hi"):
    await ctx.message.delete()
    await ctx.send(f"@everyone {message}")

@bot.command()
@is_owner()
async def summon2(ctx, *, message: str):
    role = discord.utils.get(ctx.guild.roles, name="everyone2")
    if role is None:
        return await ctx.send("Role not found.")
    if not role.mentionable:
        return await ctx.send("The role is not mentionable.")

    await ctx.send(f"{role.mention} {message}")

@bot.command()
@is_owner()
async def block(ctx, member: discord.Member):
    blacklisted_users.add(member.id)
    await ctx.send(
        f"<@{member.id}> (**{member.name}**) is now blocked from using commands.",
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
@is_owner()
async def unblock(ctx, member: discord.Member):
    blacklisted_users.discard(member.id)
    await ctx.send(
        f"<@{member.id}> (**{member.name}**) is now unblocked.",
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
@is_owner_or_mod()
async def listblocks(ctx):
    if not blacklisted_users:
        return await ctx.send("No one is blocked.")
    users = []
    for uid in blacklisted_users:
        user = ctx.guild.get_member(uid)
        if user:
            users.append(f"<@{user.id}> (**{user.name}**)")
        else:
            users.append(f"User ID: `{uid}`")
    await ctx.send(
        "Blocked users:\n" + "\n".join(users),
        allowed_mentions=discord.AllowedMentions.none()
    )

@bot.command()
async def sleep(ctx):
    sleeping_users[ctx.author.id] = datetime.datetime.now(timezone.utc)
    save_dict(SLEEP_FILE, {
        str(uid): dt.isoformat()
        for uid, dt in sleeping_users.items()
    })
    await ctx.send("You’re now in sleep mode. 💤 Good night!")

@bot.command()
async def fsleep(ctx, members: commands.Greedy[discord.Member], *, time: str = None):
    if ctx.author.id != super_owner_id:
        return

    for member in members:
        start_time = datetime.datetime.now(timezone.utc)

        if time:
            try:
                h = m = s = 0
                matches = re.findall(r'(\d+)\s*(h|m|s)', time.lower())
                for value, unit in matches:
                    if unit == "h":
                        h = int(value)
                    elif unit == "m":
                        m = int(value)
                    elif unit == "s":
                        s = int(value)
                delta = datetime.timedelta(hours=h, minutes=m, seconds=s)
                start_time -= delta
            except Exception:
                continue

        sleeping_users[member.id] = start_time

    save_dict(SLEEP_FILE, {
        str(uid): dt.isoformat()
        for uid, dt in sleeping_users.items()
    })

@bot.command()
async def wake(ctx, members: commands.Greedy[discord.Member]):
    if ctx.author.id != super_owner_id:
        return

    for member in members:
        sleeping_users.pop(member.id, None)

    save_dict(SLEEP_FILE, {
        str(uid): dt.isoformat()
        for uid, dt in sleeping_users.items()
    })

@bot.command()
async def afk(ctx, *, reason="AFK"):
    afk_users[ctx.author.id] = {
        "reason": reason,
        "since": datetime.datetime.now(timezone.utc)
    }
    save_dict(AFK_FILE, {
        str(uid): {
            "reason": data["reason"],
            "since": data["since"].isoformat()
        } for uid, data in afk_users.items()
    })

    reason_text = f": **{reason}**" if reason.lower() != "afk" else ""
    await ctx.send(f"{ctx.author.mention} is now AFK{reason_text}", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
async def setbday(ctx, date):
    try:
        datetime.datetime.strptime(date, "%d/%m")

        user_id = str(ctx.author.id)
        birthdays[user_id] = {"date": date}

        with open(bday_file, "w") as f:
            json.dump(birthdays, f, indent=2)

        await ctx.send("✔️ Birthday saved!")
    except ValueError:
        await ctx.send("Invalid date format. Use DD/MM.")

@bot.command()
async def removebday(ctx):
    user_id = str(ctx.author.id)
    if user_id in birthdays:
        del birthdays[user_id]
        with open(bday_file, "w") as f:
            json.dump(birthdays, f, indent=2)
        await ctx.send("Birthday removed.")
    else:
        await ctx.send("You haven’t set a birthday.")

@bot.command(name="away")
async def away(ctx):
    now = datetime.datetime.now(timezone.utc)

    async def format_status_embed():
        embed = discord.Embed(title="AFK & Sleeping Users", color=0x3498db)
        now = datetime.datetime.now(timezone.utc)

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

            embed.add_field(name="AFK Users", value=afk_text, inline=False)

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

            embed.add_field(name="Sleeping Users", value=sleep_text, inline=False)

        if not afk_users and not sleeping_users:
            embed.description = "No users are currently AFK or sleeping."

        embed.timestamp = now
        return embed

    status_msg = await ctx.send(embed=await format_status_embed())

    while afk_users or sleeping_users:
        await asyncio.sleep(10)
        await status_msg.edit(embed=await format_status_embed())

keep_alive()
bot.run(os.getenv("DISCORD_TOKEN"))
