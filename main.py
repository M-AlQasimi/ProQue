import discord
from discord.ext import commands, tasks
from flask import Flask
from threading import Thread
from datetime import timezone
from discord.ui import Button, View
from io import BytesIO
from discord import File, Emoji, StickerItem
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
MODS_FILE = "mods.json"
OWNERS_FILE = "owners.json"
AFK_FILE = "afk_users.json"
SLEEP_FILE = "sleeping_users.json"

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

mods = load_ids(MODS_FILE)
owners = load_ids(OWNERS_FILE)

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

intents = discord.Intents.all()
bot = commands.Bot(command_prefix='.', intents=intents)
print(f"Bot is starting with intents: {bot.intents}")

log_channel_id = 1394806479881769100
rlog_channel_id = 1394806602502115470
bday_channel_id = 1364346683709718619
super_owner_id = 885548126365171824  
mods = load_ids(MODS_FILE)
owners = load_ids(OWNERS_FILE)

autoban_ids = set()
blacklisted_users = set()
mods = set()
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
        if not channel:
            channel = await bot.fetch_channel(log_channel_id)
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
async def on_reaction_add(reaction, user):
    if user.bot:
        return
    if user.id in reaction_shut:
        try:
            await reaction.remove(user)
        except Exception as e:
            print(f"Failed to remove reaction from {user}: {e}")

@bot.event
async def on_ready():
    print(f'ProQue is online as {bot.user}')
    if not keep_alive_task.is_running():
        keep_alive_task.start()
    bot.loop.create_task(birthday_check_loop())

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
    embed.timestamp = datetime.datetime.now(timezone.utc)
    print("Sending log:", embed.title)
    try:
        await send_log(embed)
    except Exception as e:
        print(f"Failed to send log: {e}")

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

    if message.channel.id in shutdown_channels and message.author.id not in owner_ids:
        try:
            await message.delete()
        except:
            pass
        return

    if message.author.id in sleeping_users:
        start = sleeping_users.pop(message.author.id)
        save_dict(SLEEP_FILE, {
            str(uid): dt.isoformat()
            for uid, dt in sleeping_users.items()
        })
        duration = datetime.datetime.now(timezone.utc) - start
        mins, secs = divmod(int(duration.total_seconds()), 60)
        hours, mins = divmod(mins, 60)
        formatted = f"{hours}h {mins}m {secs}s" if hours else f"{mins}m {secs}s" if mins else f"{secs}s"

        await message.channel.send(
            f"Welcome back, {message.author.mention}. You were sleeping for {formatted}.",
            allowed_mentions=discord.AllowedMentions.none()
        )

    for uid in sleeping_users:
        if (
            any(user.id == uid for user in message.mentions) or
            (message.reference and message.reference.resolved and message.reference.resolved.author.id == uid)
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
            duration = datetime.datetime.now(timezone.utc) - afk_data["since"]
            mins, secs = divmod(int(duration.total_seconds()), 60)
            hours, mins = divmod(mins, 60)
            formatted = f"{hours}h {mins}m {secs}s" if hours else f"{mins}m {secs}s" if mins else f"{secs}s"

            await message.channel.send(
                f"<@{user.id}> is AFK ({formatted}): **{afk_data['reason']}**",
                allowed_mentions=discord.AllowedMentions.none()
            )
            break

    if message.author.id in afk_users:
        afk_data = afk_users.pop(message.author.id)
        save_dict(AFK_FILE, {
            str(uid): {
                "reason": data["reason"],
                "since": data["since"].isoformat()
            } for uid, data in afk_users.items()
        })
        duration = datetime.datetime.now(timezone.utc) - afk_data["since"]
        mins, secs = divmod(int(duration.total_seconds()), 60)
        hours, mins = divmod(mins, 60)
        formatted = f"{hours}h {mins}m {secs}s" if hours else f"{mins}m {secs}s" if mins else f"{secs}s"

        await message.channel.send(
            f"Welcome back, {message.author.mention}. You were AFK for {formatted}: **{afk_data['reason']}**",
            allowed_mentions=discord.AllowedMentions.none()
        )

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
        if ctx.author.id in owner_ids:
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
    messages = [m for m in messages if not m.author.bot]
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

    # Split log entries into chunks for separate embeds if too long
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

    # Send message content embeds
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

@bot.event
async def on_reaction_remove(reaction, user):
    if user.bot and user.id != super_owner_id:
        return

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
async def on_member_update(before, after):
    await asyncio.sleep(1)
    embeds = []
    action_by = None

    guild = after.guild
    async for entry in guild.audit_logs(limit=5):
        if entry.target.id == after.id:
            action_by = f"{entry.user} ({entry.user.id})"
            break

    if before.nick != after.nick:
        embed = discord.Embed(title="📝 Nickname Changed", color=discord.Color.blue())
        embed.add_field(name="User", value=f"{before} ({before.id})", inline=False)
        embed.add_field(name="Before", value=before.nick or before.name, inline=True)
        embed.add_field(name="After", value=after.nick or after.name, inline=True)
        if action_by:
            embed.add_field(name="Changed by", value=action_by, inline=False)
        embed.timestamp = datetime.datetime.now(timezone.utc)
        embeds.append(embed)

    before_roles = set(before.roles)
    after_roles = set(after.roles)
    added = after_roles - before_roles
    removed = before_roles - after_roles

    if added or removed:
        embed = discord.Embed(title="🎭 Roles Updated", color=discord.Color.teal())
        embed.add_field(name="User", value=f"{after} ({after.id})", inline=False)
        if added:
            embed.add_field(name="Added", value=", ".join(role.name for role in added), inline=True)
        if removed:
            embed.add_field(name="Removed", value=", ".join(role.name for role in removed), inline=True)
        if action_by:
            embed.add_field(name="Updated by", value=action_by, inline=False)
        embed.timestamp = datetime.datetime.now(timezone.utc)
        embeds.append(embed)

    before_timeout = getattr(before, "communication_disabled_until", None)
    after_timeout = getattr(after, "communication_disabled_until", None)
    now = datetime.datetime.now(timezone.utc)

    if before_timeout != after_timeout:
        if after_timeout and after_timeout > now:
            embed = discord.Embed(title="⏳ Member Timed Out", color=discord.Color.orange())
            embed.add_field(name="User", value=f"{after} ({after.id})", inline=False)
            embed.add_field(name="Until", value=f"<t:{int(after_timeout.timestamp())}:F>", inline=False)
            if action_by:
                embed.add_field(name="By", value=action_by, inline=False)
            embed.timestamp = now
            embeds.append(embed)

        elif (
            before_timeout
            and before_timeout > now
            and (not after_timeout or after_timeout <= now)
        ):
            embed = discord.Embed(title="✔️ Timeout Removed", color=discord.Color.green())
            embed.add_field(name="User", value=f"{after} ({after.id})", inline=False)
            if action_by:
                embed.add_field(name="By", value=action_by, inline=False)
            embed.timestamp = now
            embeds.append(embed)

    for embed in embeds:
        print("Sending log:", embed.title)
        try:
            await send_log(embed)
        except Exception as e:
            print(f"Failed to send log: {e}")

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
@is_owner()
async def addmod(ctx, member: discord.Member):
    mods.add(member.id)
    save_ids(MODS_FILE, mods)
    await ctx.send(f"Added <@{member.id}> as a mod.")

@bot.command()
@is_owner()
async def removemod(ctx, member: discord.Member):
    mods.discard(member.id)
    save_ids(MODS_FILE, mods)
    await ctx.send(f"Removed <@{member.id}> from mods.")

@bot.command()
async def listmods(ctx):
    if not mods:
        return await ctx.send("No mods found.")
    mod_mentions = [f"<@{uid}>" for uid in mods]
    await ctx.send("Mods:\n" + "\n".join(mod_mentions), allowed_mentions=discord.AllowedMentions.none())

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
    if member.id in owner_ids:
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
async def addowner(ctx, member: discord.Member):
    if ctx.author.id != super_owner_id:
        return await ctx.send("Only 𝚀𝚞𝚎 can do that.")
    owners.add(member.id)
    save_ids(OWNERS_FILE, owners)
    await ctx.send(f"Added <@{member.id}> as an owner.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
async def removeowner(ctx, member: discord.Member):
    if ctx.author.id != super_owner_id:
        return await ctx.send("Only 𝚀𝚞𝚎 can do that.")
    owners.discard(member.id)
    save_ids(OWNERS_FILE, owners)
    await ctx.send(f"Removed <@{member.id}> from owners.", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
async def clearowners(ctx):
    if ctx.author.id != super_owner_id:
        return await ctx.send("Only 𝚀𝚞𝚎 can do that.")
    owners.clear()
    owners.add(super_owner_id)
    save_ids(OWNERS_FILE, owners)
    await ctx.send("Cleared all owners.")

@bot.command()
async def listowners(ctx):
    names = []
    for oid in owner_ids:
        member = ctx.guild.get_member(oid)
        if member:
            names.append(f"<@{member.id}> ({member.name})")
    await ctx.send("Owners:\n" + ("\n".join(names) if names else "No owners found."),
                   allowed_mentions=discord.AllowedMentions.none())

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
async def poll(ctx, *, question):
    msg = await ctx.send(f"**{question}**\nYes ✔️ | No ✖️")
    await msg.add_reaction("✔️")
    await msg.add_reaction("✖️")

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
            allowed_mentions=discord.AllowedMentions.none()
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

@bot.command()
async def timer(ctx, time: str):
    match = re.match(r"(\d+)([smhd])", time)
    if not match:
        return await ctx.send("Invalid time format. Use s/m/h/d.")
    amount, unit = int(match[1]), match[2]
    unit_map = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    seconds = amount * unit_map[unit]
    await ctx.send(f"⏳ Timer started for {time}, {ctx.author.mention}. I'll ping you when it's done.")
    await asyncio.sleep(seconds)
    await ctx.send(f"⏰ {ctx.author.mention} Time's up! Your **{time}** timer is over.")

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
    await ctx.send(f"{ctx.author.mention} is now AFK: **{reason}**", allowed_mentions=discord.AllowedMentions.none())

@bot.command()
async def setbday(ctx, date):
    """Set your birthday. Format: .setbday DD/MM"""
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
    """Remove your birthday from the system."""
    user_id = str(ctx.author.id)
    if user_id in birthdays:
        del birthdays[user_id]
        with open(bday_file, "w") as f:
            json.dump(birthdays, f, indent=2)
        await ctx.send("Birthday removed.")
    else:
        await ctx.send("You haven’t set a birthday.")

keep_alive()
bot.run(os.getenv("DISCORD_TOKEN"))
