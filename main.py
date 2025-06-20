import discord
from discord.ext import commands, tasks
from flask import Flask
from threading import Thread
import asyncio
import re
import random
import datetime
import os
import aiohttp

intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.guilds = True
intents.members = True

bot = commands.Bot(command_prefix='.', intents=intents)

super_owner_id = 885548126365171824
owner_ids = {super_owner_id}
watchlist = set()
autoban_ids = set()

last_deleted = {}
last_edited = {}

def is_owner():
    async def predicate(ctx):
        return ctx.author.id in owner_ids
    return commands.check(predicate)

app = Flask('')

@app.route('/')
def home():
    return "I'm alive", 200

def run():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run)
    t.start()

@tasks.loop(minutes=4)
async def keep_alive_task():
    print("Heartbeats")

@bot.event
async def on_ready():
    print(f'ProQue is online as {bot.user}')
    if not keep_alive_task.is_running():
        keep_alive_task.start()

@bot.command()
@is_owner()
async def q(ctx):
    answer = random.choice(["Yes", "No"])
    await ctx.send(f"**{answer}**")

@bot.command()
@is_owner()
async def test(ctx):
    await ctx.send("I'm alive heh")

@bot.command()
@is_owner()
async def setnick(ctx, member: discord.Member, *, nickname: str):
    try:
        await member.edit(nick=nickname)
        await ctx.send(f"Changed {member.mention}'s name to **{nickname}**.")
    except discord.Forbidden:
        await ctx.send("I don't have permission to change that user's nickname.")
    except Exception as e:
        await ctx.send(f"Error: {e}")

@bot.command()
@is_owner()
async def start(ctx, member: discord.Member):
    if member.id == super_owner_id:
        return
    watchlist.add(member.id)

@bot.command()
@is_owner()
async def end(ctx, member: discord.Member):
    watchlist.discard(member.id)

@bot.command()
@is_owner()
async def clearwatchlist(ctx):
    watchlist.clear()
    await ctx.send("Watchlist cleared.")

@bot.command()
@is_owner()
async def addowner(ctx, member: discord.Member):
    if ctx.author.id != super_owner_id:
        return
    owner_ids.add(member.id)

@bot.command()
@is_owner()
async def removeowner(ctx, member: discord.Member):
    if ctx.author.id != super_owner_id or member.id == super_owner_id:
        return
    owner_ids.discard(member.id)

@bot.command()
@is_owner()
async def clearowners(ctx):
    if ctx.author.id != super_owner_id:
        return
    owner_ids.clear()
    owner_ids.add(super_owner_id)
    await ctx.send("All owners cleared.")

@bot.command()
@is_owner()
async def listowners(ctx):
    names = []
    for oid in owner_ids:
        member = ctx.guild.get_member(oid)
        if member:
            names.append(f"{member.display_name} ({member.name})")
    await ctx.send("Owners:\n" + "\n".join(names) if names else "No owners found.")

@bot.command()
@is_owner()
async def listtargets(ctx):
    names = []
    for uid in watchlist:
        member = ctx.guild.get_member(uid)
        if member:
            names.append(f"{member.display_name} ({member.name})")
    await ctx.send("Watched Users:\n" + "\n".join(names) if names else "No targets being watched.")

@bot.event
async def on_message(message):
    if message.author.id in watchlist and message.author.id != super_owner_id:
        try:
            await message.delete()
        except:
            pass
    await bot.process_commands(message)

@bot.command()
@is_owner()
async def purge(ctx, amount: int, member: discord.Member = None):
    await ctx.message.delete()
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
        await ctx.channel.delete_messages(deleted)

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
@is_owner()
async def mute(ctx, member: discord.Member, duration: str):
    time_units = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    seconds = int(duration[:-1]) * time_units[duration[-1]]
    await member.timeout(discord.utils.utcnow() + datetime.timedelta(seconds=seconds))
    await ctx.send(f"{member.display_name} has been muted for {duration}.")

@bot.command()
@is_owner()
async def ban(ctx, member: discord.Member, *, reason=None):
    await member.ban(reason=reason)
    await ctx.send(f"{member.display_name} has been banned.")

@bot.command()
@is_owner()
async def kick(ctx, member: discord.Member, *, reason=None):
    await member.kick(reason=reason)
    await ctx.send(f"{member.display_name} has been kicked.")

@bot.command()
@is_owner()
async def addrole(ctx, member: discord.Member, role: discord.Role):
    await member.add_roles(role)
    await ctx.send(f"Added {role.name} to {member.display_name}.")

@bot.command()
@is_owner()
async def removerole(ctx, member: discord.Member, role: discord.Role):
    await member.remove_roles(role)
    await ctx.send(f"Removed {role.name} from {member.display_name}.")

@bot.command()
@is_owner()
async def speak(ctx, *, msg):
    await ctx.message.delete()
    await ctx.send(msg)

@bot.command()
@is_owner()
async def poll(ctx, *, question):
    msg = await ctx.send(f"**{question}**\nYes ✅ | No ❌")
    await msg.add_reaction("✅")
    await msg.add_reaction("❌")

@bot.command()
@is_owner()
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
    end_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=seconds)
    while seconds > 0:
        if seconds <= 60:
            embed.set_footer(text=f"Ends in {seconds}s")
            await msg.edit(embed=embed)
        await asyncio.sleep(1)
        seconds = int((end_time - datetime.datetime.utcnow()).total_seconds())
    new_msg = await ctx.channel.fetch_message(msg.id)
    users = [u async for u in new_msg.reactions[0].users() if not u.bot]
    if users:
        winner = random.choice(users)
        await ctx.send(f"Congratulations {winner.mention} 🎉, you won **{prize}**!")
    else:
        await ctx.send("No one entered the giveaway.")

@bot.command()
@is_owner()
async def picker(ctx, *, items):
    options = [item.strip() for item in items.split(',') if item.strip()]
    if not options:
        await ctx.send("You must provide a list of items separated by commas.")
        return
    choice = random.choice(options)
    await ctx.send(f"**{choice}**")

@bot.command()
@is_owner()
async def aban(ctx, user: discord.Member = None, user_id: int = None):
    if user:
        autoban_ids.add(user.id)
        await ctx.send(f"{user.mention} will be autobanned if they join.")
    elif user_id:
        autoban_ids.add(user_id)
        await ctx.send(f"User ID {user_id} will be autobanned if they join.")
    else:
        await ctx.send("Please mention a user or provide a user ID.")

@bot.command()
@is_owner()
async def raban(ctx, user: discord.Member = None, user_id: int = None):
    if user:
        autoban_ids.discard(user.id)
        await ctx.send(f"{user.mention} has been removed from autoban list.")
    elif user_id:
        autoban_ids.discard(user_id)
        await ctx.send(f"User ID {user_id} has been removed from autoban list.")
    else:
        await ctx.send("Please mention a user or provide a user ID.")

@bot.command()
@is_owner()
async def abanlist(ctx):
    if not autoban_ids:
        await ctx.send("Autoban list is empty.")
        return
    results = []
    for uid in autoban_ids:
        user = bot.get_user(uid)
        if user:
            results.append(f"{user.name}#{user.discriminator} ({uid})")
        else:
            results.append(f"{uid}")
    await ctx.send("Autobanned IDs:\n" + "\n".join(results))

@bot.event
async def on_member_join(member):
    if member.id in autoban_ids:
        try:
            await member.ban(reason="Auto-banned on join.")
        except:
            pass

@bot.command()
@is_owner()
async def timer(ctx, duration: str):
    units = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400, 'w': 604800}
    if duration[-1] not in units:
        return await ctx.send("Invalid time format. Use s/m/h/d/w.")
    try:
        seconds = int(duration[:-1]) * units[duration[-1]]
    except:
        return await ctx.send("Invalid time format.")
    await ctx.send(f"Timer set for {duration}. I'll ping you when it's done.")
    await asyncio.sleep(seconds)
    await ctx.send(f"{ctx.author.mention} Timer done! ⏰")

@bot.command()
@is_owner()
async def alarm(ctx, date: str, mode: str = "ping"):
    try:
        alarm_time = datetime.datetime.strptime(date, "%d/%m/%Y")
        now = datetime.datetime.utcnow()
        delay = (alarm_time - now).total_seconds()
        if delay <= 0:
            return await ctx.send("The specified date is in the past.")
        await ctx.send(f"Alarm set for {date}. Mode: {mode}")
        await asyncio.sleep(delay)
        if mode == "dm":
            try:
                await ctx.author.send(f"⏰ Alarm: It's {date}!")
            except:
                await ctx.send(f"{ctx.author.mention} ⏰ Alarm: It's {date}! (Couldn't DM you)")
        else:
            await ctx.send(f"{ctx.author.mention} ⏰ Alarm: It's {date}!")
    except ValueError:
        await ctx.send("Invalid date format. Use day/month/year.")

used_quotes = set()
used_roasts = set()

async def fetch_ai_text(session, prompt):
    dummy_quotes = [
        "Life is what happens when you're busy making other plans.",
        "Be yourself; everyone else is already taken.",
        "The only limit to our realization of tomorrow is our doubts of today.",
    ]
    dummy_roasts = [
        "You're as useless as the 'ueue' in 'queue'.",
        "If I had a face like yours, I'd sue my parents.",
        "You bring everyone so much joy… when you leave the room.",
    ]
    if "quote" in prompt.lower():
        choices = [q for q in dummy_quotes if q not in used_quotes]
        if not choices:
            used_quotes.clear()
            choices = dummy_quotes
        choice = random.choice(choices)
        used_quotes.add(choice)
        return choice
    elif "roast" in prompt.lower():
        choices = [r for r in dummy_roasts if r not in used_roasts]
        if not choices:
            used_roasts.clear()
            choices = dummy_roasts
        choice = random.choice(choices)
        used_roasts.add(choice)
        return choice
    return "Hmm... I have nothing to say."

@bot.command()
async def quote(ctx):
    async with aiohttp.ClientSession() as session:
        text = await fetch_ai_text(session, "quote")
    await ctx.send(f"💬 Quote: {text}")

@bot.command()
async def roast(ctx):
    async with aiohttp.ClientSession() as session:
        text = await fetch_ai_text(session, "roast")
    await ctx.send(f"🔥 Roast: {text}")

@bot.command()
async def userinfo(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed = discord.Embed(title=f"User info - {member}", color=discord.Color.blue())
    embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)
    embed.add_field(name="ID", value=member.id, inline=True)
    embed.add_field(name="Display Name", value=member.display_name, inline=True)
    embed.add_field(name="Account Created", value=member.created_at.strftime("%Y-%m-%d %H:%M:%S"), inline=True)
    embed.add_field(name="Joined Server", value=member.joined_at.strftime("%Y-%m-%d %H:%M:%S") if member.joined_at else "Unknown", inline=True)
    roles = [r.name for r in member.roles if r.name != "@everyone"]
    embed.add_field(name="Roles", value=", ".join(roles) if roles else "None", inline=False)
    await ctx.send(embed=embed)

@bot.command()
async def avatar(ctx, member: discord.Member = None):
    member = member or ctx.author
    url = member.avatar.url if member.avatar else member.default_avatar.url
    await ctx.send(f"{member.display_name}'s avatar:\n{url}")

@bot.command()
@is_owner()
async def slowmode(ctx, seconds: int = None):
    if seconds is None or seconds == 0:
        await ctx.channel.edit(slowmode_delay=0)
        await ctx.send("Slowmode disabled.")
    elif 0 < seconds <= 21600:
        await ctx.channel.edit(slowmode_delay=seconds)
        await ctx.send(f"Slowmode set to {seconds} seconds.")
    else:
        await ctx.send("Slowmode must be between 1 and 21600 seconds.")

hangman_games = {}

@bot.command()
async def hangman(ctx):
    if ctx.channel.id in hangman_games:
        await ctx.send("A game is already running in this channel.")
        return
    words = ["python", "discord", "bot", "hangman", "programming", "asyncio"]
    word = random.choice(words)
    hangman_games[ctx.channel.id] = {
        "word": word,
        "guessed": set(),
        "wrong": 0,
        "max_wrong": 6
    }
    await ctx.send(f"Hangman started! Guess letters by typing them in chat.")

@bot.event
async def on_message(message):
    if message.author.bot:
        await bot.process_commands(message)
        return

    if message.author.id in watchlist and message.author.id != super_owner_id:
        try:
            await message.delete()
        except:
            pass

    game = hangman_games.get(message.channel.id)
    if game and message.content.lower() in "abcdefghijklmnopqrstuvwxyz" and len(message.content) == 1:
        letter = message.content.lower()
        if letter in game["guessed"]:
            await message.channel.send(f"You already guessed '{letter}'")
        elif letter in game["word"]:
            game["guessed"].add(letter)
            display = " ".join(c if c in game["guessed"] else "_" for c in game["word"])
            if all(c in game["guessed"] for c in game["word"]):
                await message.channel.send(f"Correct! The word was **{game['word']}**. You win! 🎉")
                del hangman_games[message.channel.id]
            else:
                await message.channel.send(f"Good guess!\n{display}")
        else:
            game["wrong"] += 1
            game["guessed"].add(letter)
            if game["wrong"] >= game["max_wrong"]:
                await message.channel.send(f"Game over! The word was **{game['word']}**.")
                del hangman_games[message.channel.id]
            else:
                await message.channel.send(f"Wrong guess! Attempts left: {game['max_wrong'] - game['wrong']}")
        await message.delete()
        return

    await bot.process_commands(message)

@bot.event
async def on_message_edit(before, after):
    if before.author.bot:
        return
    last_edited[before.channel.id] = (before.author, before.content, after.content)
    await bot.process_commands(after)

@bot.event
async def on_message_delete(message):
    if message.author.bot:
        return
    last_deleted[message.channel.id] = (message.author, message.content)
    await bot.process_commands(message)

@bot.command()
async def esnipe(ctx):
    snipe = last_edited.get(ctx.channel.id)
    if not snipe:
        await ctx.send("No edited message found.")
        return
    author, before, after = snipe
    await ctx.send(f"✏️ **{author}** edited their message:\nBefore: {before}\nAfter: {after}")

@bot.command()
async def dsnipe(ctx):
    snipe = last_deleted.get(ctx.channel.id)
    if not snipe:
        await ctx.send("No deleted message found.")
        return
    author, content = snipe
    await ctx.send(f"🗑️ **{author}** deleted their message:\n{content}")

@bot.command()
async def define(ctx, *, word: str):
    url = f"https://api.dictionaryapi.dev/api/v2/entries/en/{word}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                await ctx.send("Definition not found.")
                return
            data = await resp.json()
            if not data or not isinstance(data, list):
                await ctx.send("Definition not found.")
                return
            meanings = data[0].get("meanings", [])
            if not meanings:
                await ctx.send("Definition not found.")
                return
            defs = []
            for meaning in meanings[:2]:
                part = meaning.get("partOfSpeech", "")
                for d in meaning.get("definitions", [])[:2]:
                    definition = d.get("definition", "")
                    example = d.get("example", "")
                    defs.append(f"({part}) {definition}" + (f"\nExample: {example}" if example else ""))
            if not defs:
                await ctx.send("Definition not found.")
                return
            await ctx.send(f"Definitions for **{word}**:\n" + "\n\n".join(defs))
            
truths = [
    "What's your biggest fear?",
    "Have you ever lied to your best friend?",
    "What's the most embarrassing thing you've done?",
]
dares = [
    "Do 10 push-ups.",
    "Sing a song out loud.",
    "Dance for 30 seconds.",
]

@bot.command()
async def truth(ctx):
    await ctx.send(f"Truth: {random.choice(truths)}")

@bot.command()
async def dare(ctx):
    await ctx.send(f"Dare: {random.choice(dares)}")

ttt_games = {}

def format_board(board):
    def emote(c):
        if c == "X":
            return "❌"
        elif c == "O":
            return "⭕"
        else:
            return "➖"
    rows = []
    for i in range(0, 9, 3):
        rows.append(" ".join(emote(c) for c in board[i:i+3]))
    return "\n".join(rows)

def check_winner(board):
    wins = [
        (0,1,2), (3,4,5), (6,7,8),
        (0,3,6), (1,4,7), (2,5,8),
        (0,4,8), (2,4,6)
    ]
    for a,b,c in wins:
        if board[a] != "-" and board[a] == board[b] == board[c]:
            return board[a]
    if "-" not in board:
        return "Tie"
    return None

@bot.command()
async def ttt(ctx, opponent: discord.Member = None):
    if ctx.channel.id in ttt_games:
        await ctx.send("A Tic-Tac-Toe game is already running in this channel.")
        return
    if opponent is None or opponent.bot or opponent == ctx.author:
        await ctx.send("Please mention a valid opponent (not a bot or yourself).")
        return
    board = ["-"] * 9
    ttt_games[ctx.channel.id] = {
        "board": board,
        "players": [ctx.author, opponent],
        "turn": 0
    }
    await ctx.send(f"Tic-Tac-Toe started between {ctx.author.mention} (❌) and {opponent.mention} (⭕).")
    await ctx.send(format_board(board))
    await ctx.send(f"It's {ctx.author.mention}'s turn! Type a number 1-9 to place your ❌.")

@bot.event
async def on_message(message):
    if message.author.bot:
        await bot.process_commands(message)
        return

    if message.author.id in watchlist and message.author.id != super_owner_id:
        try:
            await message.delete()
        except:
            pass

    game = hangman_games.get(message.channel.id)
    if game and message.content.lower() in "abcdefghijklmnopqrstuvwxyz" and len(message.content) == 1:
        letter = message.content.lower()
        if letter in game["guessed"]:
            await message.channel.send(f"You already guessed '{letter}'")
        elif letter in game["word"]:
            game["guessed"].add(letter)
            display = " ".join(c if c in game["guessed"] else "_" for c in game["word"])
            if all(c in game["guessed"] for c in game["word"]):
                await message.channel.send(f"Correct! The word was **{game['word']}**. You win! 🎉")
                del hangman_games[message.channel.id]
            else:
                await message.channel.send(f"Good guess!\n{display}")
        else:
            game["wrong"] += 1
            game["guessed"].add(letter)
            if game["wrong"] >= game["max_wrong"]:
                await message.channel.send(f"Game over! The word was **{game['word']}**.")
                del hangman_games[message.channel.id]
            else:
                await message.channel.send(f"Wrong guess! Attempts left: {game['max_wrong'] - game['wrong']}")
        await message.delete()
        return

    ttt = ttt_games.get(message.channel.id)
    if ttt and message.author in ttt["players"]:
        content = message.content.strip()
        if content.isdigit():
            pos = int(content) - 1
            if pos < 0 or pos > 8:
                await message.channel.send("Choose a number from 1 to 9.")
            elif ttt["board"][pos] != "-":
                await message.channel.send("That spot is already taken.")
            elif ttt["players"][ttt["turn"]] != message.author:
                await message.channel.send("It's not your turn.")
            else:
                ttt["board"][pos] = "X" if ttt["turn"] == 0 else "O"
                winner = check_winner(ttt["board"])
                await message.channel.send(format_board(ttt["board"]))
                if winner == "Tie":
                    await message.channel.send("It's a tie!")
                    del ttt_games[message.channel.id]
                elif winner:
                    await message.channel.send(f"{message.author.mention} wins! 🎉")
                    del ttt_games[message.channel.id]
                else:
                    ttt["turn"] = 1 - ttt["turn"]
                    next_player = ttt["players"][ttt["turn"]]
                    symbol = "❌" if ttt["turn"] == 0 else "⭕"
                    await message.channel.send(f"It's {next_player.mention}'s turn! Type 1-9 to place your {symbol}.")
        await message.delete()
        return

    await bot.process_commands(message)

keep_alive()
bot.run(os.getenv("DISCORD_TOKEN"))
