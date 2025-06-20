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

bot = commands.Bot(command_prefix='!', intents=intents)

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
    seconds = int(duration[:-1]) * units[duration[-1]]
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

@bot.command()
async def userinfo(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed = discord.Embed(title=f"{member.display_name}'s Info", color=discord.Color.blue())
    embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)
    embed.add_field(name="ID", value=member.id, inline=True)
    embed.add_field(name="Bot?", value=member.bot, inline=True)
    embed.add_field(name="Top Role", value=member.top_role.mention, inline=True)
    embed.add_field(name="Joined Server", value=member.joined_at.strftime("%Y-%m-%d %H:%M:%S"), inline=True)
    embed.add_field(name="Account Created", value=member.created_at.strftime("%Y-%m-%d %H:%M:%S"), inline=True)
    await ctx.send(embed=embed)

@bot.command()
async def avatar(ctx, member: discord.Member = None):
    member = member or ctx.author
    avatar_url = member.avatar.url if member.avatar else member.default_avatar.url
    await ctx.send(f"{member.display_name}'s avatar:\n{avatar_url}")

quotes = [
    "Be yourself; everyone else is already taken.",
    "Success is not final, failure is not fatal: It is the courage to continue that counts.",
    "Do what you can with all you have, wherever you are.",
    "What you do speaks so loudly that I cannot hear what you say.",
]

@bot.command()
async def quote(ctx):
    await ctx.send(random.choice(quotes))

@bot.command()
@is_owner()
async def slowmode(ctx, seconds: int):
    if seconds < 0 or seconds > 21600:
        return await ctx.send("Slowmode must be between 0 and 21600 seconds (6 hours).")
    await ctx.channel.edit(slowmode_delay=seconds)
    await ctx.send(f"Set slowmode to {seconds} seconds.")

games = {}

@bot.command()
async def hangman(ctx):
    if ctx.channel.id in games:
        return await ctx.send("Game already running in this channel!")
    word_list = ["python", "discord", "hangman", "bot", "programming"]
    word = random.choice(word_list)
    hidden = ["_"] * len(word)
    attempts = 6
    games[ctx.channel.id] = {"word": word, "hidden": hidden, "attempts": attempts, "guessed": set()}
    await ctx.send(f"Hangman started! Word: {' '.join(hidden)}\nGuess letters with `!guess <letter>`")

@bot.command()
async def guess(ctx, letter: str):
    if ctx.channel.id not in games:
        return await ctx.send("No game running in this channel.")
    game = games[ctx.channel.id]
    letter = letter.lower()
    if letter in game["guessed"]:
        return await ctx.send("You already guessed that letter.")
    game["guessed"].add(letter)
    if letter in game["word"]:
        for i, c in enumerate(game["word"]):
            if c == letter:
                game["hidden"][i] = letter
        if "_" not in game["hidden"]:
            await ctx.send(f"You won! The word was {game['word']}.")
            del games[ctx.channel.id]
        else:
            await ctx.send(f"Correct! {' '.join(game['hidden'])}")
    else:
        game["attempts"] -= 1
        if game["attempts"] == 0:
            await ctx.send(f"You lost! The word was {game['word']}.")
            del games[ctx.channel.id]
        else:
            await ctx.send(f"Wrong! Attempts left: {game['attempts']}")

ttt_games = {}

@bot.command()
async def ttt(ctx, opponent: discord.Member):
    if ctx.channel.id in ttt_games:
        return await ctx.send("A game is already running in this channel.")
    if opponent.bot or opponent == ctx.author:
        return await ctx.send("Choose a valid opponent.")
    board = ["⬜"] * 9
    turn = ctx.author
    ttt_games[ctx.channel.id] = {"board": board, "turn": turn, "players": [ctx.author, opponent]}
    await ctx.send(f"Tic Tac Toe started between {ctx.author.mention} and {opponent.mention}. {ctx.author.mention} goes first.\nUse `!place <1-9>` to play.\n{''.join(board)}")

@bot.command()
async def place(ctx, pos: int):
    if ctx.channel.id not in ttt_games:
        return await ctx.send("No Tic Tac Toe game running in this channel.")
    game = ttt_games[ctx.channel.id]
    if ctx.author != game["turn"]:
        return await ctx.send("It's not your turn.")
    if pos < 1 or pos > 9:
        return await ctx.send("Position must be 1-9.")
    pos -= 1
    if game["board"][pos] != "⬜":
        return await ctx.send("That spot is taken.")
    symbol = "❌" if game["turn"] == game["players"][0] else "⭕"
    game["board"][pos] = symbol
    b = game["board"]
    wins = [(0,1,2),(3,4,5),(6,7,8),(0,3,6),(1,4,7),(2,5,8),(0,4,8),(2,4,6)]
    for a,b_,c in wins:
        if b[a] == b[b_] == b[c] != "⬜":
            await ctx.send(f"{ctx.author.mention} wins!\n{''.join(b)}")
            del ttt_games[ctx.channel.id]
            return
    if "⬜" not in b:
        await ctx.send(f"Game ended in a draw!\n{''.join(b)}")
        del ttt_games[ctx.channel.id]
        return
    game["turn"] = game["players"][1] if game["turn"] == game["players"][0] else game["players"][0]
    await ctx.send(f"Next turn: {game['turn'].mention}\n{''.join(game['board'])}")

roasts = [
    "You're as bright as a black hole, and twice as dense.",
    "You bring everyone so much joy... when you leave the room.",
    "You're like a cloud. When you disappear, it's a beautiful day.",
]

@bot.command()
async def roast(ctx, member: discord.Member = None):
    member = member or ctx.author
    await ctx.send(f"{member.display_name}, {random.choice(roasts)}")

@bot.command()
async def define(ctx, *, word):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://api.dictionaryapi.dev/api/v2/entries/en/{word}") as resp:
            if resp.status != 200:
                return await ctx.send(f"No definition found for `{word}`.")
            data = await resp.json()
            try:
                meaning = data[0]['meanings'][0]['definitions'][0]['definition']
                await ctx.send(f"**{word}**: {meaning}")
            except (IndexError, KeyError):
                await ctx.send(f"Could not parse definition for `{word}`.")

truths = [
    "What's your biggest fear?",
    "Have you ever lied to your best friend?",
    "What's a secret you have never told anyone?",
]

dares = [
    "Do 10 pushups.",
    "Sing a song loudly.",
    "Dance for 30 seconds.",
]

@bot.command()
async def truth(ctx):
    await ctx.send(random.choice(truths))

@bot.command()
async def dare(ctx):
    await ctx.send(random.choice(dares))

@bot.event
async def on_message_delete(message):
    if not message.author.bot:
        last_deleted[message.channel.id] = (message.content, message.author, message.created_at)

@bot.command()
async def dsnipe(ctx):
    if ctx.channel.id not in last_deleted:
        return await ctx.send("Nothing to snipe!")
    content, author, time = last_deleted[ctx.channel.id]
    ago = (datetime.datetime.utcnow() - time).seconds
    await ctx.send(f"Deleted message by {author.display_name} {ago}s ago:\n{content}")

@bot.event
async def on_message_edit(before, after):
    if not before.author.bot and before.content != after.content:
        last_edited[before.channel.id] = (before.content, after.content, before.author, before.created_at)

@bot.command()
async def esnipe(ctx):
    if ctx.channel.id not in last_edited:
        return await ctx.send("Nothing to snipe!")
    before_content, after_content, author, time = last_edited[ctx.channel.id]
    ago = (datetime.datetime.utcnow() - time).seconds
    await ctx.send(f"Edited message by {author.display_name} {ago}s ago:\nBefore: {before_content}\nAfter: {after_content}")

keep_alive()
bot.run(os.getenv("DISCORD_TOKEN"))
