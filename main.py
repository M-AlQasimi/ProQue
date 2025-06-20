import discord
from discord.ext import commands, tasks
from flask import Flask
from threading import Thread
import asyncio
import re
import random
import datetime
import os
import openai

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
edited_snipes = {}
deleted_snipes = {}

async def ai_generate(prompt):
    try:
        response = await openai.ChatCompletion.acreate(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=60,
            temperature=0.8,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"OpenAI error: {e}")
        return "Error"
        
openai.api_key = os.getenv("OPENAI_API_KEY")

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
    print("Heartbeat")

@bot.event
async def on_ready():
    print(f'ProQue is online as {bot.user}')
    if not keep_alive_task.is_running():
        keep_alive_task.start()

def is_owner():
    async def predicate(ctx):
        return ctx.author.id in owner_ids
    return commands.check(predicate)

@bot.event
async def on_message_delete(message):
    if message.content:
        deleted_snipes[message.channel.id] = (message.content, message.author)

@bot.event
async def on_message_edit(before, after):
    if before.content and after.content != before.content:
        edited_snipes[before.channel.id] = (before.content, after.content, before.author)

@bot.command()
@is_owner()
async def dsnipe(ctx):
    data = deleted_snipes.get(ctx.channel.id)
    if not data:
        return await ctx.send("Nothing to snipe.")
    content, author = data
    await ctx.send(f"Deleted by {author.display_name}: {content}")

@bot.command()
@is_owner()
async def esnipe(ctx):
    data = edited_snipes.get(ctx.channel.id)
    if not data:
        return await ctx.send("Nothing to snipe.")
    before, after, author = data
    await ctx.send(f"Edited by {author.display_name}:\nBefore: {before}\nAfter: {after}")

@bot.command()
async def userinfo(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed = discord.Embed(title="User Info", color=0x3498db)
    embed.set_thumbnail(url=member.avatar.url)
    embed.add_field(name="Username", value=str(member), inline=True)
    embed.add_field(name="ID", value=member.id, inline=True)
    embed.add_field(name="Joined Server", value=member.joined_at.strftime("%d %b %Y"), inline=False)
    embed.add_field(name="Created Account", value=member.created_at.strftime("%d %b %Y"), inline=False)
    await ctx.send(embed=embed)

@bot.command()
async def avatar(ctx, member: discord.Member = None):
    member = member or ctx.author
    await ctx.send(member.avatar.url)

used_quotes = set()
used_roasts = set()
used_truths = set()
used_dares = set()
used_hangman_words = set()

async def ai_generate_unique(prompt, used_set):
    for _ in range(5):
        text = await ai_generate(prompt)
        if text not in used_set:
            used_set.add(text)
            return text
    return "Error"

@bot.command()
async def quote(ctx):
    text = await ai_generate_unique("Give me a short original inspirational quote.", used_quotes)
    await ctx.send(f"💬 {text}")

@bot.command()
async def roast(ctx, member: discord.Member = None):
    target = member or ctx.author
    prompt = f"Give me a short original funny roast for {target.display_name}."
    text = await ai_generate_unique(prompt, used_roasts)
    await ctx.send(f"{text}")

@bot.command()
async def truth(ctx):
    text = await ai_generate_unique("Give me a short personal question for a truth or dare game.", used_truths)
    await ctx.send(f"Truth: {text}")

@bot.command()
async def dare(ctx):
    text = await ai_generate_unique("Give me a short fun dare for a truth or dare game.", used_dares)
    await ctx.send(f"Dare: {text}")

active_hangman_games = {}

@bot.command()
async def hangman(ctx):
    if ctx.author.id in active_hangman_games:
        return await ctx.send("You already have an active hangman game. Use .guess <letter> to guess.")
    word = await ai_generate_unique("Give me a single random English word for hangman (no definition).", used_hangman_words)
    if not word.isalpha() or ' ' in word:
        return await ctx.send("Error")
    word = word.lower()
    display = ["_" for _ in word]
    tries = 6
    guessed = set()
    active_hangman_games[ctx.author.id] = {
        "word": word,
        "display": display,
        "tries": tries,
        "guessed": guessed
    }
    await ctx.send(f"Hangman started!\n{' '.join(display)}\nYou have {tries} tries.")

@bot.command()
async def guess(ctx, letter: str):
    game = active_hangman_games.get(ctx.author.id)
    if not game:
        return await ctx.send("You don't have an active hangman game. Start one with .hangman.")
    if len(letter) != 1 or not letter.isalpha():
        return await ctx.send("Please guess a single letter.")
    letter = letter.lower()
    if letter in game["guessed"]:
        return await ctx.send("You already guessed that letter.")
    game["guessed"].add(letter)
    if letter in game["word"]:
        for i, c in enumerate(game["word"]):
            if c == letter:
                game["display"][i] = letter
        await ctx.send(f"✅ Correct!\n{' '.join(game['display'])}")
    else:
        game["tries"] -= 1
        await ctx.send(f"❌ Wrong. {game['tries']} tries left.\n{' '.join(game['display'])}")
    if "_" not in game["display"]:
        await ctx.send(f"You won! 🎉 The word was {game['word']}.")
        del active_hangman_games[ctx.author.id]
    elif game["tries"] <= 0:
        await ctx.send(f"Game over. The word was {game['word']}.")
        del active_hangman_games[ctx.author.id]

ttt_games = {}

@bot.command()
async def ttt(ctx, opponent: discord.Member):
    if ctx.channel.id in ttt_games:
        return await ctx.send("A game is already in progress here.")
    board = [":white_large_square:"] * 9
    players = [ctx.author, opponent]
    turn = 0
    ttt_games[ctx.channel.id] = {
        "board": board,
        "players": players,
        "turn": turn
    }
    def format_board():
        return "\n".join(["".join(board[i:i+3]) for i in range(0, 9, 3)])
    await ctx.send(f"Tic Tac Toe started!\n{players[0].mention} vs {players[1].mention}")
    await ctx.send(format_board())

@bot.command()
async def place(ctx, pos: int):
    game = ttt_games.get(ctx.channel.id)
    if not game:
        return await ctx.send("No active Tic Tac Toe game in this channel. Start one with .ttt @user.")
    if ctx.author != game["players"][game["turn"]]:
        return await ctx.send("It's not your turn.")
    if not (1 <= pos <= 9):
        return await ctx.send("Position must be between 1 and 9.")
    board = game["board"]
    pos -= 1
    if board[pos] != ":white_large_square:":
        return await ctx.send("That spot is already taken.")
    board[pos] = ":regional_indicator_x:" if game["turn"] == 0 else ":o2:"
    def format_board():
        return "\n".join(["".join(board[i:i+3]) for i in range(0, 9, 3)])
    await ctx.send(format_board())
    win_positions = [(0,1,2),(3,4,5),(6,7,8),(0,3,6),(1,4,7),(2,5,8),(0,4,8),(2,4,6)]
    for a,b,c in win_positions:
        if board[a] == board[b] == board[c] and board[a] != ":white_large_square:":
            await ctx.send(f"{game['players'][game['turn']].mention} wins!")
            del ttt_games[ctx.channel.id]
            return
    if ":white_large_square:" not in board:
        await ctx.send("It's a draw!")
        del ttt_games[ctx.channel.id]
        return
    game["turn"] = 1 - game["turn"]

@bot.command()
async def q(ctx):
    answer = random.choice(["Yes", "No"])
    await ctx.send(f"**{answer}**")

@bot.command()
@is_owner()
async def setnick(ctx, member: discord.Member, *, nickname: str):
    try:
        await member.edit(nick=nickname)
        await ctx.send(f"Changed {member.mention}'s nickname to **{nickname}**.")
    except discord.Forbidden:
        await ctx.send("I don't have permission to change that user's nickname.")
    except Exception as e:
        await ctx.send(f"Error: {e}")

@bot.command()
@is_owner()
async def start(ctx, member: discord.Member):
    if member.id == super_owner_id:
        return await ctx.send("Cannot watch super owner.")
    watchlist.add(member.id)
    await ctx.send(f"Started watching {member.mention}.")

@bot.command()
@is_owner()
async def end(ctx, member: discord.Member):
    watchlist.discard(member.id)
    await ctx.send(f"Stopped watching {member.mention}.")

@bot.command()
@is_owner()
async def clearwatchlist(ctx):
    watchlist.clear()
    await ctx.send("Watchlist cleared.")

@bot.command()
@is_owner()
async def addowner(ctx, member: discord.Member):
    if ctx.author.id != super_owner_id:
        return await ctx.send("Only super owner can add owners.")
    owner_ids.add(member.id)
    await ctx.send(f"Added {member.mention} as owner.")

@bot.command()
@is_owner()
async def removeowner(ctx, member: discord.Member):
    if ctx.author.id != super_owner_id or member.id == super_owner_id:
        return await ctx.send("Only super owner can remove owners except super owner.")
    owner_ids.discard(member.id)
    await ctx.send(f"Removed {member.mention} from owners.")

@bot.command()
@is_owner()
async def clearowners(ctx):
    if ctx.author.id != super_owner_id:
        return await ctx.send("Only super owner can clear owners.")
    owner_ids.clear()
    owner_ids.add(super_owner_id)
    await ctx.send("Cleared all owners except super owner.")

@bot.command()
@is_owner()
async def listowners(ctx):
    names = []
    for oid in owner_ids:
        member = ctx.guild.get_member(oid)
        if member:
            names.append(f"{member.display_name} ({member.name})")
    await ctx.send("Owners:\n" + ("\n".join(names) if names else "No owners found."))

@bot.command()
@is_owner()
async def listtargets(ctx):
    names = []
    for uid in watchlist:
        member = ctx.guild.get_member(uid)
        if member:
            names.append(f"{member.display_name} ({member.name})")
    await ctx.send("Watched Users:\n" + ("\n".join(names) if names else "No targets being watched."))

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
    seconds = int(duration[:-1]) * time_units.get(duration[-1], 0)
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
        await ctx.send(f"Congratulations {winner.mention} 🎉! You won **{prize}**!")
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
async def aban(ctx, member: discord.Member):
    autoban_ids.add(member.id)
    await ctx.send(f"{member.display_name} will be autobanned on join.")

@bot.command()
@is_owner()
async def raban(ctx, member: discord.Member):
    autoban_ids.discard(member.id)
    await ctx.send(f"{member.display_name} removed from autoban list.")

@bot.command()
@is_owner()
async def abanlist(ctx):
    if not autoban_ids:
        return await ctx.send("No autobanned users.")
    results = []
    for uid in autoban_ids:
        member = ctx.guild.get_member(uid)
        if member:
            results.append(f"{member.display_name} ({uid})")
        else:
            results.append(f"User ID: {uid}")
    await ctx.send("Autoban List:\n" + "\n".join(results))

@bot.event
async def on_member_join(member):
    if member.id in autoban_ids:
        try:
            await member.ban(reason="Autobanned by bot")
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
    now = datetime.datetime.utcnow()
    if alarm_time <= now:
        return await ctx.send("Date must be in the future.")
    delta = (alarm_time - now).total_seconds()
    await ctx.send(f"Alarm set for {date}.")
    await asyncio.sleep(delta)
    await ctx.send(f"⏰ Alarm: {date} reached!")

@bot.event
async def on_message(message):
    if message.author.id in watchlist and message.author.id != super_owner_id:
        try:
            await message.delete()
        except:
            pass
    await bot.process_commands(message)

keep_alive()
bot.run(os.getenv("DISCORD_TOKEN"))
