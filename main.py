import discord
from discord.ext import commands, tasks
from flask import Flask
from threading import Thread
import asyncio
import re
import random
import datetime
import os

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
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    raise error

@bot.event
async def on_message_edit(before, after):
    if before.author.bot:
        return
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
async def test(ctx):
    await ctx.send("I'm alive heh")

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

from discord.ui import Button, View

ttt_games = {}
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
            await game["msg"].edit(content=f"🎉 {interaction.user.mention} wins!", view=game["view"])
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
        await interaction.response.edit_message(content="✅ Challenge accepted!", view=None)

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.danger)
    async def decline(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.opponent:
            return await interaction.response.send_message("You're not the challenged player.", ephemeral=True)
        self.declined = True
        self.stop()
        await interaction.response.edit_message(content="❌ Challenge declined.", view=None)

    async def on_timeout(self):
        if not self.accepted and not self.declined:
            await self.ctx.send(f"{self.opponent.mention} didn't respond in time. Game canceled.")

@bot.command()
async def ttt(ctx, opponent: discord.Member):
    if ctx.channel.id in ttt_games:
        return await ctx.send("A game is already in progress in this channel.")
    if opponent.bot or opponent == ctx.author:
        return await ctx.send("Choose a real opponent.")

    view = AcceptView(ctx, opponent)
    await ctx.send(f"{opponent.mention}, {ctx.author.mention} challenged you to a game of **Tic Tac Toe**.\nClick below to accept or decline:", view=view)
    await view.wait()

    if not view.accepted:
        return  

    board = [['⬜'] * 3 for _ in range(3)]
    game_view = TicTacToeView()
    msg = await ctx.send("🎮 Game started!", view=game_view)
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
            await game["msg"].edit(content=f"{current.mention}, it's your turn! ({time_left}s)", view=game["view"])
            await asyncio.sleep(1)
            time_left -= 1

        await game["msg"].edit(content=f"⏱️ {current.mention} took too long. Game over!", view=game["view"])
        await disable_all_buttons(game["view"])
        del ttt_games[channel.id]

    game["timeout_task"] = asyncio.create_task(countdown())

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
    await ctx.send(f"⏰ Alarm set for {date}, {ctx.author.mention}. I'll ping you then.")
    await asyncio.sleep(delta)
    await ctx.send(f"🔔 {ctx.author.mention} It's **{date}**! Here's your alarm.")

keep_alive()
bot.run(os.getenv("DISCORD_TOKEN"))
