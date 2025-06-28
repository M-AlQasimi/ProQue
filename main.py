import discord
from discord.ext import commands, tasks
from flask import Flask
from threading import Thread
from datetime import timezone
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
blacklisted_users = set()
edited_snipes = {}
deleted_snipes = {}
removed_reactions = {}

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
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
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
        if ctx.author.id in owner_ids:
            await ctx.send("Error.")
        else:
            await ctx.send("You can't use that heh")

@bot.event
async def on_message_delete(message):
    if message.author.bot or message.author.id == super_owner_id or (not message.content and not message.attachments):
        return
    content = message.content
    if message.attachments:
        content += "\n" + "\n".join([att.url for att in message.attachments])
    deleted_snipes.setdefault(message.channel.id, []).insert(0, (content, message.author, message.created_at))
    deleted_snipes[message.channel.id] = deleted_snipes[message.channel.id][:10]

@bot.event
async def on_message_edit(before, after):
    if before.author.bot:
        return
    if before.author.id != super_owner_id and before.content and after.content != before.content:
        edited_snipes.setdefault(before.channel.id, []).insert(0, (before.content, after.content, before.author, datetime.datetime.utcnow().replace(tzinfo=timezone.utc)))
        edited_snipes[before.channel.id] = edited_snipes[before.channel.id][:10]

@bot.event
async def on_reaction_remove(reaction, user):
    if user.bot or user.id == super_owner_id:
        return
    msg = reaction.message
    entry = (user, reaction.emoji, msg, datetime.datetime.utcnow().replace(tzinfo=timezone.utc))
    removed_reactions.setdefault(msg.channel.id, []).insert(0, entry)
    removed_reactions[msg.channel.id] = removed_reactions[msg.channel.id][:10]

@bot.check
async def block_blacklisted(ctx):
    return ctx.author.id not in blacklisted_users
        
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
                time_str = timestamp.strftime("%d %b %Y • %H:%M UTC")
                response += f"#{i} - Deleted by {author.display_name} at {time_str}:\n{content}\n\n"
            await ctx.send(response[:2000])
        else:
            n = int(index) - 1
            messages = deleted_snipes.get(ctx.channel.id, [])
            if not messages or n >= len(messages) or n < 0:
                return await ctx.send("Nothing to snipe.")
            content, author, timestamp = messages[n]
            time_str = timestamp.strftime("%d %b %Y • %H:%M UTC")
            await ctx.send(f"Deleted by {author.mention} at {time_str}:\n{content}", allowed_mentions=discord.AllowedMentions.none())
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
            for i, (before, after, author, timestamp) in enumerate(selected, 1):
                time_str = timestamp.strftime("%d %b %Y • %H:%M UTC")
                response += f"#{i} - Edited by {author.display_name} at {time_str}:\n**Before:** {before}\n**After:** {after}\n\n"
            await ctx.send(response[:2000])
        else:
            n = int(index) - 1
            if not messages or n >= len(messages) or n < 0:
                return await ctx.send("Nothing to snipe.")
            before, after, author, timestamp = messages[n]
            time_str = timestamp.strftime("%d %b %Y • %H:%M UTC")
            await ctx.send(
                f"Edited by {author.mention} at {time_str}:\n**Before:** {before}\n**After:** {after}",
                allowed_mentions=discord.AllowedMentions.none()
            )
    except:
        await ctx.send("Invalid index. Use a number like `.esnipe 2` or `.esnipe -3`.")

@bot.command()
async def rsnipe(ctx, index: str = "1"):
    try:
        if index.startswith("-"):
            count = int(index[1:])
            logs = removed_reactions.get(ctx.channel.id, [])
            if not logs or count < 1:
                return await ctx.send("Nothing to snipe.")
            selected = logs[:count]
            response = ""
            for i, (user, emoji, msg, timestamp) in enumerate(selected, 1):
                time_str = timestamp.strftime("%d %b %Y • %H:%M UTC")
                response += f"#{i} - {user.display_name} removed {emoji} from [this message]({msg.jump_url}) at {time_str}.\n\n"
            await ctx.send(response[:2000])
        else:
            n = int(index) - 1
            logs = removed_reactions.get(ctx.channel.id, [])
            if not logs or n >= len(logs) or n < 0:
                return await ctx.send("Nothing to snipe.")
            user, emoji, msg, timestamp = logs[n]
            time_str = timestamp.strftime("%d %b %Y • %H:%M UTC")
            await ctx.send(f"{user.display_name} removed {emoji} from [this message]({msg.jump_url}) at {time_str}.")
    except:
        await ctx.send("Invalid index. Use a number like `.rsnipe 2` or `.rsnipe -3`.")

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
async def shut(ctx, member: discord.Member):
    if member.id == super_owner_id:
        return
    watchlist.add(member.id)

@bot.command()
@is_owner()
async def unshut(ctx, member: discord.Member):
    if member.id in owner_ids and ctx.author.id != super_owner_id:
        return await ctx.send("Only Que can stop watching owners.")
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
            names.append(f"**{member.display_name}** ({member.name})")
    await ctx.send("Targets:\n" + ("\n".join(names) if names else "No targets being watched."))

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
    await ctx.send(f"**{member.display_name}** has been muted for {duration}.")

@bot.command()
@is_owner()
async def ban(ctx, user: discord.User, *, reason=None):
    await ctx.guild.ban(user, reason=reason)
    await ctx.send(f"**{user.display_name if hasattr(user, 'display_name') else user.name}** has been banned.")

@bot.command()
@is_owner()
async def unban(ctx, *, user: str):
    try:
        name, id_or_discriminator = None, None

        if user.isdigit():
            user_obj = await bot.fetch_user(int(user))
        elif "#" in user:
            name, discriminator = user.split("#")
            bans = await ctx.guild.bans()
            user_obj = next((ban.user for ban in bans if ban.user.name == name and ban.user.discriminator == discriminator), None)
        else:
            await ctx.send("Please mention the user or provide their ID.")
            return

        if not user_obj:
            await ctx.send("User not found in ban list.")
            return

        await ctx.guild.unban(user_obj)
        await ctx.send(f"**{user_obj}** has been unbanned.")
    except Exception as e:
        await ctx.send("Failed to unban user.")

@bot.command()
async def listbans(ctx):
    bans = await ctx.guild.bans()
    if not bans:
        return await ctx.send("No banned users in this server.")
    
    ban_list = [f"**{ban.user}** (ID: {ban.user.id})" for ban in bans]
    msg = "\n".join(ban_list)
    
    if len(msg) > 2000:
        await ctx.send("Too many banned users to display.")
    else:
        await ctx.send(f"**Banned Users:**\n{msg}")

@bot.command()
@is_owner()
async def kick(ctx, member: discord.Member, *, reason=None):
    await member.kick(reason=reason)
    await ctx.send(f"**{member.display_name}** has been kicked.")

@bot.command()
@is_owner()
async def addrole(ctx, member: discord.Member, role: discord.Role):
    await member.add_roles(role)
    await ctx.send(f"Added {role.name} to **{member.display_name}**.")

@bot.command()
@is_owner()
async def removerole(ctx, member: discord.Member, role: discord.Role):
    await member.remove_roles(role)
    await ctx.send(f"Removed {role.name} from **{member.display_name}**.")

@bot.command()
@is_owner()
async def speak(ctx, *, msg):
    await ctx.message.delete()
    await ctx.send(msg)

@bot.command()
async def poll(ctx, *, question):
    msg = await ctx.send(f"**{question}**\nYes ✔️ | No ✖️")
    await msg.add_reaction("✔️")
    await msg.add_reaction("✖️")

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
async def aban(ctx, target):
    try:
        user = await commands.UserConverter().convert(ctx, target)
        autoban_ids.add(user.id)
        await ctx.send(f"**{user.display_name}** added to the autoban list.")
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
        await ctx.send(f"**{user.display_name}** removed from autoban list.")
    except:
        try:
            user_id = int(target)
            autoban_ids.discard(user_id)
            await ctx.send(f"User ID `{user_id}` removed from autoban list.")
        except:
            await ctx.send("Invalid user or ID.")

@bot.command()
@is_owner()
async def abanlist(ctx):
    if not autoban_ids:
        return await ctx.send("No autobanned users.")
    results = []
    for uid in autoban_ids:
        member = ctx.guild.get_member(uid)
        if member:
            results.append(f"**{member.display_name}** ({uid})")
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
    await ctx.send(f"✖️ **{member.display_name}** is now blocked from using commands.")

@bot.command()
@is_owner()
async def unblock(ctx, member: discord.Member):
    blacklisted_users.discard(member.id)
    await ctx.send(f"✔️ **{member.display_name}** is now unblocked.")

@bot.command()
@is_owner()
async def blocked(ctx):
    if not blacklisted_users:
        return await ctx.send("No one is blocked.")
    users = []
    for uid in blacklisted_users:
        user = ctx.guild.get_member(uid)
        users.append(f"**{user.display_name}**" if user else f"User ID: `{uid}`")
    await ctx.send("Blocked users:\n" + "\n".join(users))

keep_alive()
bot.run(os.getenv("DISCORD_TOKEN"))
