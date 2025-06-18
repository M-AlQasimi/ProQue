import discord
from discord.ext import commands, tasks
from flask import Flask
from threading import Thread
import asyncio
import re
import random
import datetime
import os
import requests

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

def is_owner():
    async def predicate(ctx):
        return ctx.author.id in owner_ids
    return commands.check(predicate)

app = Flask('')

@app.route('/')
def home():
    return "I'm alive", 200

def run():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

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
async def translate(ctx):
    if ctx.message.reference is None:
        await ctx.send("Reply to a message to translate.")
        return

    try:
        replied_msg = await ctx.channel.fetch_message(ctx.message.reference.message_id)
        text_to_translate = replied_msg.content
    except:
        await ctx.send("Couldn't get the replied message.")
        return

    try:
        detect_resp = requests.post("https://libretranslate.de/detect", data={"q": text_to_translate})
        detected_lang = detect_resp.json()[0].get("language", None)

        if not detected_lang:
            await ctx.send("Couldn't detect the language.")
            return

        if detected_lang == "en":
            await ctx.send("That’s already in English.")
            return

        trans_resp = requests.post("https://libretranslate.de/translate", data={
            "q": text_to_translate,
            "source": detected_lang,
            "target": "en"
        })

        translated_text = trans_resp.json().get("translatedText", None)
        if not translated_text:
            await ctx.send("Translation failed.")
            return

        await ctx.send(f"**Translated:** {translated_text}")

    except Exception as e:
        await ctx.send(f"Error: {str(e)}")

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
    if member is None:
        await ctx.channel.purge(limit=amount + 1)
    else:
        def check(m):
            return m.author == member
        deleted = await ctx.channel.purge(limit=1000, check=check)
        await ctx.send(f"Deleted {min(len(deleted), amount)} messages from {member.display_name}.", delete_after=5)

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
async def aban(ctx, user_id: int):
    autoban_ids.add(user_id)
    await ctx.send(f"User ID {user_id} will be autobanned if they join.")

@bot.command()
@is_owner()
async def aunban(ctx, user_id: int):
    autoban_ids.discard(user_id)
    await ctx.send(f"User ID {user_id} has been removed from autoban list.")

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

if __name__ == "__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
