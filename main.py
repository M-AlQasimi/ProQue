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
    print("Heartbeats")

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
async def dsnipe(ctx):
    data = deleted_snipes.get(ctx.channel.id)
    if not data:
        return await ctx.send("Nothing to snipe.")
    content, author = data
    await ctx.send(f"Deleted by {author.display_name}: `{content}`")

@bot.command()
async def esnipe(ctx):
    data = edited_snipes.get(ctx.channel.id)
    if not data:
        return await ctx.send("Nothing to snipe.")
    before, after, author = data
    await ctx.send(f"Edited by {author.display_name}:\nBefore: `{before}`\nAfter: `{after}`")

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

async def ai_generate(prompt):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=50,
            temperature=1.2
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Failed to generate: {e}"

@bot.command()
async def quote(ctx):
    text = await ai_generate("Give me a short original inspirational quote.")
    await ctx.send(f"💬 {text}")

@bot.command()
async def roast(ctx):
    text = await ai_generate("Give me a short original funny roast.")
    await ctx.send(f"🔥 {text}")

truths = [
    "What's the most embarrassing thing you've ever done?",
    "What's your biggest fear?",
    "What's a secret you've never told anyone?"
]

dares = [
    "Say the alphabet backward.",
    "Send a funny selfie in chat.",
    "Speak in an accent for 1 minute."
]

@bot.command()
async def truth(ctx):
    await ctx.send(f"🧐 Truth: {random.choice(truths)}")

@bot.command()
async def dare(ctx):
    await ctx.send(f"😈 Dare: {random.choice(dares)}")

@bot.command()
async def define(ctx, *, word: str):
    definition = await ai_generate(f"Define the word: {word}")
    await ctx.send(f"📘 {word.capitalize()}: {definition}")

@bot.command()
async def slowmode(ctx, seconds: int = 0):
    await ctx.channel.edit(slowmode_delay=seconds)
    if seconds == 0:
        await ctx.send("Slowmode disabled.")
    else:
        await ctx.send(f"Slowmode set to {seconds} seconds.")

hangman_words = {
    "1": ["dog", "hat", "sun", "cup", "map"],
    "2": ["python", "rocket", "planet", "winter"],
    "3": ["asteroid", "universe", "electricity", "mysterious"]
}

active_games = {}

@bot.command()
async def hangman(ctx, level: str = "1"):
    if level not in hangman_words:
        return await ctx.send("Choose level 1, 2, or 3.")
    word = random.choice(hangman_words[level])
    display = ["_" for _ in word]
    tries = 6
    guessed = set()

    await ctx.send(f"🎮 Hangman (Level {level}) started!\n`{' '.join(display)}`\nYou have {tries} tries.")

    def check(m):
        return m.channel == ctx.channel and m.author == ctx.author and len(m.content) == 1 and m.content.isalpha()

    while "_" in display and tries > 0:
        try:
            msg = await bot.wait_for("message", check=check, timeout=60.0)
        except asyncio.TimeoutError:
            return await ctx.send("⏰ Time's up!")

        guess = msg.content.lower()
        if guess in guessed:
            await ctx.send("Already guessed.")
            continue
        guessed.add(guess)

        if guess in word:
            for i, c in enumerate(word):
                if c == guess:
                    display[i] = guess
            await ctx.send(f"✅ Correct!\n`{' '.join(display)}`")
        else:
            tries -= 1
            await ctx.send(f"❌ Wrong. {tries} tries left.\n`{' '.join(display)}`")

    if "_" not in display:
        await ctx.send(f"🎉 You won! The word was `{word}`.")
    else:
        await ctx.send(f"💀 Game over. The word was `{word}`.")

games = {}

@bot.command()
async def ttt(ctx, opponent: discord.Member):
    if ctx.channel.id in games:
        return await ctx.send("A game is already in progress here.")

    board = [":white_large_square:"] * 9
    players = [ctx.author, opponent]
    turn = 0

    def format_board():
        return "\n".join(["".join(board[i:i+3]) for i in range(0, 9, 3)])

    await ctx.send(f"Tic Tac Toe started!\n{players[0].mention} vs {players[1].mention}")
    await ctx.send(format_board())

    def check(m):
        return m.channel == ctx.channel and m.author == players[turn] and m.content.isdigit() and 1 <= int(m.content) <= 9

    while True:
        await ctx.send(f"{players[turn].mention}'s turn (1-9):")
        try:
            msg = await bot.wait_for("message", check=check, timeout=60.0)
        except asyncio.TimeoutError:
            await ctx.send("Game ended due to inactivity.")
            del games[ctx.channel.id]
            return

        pos = int(msg.content) - 1
        if board[pos] != ":white_large_square:":
            await ctx.send("Spot already taken.")
            continue

        board[pos] = ":regional_indicator_x:" if turn == 0 else ":o2:"
        await ctx.send(format_board())

        win_pos = [(0,1,2),(3,4,5),(6,7,8),(0,3,6),(1,4,7),(2,5,8),(0,4,8),(2,4,6)]
        for a,b,c in win_pos:
            if board[a] == board[b] == board[c] and board[a] != ":white_large_square:":
                await ctx.send(f"{players[turn].mention} wins!")
                del games[ctx.channel.id]
                return

        if ":white_large_square:" not in board:
            await ctx.send("It's a draw!")
            del games[ctx.channel.id]
            return

        turn = 1 - turn

keep_alive()
bot.run(os.getenv("DISCORD_TOKEN"))
