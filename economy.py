import asyncio
import random
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone
import discord
from discord.ext import commands

db_ready = False

# Bot reference - set in setup()
bot = None

def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor, connect_timeout=5)

def init_db():
    global db_ready
    if not os.getenv("DATABASE_URL"):
        print("⚠️ DATABASE_URL not set - economy system disabled")
        return

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS economy (
                user_id BIGINT PRIMARY KEY,
                balance BIGINT DEFAULT 0,
                daily_streak INTEGER DEFAULT 0,
                weekly_streak INTEGER DEFAULT 0,
                monthly_streak INTEGER DEFAULT 0,
                last_daily TIMESTAMP,
                last_weekly TIMESTAMP,
                last_monthly TIMESTAMP,
                total_earned BIGINT DEFAULT 0,
                total_won BIGINT DEFAULT 0,
                total_lost BIGINT DEFAULT 0,
                steal_blacklist BIGINT[] DEFAULT '{}'
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        db_ready = True
        print("✅ Economy DB initialized (PostgreSQL)")
    except Exception as e:
        print(f"❌ Economy DB init failed: {e}")
        db_ready = False

def get_user(user_id):
    for attempt in range(3):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM economy WHERE user_id = %s", (user_id,))
            user = cur.fetchone()
            cur.close()
            conn.close()

            if user is not None:
                return user

            # User doesn't exist — create
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO economy (user_id, balance) VALUES (%s, 0) ON CONFLICT (user_id) DO NOTHING RETURNING *",
                (user_id,)
            )
            user = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()
            return user
        except psycopg2.OperationalError:
            if attempt < 2:
                continue
            raise

def update_user(user_id, **kwargs):
    for attempt in range(3):
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            set_clauses = []
            values = []
            for key, value in kwargs.items():
                set_clauses.append(f"{key} = %s")
                values.append(value)

            values.append(user_id)
            query = f"UPDATE economy SET {', '.join(set_clauses)} WHERE user_id = %s"
            cur.execute(query, values)
            conn.commit()
            cur.close()
            conn.close()
            return
        except psycopg2.OperationalError:
            if attempt < 2:
                continue
            raise

def format_balance(amount):
    return f"{amount:,} 𝚀"

def is_super_owner(user_id):
    return user_id == 885548126365171824

# Individual commands - accessible as .bal, .daily, etc.
@commands.command()
async def bal(ctx, member: discord.Member = None):
    if not db_ready:
        await ctx.send("❌ Economy system not configured.")
        return

    user = ctx.author if not member else member
    try:
        data = get_user(user.id)
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    embed = discord.Embed(
        title=f"{user.name}'s Balance",
        color=discord.Color.gold()
    )
    embed.add_field(name="Balance", value=format_balance(data['balance']), inline=False)
    embed.add_field(name="Daily Streak", value=f"{data['daily_streak']} days", inline=True)
    embed.add_field(name="Weekly Streak", value=f"{data['weekly_streak']} weeks", inline=True)
    embed.add_field(name="Monthly Streak", value=f"{data['monthly_streak']} months", inline=True)
    embed.add_field(name="Total Earned", value=format_balance(data['total_earned']), inline=True)
    embed.add_field(name="Total Won", value=format_balance(data['total_won']), inline=True)
    embed.add_field(name="Total Lost", value=format_balance(data['total_lost']), inline=True)

    await ctx.send(embed=embed)

@commands.command()
async def daily(ctx):
    if not db_ready:
        await ctx.send("❌ Economy system not configured.")
        return

    user_id = ctx.author.id
    try:
        data = get_user(user_id)
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    now = datetime.now(timezone.utc)

    if data['last_daily']:
        last_daily = data['last_daily'].replace(tzinfo=timezone.utc) if data['last_daily'].tzinfo is None else data['last_daily']
        elapsed = (now - last_daily).total_seconds()

        if elapsed < 86400:
            hours_left = int((86400 - elapsed) / 3600)
            minutes_left = int(((86400 - elapsed) % 3600) / 60)
            await ctx.send(f"⏰ You can claim daily in **{hours_left}h {minutes_left}m**")
            return

    streak = data['daily_streak'] + 1
    base_reward = random.randint(100, 500)
    streak_bonus = min(streak * 10, 200)
    reward = base_reward + streak_bonus

    try:
        update_user(
            user_id,
            balance=data['balance'] + reward,
            daily_streak=streak,
            last_daily=now,
            total_earned=data['total_earned'] + reward
        )
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    await ctx.send(f"🎉 You claimed **{format_balance(reward)}**!\nStreak: **{streak}** days (+{streak_bonus} bonus)")

@commands.command()
async def weekly(ctx):
    if not db_ready:
        await ctx.send("❌ Economy system not configured.")
        return

    user_id = ctx.author.id
    try:
        data = get_user(user_id)
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    now = datetime.now(timezone.utc)

    if data['last_weekly']:
        last_weekly = data['last_weekly'].replace(tzinfo=timezone.utc) if data['last_weekly'].tzinfo is None else data['last_weekly']
        elapsed = (now - last_weekly).total_seconds()

        if elapsed < 604800:
            days_left = int((604800 - elapsed) / 86400)
            await ctx.send(f"⏰ You can claim weekly in **{days_left}** days")
            return

    streak = data['weekly_streak'] + 1
    base_reward = random.randint(1000, 3000)
    streak_bonus = min(streak * 50, 500)
    reward = base_reward + streak_bonus

    try:
        update_user(
            user_id,
            balance=data['balance'] + reward,
            weekly_streak=streak,
            last_weekly=now,
            total_earned=data['total_earned'] + reward
        )
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    await ctx.send(f"🎉 You claimed **{format_balance(reward)}**!\nWeekly streak: **{streak}** weeks (+{streak_bonus} bonus)")

@commands.command()
async def monthly(ctx):
    if not db_ready:
        await ctx.send("❌ Economy system not configured.")
        return

    user_id = ctx.author.id
    try:
        data = get_user(user_id)
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    now = datetime.now(timezone.utc)

    if data['last_monthly']:
        last_monthly = data['last_monthly'].replace(tzinfo=timezone.utc) if data['last_monthly'].tzinfo is None else data['last_monthly']
        elapsed = (now - last_monthly).total_seconds()

        if elapsed < 2592000:
            days_left = int((2592000 - elapsed) / 86400)
            await ctx.send(f"⏰ You can claim monthly in **{days_left}** days")
            return

    streak = data['monthly_streak'] + 1
    base_reward = random.randint(10000, 25000)
    streak_bonus = min(streak * 500, 5000)
    reward = base_reward + streak_bonus

    try:
        update_user(
            user_id,
            balance=data['balance'] + reward,
            monthly_streak=streak,
            last_monthly=now,
            total_earned=data['total_earned'] + reward
        )
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    await ctx.send(f"🎉 You claimed **{format_balance(reward)}**!\nMonthly streak: **{streak}** months (+{streak_bonus} bonus)")

@commands.command()
async def gamble(ctx, amount: int):
    if not db_ready:
        await ctx.send("❌ Economy system not configured.")
        return

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    user_id = ctx.author.id
    try:
        data = get_user(user_id)
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    if amount > data['balance']:
        await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
        return

    if amount > 10000:
        await ctx.send("❌ Max gamble is 10,000 𝚀")
        return

    win = random.choice([True, False])

    try:
        if win:
            update_user(
                user_id,
                balance=data['balance'] + amount,
                total_won=data['total_won'] + amount
            )
            await ctx.send(f"🎉 You won! **{format_balance(amount)}** → **{format_balance(data['balance'] + amount)}**")
        else:
            update_user(
                user_id,
                balance=data['balance'] - amount,
                total_lost=data['total_lost'] + amount
            )
            await ctx.send(f"💸 You lost... **{format_balance(amount)}** → **{format_balance(data['balance'] - amount)}**")
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

@commands.command()
async def roulette(ctx, amount: int, color: str):
    if not db_ready:
        await ctx.send("❌ Economy system not configured.")
        return

    color = color.lower()
    if color not in ['red', 'black', 'green']:
        await ctx.send("❌ Use: `.roulette <amount> <red|black|green>`")
        return

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    user_id = ctx.author.id
    try:
        data = get_user(user_id)
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    if amount > data['balance']:
        await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
        return

    if amount > 5000:
        await ctx.send("❌ Max bet is 5,000 𝚀")
        return

    outcomes = ['red'] * 18 + ['black'] * 18 + ['green'] * 2
    result = random.choice(outcomes)

    multipliers = {'red': 2, 'black': 2, 'green': 10}

    try:
        if result == color:
            winnings = amount * multipliers[color] - amount
            update_user(
                user_id,
                balance=data['balance'] + winnings,
                total_won=data['total_won'] + winnings
            )
            await ctx.send(f"🎉 **{color.upper()}**! You won **{format_balance(winnings)}**!")
        else:
            update_user(
                user_id,
                balance=data['balance'] - amount,
                total_lost=data['total_lost'] + amount
            )
            await ctx.send(f"💸 It was **{result.upper()}**. You lost **{format_balance(amount)}**")
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

@commands.command()
async def slots(ctx, amount: int):
    if not db_ready:
        await ctx.send("❌ Economy system not configured.")
        return

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    user_id = ctx.author.id
    try:
        data = get_user(user_id)
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    if amount > data['balance']:
        await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
        return

    if amount > 5000:
        await ctx.send("❌ Max bet is 5,000 𝚀")
        return

    emojis = ['🍒', '🍋', '🍊', '🍇', '💎', '⭐']
    weights = [30, 25, 20, 15, 8, 2]

    reel1 = random.choices(emojis, weights=weights)[0]
    reel2 = random.choices(emojis, weights=weights)[0]
    reel3 = random.choices(emojis, weights=weights)[0]

    result = f"{reel1} {reel2} {reel3}"

    try:
        if reel1 == reel2 == reel3:
            multiplier = (emojis.index(reel1) + 1) * 3
            winnings = amount * multiplier
            update_user(
                user_id,
                balance=data['balance'] + winnings,
                total_won=data['total_won'] + winnings
            )
            await ctx.send(f"🎰 {result}\n🎉 JACKPOT! **{format_balance(winnings)}**!")
        elif reel1 == reel2 or reel2 == reel3 or reel1 == reel3:
            winnings = amount
            update_user(
                user_id,
                balance=data['balance'] + winnings,
                total_won=data['total_won'] + winnings
            )
            await ctx.send(f"🎰 {result}\n✨ Small win! **{format_balance(winnings)}**!")
        else:
            update_user(
                user_id,
                balance=data['balance'] - amount,
                total_lost=data['total_lost'] + amount
            )
            await ctx.send(f"🎰 {result}\n💸 No luck this time...")
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

@commands.command()
async def give(ctx, member: discord.Member, amount: int):
    if not db_ready:
        await ctx.send("❌ Economy system not configured.")
        return

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    if member.id == ctx.author.id:
        await ctx.send("❌ Can't transfer to yourself.")
        return

    user_id = ctx.author.id
    try:
        data = get_user(user_id)
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    if amount > data['balance']:
        await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
        return

    try:
        update_user(user_id, balance=data['balance'] - amount)
        receiver_data = get_user(member.id)
        update_user(member.id, balance=receiver_data['balance'] + amount)
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    await ctx.send(f"💸 You gave **{format_balance(amount)}** to **{member.name}**")

@commands.command(name="lb")
async def lb(ctx):
    if not db_ready:
        await ctx.send("❌ Economy system not configured.")
        return

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT user_id, balance FROM economy ORDER BY balance DESC LIMIT 10")
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    embed = discord.Embed(
        title="🏆 Leaderboard",
        color=discord.Color.gold()
    )

    for i, row in enumerate(rows, 1):
        try:
            user = await ctx.bot.fetch_user(row['user_id'])
            name = user.name
        except:
            name = f"User {row['user_id']}"

        embed.add_field(
            name=f"{i}. {name}",
            value=format_balance(row['balance']),
            inline=False
        )

    await ctx.send(embed=embed)

@commands.command()
async def add(ctx, member: discord.Member, amount: int):
    if not is_super_owner(ctx.author.id):
        await ctx.send("❌ Bot owner only.")
        return

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    try:
        target_data = get_user(member.id)
        update_user(
            member.id,
            balance=target_data['balance'] + amount,
            total_earned=target_data['total_earned'] + amount
        )
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    await ctx.send(f"✅ Added **{format_balance(amount)}** to **{member.name}**")

@commands.command()
async def remove(ctx, member: discord.Member, amount: int):
    if not is_super_owner(ctx.author.id):
        await ctx.send("❌ Bot owner only.")
        return

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    try:
        target_data = get_user(member.id)
        new_balance = max(0, target_data['balance'] - amount)
        update_user(member.id, balance=new_balance)
    except Exception:
        await ctx.send("❌ Database error. Try again in a moment.")
        return

    await ctx.send(f"✅ Removed **{format_balance(amount)}** from **{member.name}**")

async def setup(bot_ref):
    """Called when the cog is loaded"""
    global bot
    bot = bot_ref
    print("Initializing economy system...")
    init_db()
    print(f"Economy db_ready = {db_ready}")

    # Register all economy commands
    bot.add_command(bal)
    bot.add_command(daily)
    bot.add_command(weekly)
    bot.add_command(monthly)
    bot.add_command(gamble)
    bot.add_command(roulette)
    bot.add_command(slots)
    bot.add_command(give)
    bot.add_command(lb)
    bot.add_command(add)
    bot.add_command(remove)
