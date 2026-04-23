import asyncio
import random
import os
import json
import sqlite3
import discord
from discord.ext import commands
from datetime import datetime, timezone
import threading
import psycopg2
from psycopg2.extras import RealDictCursor

# Database setup - PostgreSQL
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    """Create tables if they don't exist."""
    if not DATABASE_URL:
        print("⚠️ DATABASE_URL not set - economy system disabled")
        return False
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS economy (
                user_id BIGINT PRIMARY KEY,
                balance BIGINT DEFAULT 0,
                daily_streak INT DEFAULT 0,
                weekly_streak INT DEFAULT 0,
                monthly_streak INT DEFAULT 0,
                last_daily TIMESTAMP DEFAULT NULL,
                last_weekly TIMESTAMP DEFAULT NULL,
                last_monthly TIMESTAMP DEFAULT NULL,
                total_earned BIGINT DEFAULT 0,
                total_won BIGINT DEFAULT 0,
                total_lost BIGINT DEFAULT 0,
                steal_blacklist BIGINT[] DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Economy DB initialized (PostgreSQL)")
        return True
    except Exception as e:
        print(f"❌ Economy DB init failed: {e}")
        return False

db_ready = init_db()
lock = threading.Lock()

def get_user(user_id):
    """Get user data from DB, create if doesn't exist."""
    if not db_ready:
        return None
    
    with lock:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM economy WHERE user_id = %s", (user_id,))
        user = cur.fetchone()
        cur.close()
        conn.close()
    
    if not user:
        with lock:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("INSERT INTO economy (user_id, balance) VALUES (%s, 0) RETURNING *", (user_id,))
            conn.commit()
            user = cur.fetchone()
            cur.close()
            conn.close()
    
    return dict(user)

def update_user(user_id, **kwargs):
    """Update user data."""
    if not db_ready:
        return
    
    if not kwargs:
        return
    
    with lock:
        conn = get_db_connection()
        cur = conn.cursor()
        
        set_clause = ", ".join([f"{k} = %s" for k in kwargs.keys()])
        values = list(kwargs.values()) + [user_id]
        
        cur.execute(f"UPDATE economy SET {set_clause} WHERE user_id = %s", values)
        conn.commit()
        cur.close()
        conn.close()

# Currency symbol
CURRENCY_SYMBOL = "𝚀"

def format_balance(amount):
    return f"{amount:,} {CURRENCY_SYMBOL}"

def is_super_owner(user_id):
    return user_id == 885548126365171824

class EconomyCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
    
    @commands.command(name="pqbal", aliases=["pqbal", "bal", ".bal"])
    async def bal(self, ctx, member: discord.Member = None):
        if not db_ready:
            await ctx.send("❌ Economy system not configured.")
            return
        
        user = ctx.author if not member else member
        data = get_user(user.id)
        
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
    
    @commands.command(name="pqdaily", aliases=["pqdaily", "daily", ".daily"])
    async def daily(self, ctx):
        if not db_ready:
            await ctx.send("❌ Economy system not configured.")
            return
        
        user_id = ctx.author.id
        data = get_user(user_id)
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
        
        update_user(
            user_id,
            balance=data['balance'] + reward,
            daily_streak=streak,
            last_daily=now,
            total_earned=data['total_earned'] + reward
        )
        
        await ctx.send(f"🎉 You claimed **{format_balance(reward)}**!\nStreak: **{streak}** days (+{streak_bonus} bonus)")
    
    @commands.command(name="pqweekly", aliases=["pqweekly", "weekly", ".weekly"])
    async def weekly(self, ctx):
        if not db_ready:
            await ctx.send("❌ Economy system not configured.")
            return
        
        user_id = ctx.author.id
        data = get_user(user_id)
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
        
        update_user(
            user_id,
            balance=data['balance'] + reward,
            weekly_streak=streak,
            last_weekly=now,
            total_earned=data['total_earned'] + reward
        )
        
        await ctx.send(f"🎉 You claimed **{format_balance(reward)}**!\nWeekly streak: **{streak}** weeks (+{streak_bonus} bonus)")
    
    @commands.command(name="pqmonthly", aliases=["pqmonthly", "monthly", ".monthly"])
    async def monthly(self, ctx):
        if not db_ready:
            await ctx.send("❌ Economy system not configured.")
            return
        
        user_id = ctx.author.id
        data = get_user(user_id)
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
        
        update_user(
            user_id,
            balance=data['balance'] + reward,
            monthly_streak=streak,
            last_monthly=now,
            total_earned=data['total_earned'] + reward
        )
        
        await ctx.send(f"🎉 You claimed **{format_balance(reward)}**!\nMonthly streak: **{streak}** months (+{streak_bonus} bonus)")
    
    @commands.command(name="pqwork", aliases=["pqwork", "work", ".work"])
    async def work(self, ctx):
        if not db_ready:
            await ctx.send("❌ Economy system not configured.")
            return
        
        user_id = ctx.author.id
        data = get_user(user_id)
        now = datetime.now(timezone.utc)
        
        if data['last_daily']:
            last_work = data['last_daily'].replace(tzinfo=timezone.utc) if data['last_daily'].tzinfo is None else data['last_daily']
            elapsed = (now - last_work).total_seconds()
            
            if elapsed < 3600:
                minutes_left = int((3600 - elapsed) / 60)
                await ctx.send(f"⏰ You can work again in **{minutes_left}** minutes")
                return
        
        jobs = [
            ("delivered pizzas", random.randint(50, 150)),
            ("fixed bugs in code", random.randint(100, 300)),
            ("walked dogs", random.randint(30, 80)),
            ("tutored students", random.randint(150, 400)),
            ("flipped burgers", random.randint(40, 100)),
            ("did freelance work", random.randint(200, 500)),
        ]
        
        job, reward = random.choice(jobs)
        
        update_user(
            user_id,
            balance=data['balance'] + reward,
            last_daily=now,
            total_earned=data['total_earned'] + reward
        )
        
        await ctx.send(f"💼 You **{job}** and earned **{format_balance(reward)}**!")
    
    @commands.command(name="pqgamble", aliases=["pqgamble", "gamble", ".gamble"])
    async def gamble(self, ctx, amount: int):
        if not db_ready:
            await ctx.send("❌ Economy system not configured.")
            return
        
        if amount <= 0:
            await ctx.send("❌ Amount must be positive.")
            return
        
        user_id = ctx.author.id
        data = get_user(user_id)
        
        if amount > data['balance']:
            await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
            return
        
        if amount > 10000:
            await ctx.send("❌ Max gamble is 10,000 𝚀")
            return
        
        win = random.choice([True, False])
        
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
    
    @commands.command(name="pqroulette", aliases=["pqroulette", "roulette", ".roulette"])
    async def roulette(self, ctx, amount: int, color: str):
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
        data = get_user(user_id)
        
        if amount > data['balance']:
            await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
            return
        
        if amount > 5000:
            await ctx.send("❌ Max bet is 5,000 𝚀")
            return
        
        outcomes = ['red'] * 18 + ['black'] * 18 + ['green'] * 2
        result = random.choice(outcomes)
        
        multipliers = {'red': 2, 'black': 2, 'green': 10}
        
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
    
    @commands.command(name="pqslots", aliases=["pqslots", "slots", ".slots"])
    async def slots(self, ctx, amount: int):
        if not db_ready:
            await ctx.send("❌ Economy system not configured.")
            return
        
        if amount <= 0:
            await ctx.send("❌ Amount must be positive.")
            return
        
        user_id = ctx.author.id
        data = get_user(user_id)
        
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
    
    @commands.command(name="pqgive", aliases=["pqgive", "give", ".give"])
    async def give(self, ctx, member: discord.Member, amount: int):
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
        data = get_user(user_id)
        
        if amount > data['balance']:
            await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
            return
        
        update_user(user_id, balance=data['balance'] - amount)
        
        receiver_data = get_user(member.id)
        update_user(member.id, balance=receiver_data['balance'] + amount)
        
        await ctx.send(f"💸 You gave **{format_balance(amount)}** to **{member.name}**")
    
    @commands.command(name="pqleaderboard", aliases=["pqleaderboard", "lb", "pqlb", "leaderboard", ".lb", ".leaderboard"])
    @commands.alias("lb")
    async def leaderboard(self, ctx):
        if not db_ready:
            await ctx.send("❌ Economy system not configured.")
            return
        
        with lock:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT user_id, balance FROM economy ORDER BY balance DESC LIMIT 10")
            rows = cur.fetchall()
            cur.close()
            conn.close()
        
        embed = discord.Embed(
            title="🏆 Leaderboard",
            color=discord.Color.gold()
        )
        
        for i, row in enumerate(rows, 1):
            try:
                user = await self.bot.fetch_user(row['user_id'])
                name = user.name
            except:
                name = f"User {row['user_id']}"
            
            embed.add_field(
                name=f"{i}. {name}",
                value=format_balance(row['balance']),
                inline=False
            )
        
        await ctx.send(embed=embed)
    
    @commands.command(name="pqsteal", aliases=["pqsteal", "steal", ".steal"])
    async def steal(self, ctx, member: discord.Member):
        if not db_ready:
            await ctx.send("❌ Economy system not configured.")
            return
        
        if member.id == ctx.author.id:
            await ctx.send("❌ Can't steal from yourself.")
            return
        
        user_id = ctx.author.id
        data = get_user(user_id)
        target_data = get_user(member.id)
        
        blacklist = target_data['steal_blacklist'] or []
        if user_id in blacklist:
            await ctx.send("❌ You are blacklisted from this user.")
            return
        
        success = random.random() < 0.4
        
        if success:
            stolen = random.randint(50, min(500, target_data['balance']))
            update_user(user_id, balance=data['balance'] + stolen)
            update_user(member.id, balance=target_data['balance'] - stolen)
            await ctx.send(f"😈 You stole **{format_balance(stolen)}** from **{member.name}**!")
        else:
            blacklist = list(blacklist) if blacklist else []
            blacklist.append(user_id)
            update_user(member.id, steal_blacklist=blacklist)
            await ctx.send(f"💸 Failed! **{member.name}** caught you. Blacklisted.")
    
    @commands.command(name="pqadd", aliases=["pqadd", "add", ".add"])
    async def add(self, ctx, member: discord.Member, amount: int):
        if not is_super_owner(ctx.author.id):
            await ctx.send("❌ Bot owner only.")
            return
        
        if amount <= 0:
            await ctx.send("❌ Amount must be positive.")
            return
        
        target_data = get_user(member.id)
        update_user(
            member.id,
            balance=target_data['balance'] + amount,
            total_earned=target_data['total_earned'] + amount
        )
        
        await ctx.send(f"✅ Added **{format_balance(amount)}** to **{member.name}**")
    
    @commands.command(name="pqremove", aliases=["pqremove", "remove", ".remove"])
    async def remove(self, ctx, member: discord.Member, amount: int):
        if not is_super_owner(ctx.author.id):
            await ctx.send("❌ Bot owner only.")
            return
        
        if amount <= 0:
            await ctx.send("❌ Amount must be positive.")
            return
        
        target_data = get_user(member.id)
        new_balance = max(0, target_data['balance'] - amount)
        update_user(member.id, balance=new_balance)
        
        await ctx.send(f"✅ Removed **{format_balance(amount)}** from **{member.name}**")

def setup(bot):
    bot.add_cog(EconomyCog(bot))