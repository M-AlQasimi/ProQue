import asyncio
import random
from datetime import datetime, timezone
import discord
from discord.ext import commands
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Database setup
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
        print("✅ Economy DB initialized")
        return True
    except Exception as e:
        print(f"❌ Economy DB init failed: {e}")
        return False

db_ready = init_db()

def get_user(user_id):
    """Get user data from DB, create if doesn't exist."""
    if not db_ready:
        return None
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM economy WHERE user_id = %s", (user_id,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    
    if not user:
        # Create new user
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO economy (user_id, balance) VALUES (%s, 0) RETURNING *", (user_id,))
        conn.commit()
        user = cur.fetchone()
        cur.close()
        conn.close()
    
    return user

def update_user(user_id, **kwargs):
    """Update user data."""
    if not db_ready:
        return
    
    if not kwargs:
        return
    
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
    """Format balance with 𝚀 symbol."""
    return f"{amount:,} {CURRENCY_SYMBOL}"

def is_super_owner(user_id):
    return user_id == 885548126365171824

class EconomyCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
    
    @commands.command(name="bal")
    async def bal(self, ctx, member: discord.Member = None):
        """Check balance."""
        if not db_ready:
            await ctx.send("❌ Economy system is not configured.")
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
    
    @commands.command(name="daily")
    async def daily(self, ctx):
        """Daily reward."""
        if not db_ready:
            await ctx.send("❌ Economy system is not configured.")
            return
        
        user_id = ctx.author.id
        data = get_user(user_id)
        now = datetime.now(timezone.utc)
        
        if data['last_daily']:
            last_daily = data['last_daily'].replace(tzinfo=timezone.utc) if data['last_daily'].tzinfo is None else data['last_daily']
            elapsed = (now - last_daily).total_seconds()
            
            if elapsed < 86400:  # 24 hours
                hours_left = int((86400 - elapsed) / 3600)
                minutes_left = int(((86400 - elapsed) % 3600) / 60)
                await ctx.send(f"⏰ You can claim your daily in **{hours_left}h {minutes_left}m**")
                return
        
        # Calculate reward with streak bonus
        streak = data['daily_streak'] + 1
        base_reward = random.randint(100, 500)
        streak_bonus = min(streak * 10, 200)  # Max 200 bonus
        reward = base_reward + streak_bonus
        
        # Update
        update_user(
            user_id,
            balance=data['balance'] + reward,
            daily_streak=streak,
            last_daily=now,
            total_earned=data['total_earned'] + reward
        )
        
        await ctx.send(f"🎉 You claimed **{format_balance(reward)}**!\nStreak: **{streak}** days (+{streak_bonus} bonus)")
    
    @commands.command(name="weekly")
    async def weekly(self, ctx):
        """Weekly reward."""
        if not db_ready:
            await ctx.send("❌ Economy system is not configured.")
            return
        
        user_id = ctx.author.id
        data = get_user(user_id)
        now = datetime.now(timezone.utc)
        
        if data['last_weekly']:
            last_weekly = data['last_weekly'].replace(tzinfo=timezone.utc) if data['last_weekly'].tzinfo is None else data['last_weekly']
            elapsed = (now - last_weekly).total_seconds()
            
            if elapsed < 604800:  # 7 days
                days_left = int((604800 - elapsed) / 86400)
                await ctx.send(f"⏰ You can claim your weekly in **{days_left}** days")
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
    
    @commands.command(name="monthly")
    async def monthly(self, ctx):
        """Monthly reward."""
        if not db_ready:
            await ctx.send("❌ Economy system is not configured.")
            return
        
        user_id = ctx.author.id
        data = get_user(user_id)
        now = datetime.now(timezone.utc)
        
        if data['last_monthly']:
            last_monthly = data['last_monthly'].replace(tzinfo=timezone.utc) if data['last_monthly'].tzinfo is None else data['last_monthly']
            elapsed = (now - last_monthly).total_seconds()
            
            if elapsed < 2592000:  # 30 days
                days_left = int((2592000 - elapsed) / 86400)
                await ctx.send(f"⏰ You can claim your monthly in **{days_left}** days")
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
    
    @commands.command(name="work")
    async def work(self, ctx):
        """Work for money (1hr cooldown)."""
        if not db_ready:
            await ctx.send("❌ Economy system is not configured.")
            return
        
        user_id = ctx.author.id
        data = get_user(user_id)
        now = datetime.now(timezone.utc)
        
        if data['last_daily']:
            last_work = data['last_daily'].replace(tzinfo=timezone.utc) if data['last_daily'].tzinfo is None else data['last_daily']
            elapsed = (now - last_work).total_seconds()
            
            if elapsed < 3600:  # 1 hour
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
            last_daily=now,  # Reuse daily timestamp for work cooldown
            total_earned=data['total_earned'] + reward
        )
        
        await ctx.send(f"💼 You **{job}** and earned **{format_balance(reward)}**!")
    
    @commands.command(name="gamble")
    async def gamble(self, ctx, amount: int):
        """50/50 gamble."""
        if not db_ready:
            await ctx.send("❌ Economy system is not configured.")
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
    
    @commands.command(name="roulette")
    async def roulette(self, ctx, amount: int, color: str):
        """Roulette: red (2x), black (2x), green (10x)."""
        if not db_ready:
            await ctx.send("❌ Economy system is not configured.")
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
        
        # Roulette outcomes
        outcomes = ['red'] * 18 + ['black'] * 18 + ['green'] * 2
        result = random.choice(outcomes)
        
        multipliers = {'red': 2, 'black': 2, 'green': 10}
        
        if result == color:
            winnings = amount * multipliers[color] - amount  # Net profit
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
    
    @commands.command(name="slots")
    async def slots(self, ctx, amount: int):
        """Slot machine."""
        if not db_ready:
            await ctx.send("❌ Economy system is not configured.")
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
        weights = [30, 25, 20, 15, 8, 2]  # Rarer = higher multiplier
        
        reel1 = random.choices(emojis, weights=weights)[0]
        reel2 = random.choices(emojis, weights=weights)[0]
        reel3 = random.choices(emojis, weights=weights)[0]
        
        result = f"{reel1} {reel2} {reel3}"
        
        # Calculate winnings
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
    
    @commands.command(name="give")
    async def give(self, ctx, member: discord.Member, amount: int):
        """Transfer Quesos to another user."""
        if not db_ready:
            await ctx.send("❌ Economy system is not configured.")
            return
        
        if amount <= 0:
            await ctx.send("❌ Amount must be positive.")
            return
        
        if member.id == ctx.author.id:
            await ctx.send("❌ You can't transfer to yourself.")
            return
        
        user_id = ctx.author.id
        data = get_user(user_id)
        
        if amount > data['balance']:
            await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
            return
        
        # Deduct from sender
        update_user(user_id, balance=data['balance'] - amount)
        
        # Add to receiver
        receiver_data = get_user(member.id)
        update_user(member.id, balance=receiver_data['balance'] + amount)
        
        await ctx.send(f"💸 You gave **{format_balance(amount)}** to **{member.name}**")
    
    @commands.command(name="leaderboard")
    @commands.alias("lb")
    async def leaderboard(self, ctx):
        """Top Quesos holders."""
        if not db_ready:
            await ctx.send("❌ Economy system is not configured.")
            return
        
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
    
    @commands.command(name="steal")
    async def steal(self, ctx, member: discord.Member):
        """Attempt to steal Quesos (risky!)."""
        if not db_ready:
            await ctx.send("❌ Economy system is not configured.")
            return
        
        if member.id == ctx.author.id:
            await ctx.send("❌ You can't steal from yourself.")
            return
        
        user_id = ctx.author.id
        data = get_user(user_id)
        target_data = get_user(member.id)
        
        # Check if blacklisted
        if target_data['steal_blacklist'] and user_id in target_data['steal_blacklist']:
            await ctx.send("❌ You are blacklisted from stealing from this user.")
            return
        
        # 40% chance success
        success = random.random() < 0.4
        
        if success:
            stolen = random.randint(50, min(500, target_data['balance']))
            update_user(user_id, balance=data['balance'] + stolen)
            update_user(member.id, balance=target_data['balance'] - stolen)
            await ctx.send(f"😈 You stole **{format_balance(stolen)}** from **{member.name}**!")
        else:
            # Add to blacklist
            blacklist = list(target_data['steal_blacklist']) if target_data['steal_blacklist'] else []
            blacklist.append(user_id)
            update_user(member.id, steal_blacklist=blacklist)
            await ctx.send(f"💸 Failed! **{member.name}** caught you. You're blacklisted from stealing from them.")
    
    @commands.command(name="add")
    async def add(self, ctx, member: discord.Member, amount: int):
        """Add Quesos (super owner only)."""
        if not is_super_owner(ctx.author.id):
            await ctx.send("❌ This command is for the bot owner only.")
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
    
    @commands.command(name="remove")
    async def remove(self, ctx, member: discord.Member, amount: int):
        """Remove Quesos (super owner only)."""
        if not is_super_owner(ctx.author.id):
            await ctx.send("❌ This command is for the bot owner only.")
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