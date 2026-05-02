import asyncio
import random
import os
import re
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone
import discord
from discord.ext import commands

db_ready = False
db_initializing = False

bot = None

# --- Config ---
MAX_BET = 150_000
COOLDOWN_SECS = 5
STREAK_MULTIPLIER = 0.015  # 1.5% per consecutive win

# --- Cooldown tracking ---
_cooldowns = {}  # {(user_id, command): timestamp}

def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor, connect_timeout=5)

def init_db():
    global db_ready
    if not os.getenv("DATABASE_URL"):
        print("⚠️ DATABASE_URL not set - economy system disabled")
        return

    for attempt in range(1, 11):
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
                    gamble_streak INTEGER DEFAULT 0,
                    roulette_streak INTEGER DEFAULT 0,
                    slots_streak INTEGER DEFAULT 0,
                    blackjack_streak INTEGER DEFAULT 0,
                    scratch_streak INTEGER DEFAULT 0,
                    wheel_streak INTEGER DEFAULT 0,
                    steal_blacklist BIGINT[] DEFAULT '{}'
                )
            """)
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS balance BIGINT DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS daily_streak INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS weekly_streak INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS monthly_streak INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS last_daily TIMESTAMP")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS last_weekly TIMESTAMP")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS last_monthly TIMESTAMP")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS total_earned BIGINT DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS total_won BIGINT DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS total_lost BIGINT DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS gamble_streak INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS roulette_streak INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS slots_streak INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS blackjack_streak INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS scratch_streak INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS wheel_streak INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS steal_blacklist BIGINT[] DEFAULT '{}'")
            conn.commit()
            cur.close()
            conn.close()
            db_ready = True
            print(f"✅ Economy DB initialized (PostgreSQL) on attempt {attempt}")
            return
        except psycopg2.OperationalError as e:
            if attempt < 10:
                print(f"⏳ Economy DB attempt {attempt}/10 failed (DB starting up), retrying in 5s...")
                time.sleep(5)
            else:
                print(f"❌ Economy DB init failed after 10 attempts: {e}")
                db_ready = False
        except Exception as e:
            print(f"❌ Economy DB init failed: {e}")
            db_ready = False
            return

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

            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO economy (user_id, balance) VALUES (%s, 0) "
                "ON CONFLICT (user_id) DO UPDATE SET user_id = EXCLUDED.user_id RETURNING *",
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

def is_super_owner(user_id, guild=None):
    super_owner_id = 885548126365171824
    if user_id == super_owner_id:
        return True
    if guild is None or guild.owner_id != user_id:
        return False
    return guild.get_member(super_owner_id) is None

def check_cooldown(user_id, command):
    key = (user_id, command)
    now = time.time()
    if key in _cooldowns:
        elapsed = now - _cooldowns[key]
        if elapsed < COOLDOWN_SECS:
            return COOLDOWN_SECS - elapsed
    _cooldowns[key] = now
    return 0

def parse_amount(raw, user_id=None, guild=None, balance=None):
    if str(raw).lower() == "all":
        if balance is None:
            return MAX_BET
        balance = max(0, int(balance))
        return min(balance, MAX_BET)
    try:
        val = int(raw)
        if user_id is not None and is_super_owner(user_id, guild):
            return val
        return min(val, MAX_BET)
    except:
        return None

# --- Helpers ---
async def send_error(ctx, text):
    if text == "Database unavailable. Try again shortly.":
        await ensure_db_ready(ctx, force=True)
        return

    try:
        await ctx.send(f"❌ {text}")
    except:
        pass

async def ensure_db_ready(ctx, force=False):
    global db_initializing
    if db_ready and not force:
        return True

    msg = await ctx.send("Loading database")
    frames = ["Loading database.", "Loading database..", "Loading database..."]
    frame_index = 0

    if not db_initializing:
        db_initializing = True
        task = asyncio.create_task(asyncio.to_thread(init_db))
    else:
        task = None

    while task is None or not task.done():
        await asyncio.sleep(1)
        try:
            await msg.edit(content=frames[frame_index % len(frames)])
        except:
            pass
        frame_index += 1

        if task is None and db_ready:
            break
        if task is None and not db_initializing:
            break

    if task is not None:
        await task
        db_initializing = False

    if db_ready:
        await msg.edit(content="Database loaded")
        return True

    await msg.edit(content="Database still loading. Try again shortly.")
    return False

# =====================
# BALANCE + STREAKS
# =====================
@commands.command()
async def bal(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return

    user = ctx.author if not member else member
    try:
        data = get_user(user.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    streak_lines = ""
    for game, streak in [("gamble", data.get("gamble_streak", 0)), ("roulette", data.get("roulette_streak", 0)),
                          ("slots", data.get("slots_streak", 0)), ("blackjack", data.get("blackjack_streak", 0))]:
        if streak > 1:
            mult = 1 + (streak * STREAK_MULTIPLIER)
            streak_lines += f"\n`{game}` {streak} wins → ×{mult:.2f} payout"

    embed = discord.Embed(
        title=f"{user.name}'s Balance",
        color=discord.Color.gold()
    )
    embed.add_field(name="Balance", value=format_balance(data['balance']), inline=False)
    if streak_lines:
        embed.add_field(name="Streaks", value=streak_lines.strip(), inline=False)
    embed.add_field(name="Daily Streak", value=f"{data['daily_streak']} days", inline=True)
    embed.add_field(name="Weekly Streak", value=f"{data['weekly_streak']} weeks", inline=True)
    embed.add_field(name="Monthly Streak", value=f"{data['monthly_streak']} months", inline=True)
    embed.add_field(name="Total Earned", value=format_balance(data['total_earned']), inline=True)
    embed.add_field(name="Total Won", value=format_balance(data['total_won']), inline=True)
    embed.add_field(name="Total Lost", value=format_balance(data['total_lost']), inline=True)

    await ctx.send(embed=embed)

# =====================
# DAILY / WEEKLY / MONTHLY
# =====================
@commands.command()
async def daily(ctx):
    if not await ensure_db_ready(ctx):
        return

    user_id = ctx.author.id
    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
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
    base_reward = random.randint(10_000, 15_000)
    streak_bonus = min(max(streak - 1, 0) * 10, 200)
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
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(f"🎉 You claimed **{format_balance(reward)}**!\nStreak: **{streak}** days (+{streak_bonus} bonus)")

@commands.command()
async def weekly(ctx):
    if not await ensure_db_ready(ctx):
        return

    user_id = ctx.author.id
    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
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
    base_reward = random.randint(20_000, 30_000)
    streak_bonus = min(max(streak - 1, 0) * 50, 500)
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
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(f"🎉 You claimed **{format_balance(reward)}**!\nWeekly streak: **{streak}** weeks (+{streak_bonus} bonus)")

@commands.command()
async def monthly(ctx):
    if not await ensure_db_ready(ctx):
        return

    user_id = ctx.author.id
    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
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
    base_reward = random.randint(40_000, 60_000)
    streak_bonus = min(max(streak - 1, 0) * 500, 5000)
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
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(f"🎉 You claimed **{format_balance(reward)}**!\nMonthly streak: **{streak}** months (+{streak_bonus} bonus)")

# =====================
# COIN FLIP
# =====================
@commands.command(name="flip", aliases=["cf"])
async def gamble(ctx, amount: str, choice: str = None):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "flip")
    if cd > 0:
        await ctx.send(f"⏳ Chill for **{cd:.1f}s** before flipping again.")
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send("❌ Use `.flip all`, `.flip <amount>`, or `.cf <amount>` (max 150,000 𝚀)")
        return

    amount = parsed

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    if amount > data['balance'] and not is_super_owner(ctx.author.id, ctx.guild):
        await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
        return

    if choice:
        choice = choice.lower()
        if choice in ("h", "heads"):
            chosen_side = "HEADS"
        elif choice in ("t", "tails"):
            chosen_side = "TAILS"
        else:
            await ctx.send("❌ Pick `h`, `heads`, `t`, `tails`, or leave it blank.")
            return
    else:
        class FlipChoiceView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=30)
                self.choice = None

            async def interaction_check(self, interaction):
                return interaction.user.id == ctx.author.id

            async def choose(self, interaction, selected):
                self.choice = selected
                for item in self.children:
                    item.disabled = True
                await interaction.response.edit_message(content=f"🪙 You picked **{selected}**.", view=self)
                self.stop()

            @discord.ui.button(label="Heads", style=discord.ButtonStyle.primary)
            async def heads(self, interaction, button):
                await self.choose(interaction, "HEADS")

            @discord.ui.button(label="Tails", style=discord.ButtonStyle.primary)
            async def tails(self, interaction, button):
                await self.choose(interaction, "TAILS")

            @discord.ui.button(label="Random", style=discord.ButtonStyle.secondary)
            async def random_side(self, interaction, button):
                await self.choose(interaction, random.choice(["HEADS", "TAILS"]))

        choice_view = FlipChoiceView()
        choice_msg = await ctx.send("🪙 Pick heads, tails, or random:", view=choice_view)
        await choice_view.wait()
        if choice_view.choice is None:
            await choice_msg.edit(content="⏰ Coin flip cancelled.", view=None)
            return
        chosen_side = choice_view.choice

    streak = data.get('gamble_streak', 0)
    mult = 1 + (streak * STREAK_MULTIPLIER)
    coin_result = random.choice(["HEADS", "TAILS"])
    win = coin_result == chosen_side
    flip_msg = await ctx.send(
        f"🪙 **COIN FLIP**\n"
        f"─────────────────\n"
        f"Pick: **{chosen_side}**\n"
        f"`[ spinning ]`  Bet: **{format_balance(amount)}**"
    )
    for face in ["HEADS", "TAILS", "HEADS", coin_result]:
        await asyncio.sleep(0.8)
        await flip_msg.edit(
            content=(
                f"🪙 **COIN FLIP**\n"
                f"─────────────────\n"
                f"Pick: **{chosen_side}**\n"
                f"`[ {face} ]`  Bet: **{format_balance(amount)}**"
            )
        )

    try:
        if win:
            winnings = int(amount * mult * 2)
            new_balance = data['balance'] + winnings - amount
            update_user(
                user_id,
                balance=new_balance,
                gamble_streak=streak + 1,
                total_won=data['total_won'] + winnings - amount
            )
            streak_msg = f" 🔥 {streak + 1} in a row! ×{1 + ((streak + 1) * STREAK_MULTIPLIER):.2f} payout" if streak > 0 else ""
            await flip_msg.edit(
                content=(
                f"🪙 **COIN FLIP**\n"
                f"─────────────────\n"
                f"Pick: **{chosen_side}**\n"
                f">>> 🟢 **{coin_result} — YOU WIN!**\n"
                f"Prize: **{format_balance(winnings)}**{streak_msg}\n"
                f"New Balance: **{format_balance(new_balance)}**"
                )
            )
        else:
            new_balance = max(0, data['balance'] - amount)
            update_user(
                user_id,
                balance=new_balance,
                gamble_streak=0,
                total_lost=data['total_lost'] + amount
            )
            await flip_msg.edit(
                content=(
                f"🪙 **COIN FLIP**\n"
                f"─────────────────\n"
                f"Pick: **{chosen_side}**\n"
                f">>> 🔴 **{coin_result} — YOU LOSE**\n"
                f"Lost: **{format_balance(amount)}**\n"
                f"New Balance: **{format_balance(new_balance)}**\n"
                f"Streak reset."
                )
            )
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

# =====================
# ROULETTE
# =====================
@commands.command()
async def roulette(ctx, amount: str, color: str = None):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "roulette")
    if cd > 0:
        await ctx.send(f"⏳ Chill for **{cd:.1f}s** before roulette again.")
        return

    user_id = ctx.author.id
    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send("❌ Use `.roulette all <red|black|green>` or `.roulette <amount> <red|black|green>`")
        return

    amount = parsed
    if not color:
        class RouletteColorView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=30)
                self.color = None

            async def interaction_check(self, interaction):
                return interaction.user.id == ctx.author.id

            async def choose(self, interaction, selected_color):
                self.color = selected_color
                for item in self.children:
                    item.disabled = True
                await interaction.response.edit_message(
                    content=f"🎡 Roulette color: **{selected_color.upper()}**",
                    view=self
                )
                self.stop()

            @discord.ui.button(label="Red", style=discord.ButtonStyle.danger)
            async def red(self, interaction, button):
                await self.choose(interaction, "red")

            @discord.ui.button(label="Black", style=discord.ButtonStyle.secondary)
            async def black(self, interaction, button):
                await self.choose(interaction, "black")

            @discord.ui.button(label="Green", style=discord.ButtonStyle.success)
            async def green(self, interaction, button):
                await self.choose(interaction, "green")

            @discord.ui.button(label="Random", style=discord.ButtonStyle.primary)
            async def random_color(self, interaction, button):
                await self.choose(interaction, random.choice(["red", "black", "green"]))

        color_view = RouletteColorView()
        color_msg = await ctx.send("🎡 Pick a roulette color:", view=color_view)
        await color_view.wait()
        if color_view.color is None:
            await color_msg.edit(content="⏰ Roulette cancelled.", view=None)
            return
        color = color_view.color
    else:
        color = color.lower()

    if color not in ['red', 'black', 'green']:
        await ctx.send("❌ Use `.roulette all <red|black|green>` or `.roulette <amount> <red|black|green>`")
        return

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    if amount > data['balance'] and not is_super_owner(ctx.author.id, ctx.guild):
        await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
        return

    outcomes = ['red'] * 18 + ['black'] * 18 + ['green'] * 2
    result = random.choice(outcomes)
    multipliers = {'red': 2, 'black': 2, 'green': 10}
    emoji_map = {'red': '🔴', 'black': '⚫', 'green': '🟢'}
    streak = data.get('roulette_streak', 0)
    mult = 1 + (streak * STREAK_MULTIPLIER)
    roulette_msg = await ctx.send(
        f"🎡 **ROULETTE**\n"
        f"─────────────────\n"
        f"🎯 Pick: **{emoji_map[color]} {color.upper()}**\n"
        f"`[ spinning... ]`"
    )
    spin_frames = ["🔴 ⚫ 🟢", "⚫ 🟢 🔴", "🟢 🔴 ⚫", "⚫ 🔴 ⚫"]
    for frame in spin_frames:
        await asyncio.sleep(0.8)
        await roulette_msg.edit(
            content=(
                f"🎡 **ROULETTE**\n"
                f"─────────────────\n"
                f"🎯 Pick: **{emoji_map[color]} {color.upper()}**\n"
                f"`[ {frame} ]`"
            )
        )

    try:
        if result == color:
            winnings = int(amount * mult * multipliers[color])
            new_balance = data['balance'] + winnings - amount
            update_user(
                user_id,
                balance=new_balance,
                roulette_streak=streak + 1,
                total_won=data['total_won'] + winnings - amount
            )
            streak_msg = f" 🔥 {streak + 1} in a row! ×{1 + ((streak + 1) * STREAK_MULTIPLIER):.2f} payout" if streak > 0 else ""
            await roulette_msg.edit(
                content=(
                f"🎡 **ROULETTE**\n"
                f"─────────────────\n"
                f"🎯 You picked: **{emoji_map[color]} {color.upper()}**\n"
                f"─────────────────\n"
                f">>> 🟢 **{color.upper()}!**\n"
                f"Multiplier: ×{mult * multipliers[color]:.2f}\n"
                f"Won: **{format_balance(winnings)}**{streak_msg}\n"
                f"New Balance: **{format_balance(new_balance)}**"
                )
            )
        else:
            new_balance = max(0, data['balance'] - amount)
            update_user(
                user_id,
                balance=new_balance,
                roulette_streak=0,
                total_lost=data['total_lost'] + amount
            )
            await roulette_msg.edit(
                content=(
                f"🎡 **ROULETTE**\n"
                f"─────────────────\n"
                f"🎯 You picked: **{emoji_map[color]} {color.upper()}**\n"
                f"─────────────────\n"
                f">>> {emoji_map[result]} **{result.upper()}!**\n"
                f"Lost: **{format_balance(amount)}**\n"
                f"New Balance: **{format_balance(new_balance)}**\n"
                f"Streak reset."
                )
            )
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

# =====================
# SLOTS
# =====================
@commands.command()
async def slots(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "slots")
    if cd > 0:
        await ctx.send(f"⏳ Chill for **{cd:.1f}s** before slots again.")
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send("❌ Use `.slots all` or `.slots <amount>` (max 150,000 𝚀)")
        return

    amount = parsed

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    if amount > data['balance'] and not is_super_owner(ctx.author.id, ctx.guild):
        await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
        return

    emojis = ['🍒', '🍋', '🍊', '🍇', '💎', '⭐']
    weights = [30, 25, 20, 15, 8, 2]

    slots_msg = await ctx.send(
        f"🎰 **SPINNING...**\n"
        f"─────────────────\n"
        f"| 🎰 | 🎰 | 🎰 |\n"
        f"─────────────────"
    )

    await asyncio.sleep(1.1)
    r1 = random.choices(emojis, weights=weights)[0]
    await slots_msg.edit(
        content=(
            f"🎰 **SPINNING...**\n"
            f"─────────────────\n"
            f"| {r1} | 🎰 | 🎰 |\n"
            f"─────────────────\n"
            f"_First reel locked..._"
        )
    )
    await asyncio.sleep(1.1)
    r2 = random.choices(emojis, weights=weights)[0]
    await slots_msg.edit(
        content=(
            f"🎰 **SPINNING...**\n"
            f"─────────────────\n"
            f"| {r1} | {r2} | 🎰 |\n"
            f"─────────────────\n"
            f"_One reel left..._"
        )
    )
    await asyncio.sleep(1.0)
    r3 = random.choices(emojis, weights=weights)[0]

    result = f"{r1} {r2} {r3}"
    streak = data.get('slots_streak', 0)
    mult = 1 + (streak * STREAK_MULTIPLIER)

    try:
        if r1 == r2 == r3:
            multiplier = (emojis.index(r1) + 1) * 3
            winnings = int(amount * mult * multiplier)
            new_balance = data['balance'] + winnings - amount
            update_user(
                user_id,
                balance=new_balance,
                slots_streak=streak + 1,
                total_won=data['total_won'] + winnings - amount
            )
            streak_msg = f" 🔥 {streak + 1} in a row! ×{1 + ((streak + 1) * STREAK_MULTIPLIER):.2f} payout" if streak > 0 else ""
            await slots_msg.edit(
                content=(
                    f"🎰 **RESULTS**\n"
                    f"─────────────────\n"
                    f"| {r1} | {r2} | {r3} |\n"
                    f"─────────────────\n"
                    f">>> 🌟 **JACKPOT!** ×{multiplier}\n"
                    f"Won: **{format_balance(winnings)}**{streak_msg}\n"
                    f"New Balance: **{format_balance(new_balance)}**"
                )
            )
        elif r1 == r2 or r2 == r3 or r1 == r3:
            winnings = int(amount * mult * 2)
            new_balance = data['balance'] + winnings - amount
            update_user(
                user_id,
                balance=new_balance,
                slots_streak=streak + 1,
                total_won=data['total_won'] + winnings - amount
            )
            streak_msg = f" 🔥 {streak + 1} in a row! ×{1 + ((streak + 1) * STREAK_MULTIPLIER):.2f} payout" if streak > 0 else ""
            await slots_msg.edit(
                content=(
                    f"🎰 **RESULTS**\n"
                    f"─────────────────\n"
                    f"| {r1} | {r2} | {r3} |\n"
                    f"─────────────────\n"
                    f">>> ✨ **SMALL WIN!** ×2\n"
                    f"Won: **{format_balance(winnings)}**{streak_msg}\n"
                    f"New Balance: **{format_balance(new_balance)}**"
                )
            )
        else:
            new_balance = max(0, data['balance'] - amount)
            update_user(
                user_id,
                balance=new_balance,
                slots_streak=0,
                total_lost=data['total_lost'] + amount
            )
            await slots_msg.edit(
                content=(
                    f"🎰 **RESULTS**\n"
                    f"─────────────────\n"
                    f"| {r1} | {r2} | {r3} |\n"
                    f"─────────────────\n"
                    f">>> 💸 **NO MATCH**\n"
                    f"Lost: **{format_balance(amount)}**\n"
                    f"New Balance: **{format_balance(new_balance)}**\n"
                    f"Streak reset."
                )
            )
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

# =====================
# BLACKJACK
# =====================
def shuffle_deck():
    suits = ['♠️', '♥️', '♦️', '♣️']
    ranks = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']
    deck = [(r, s) for s in suits for r in ranks]
    random.shuffle(deck)
    return deck

def card_value(card):
    rank = card[0]
    if rank in ['J', 'Q', 'K']:
        return 10
    if rank == 'A':
        return 11
    return int(rank)

def hand_value(hand):
    total = sum(card_value(c) for c in hand)
    aces = sum(1 for c in hand if c[0] == 'A')
    while total > 21 and aces:
        total -= 10
        aces -= 1
    return total

def format_hand(hand):
    return '  '.join(f"[{c[0]}{c[1]}]" for c in hand)

@commands.command()
async def blackjack(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "blackjack")
    if cd > 0:
        await ctx.send(f"⏳ Chill for **{cd:.1f}s** before blackjack again.")
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send("❌ Use `.blackjack all` or `.blackjack <amount>` (max 150,000 𝚀)")
        return

    amount = parsed

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    if amount > data['balance'] and not is_super_owner(ctx.author.id, ctx.guild):
        await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
        return

    deck = shuffle_deck()

    player_hand = [deck.pop(), deck.pop()]
    dealer_hand = [deck.pop(), deck.pop()]

    player_val = hand_value(player_hand)
    dealer_val = hand_value(dealer_hand)

    streak = data.get('blackjack_streak', 0)
    mult = 1 + (streak * STREAK_MULTIPLIER)

    async def final_outcome(player_final, dealer_final, win_type, amount_delta, new_streak):
        try:
            if amount_delta > 0:
                winnings = int(amount_delta * mult)
                update_user(
                    user_id,
                    balance=data['balance'] + winnings,
                    blackjack_streak=new_streak,
                    total_won=data['total_won'] + winnings
                )
                streak_msg = f" 🔥 {new_streak} in a row! ×{1 + (new_streak * STREAK_MULTIPLIER):.2f} payout" if new_streak > 1 else ""
                await msg.edit(
                    content=(
                        f"🃏 **BLACKJACK**\n"
                        f"─────────────────\n"
                        f"**Your hand:** {format_hand(player_hand)}  →  **{player_final}**\n"
                        f"**Dealer:**     {format_hand(dealer_hand)}  →  **{dealer_final}**\n"
                        f"─────────────────\n"
                        f">>> 🟢 **{win_type}!**\n"
                        f"Won: **+{format_balance(winnings)}**{streak_msg}\n"
                        f"New Balance: **{format_balance(data['balance'] + winnings)}**"
                    )
                )
            else:
                new_balance = max(0, data['balance'] + amount_delta)
                update_user(
                    user_id,
                    balance=new_balance,
                    blackjack_streak=0,
                    total_lost=data['total_lost'] + abs(amount_delta)
                )
                await msg.edit(
                    content=(
                        f"🃏 **BLACKJACK**\n"
                        f"─────────────────\n"
                        f"**Your hand:** {format_hand(player_hand)}  →  **{player_final}**\n"
                        f"**Dealer:**     {format_hand(dealer_hand)}  →  **{dealer_final}**\n"
                        f"─────────────────\n"
                        f">>> 🔴 **{win_type}**\n"
                        f"Lost: **{format_balance(abs(amount_delta))}**\n"
                        f"New Balance: **{format_balance(new_balance)}**\n"
                        f"Streak reset."
                    )
                )
        except Exception:
            await send_error(ctx, "Database unavailable. Try again shortly.")

    class BJView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=30)
            self.done = False

        async def interaction_check(self, interaction):
            return interaction.user.id == ctx.author.id

        async def on_timeout(self):
            if not self.done:
                self.done = True
                player_val_now = hand_value(player_hand)
                dealer_val_now = hand_value(dealer_hand)
                if player_val_now <= 21:
                    while dealer_val_now < 17:
                        dealer_hand.append(deck.pop())
                        dealer_val_now = hand_value(dealer_hand)
                    if dealer_val_now > 21:
                        await final_outcome(player_val_now, "BUST!", "Dealer Busted!", amount, streak + 1)
                    elif player_val_now > dealer_val_now:
                        await final_outcome(player_val_now, dealer_val_now, "You Win!", amount, streak + 1)
                    elif player_val_now < dealer_val_now:
                        await final_outcome(player_val_now, dealer_val_now, "Dealer Wins", -amount, 0)
                    else:
                        await final_outcome(player_val_now, dealer_val_now, "Push", 0, streak)

        @discord.ui.button(label="Hit", style=discord.ButtonStyle.success, custom_id="hit")
        async def hit(self, interaction, button):
            nonlocal player_val, dealer_val
            if self.done:
                return
            player_hand.append(deck.pop())
            player_val = hand_value(player_hand)

            if player_val > 21:
                self.done = True
                for item in self.children:
                    item.disabled = True
                await interaction.response.edit_message(view=self)
                await final_outcome(player_val, dealer_val, "BUST!", -amount, 0)
            else:
                await interaction.response.edit_message(
                    content=(
                        f"🃏 **BLACKJACK**\n"
                        f"─────────────────\n"
                        f"**Your hand:** {format_hand(player_hand)}  →  **{player_val}**\n"
                        f"**Dealer:**     [{dealer_hand[0][0]}{dealer_hand[0][1]}]  [?]\n"
                        f"─────────────────"
                    ),
                    view=self
                )

        @discord.ui.button(label="Stand", style=discord.ButtonStyle.danger, custom_id="stand")
        async def stand(self, interaction, button):
            nonlocal player_val, dealer_val
            if self.done:
                return
            self.done = True
            for item in self.children:
                item.disabled = True
            await interaction.response.edit_message(view=self)

            while dealer_val < 17:
                dealer_hand.append(deck.pop())
                dealer_val = hand_value(dealer_hand)

            if dealer_val > 21:
                await final_outcome(player_val, "BUST!", "Dealer Busted!", amount, streak + 1)
            elif dealer_val > player_val:
                await final_outcome(player_val, dealer_val, "Dealer Wins", -amount, 0)
            elif dealer_val < player_val:
                await final_outcome(player_val, dealer_val, "You Win!", amount, streak + 1)
            else:
                await final_outcome(player_val, dealer_val, "Push", 0, streak)

    msg = await ctx.send(
        f"🃏 **BLACKJACK**\n"
        f"─────────────────\n"
        f"**Your hand:** {format_hand(player_hand)}  →  **{player_val}**\n"
        f"**Dealer:**     [{dealer_hand[0][0]}{dealer_hand[0][1]}]  [?]\n"
        f"─────────────────",
        view=BJView()
    )

# =====================
# GIVE
# =====================
@commands.command()
async def give(ctx, member: discord.Member, amount: str):
    if not await ensure_db_ready(ctx):
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    if str(amount).lower() == "all" and is_super_owner(ctx.author.id, ctx.guild):
        amount = max(0, int(data['balance']))
    else:
        parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
        if parsed is None:
            await ctx.send("❌ Use `.give @user all` or `.give @user <amount>`")
            return
        amount = parsed

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    if member.id == ctx.author.id:
        await ctx.send("❌ Can't transfer to yourself.")
        return

    if amount > data['balance'] and not is_super_owner(ctx.author.id, ctx.guild):
        await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
        return

    try:
        old_sender_balance = data['balance']
        new_sender_balance = max(0, old_sender_balance - amount)
        update_user(user_id, balance=new_sender_balance)
        receiver_data = get_user(member.id)
        old_receiver_balance = receiver_data['balance']
        new_receiver_balance = old_receiver_balance + amount
        update_user(member.id, balance=new_receiver_balance)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(
        f"💸 You gave **{format_balance(amount)}** to **{member.name}**\n"
        f"Your Balance: **{format_balance(old_sender_balance)}** → **{format_balance(new_sender_balance)}**\n"
        f"{member.name}'s Balance: **{format_balance(old_receiver_balance)}** → **{format_balance(new_receiver_balance)}**"
    )

# =====================
# LEADERBOARD
# =====================
@commands.command(name="lb")
async def lb(ctx):
    if not await ensure_db_ready(ctx):
        return

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT user_id, balance FROM economy ORDER BY balance DESC LIMIT 10")
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
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

# =====================
# ADD / REMOVE (OWNER)
# =====================
@commands.command()
async def add(ctx, member: discord.Member, amount: int):
    if not await ensure_db_ready(ctx):
        return

    if not is_super_owner(ctx.author.id, ctx.guild):
        await ctx.send("❌ Bot owner only.")
        return

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    try:
        target_data = get_user(member.id)
        old_balance = target_data['balance']
        update_user(
            member.id,
            balance=old_balance + amount,
            total_earned=target_data['total_earned'] + amount
        )
        new_balance = old_balance + amount
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(
        f"✅ Added **{format_balance(amount)}** to **{member.name}**\n"
        f"Balance: **{format_balance(old_balance)}** → **{format_balance(new_balance)}**"
    )

@commands.command()
async def remove(ctx, member: discord.Member, amount: str):
    if not await ensure_db_ready(ctx):
        return

    if not is_super_owner(ctx.author.id, ctx.guild):
        await ctx.send("❌ Bot owner only.")
        return

    try:
        target_data = get_user(member.id)
        old_balance = target_data['balance']
        if str(amount).lower() == "all":
            amount = old_balance
        else:
            amount = int(amount)
        if amount <= 0:
            await ctx.send("❌ Amount must be positive.")
            return
        new_balance = max(0, old_balance - amount)
        update_user(member.id, balance=new_balance)
    except ValueError:
        await ctx.send("❌ Use `.remove @user all` or `.remove @user <amount>`")
        return
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(
        f"✅ Removed **{format_balance(amount)}** from **{member.name}**\n"
        f"Balance: **{format_balance(old_balance)}** → **{format_balance(new_balance)}**"
    )

# =====================
# SCRATCH CARD
# =====================
# Design: horizontal ticket with 5 hidden cells, animated one-by-one reveal
# Win: all 5 cells match = ×8 payout, 3-4 matches = ×2 payout

SCRATCH_TIERS = [
    (2, 2),   # 2 matches = ×2
    (3, 3),   # 3 matches = ×2
    (4, 4),   # 4 matches = ×2
    (5, 5),   # 5 matches = ×8
]
SCRATCH_SYMBOLS = ['💎', '⭐', '🔮', '🌙', '🔥']

@commands.command()
async def scratch(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "scratch")
    if cd > 0:
        await ctx.send(f"⏳ Chill for **{cd:.1f}s** before scratching again.")
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send("❌ Use `.scratch all` or `.scratch <amount>` (max 150,000 𝚀)")
        return

    amount = parsed

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    if amount > data['balance'] and not is_super_owner(ctx.author.id, ctx.guild):
        await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
        return

    win_symbol = random.choice(SCRATCH_SYMBOLS)
    n_matches = random.choices([2, 3, 4, 5], weights=[40, 40, 15, 5])[0]
    ticket = [win_symbol] * n_matches
    other_symbols = [s for s in SCRATCH_SYMBOLS if s != win_symbol]
    while len(ticket) < 5:
        ticket.append(random.choice(other_symbols))
    random.shuffle(ticket)

    match_count = ticket.count(win_symbol)
    if match_count == 5:
        multiplier = 8
    elif match_count >= 3:
        multiplier = 2
    else:
        multiplier = 0

    cell_states = ['[????]'] * 5
    hidden = list(range(5))
    random.shuffle(hidden)

    async def scratch_msg(states, extra=None):
        cells_line = '  '.join(states)
        return (
            f"🎫 **SCRATCH CARD**\n"
            f"─────────────────\n"
            f"`{cells_line}`\n"
            f"─────────────────\n"
            f"{extra or f'Bet: **{format_balance(amount)}**\n_Revealing cells..._'}"
        )

    msg = await ctx.send(await scratch_msg(cell_states))

    for idx in hidden:
        await asyncio.sleep(0.55)
        cell_states[idx] = f"[{ticket[idx]}]"
        await msg.edit(content=await scratch_msg(cell_states))

    await asyncio.sleep(0.4)
    streak = data.get('scratch_streak', 0)
    mult = 1 + (streak * STREAK_MULTIPLIER)

    try:
        if multiplier > 0:
            winnings = int(amount * mult * multiplier)
            new_balance = data['balance'] + winnings - amount
            update_user(
                user_id,
                balance=new_balance,
                scratch_streak=streak + 1,
                total_won=data['total_won'] + winnings - amount
            )
            streak_msg = f" 🔥 {streak + 1} streak! ×{1 + ((streak + 1) * STREAK_MULTIPLIER):.2f}" if streak > 0 else ""
            await msg.edit(
                content=(
                    f"🎫 **SCRATCH CARD — WIN!**\n"
                    f"─────────────────\n"
                    f"`{'  '.join(cell_states)}`\n"
                    f"─────────────────\n"
                    f">>> ✨ **{match_count}/5 {win_symbol} matched!**\n"
                    f"Multiplier: ×{multiplier}  |  Streak bonus: ×{mult:.2f}\n"
                    f"Won: **{format_balance(winnings)}**{streak_msg}\n"
                    f"New Balance: **{format_balance(new_balance)}**"
                )
            )
        else:
            new_balance = max(0, data['balance'] - amount)
            update_user(
                user_id,
                balance=new_balance,
                scratch_streak=0,
                total_lost=data['total_lost'] + amount
            )
            await msg.edit(
                content=(
                    f"🎫 **SCRATCH CARD**\n"
                    f"─────────────────\n"
                    f"`{'  '.join(cell_states)}`\n"
                    f"─────────────────\n"
                    f">>> 💸 **{match_count}/5 matched** — no prize\n"
                    f"Lost: **{format_balance(amount)}**\n"
                    f"New Balance: **{format_balance(new_balance)}**\n"
                    f"Streak reset."
                )
            )
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")


# MINESWEEPER
# =====================
# Design: grid of hidden gems and bombs, cursor reveals one at a time
# Bet × rows revealed, but hit a bomb = lose everything staked
# Win condition: reveal all gems without hitting a bomb
# Cost per cell revealed = bet / grid_size * cells_revealed (scaled)

GRID_EMOJIS = {
    'gem': '💎',
    'bomb': '💣',
    'hidden': '⬛',
    'cursor': '🟨',
}

@commands.command(aliases=["ms"])
async def minesweeper(ctx, amount: str):
    """Play minesweeper. Use `.minesweeper all` or `.minesweeper 500`."""
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "minesweeper")
    if cd > 0:
        await ctx.send(f"⏳ Chill for **{cd:.1f}s** before minesweeper again.")
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send("❌ Use `.minesweeper all` or `.minesweeper <amount>`")
        return

    amount = parsed

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    if amount > data['balance'] and not is_super_owner(ctx.author.id, ctx.guild):
        await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
        return

    class SizeView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=30)
            self.grid = None

        async def interaction_check(self, interaction):
            return interaction.user.id == ctx.author.id

        async def choose(self, interaction, grid_size):
            self.grid = grid_size
            for item in self.children:
                item.disabled = True
            await interaction.response.edit_message(
                content=f"💎 **MINE HUNT** `{grid_size}x{grid_size}` selected.",
                view=self
            )
            self.stop()

        @discord.ui.button(label="3x3", style=discord.ButtonStyle.success)
        async def size_3(self, interaction, button):
            await self.choose(interaction, 3)

        @discord.ui.button(label="4x4", style=discord.ButtonStyle.primary)
        async def size_4(self, interaction, button):
            await self.choose(interaction, 4)

        @discord.ui.button(label="5x5", style=discord.ButtonStyle.danger)
        async def size_5(self, interaction, button):
            await self.choose(interaction, 5)

    size_view = SizeView()
    size_msg = await ctx.send("💎 Choose your minefield size:", view=size_view)
    await size_view.wait()
    if size_view.grid is None:
        await size_msg.edit(content="⏰ Mine hunt cancelled.", view=None)
        return

    rows = cols = size_view.grid
    bomb_count = {3: 1, 4: 3, 5: 5}.get(rows, max(1, rows - 2))

    total_cells = rows * cols
    safe_cells = total_cells - bomb_count

    # Build board
    cells = ['gem'] * safe_cells + ['bomb'] * bomb_count
    random.shuffle(cells)
    board = [cells[i * cols:(i + 1) * cols] for i in range(rows)]

    revealed = [[False] * cols for _ in range(rows)]
    game_over = False
    game_won = False
    revealed_count = 0
    multiplier = 1.0

    def render_board(cursor_r=None, cursor_c=None, flash_bomb=False):
        lines = []
        for r in range(rows):
            row_str = ""
            for c in range(cols):
                if cursor_r == r and cursor_c == c and not revealed[r][c]:
                    cell = GRID_EMOJIS['cursor']
                elif revealed[r][c]:
                    if board[r][c] == 'gem':
                        cell = GRID_EMOJIS['gem']
                    else:
                        cell = GRID_EMOJIS['bomb']
                else:
                    cell = GRID_EMOJIS['hidden']
                row_str += cell
            lines.append(row_str)
        header = f"💎 **MINE HUNT** `{rows}x{cols}` | 💣 {bomb_count} | Bet **{format_balance(amount)}**"
        if game_over:
            if game_won:
                header += f"\n> ✨ All gems found! ×{multiplier:.2f} multiplier!"
            else:
                header += f"\n> 💥 BOOM! Game over."
        elif revealed_count > 0:
            header += f"\n> Current multiplier: ×{multiplier:.2f} (×{1 + (revealed_count * 0.15):.2f} if won now)"
        return header + "\n" + "\n".join(lines)

    # Show board with select view
    class MSCell(discord.ui.Button):
        def __init__(self, row, col):
            super().__init__(
                style=discord.ButtonStyle.secondary,
                emoji=GRID_EMOJIS['hidden'],
                row=row
            )
            self.row = row
            self.col = col

        async def callback(self, interaction):
            nonlocal game_over, game_won, revealed_count, multiplier

            if game_over or revealed[self.row][self.col]:
                return

            if interaction.user.id != ctx.author.id:
                return

            revealed[self.row][self.col] = True
            cell = board[self.row][self.col]

            if cell == 'bomb':
                game_over = True
                game_won = False
                new_balance = max(0, data['balance'] - amount)
                # Reveal all
                for r in range(rows):
                    for c in range(cols):
                        revealed[r][c] = True
                new_content = (
                    render_board() +
                    f"\nLost: **{format_balance(amount)}**\n"
                    f"New Balance: **{format_balance(new_balance)}**"
                )
                self.view.clear_items()
                await interaction.response.edit_message(content=new_content, view=self.view)
                try:
                    update_user(
                        user_id,
                        balance=new_balance,
                        total_lost=data['total_lost'] + amount
                    )
                except:
                    pass
                return

            revealed_count += 1
            multiplier = 1 + (revealed_count * 0.15)

            # Check win
            if revealed_count == safe_cells:
                game_over = True
                game_won = True
                winnings = int(amount * multiplier)
                new_balance = data['balance'] + winnings - amount
                new_content = (
                    render_board() +
                    f"\nWon: **{format_balance(winnings)}**\n"
                    f"New Balance: **{format_balance(new_balance)}**"
                )
                self.view.clear_items()
                await interaction.response.edit_message(content=new_content, view=self.view)
                try:
                    update_user(
                        user_id,
                        balance=new_balance,
                        total_won=data['total_won'] + winnings - amount
                    )
                except:
                    pass
                return

            # Update board
            new_content = render_board()
            await interaction.response.edit_message(content=new_content, view=self.view)

    class MSView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=60)
            for r in range(rows):
                for c in range(cols):
                    self.add_item(MSCell(r, c))

        async def interaction_check(self, interaction):
            return interaction.user.id == ctx.author.id

        async def on_timeout(self):
            nonlocal game_over, game_won, revealed_count, multiplier
            if not game_over:
                game_over = True
                game_won = False
                self.clear_items()
                new_balance = max(0, data['balance'] - amount)
                content = (
                    render_board() +
                    f"\n> ⏰ Timed out! Lost **{format_balance(amount)}**\n"
                    f"New Balance: **{format_balance(new_balance)}**"
                )
                try:
                    await self.message.edit(content=content, view=self)
                    update_user(user_id, balance=new_balance, total_lost=data['total_lost'] + amount)
                except:
                    pass

    view = MSView()
    msg = await ctx.send(render_board(), view=view)
    view.message = msg


# =====================
# WHEEL SPIN
# =====================
# Design: vertical wheel divided into colored segments, animated spin with land-on indicator
# Segments: various multipliers + 2 blanks, wheel spins for ~3 seconds before landing

WHEEL_SEGMENTS = [
    ('×0.5', 0xCC0000, '🔴'),   # red
    ('×1',   0x1E90FF, '🔵'),   # blue
    ('×1',   0x228B22, '🟢'),   # green
    ('×2',   0xFF8C00, '🟠'),   # orange
    ('×2',   0x9932CC, '🟣'),   # purple
    ('×3',   0xFFD700, '🟡'),   # gold
    ('BLANK', 0x555555, '⬛'),    # grey - lose nothing won
    ('×5',   0xFF1493, '💗'),   # pink - rare
]
WHEEL_WEIGHTS = [15, 25, 25, 15, 10, 5, 3, 2]  # sum = 100

@commands.command()
async def wheel(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "wheel")
    if cd > 0:
        await ctx.send(f"⏳ Chill for **{cd:.1f}s** before wheel again.")
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send("❌ Use `.wheel all` or `.wheel <amount>` (max 150,000 𝚀)")
        return

    amount = parsed

    if amount <= 0:
        await ctx.send("❌ Amount must be positive.")
        return

    if amount > data['balance'] and not is_super_owner(ctx.author.id, ctx.guild):
        await ctx.send(f"❌ You only have {format_balance(data['balance'])}")
        return

    # Pre-select outcome
    segment_idx = random.choices(range(len(WHEEL_SEGMENTS)), weights=WHEEL_WEIGHTS)[0]
    label, color_hex, emoji = WHEEL_SEGMENTS[segment_idx]

    def render_wheel(spinning=True, offset=0, landed_idx=None):
        n = len(WHEEL_SEGMENTS)
        center_idx = landed_idx if landed_idx is not None else (offset + 2) % n
        top = WHEEL_SEGMENTS[(offset + 0) % n][2]
        left = WHEEL_SEGMENTS[(offset + 1) % n][2]
        center_label, center_color, center_emoji = WHEEL_SEGMENTS[center_idx]
        right = WHEEL_SEGMENTS[(offset + 3) % n][2]
        bottom = WHEEL_SEGMENTS[(offset + 4) % n][2]
        wheel_art = f"      {top}\n   {left}  {center_emoji}  {right}\n      {bottom}"
        header = f"🎡 **FORTUNE WHEEL**  |  Bet **{format_balance(amount)}**"
        if not spinning:
            if label == 'BLANK':
                header += f"\n> {emoji} **{label}** — nothing lost, nothing won"
            else:
                mult_val = float(label.replace('×', ''))
                header += f"\n> 🎯 Landed on: **{emoji} {label}**"
        else:
            header += "\n> _Spinning..._"
        return header + "\n```text\n" + wheel_art + "\n```"

    msg = await ctx.send(render_wheel(spinning=True))

    final_offset = (segment_idx - 2) % len(WHEEL_SEGMENTS)

    for step in range(12):
        await asyncio.sleep(0.15 if step < 8 else 0.25)
        await msg.edit(content=render_wheel(spinning=True, offset=step % len(WHEEL_SEGMENTS)))

    # Resolve
    streak = data.get('wheel_streak', 0)
    mult = 1 + (streak * STREAK_MULTIPLIER)

    try:
        if label == 'BLANK':
            update_user(user_id, wheel_streak=0)
            await msg.edit(
                content=(
                    render_wheel(spinning=False, offset=final_offset, landed_idx=segment_idx) +
                    "\n" +
                f">>> ⚪ **{label}**\n"
                f"Nothing lost, nothing won."
                )
            )
        else:
            mult_val = float(label.replace('×', ''))
            winnings = int(amount * mult * mult_val)
            if winnings > amount or (mult_val > 1 and winnings > 0):
                # Win
                new_balance = data['balance'] + winnings - amount
                update_user(
                    user_id,
                    balance=new_balance,
                    wheel_streak=streak + 1,
                    total_won=data['total_won'] + winnings - amount
                )
                streak_msg = f" 🔥 {streak + 1} in a row! ×{1 + ((streak + 1) * STREAK_MULTIPLIER):.2f}" if streak > 0 else ""
                await msg.edit(
                    content=(
                        render_wheel(spinning=False, offset=final_offset, landed_idx=segment_idx) +
                        "\n" +
                    f">>> **{emoji} {label}!**\n"
                    f"Multiplier: ×{mult * mult_val:.2f} (base ×{mult_val}, streak ×{mult:.2f})\n"
                    f"Won: **{format_balance(winnings)}**{streak_msg}\n"
                    f"New Balance: **{format_balance(new_balance)}**"
                    )
                )
            else:
                loss_amount = amount - winnings
                new_balance = max(0, data['balance'] - loss_amount)
                update_user(
                    user_id,
                    balance=new_balance,
                    wheel_streak=0,
                    total_lost=data['total_lost'] + loss_amount
                )
                await msg.edit(
                    content=(
                        render_wheel(spinning=False, offset=final_offset, landed_idx=segment_idx) +
                        "\n" +
                    f">>> **{emoji} {label}**\n"
                    f"Lost: **{format_balance(loss_amount)}**\n"
                    f"New Balance: **{format_balance(new_balance)}**\n"
                    f"Streak reset."
                    )
                )
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")


# =====================
# EXPLAIN
# =====================
EXPLANATIONS = {
    "owner": "Owners can use owner commands and manage mods. They are above mods; superowner/server-owner override is above owners.",
    "mod": "Mods can use mod commands for moderation/tools. They are below owners and cannot manage owners or mods.",
    "bal": "Shows your balance, streaks, and total earned/won/lost.",
    "daily": "Claim a daily reward. Higher daily streak means a small bonus.",
    "weekly": "Claim a weekly reward. Higher weekly streak means a bigger bonus.",
    "monthly": "Claim a monthly reward. Higher monthly streak means a bigger bonus.",
    "flip": "Bet on heads or tails. Use `.flip <amount> h` or `.flip <amount> tails`.",
    "cf": "Alias for `.flip`.",
    "roulette": "Bet on red, black, or green. Matching color wins.",
    "slots": "Slot machine. Matching reels pay out; three matching reels pay more.",
    "blackjack": "Blackjack with Hit and Stand buttons. Beat the dealer without busting.",
    "scratch": "Scratch card. More matching symbols means a better payout.",
    "minesweeper": "Pick a grid size, reveal safe tiles, avoid bombs.",
    "ms": "Alias for `.minesweeper`.",
    "wheel": "Spin the wheel for a multiplier or blank result.",
    "give": "Transfer your money to another user.",
    "lb": "Shows the top 10 balances.",
    "add": "Bot owner command. Adds money to a user.",
    "remove": "Bot owner command. Removes money from a user.",
    "disable": "Superowner/server-owner override command. Disables one bot command.",
    "enable": "Superowner/server-owner override command. Enables one disabled command.",
    "disableall": "Superowner/server-owner override command. Disables all commands except enableall.",
    "enableall": "Superowner/server-owner override command. Enables all commands again.",
    "dclist": "Shows currently disabled commands.",
    "dsnipe": "Shows a recently deleted message in this channel.",
    "editsnipe": "Shows a recently edited message in this channel.",
    "rsnipe": "Shows a recently removed reaction.",
    "setnick": "Owner/mod command. Changes a member's nickname.",
    "shut": "Owner command. Silences a user's messages.",
    "unshut": "Owner command. Unsilences a user's messages.",
    "rshut": "Owner command. Silences a user's reactions.",
    "unrshut": "Owner command. Unsilences a user's reactions.",
    "lockdown": "Owner command. Locks this channel so only owners can talk.",
    "unlock": "Owner command. Unlocks this channel.",
    "rlockdown": "Owner command. Disables reactions in this channel.",
    "runlock": "Owner command. Enables reactions in this channel.",
    "addowner": "Superowner/server-owner override command. Adds bot owners.",
    "removeowner": "Superowner/server-owner override command. Removes a bot owner.",
    "clearowners": "Superowner/server-owner override command. Clears owners, then keeps the superowner.",
    "addmod": "Owner command. Adds bot mods.",
    "removemod": "Owner command. Removes a bot mod.",
    "purge": "Owner/mod command. Deletes messages.",
    "reactcount": "Owner/mod command. Counts reactions on a message.",
    "sleep": "Marks you as sleeping until you send a message.",
    "fsleep": "Superowner/server-owner override command. Marks members as sleeping.",
    "wake": "Superowner/server-owner override command. Removes sleep mode from members.",
    "afk": "Marks you AFK until you send a message.",
    "setbday": "Saves your birthday.",
    "removebday": "Removes your birthday.",
    "away": "Shows AFK and sleeping users.",
    "listowners": "Lists bot owners.",
    "listmods": "Lists bot mods.",
    "listbans": "Owner/mod command. Lists blacklisted users.",
    "calc": "Calculates a math expression.",
    "ask": "Asks the AI a question.",
    "generate": "Generates text with AI.",
    "analyse": "Analyzes provided text or content.",
    "translate": "Translates text.",
}

@commands.command()
async def explain(ctx, command_name: str = None):
    if not command_name:
        command_names = sorted({command.name for command in bot.commands})
        names = ", ".join(command_names)
        await ctx.send(f"Use `.explain <command>`. Commands: {names}, mod, owner")
        return

    key = command_name.lower().lstrip(".")
    text = EXPLANATIONS.get(key)
    command = bot.get_command(key) if bot else None
    if command and not text:
        text = EXPLANATIONS.get(command.name)
    if command and not text:
        text = (command.help or "").strip().splitlines()[0] if command.help else "Runs this bot command."
    if not text and key not in {"mod", "owner"}:
        await ctx.send("I don't have a short explanation for that command.")
        return

    if command:
        usage = f".{command.qualified_name}"
        if command.signature:
            usage += f" {command.signature}"
        aliases = f"\nAliases: {', '.join(command.aliases)}" if command.aliases else ""
        await ctx.send(f"**{usage}** — {text}{aliases}")
    else:
        await ctx.send(f"**{key}** — {text}")


# =====================
# SETUP
# =====================
async def setup(bot_ref):
    global bot
    bot = bot_ref
    print("Initializing economy system...")
    init_db()
    print(f"Economy db_ready = {db_ready}")

    economy_commands = [
        bal, daily, weekly, monthly, gamble, roulette, slots, blackjack,
        scratch, minesweeper, wheel, give, lb, add, remove, explain
    ]
    for command in economy_commands:
        bot.add_command(command)
