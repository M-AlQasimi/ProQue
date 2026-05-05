import asyncio
import random
import os
import re
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone, timedelta
import discord
from discord.ext import commands

db_ready = False
db_initializing = False
db_init_task = None

bot = None

# --- Config ---
MAX_BET = 200_000
COOLDOWN_SECS = 10
STREAK_MULTIPLIER = 0.015  # 1.5% per consecutive win
CURRENCY_EMOJI = "<:Qoins:1500255107428782100>"
QASH_EMOJI = "<:Qash:1500235432011497703>"
Q_DENIED = "<:QDenied:1500427032020914266>"
Q_FLIP = "<:QFlip:1500427033753423993>"
Q_LEVEL_UP = "<:QLevelUp:1500427035292598383>"
Q_MINE = "<:QMine:1500427037301542932>"
QOIN_BAG = "<:QoinBag:1500427038748573777>"
QOIN_CHEST = "<:QoinChest:1500427040212516904>"
QOIN_TRANSFER = "<:QoinTransfer:1500427041735180318>"
Q_QUEST = "<:QQuest:1500427043429417101>"
Q_SHOP = "<:QShop:1500427045019189338>"
Q_SLOTS = "<:QSlots:1500427046365565020>"
Q_SUCCESS = "<:QSuccess:1500427048865235104>"
Q_TIMER = "<:QTimer:1500427051209986188>"
Q_TICKET = "<:QTicket:1500491388985282761>"
Q_WHEEL = "<:QWheel:1500427053160468480>"
Q_XP = "<:QXP:1500427054930333778>"
Q_FLIP_SPIN = "<a:QFlipSpin:1500427305216901160>"
Q_LEVEL_PULSE = "<a:QLevelPulse:1500427307230429284>"
Q_MINE_SPARK = "<a:QMineSpark:1500427308903829586>"
Q_TIMER_TICK = "<a:QTimerTick:1500427311395246180>"
Q_WHEEL_SPIN = "<a:QWheelSpin:1500427313760964691>"
Q_LUCKY_CHARM = "<:QLuckyCharm:1500502953239380089>"
Q_XP_TONIC = "<:QXPTonic:1500502985707618574>"
Q_QUESO_MAGNET = "<:QQuesoMagnet:1500502961162289294>"
Q_DAILY_SPICE = "<:QDailySpice:1500502941927342151>"
Q_STREAK_POLISH = "<:QStreakPolish:1500502970050023445>"
Q_GOLD_BADGE = "<:QGoldBadge:1500502947698577418>"
Q_HIGH_ROLLER = "<:QHighRoller:1500502949594665080>"
Q_VELVET_FRAME = "<:QVelvetFrame:1500502979306852484>"
Q_TICKET_CHARM = "<:QTicketCharm:1500502975746146356>"
Q_COOLDOWN_CLOCK = "<:QCooldownClock:1500502940107149403>"
Q_ROYAL_CROWN = "<:QRoyalCrown:1500502964048232570>"
Q_ACCEPT = "<:QAccept:1500516711114477709>"
Q_ALARM = "<:QAlarm:1500516713094054008>"
Q_ATTACHMENT = "<:QAttachment:1500516714641887402>"
Q_BELL = "<:QBell:1500516716344639618>"
Q_BIRTHDAY = "<:QBirthday:1500516717976097004>"
Q_BOOK = "<:QBook:1500516719771385926>"
Q_BROOM = "<:QBroom:1500516722170396772>"
Q_CARDS = "<:QCards:1500516723860701395>"
Q_CONFETTI = "<:QConfetti:1500516725618118736>"
Q_CONNECT_WHITE = "<:QConnectWhite:1500516729384603708>"
Q_CONNECT_BLACK = "<:QConnectBlack:1500516727547498706>"
Q_EDIT = "<:QEdit:1500516736942866653>"
Q_GAME_O = "<:QGameO:1500516742865227778>"
Q_GAME_TIMEOUT = "<:QGameTimeout:1500516745088077864>"
Q_GAME_WIN = "<:QGameWin:1500516747369910302>"
Q_GAME_X = "<:QGameX:1500516749781504041>"
Q_GIFT = "<:QGift:1500516751467872326>"
Q_HAMMER = "<:QHammer:1500516755301335161>"
Q_IMAGE = "<:QImage:1500516761097863348>"
Q_LOCK = "<:QLock:1500516764369424454>"
Q_PERMISSIONS = "<:QPermissions:1500516773475123412>"
Q_POLL = "<:QPoll:1500516775182336050>"
Q_REACTION = "<:QReaction:1500516779355668573>"
Q_REJECT = "<:QReject:1500516781931106344>"
Q_ROLES = "<:QRoles:1500516783570948330>"
Q_SLEEP = "<:QSleep:1500516788926939220>"
Q_STREAK_FIRE = "<:QStreakFire:1500516793020711035>"
Q_TARGET = "<:QTarget:1500516799710761021>"
Q_THINKING = "<:QThinking:1500516802008973505>"
Q_TIMEOUT = "<:QTimeout:1500516806119522610>"
Q_TRASH = "<:QTrash:1500516810909290716>"
Q_USER_EDIT = "<:QUserEdit:1500516813119946992>"
Q_VOICE = "<:QVoice:1500516816701886535>"
Q_WARNING = "<:QWarning:1500516819604209704>"
Q_ROULETTE_RED = "<:QRouletteRed:1500878371293495346>"
Q_ROULETTE_BLACK = "<:QRouletteBlack:1500878367174557706>"
Q_ROULETTE_GREEN = "<:QRouletteGreen:1500878369271840891>"
Q_SLOT_STAR = "<:QSlotStar:1500872961702363229>"
Q_SLOT_DIAMOND = "<:QSlotDiamond:1500872954886619317>"
Q_SLOT_CROWN = "<:QSlotCrown:1500872952240144534>"
Q_SLOT_JACKPOT = "<:QSlotJackpot:1500872957348806816>"
Q_SCRATCH_MARK = "<:QScratchMark:1500872869427810304>"
Q_MS_HIDDEN = "<:QMineTile:1500878239659200602>"
Q_MS_CURSOR = "<:QMineCursor:1500878238174417087>"
Q_CARD_SPADE = "<:QCardSpade:1500873118011621437>"
Q_CARD_HEART = "<:QCardHeart:1500873114400198779>"
Q_CARD_DIAMOND = "<:QCardDiamond:1500873111459991754>"
Q_CARD_CLUB = "<:QCardClub:1500873108960182434>"
Q_WHEEL_RED = "<:QWheelRed:1500878498913452133>"
Q_WHEEL_BLUE = "<:QWheelBlue:1500878487249096905>"
Q_WHEEL_GREEN = "<:QWheelGreen:1500878491753906306>"
Q_WHEEL_ORANGE = "<:QWheelOrange:1500878493309866206>"
Q_WHEEL_PURPLE = "<:QWheelPurple:1500878497118289930>"
Q_WHEEL_GOLD = "<:QWheelGold:1500878489430261780>"
Q_WHEEL_BLANK = "<:QWheelBlank:1500878485378564308>"
Q_WHEEL_PINK = "<:QWheelPink:1500878495159685341>"
CHAT_XP_COOLDOWN_SECS = 60
LEVEL_REWARD_BASE = 300_000
LEVEL_REWARD_STEP = 50_000
TRANSFER_TAX_RATE = 0.03
LOTTERY_TICKET_COST = 100_000
LOTTERY_HOUSE_CUT = 0.10
MAIN_QUESTS = {
    "daily_30": {
        "name": "Daily Devotee",
        "description": "Claim daily 30 days in a row.",
        "field": "daily_streak",
        "target": 30,
        "reward": 750_000,
    },
    "weekly_8": {
        "name": "Eight-Week Regular",
        "description": "Claim weekly 8 weeks in a row.",
        "field": "weekly_streak",
        "target": 8,
        "reward": 2_000_000,
    },
    "monthly_5": {
        "name": "Five-Month Monarch",
        "description": "Claim monthly 5 months in a row.",
        "field": "monthly_streak",
        "target": 5,
        "reward": 10_000_000,
    },
}
QUEST_POOLS = {
    "daily": [
        ("Warm Up", "Send 10 XP-eligible messages.", "messages_sent", 10, 75_000),
        ("Small Saver", "Reach 250,000 quesos.", "balance", 250_000, 100_000),
        ("Fresh Profile", "Reach level 3.", "level", 3, 125_000),
        ("Tiny Investor", "Own 1 shop item.", "items", 1, 150_000),
    ],
    "weekly": [
        ("Chatterbox", "Send 75 XP-eligible messages.", "messages_sent", 75, 500_000),
        ("Millionaire Mood", "Reach 1,000,000 quesos.", "balance", 1_000_000, 650_000),
        ("Level Climber", "Reach level 10.", "level", 10, 800_000),
        ("Collector", "Own 5 shop items.", "items", 5, 1_000_000),
    ],
    "monthly": [
        ("Server Legend", "Send 300 XP-eligible messages.", "messages_sent", 300, 2_000_000),
        ("Treasure Room", "Reach 10,000,000 quesos.", "balance", 10_000_000, 3_000_000),
        ("Ascended", "Reach level 25.", "level", 25, 4_000_000),
        ("Shop Royalty", "Own 15 shop items.", "items", 15, 5_000_000),
    ],
}
SHOP_ITEMS = {
    "lucky_charm": {
        "category": "Gambling",
        "name": "Lucky Charm",
        "emoji": Q_LUCKY_CHARM,
        "cost": 500_000,
        "max_qty": 10,
        "description": "+1% gambling payout on wins per charm.",
    },
    "xp_tonic": {
        "category": "Leveling",
        "name": "XP Tonic",
        "emoji": Q_XP_TONIC,
        "cost": 350_000,
        "max_qty": 5,
        "description": "+5% chat XP per tonic.",
    },
    "queso_magnet": {
        "category": "Leveling",
        "name": "Queso Magnet",
        "emoji": Q_QUESO_MAGNET,
        "cost": 900_000,
        "max_qty": 5,
        "description": "+5% level-up queso rewards per magnet.",
    },
    "daily_spice": {
        "category": "Claims",
        "name": "Daily Spice",
        "emoji": Q_DAILY_SPICE,
        "cost": 250_000,
        "max_qty": 10,
        "description": "+2% daily, weekly, and monthly claim rewards per spice.",
    },
    "streak_polish": {
        "category": "Gambling",
        "name": "Streak Polish",
        "emoji": Q_STREAK_POLISH,
        "cost": 650_000,
        "max_qty": 8,
        "description": "+0.5% gambling payout per polish. Stacks with Lucky Charm.",
    },
    "gold_badge": {
        "category": "Cosmetics",
        "name": "Gold Badge",
        "emoji": Q_GOLD_BADGE,
        "cost": 1_000_000,
        "max_qty": 1,
        "description": "A profile badge for people with taste and dangerous levels of queso.",
    },
    "high_roller": {
        "category": "Cosmetics",
        "name": "High Roller Title",
        "emoji": Q_HIGH_ROLLER,
        "cost": 2_500_000,
        "max_qty": 1,
        "description": "A profile title shown on `.profile`.",
    },
    "velvet_frame": {
        "category": "Cosmetics",
        "name": "Velvet Profile Frame",
        "emoji": Q_VELVET_FRAME,
        "cost": 1_750_000,
        "max_qty": 1,
        "description": "Adds a velvet profile flair on `.profile`.",
    },
    "ticket_charm": {
        "category": "Lottery",
        "name": "Ticket Charm",
        "emoji": Q_TICKET_CHARM,
        "cost": 1_200_000,
        "max_qty": 5,
        "description": "+2% bonus lottery tickets per charm when buying tickets.",
    },
    "cooldown_clock": {
        "category": "Utility",
        "name": "Cooldown Clock",
        "emoji": Q_COOLDOWN_CLOCK,
        "cost": 1_500_000,
        "max_qty": 5,
        "description": "-4% gambling cooldown per clock.",
    },
    "royal_crown": {
        "category": "Cosmetics",
        "name": "Royal Q Crown",
        "emoji": Q_ROYAL_CROWN,
        "cost": 5_000_000,
        "max_qty": 1,
        "description": "Upgrades your profile title to Royal High Roller.",
    },
}

economy_log_callback = None
lottery_task = None
db_keepalive_task = None
lottery_view_registered = False

# --- Cooldown tracking ---
_cooldowns = {}  # {(user_id, command): timestamp}
_command_cooldowns = {}  # {user_id: timestamp}
QUEWO_COOLDOWN_EXEMPT = {
    "bal", "profile", "quests", "shop", "cooldowns", "transactions",
    "lottery", "lotterystats", "daily", "weekly", "monthly",
    "econhelp", "economyhelp", "quewohelp", "ehelp", "explain",
}

def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor, connect_timeout=5)

def ping_db():
    global db_ready
    if not os.getenv("DATABASE_URL"):
        return False

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        db_ready = True
        return True
    except Exception as e:
        db_ready = False
        print(f"Database keep-alive failed: {type(e).__name__} - {e}")
        return False

def init_db():
    global db_ready
    if not os.getenv("DATABASE_URL"):
        print("⚠️ DATABASE_URL not set - Quewo system disabled")
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
                    level INTEGER DEFAULT 1,
                    xp BIGINT DEFAULT 0,
                    messages_sent BIGINT DEFAULT 0,
                    last_xp TIMESTAMP,
                    inventory TEXT[] DEFAULT '{}',
                    achievements TEXT[] DEFAULT '{}',
                    quest_claims TEXT[] DEFAULT '{}',
                    steal_blacklist BIGINT[] DEFAULT '{}'
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS economy_transactions (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    kind TEXT NOT NULL,
                    amount BIGINT NOT NULL,
                    note TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lottery_config (
                    guild_id BIGINT PRIMARY KEY,
                    channel_id BIGINT NOT NULL,
                    thread_id BIGINT,
                    message_id BIGINT,
                    role_id BIGINT,
                    period_seconds BIGINT NOT NULL,
                    next_draw TIMESTAMP NOT NULL,
                    pot BIGINT NOT NULL DEFAULT 0,
                    ticket_cost BIGINT NOT NULL DEFAULT 100000,
                    house_cut DOUBLE PRECISION NOT NULL DEFAULT 0.10
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lottery_tickets (
                    guild_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    tickets INTEGER NOT NULL DEFAULT 0,
                    spent BIGINT NOT NULL DEFAULT 0,
                    pot_add BIGINT NOT NULL DEFAULT 0,
                    PRIMARY KEY (guild_id, user_id)
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
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS level INTEGER DEFAULT 1")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS xp BIGINT DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS messages_sent BIGINT DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS last_xp TIMESTAMP")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS inventory TEXT[] DEFAULT '{}'")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS achievements TEXT[] DEFAULT '{}'")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS quest_claims TEXT[] DEFAULT '{}'")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS steal_blacklist BIGINT[] DEFAULT '{}'")
            cur.execute("ALTER TABLE lottery_config ADD COLUMN IF NOT EXISTS thread_id BIGINT")
            cur.execute("ALTER TABLE lottery_config ADD COLUMN IF NOT EXISTS message_id BIGINT")
            cur.execute("ALTER TABLE lottery_config ADD COLUMN IF NOT EXISTS role_id BIGINT")
            cur.execute("ALTER TABLE lottery_config ADD COLUMN IF NOT EXISTS ticket_cost BIGINT NOT NULL DEFAULT 100000")
            cur.execute("ALTER TABLE lottery_config ADD COLUMN IF NOT EXISTS house_cut DOUBLE PRECISION NOT NULL DEFAULT 0.10")
            cur.execute("ALTER TABLE lottery_tickets ADD COLUMN IF NOT EXISTS spent BIGINT NOT NULL DEFAULT 0")
            cur.execute("ALTER TABLE lottery_tickets ADD COLUMN IF NOT EXISTS pot_add BIGINT NOT NULL DEFAULT 0")
            conn.commit()
            cur.close()
            conn.close()
            db_ready = True
            print(f"✅ Quewo DB initialized (PostgreSQL) on attempt {attempt}")
            return
        except psycopg2.OperationalError as e:
            if attempt < 10:
                print(f"⏳ Quewo DB attempt {attempt}/10 failed (DB starting up), retrying in 5s...")
                time.sleep(5)
            else:
                print(f"❌ Quewo DB init failed after 10 attempts: {e}")
                db_ready = False
        except Exception as e:
            print(f"❌ Quewo DB init failed: {e}")
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

def log_transaction(user_id, kind, amount, note=""):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, %s, %s, %s)",
            (user_id, kind, amount, note)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Transaction log failed: {type(e).__name__} - {e}")

def bulk_add_users(user_ids, amount, actor_id, note):
    unique_ids = sorted(set(int(user_id) for user_id in user_ids))
    if not unique_ids:
        return 0

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO economy (user_id, balance)
        SELECT user_id, 0 FROM unnest(%s::bigint[]) AS user_id
        ON CONFLICT (user_id) DO NOTHING
        """,
        (unique_ids,)
    )
    cur.execute(
        """
        UPDATE economy
        SET balance = balance + %s,
            total_earned = total_earned + %s
        WHERE user_id = ANY(%s::bigint[])
        """,
        (amount, amount, unique_ids)
    )
    cur.execute(
        """
        INSERT INTO economy_transactions (user_id, kind, amount, note)
        SELECT user_id, 'owner_add', %s, %s FROM unnest(%s::bigint[]) AS user_id
        """,
        (amount, f"Bulk by {actor_id}: {note}", unique_ids)
    )
    conn.commit()
    cur.close()
    conn.close()
    return len(unique_ids)

def bulk_set_balances(user_ids, amount, actor_id, note):
    unique_ids = sorted(set(int(user_id) for user_id in user_ids))
    if not unique_ids:
        return 0

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO economy (user_id, balance)
        SELECT user_id, 0 FROM unnest(%s::bigint[]) AS user_id
        ON CONFLICT (user_id) DO NOTHING
        """,
        (unique_ids,)
    )
    cur.execute(
        """
        UPDATE economy
        SET balance = %s
        WHERE user_id = ANY(%s::bigint[])
        """,
        (amount, unique_ids)
    )
    cur.execute(
        """
        INSERT INTO economy_transactions (user_id, kind, amount, note)
        SELECT user_id, 'owner_set', %s, %s FROM unnest(%s::bigint[]) AS user_id
        """,
        (amount, f"Set by {actor_id}: {note}", unique_ids)
    )
    conn.commit()
    cur.close()
    conn.close()
    return len(unique_ids)

def get_recent_transactions(user_id, limit=10):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT kind, amount, note, created_at FROM economy_transactions WHERE user_id = %s ORDER BY id DESC LIMIT %s",
        (user_id, limit)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def period_key(period):
    now = datetime.now(timezone.utc)
    if period == "daily":
        return now.strftime("%Y-%m-%d")
    if period == "weekly":
        year, week, _ = now.isocalendar()
        return f"{year}-W{week:02d}"
    return now.strftime("%Y-%m")

def selected_period_quests(user_id, period):
    pool = QUEST_POOLS[period]
    seed = f"{user_id}:{period}:{period_key(period)}"
    rng = random.Random(seed)
    count = min(3, len(pool))
    return rng.sample(pool, count)

def quest_progress(data, metric):
    if metric == "items":
        return len(user_inventory(data))
    return int(data.get(metric) or 0)

def quest_claim_id(period, quest_name):
    return f"{period}:{period_key(period)}:{quest_name}"

def achievement_ids(data):
    return list(data.get("achievements") or [])

def quest_claim_ids(data):
    return list(data.get("quest_claims") or [])

def maybe_award_main_quest(user_id, data, quest_id):
    quest = MAIN_QUESTS[quest_id]
    achievements = achievement_ids(data)
    if quest_id in achievements:
        return 0
    if int(data.get(quest["field"]) or 0) < quest["target"]:
        return 0

    achievements.append(quest_id)
    reward = quest["reward"]
    update_user(
        user_id,
        achievements=achievements,
        balance=data["balance"] + reward,
        total_earned=data["total_earned"] + reward
    )
    log_transaction(user_id, "achievement", reward, quest["name"])
    return reward

def period_seconds_from_text(raw):
    raw = raw.strip().casefold()
    matches = re.findall(r"(\d+)\s*([dhm])", raw)
    if not matches:
        return None
    total = 0
    for value, unit in matches:
        value = int(value)
        if unit == "d":
            total += value * 86400
        elif unit == "h":
            total += value * 3600
        elif unit == "m":
            total += value * 60
    return total if total >= 300 else None

def get_lottery_config(guild_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM lottery_config WHERE guild_id = %s", (guild_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def lottery_ticket_cost(config):
    return int(config.get("ticket_cost") or LOTTERY_TICKET_COST)

def lottery_house_cut(config):
    return float(config.get("house_cut") if config.get("house_cut") is not None else LOTTERY_HOUSE_CUT)

def save_lottery_config(guild_id, channel_id, period_seconds, next_draw, pot=0, thread_id=None, role_id=None, ticket_cost=None, house_cut=None, message_id=None):
    ticket_cost = ticket_cost if ticket_cost is not None else LOTTERY_TICKET_COST
    house_cut = house_cut if house_cut is not None else LOTTERY_HOUSE_CUT
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO lottery_config (guild_id, channel_id, thread_id, message_id, role_id, period_seconds, next_draw, pot, ticket_cost, house_cut)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (guild_id) DO UPDATE SET
            channel_id = EXCLUDED.channel_id,
            thread_id = EXCLUDED.thread_id,
            message_id = EXCLUDED.message_id,
            role_id = EXCLUDED.role_id,
            period_seconds = EXCLUDED.period_seconds,
            next_draw = EXCLUDED.next_draw,
            ticket_cost = EXCLUDED.ticket_cost,
            house_cut = EXCLUDED.house_cut,
            pot = lottery_config.pot
        """,
        (guild_id, channel_id, thread_id, message_id, role_id, period_seconds, next_draw, pot, ticket_cost, house_cut)
    )
    conn.commit()
    cur.close()
    conn.close()

def update_lottery_config(guild_id, **kwargs):
    if not kwargs:
        return
    conn = get_db_connection()
    cur = conn.cursor()
    set_clauses = []
    values = []
    for key, value in kwargs.items():
        set_clauses.append(f"{key} = %s")
        values.append(value)
    values.append(guild_id)
    cur.execute(f"UPDATE lottery_config SET {', '.join(set_clauses)} WHERE guild_id = %s", values)
    conn.commit()
    cur.close()
    conn.close()

def lottery_ticket_rows(guild_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id, tickets, spent, pot_add FROM lottery_tickets WHERE guild_id = %s AND tickets > 0", (guild_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def all_lotteries_due():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM lottery_config WHERE next_draw <= %s", (datetime.now(timezone.utc),))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def all_lottery_configs():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM lottery_config")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def add_lottery_tickets(guild_id, user_id, tickets, pot_add, spent):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO lottery_tickets (guild_id, user_id, tickets, spent, pot_add)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (guild_id, user_id) DO UPDATE SET
            tickets = lottery_tickets.tickets + EXCLUDED.tickets,
            spent = lottery_tickets.spent + EXCLUDED.spent,
            pot_add = lottery_tickets.pot_add + EXCLUDED.pot_add
        """,
        (guild_id, user_id, tickets, spent, pot_add)
    )
    cur.execute("UPDATE lottery_config SET pot = pot + %s WHERE guild_id = %s", (pot_add, guild_id))
    conn.commit()
    cur.close()
    conn.close()

def bulk_adjust_lottery_tickets(guild_id, user_ids, tickets, mode, actor_id):
    unique_ids = sorted(set(int(user_id) for user_id in user_ids))
    if not unique_ids:
        return 0
    if mode not in {"add", "set"}:
        raise ValueError("mode must be add or set")

    conn = get_db_connection()
    cur = conn.cursor()
    if mode == "add":
        cur.execute(
            """
            INSERT INTO lottery_tickets (guild_id, user_id, tickets, spent, pot_add)
            SELECT %s, user_id, %s, 0, 0 FROM unnest(%s::bigint[]) AS user_id
            ON CONFLICT (guild_id, user_id) DO UPDATE SET
                tickets = lottery_tickets.tickets + EXCLUDED.tickets
            """,
            (guild_id, tickets, unique_ids)
        )
        kind = "lottery_admin_add"
        note = f"Added {tickets} free tickets by {actor_id}"
    else:
        cur.execute(
            """
            INSERT INTO lottery_tickets (guild_id, user_id, tickets, spent, pot_add)
            SELECT %s, user_id, %s, 0, 0 FROM unnest(%s::bigint[]) AS user_id
            ON CONFLICT (guild_id, user_id) DO UPDATE SET
                tickets = EXCLUDED.tickets
            """,
            (guild_id, tickets, unique_ids)
        )
        kind = "lottery_admin_set"
        note = f"Set tickets to {tickets} by {actor_id}"
    cur.execute(
        """
        INSERT INTO economy_transactions (user_id, kind, amount, note)
        SELECT user_id, %s, %s, %s FROM unnest(%s::bigint[]) AS user_id
        """,
        (kind, tickets, note, unique_ids)
    )
    conn.commit()
    cur.close()
    conn.close()
    return len(unique_ids)

def reset_lottery_round(guild_id, next_draw):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM lottery_tickets WHERE guild_id = %s", (guild_id,))
    cur.execute("UPDATE lottery_config SET pot = 0, next_draw = %s WHERE guild_id = %s", (next_draw, guild_id))
    conn.commit()
    cur.close()
    conn.close()

def refund_lottery_round(guild_id, rows, ticket_cost=None):
    refunds = []
    fallback_ticket_cost = ticket_cost or LOTTERY_TICKET_COST
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for row in rows:
            user_id = int(row["user_id"])
            spent = int(row.get("spent") or 0)
            if spent <= 0:
                spent = int(row.get("tickets") or 0) * int(fallback_ticket_cost)
            if spent <= 0:
                continue
            cur.execute(
                """
                INSERT INTO economy (user_id, balance)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
                    balance = economy.balance + EXCLUDED.balance
                """,
                (user_id, spent)
            )
            cur.execute(
                """
                INSERT INTO economy_transactions (user_id, kind, amount, note)
                VALUES (%s, %s, %s, %s)
                """,
                (user_id, "lottery_refund", spent, f"Guild {guild_id}; minimum players not met")
            )
            refunds.append((user_id, spent))
        conn.commit()
    finally:
        cur.close()
        conn.close()
    return refunds

def delete_lottery_config(guild_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM lottery_tickets WHERE guild_id = %s", (guild_id,))
    cur.execute("DELETE FROM lottery_config WHERE guild_id = %s", (guild_id,))
    conn.commit()
    cur.close()
    conn.close()

def set_lottery_role(guild_id, role_id):
    update_lottery_config(guild_id, role_id=role_id)

def lottery_instructions(period_seconds, ticket_cost=LOTTERY_TICKET_COST, house_cut=LOTTERY_HOUSE_CUT):
    return (
        f"{Q_TICKET} **Lottery Tickets**\n"
        "Prize: **the full current pot** goes to one winner each draw.\n"
        "Buy tickets with the buttons on the lottery panel.\n"
        f"Each ticket costs **{format_balance(ticket_cost)}**.\n"
        f"**{house_cut * 100:.1f}%** of ticket sales is burned and the rest goes into the pot.\n"
        "Each ticket is one entry; more tickets means better odds.\n"
        "At least **5 unique players** must join or the draw restarts with no winner.\n"
        f"Draws happen every **{format_duration(period_seconds)}**."
    )

async def prepare_lottery_channel(guild, channel, period_seconds, ticket_cost=LOTTERY_TICKET_COST, house_cut=LOTTERY_HOUSE_CUT):
    everyone = guild.default_role
    try:
        overwrite = channel.overwrites_for(everyone)
        overwrite.send_messages = False
        overwrite.create_public_threads = False
        overwrite.create_private_threads = False
        await channel.set_permissions(everyone, overwrite=overwrite, reason="Lottery channel setup")
    except Exception as e:
        print(f"Lottery channel lock skipped: {type(e).__name__} - {e}")
    return channel

async def recreate_lottery_role(guild, old_role_id=None):
    if old_role_id:
        old_role = guild.get_role(old_role_id)
        if old_role:
            try:
                await old_role.delete(reason="New lottery round")
            except Exception as e:
                print(f"Lottery old role delete skipped: {type(e).__name__} - {e}")

    try:
        return await guild.create_role(
            name="Lottery Participants",
            mentionable=True,
            reason="Lottery participant ping role"
        )
    except Exception as e:
        print(f"Lottery role create failed: {type(e).__name__} - {e}")
        return None

async def assign_lottery_role(guild, user_id, role_id):
    if not role_id:
        return
    member = guild.get_member(user_id)
    role = guild.get_role(role_id)
    if not member or not role:
        return
    try:
        await member.add_roles(role, reason="Bought lottery tickets")
    except Exception as e:
        print(f"Lottery role assign skipped: {type(e).__name__} - {e}")

def lottery_role_mention(config):
    return f"<@&{config['role_id']}>" if config.get("role_id") else ""

def build_lottery_embed(guild, config):
    rows = lottery_ticket_rows(config["guild_id"])
    total_tickets = sum(int(row["tickets"]) for row in rows)
    unique_players = len(rows)
    ticket_cost = lottery_ticket_cost(config)
    house_cut = lottery_house_cut(config)
    next_draw = config["next_draw"]
    if next_draw.tzinfo is None:
        next_draw = next_draw.replace(tzinfo=timezone.utc)

    embed = discord.Embed(
        title=f"{QOIN_CHEST} Lottery",
        description=(
            f"{Q_TICKET} Use the buttons below to buy tickets.\n"
            "Button confirmations are private, so the channel stays clean."
        ),
        color=discord.Color.gold()
    )
    embed.add_field(name="Prize / Current Pot", value=format_balance(config["pot"]), inline=True)
    embed.add_field(name=f"{Q_TICKET} Tickets", value=f"{total_tickets:,}", inline=True)
    embed.add_field(name="Players", value=f"{unique_players:,}/5 minimum", inline=True)
    embed.add_field(name=f"{Q_TICKET} Ticket Price", value=format_balance(ticket_cost), inline=True)
    embed.add_field(name="House Cut", value=f"{house_cut * 100:.1f}% burned", inline=True)
    embed.add_field(name="Next Draw", value=f"<t:{int(next_draw.timestamp())}:R>", inline=True)
    if config.get("role_id"):
        embed.add_field(name="Participant Role", value=f"<@&{config['role_id']}>", inline=True)

    if rows:
        leaders = []
        for index, row in enumerate(sorted(rows, key=lambda row: row["tickets"], reverse=True)[:5], 1):
            odds = (int(row["tickets"]) / total_tickets * 100) if total_tickets else 0
            leaders.append(f"{index}. {user_mention(row['user_id'])} - **{int(row['tickets']):,}** tickets ({odds:.1f}%)")
        embed.add_field(name=f"{Q_TICKET} Top Holders", value="\n".join(leaders), inline=False)
    else:
        embed.add_field(name=f"{Q_TICKET} Top Holders", value="No tickets bought this round.", inline=False)

    embed.set_footer(text="At least 5 unique players are needed, or tickets are refunded.")
    return embed

def buy_lottery_tickets_sync(guild_id, user_id, tickets):
    if tickets <= 0:
        return {"ok": False, "message": f"{Q_DENIED} Ticket amount must be positive."}

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM lottery_config WHERE guild_id = %s FOR UPDATE", (guild_id,))
        config = cur.fetchone()
        if config is None:
            conn.rollback()
            return {"ok": False, "message": "Lottery is not set up yet."}

        ticket_cost = lottery_ticket_cost(config)
        house_cut = lottery_house_cut(config)
        total_cost = tickets * ticket_cost
        pot_add = int(total_cost * (1 - house_cut))
        burned = total_cost - pot_add

        cur.execute(
            "INSERT INTO economy (user_id, balance) VALUES (%s, 0) "
            "ON CONFLICT (user_id) DO NOTHING",
            (user_id,)
        )
        cur.execute("SELECT * FROM economy WHERE user_id = %s FOR UPDATE", (user_id,))
        data = cur.fetchone()
        if data["balance"] < total_cost:
            conn.rollback()
            return {
                "ok": False,
                "message": f"{Q_DENIED} You need {format_balance(total_cost)}, but you only have {format_balance(data['balance'])}.",
            }

        bonus_tickets = int(tickets * item_bonus(data, "ticket_charm", 0.02, 5))
        total_entries = tickets + bonus_tickets
        new_balance = data["balance"] - total_cost
        new_pot = int(config["pot"] or 0) + pot_add

        cur.execute("UPDATE economy SET balance = %s WHERE user_id = %s", (new_balance, user_id))
        cur.execute(
            """
            INSERT INTO lottery_tickets (guild_id, user_id, tickets, spent, pot_add)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET
                tickets = lottery_tickets.tickets + EXCLUDED.tickets,
                spent = lottery_tickets.spent + EXCLUDED.spent,
                pot_add = lottery_tickets.pot_add + EXCLUDED.pot_add
            """,
            (guild_id, user_id, total_entries, total_cost, pot_add)
        )
        cur.execute("UPDATE lottery_config SET pot = %s WHERE guild_id = %s", (new_pot, guild_id))
        cur.execute(
            "INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, %s, %s, %s)",
            (user_id, "lottery_tickets", -total_cost, f"{tickets} tickets; {bonus_tickets} bonus; {burned} burned")
        )
        conn.commit()
        config["pot"] = new_pot
        return {
            "ok": True,
            "config": config,
            "tickets": tickets,
            "bonus_tickets": bonus_tickets,
            "total_entries": total_entries,
            "total_cost": total_cost,
            "pot_add": pot_add,
            "burned": burned,
            "new_balance": new_balance,
            "new_pot": new_pot,
        }
    finally:
        cur.close()
        conn.close()

def lottery_purchase_message(result):
    return (
        f"{Q_TICKET} Bought **{result['tickets']:,}** lottery tickets for **{format_balance(result['total_cost'])}**.\n"
        f"Bonus Tickets: **+{result['bonus_tickets']:,}** | Total Entries: **{result['total_entries']:,}**\n"
        f"Prize Pot +**{format_balance(result['pot_add'])}** | Burned **{format_balance(result['burned'])}**\n"
        f"Current Prize: **{format_balance(result['new_pot'])}**\n"
        f"New Balance: **{format_balance(result['new_balance'])}**"
    )

async def refresh_lottery_message(guild, config=None, create_if_missing=True):
    if not guild:
        return None
    if config is None:
        config = await asyncio.to_thread(get_lottery_config, guild.id)
    if not config:
        return None

    channel = guild.get_channel(config["channel_id"])
    if channel is None and bot:
        try:
            channel = await bot.fetch_channel(config["channel_id"])
        except Exception:
            channel = None
    if not channel:
        return None

    embed = await asyncio.to_thread(build_lottery_embed, guild, config)
    view = LotteryPanelView()
    message = None
    message_id = config.get("message_id")
    if message_id:
        try:
            message = await channel.fetch_message(message_id)
            await message.edit(embed=embed, view=view, allowed_mentions=discord.AllowedMentions.none())
            return message
        except Exception as e:
            print(f"Lottery panel refresh will recreate message: {type(e).__name__} - {e}")

    if not create_if_missing:
        return None
    message = await channel.send(embed=embed, view=view, allowed_mentions=discord.AllowedMentions.none())
    await asyncio.to_thread(update_lottery_config, guild.id, message_id=message.id)
    return message

async def clear_lottery_channel(channel):
    if channel is None:
        return 0
    try:
        deleted = await channel.purge(limit=None, reason="Lottery round finished")
        return len(deleted)
    except Exception as e:
        print(f"Lottery channel purge failed, trying manual delete: {type(e).__name__} - {e}")

    deleted_count = 0
    try:
        async for message in channel.history(limit=None):
            try:
                await message.delete()
                deleted_count += 1
                if deleted_count % 10 == 0:
                    await asyncio.sleep(1)
            except Exception as e:
                print(f"Lottery message delete skipped: {type(e).__name__} - {e}")
    except Exception as e:
        print(f"Lottery channel manual clear failed: {type(e).__name__} - {e}")
    return deleted_count

async def restore_lottery_panels():
    global lottery_view_registered
    if not bot:
        return
    if not lottery_view_registered:
        bot.add_view(LotteryPanelView())
        lottery_view_registered = True
    if not db_ready:
        return
    try:
        configs = await asyncio.to_thread(all_lottery_configs)
    except Exception as e:
        print(f"Lottery panel restore failed: {type(e).__name__} - {e}")
        return
    for config in configs:
        guild = bot.get_guild(config["guild_id"])
        if guild:
            await refresh_lottery_message(guild, config)

async def handle_lottery_purchase(interaction, tickets):
    if interaction.guild is None:
        await interaction.response.send_message(f"{Q_DENIED} Lottery tickets only work in servers.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        result = await asyncio.to_thread(buy_lottery_tickets_sync, interaction.guild.id, interaction.user.id, tickets)
        if not result.get("ok"):
            await interaction.followup.send(result["message"], ephemeral=True)
            return
        await assign_lottery_role(interaction.guild, interaction.user.id, result["config"].get("role_id"))
        await refresh_lottery_message(interaction.guild, result["config"])
        await interaction.followup.send(lottery_purchase_message(result), ephemeral=True)
    except Exception as e:
        print(f"Lottery button purchase failed: {type(e).__name__} - {e}")
        await interaction.followup.send(f"{Q_DENIED} Database unavailable. Try again shortly.", ephemeral=True)

async def send_lottery_stats(interaction):
    if interaction.guild is None:
        await interaction.response.send_message(f"{Q_DENIED} Lottery stats only work in servers.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        config = await asyncio.to_thread(get_lottery_config, interaction.guild.id)
        if config is None:
            await interaction.followup.send("Lottery is not set up yet.", ephemeral=True)
            return
        embed = await asyncio.to_thread(build_lottery_embed, interaction.guild, config)
        rows = await asyncio.to_thread(lottery_ticket_rows, interaction.guild.id)
        own = next((row for row in rows if int(row["user_id"]) == interaction.user.id), None)
        own_text = f"You have **{int(own['tickets']):,}** entries this round." if own else "You have no tickets this round."
        await interaction.followup.send(own_text, embed=embed, ephemeral=True)
    except Exception as e:
        print(f"Lottery stats button failed: {type(e).__name__} - {e}")
        await interaction.followup.send(f"{Q_DENIED} Database unavailable. Try again shortly.", ephemeral=True)

class LotteryCustomAmountModal(discord.ui.Modal, title="Buy Lottery Tickets"):
    amount = discord.ui.TextInput(label="Tickets", placeholder="Example: 25", min_length=1, max_length=8)

    async def on_submit(self, interaction):
        try:
            tickets = int(str(self.amount.value).replace(",", "").strip())
        except ValueError:
            await interaction.response.send_message(f"{Q_DENIED} Enter a whole number of tickets.", ephemeral=True)
            return
        await handle_lottery_purchase(interaction, tickets)

class LotteryPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Buy 1", emoji=Q_TICKET, style=discord.ButtonStyle.green, custom_id="lottery:buy:1")
    async def buy_one(self, interaction, button):
        await handle_lottery_purchase(interaction, 1)

    @discord.ui.button(label="Buy 5", emoji=Q_TICKET, style=discord.ButtonStyle.green, custom_id="lottery:buy:5")
    async def buy_five(self, interaction, button):
        await handle_lottery_purchase(interaction, 5)

    @discord.ui.button(label="Buy 10", emoji=Q_TICKET, style=discord.ButtonStyle.green, custom_id="lottery:buy:10")
    async def buy_ten(self, interaction, button):
        await handle_lottery_purchase(interaction, 10)

    @discord.ui.button(label="Custom", emoji=Q_EDIT, style=discord.ButtonStyle.blurple, custom_id="lottery:buy:custom")
    async def buy_custom(self, interaction, button):
        await interaction.response.send_modal(LotteryCustomAmountModal())

    @discord.ui.button(label="Stats", emoji=QOIN_CHEST, style=discord.ButtonStyle.gray, custom_id="lottery:stats")
    async def stats(self, interaction, button):
        await send_lottery_stats(interaction)

async def announce_lottery_update(guild, config, message):
    channel = guild.get_channel(config["channel_id"]) if guild else None
    if channel is None and bot:
        try:
            channel = await bot.fetch_channel(config["channel_id"])
        except Exception:
            channel = None
    if not channel:
        return
    role_mention = lottery_role_mention(config)
    await refresh_lottery_message(guild, config)
    await channel.send(
        f"{role_mention} {QOIN_CHEST} **Lottery Update**\n{message}".strip(),
        allowed_mentions=discord.AllowedMentions(roles=True)
    )

def award_chat_xp(user_id):
    data = get_user(user_id)
    now = datetime.now(timezone.utc)
    last_xp = data.get("last_xp")
    if last_xp:
        last_xp = last_xp.replace(tzinfo=timezone.utc) if last_xp.tzinfo is None else last_xp
        if (now - last_xp).total_seconds() < CHAT_XP_COOLDOWN_SECS:
            return None

    gained_xp = max(1, int(random.randint(15, 25) * xp_multiplier(data)))
    level = data.get("level") or 1
    xp = (data.get("xp") or 0) + gained_xp
    messages_sent = (data.get("messages_sent") or 0) + 1
    levels_gained = 0
    reward = 0

    while xp >= xp_needed_for_level(level):
        xp -= xp_needed_for_level(level)
        level += 1
        levels_gained += 1
        reward += int(level_reward_for(level) * level_reward_multiplier(data))

    updates = {
        "xp": xp,
        "level": level,
        "messages_sent": messages_sent,
        "last_xp": now,
    }
    if reward:
        updates["balance"] = data["balance"] + reward
        updates["total_earned"] = data["total_earned"] + reward
    update_user(user_id, **updates)
    if reward:
        log_transaction(user_id, "level_reward", reward, f"Reached level {level}")

    return {
        "xp": xp,
        "xp_gained": gained_xp,
        "level": level,
        "levels_gained": levels_gained,
        "reward": reward,
    }

def format_balance(amount):
    return f"{amount:,} {CURRENCY_EMOJI}"

def xp_needed_for_level(level):
    return 100 + max(level - 1, 0) * 75

def level_reward_for(level):
    return LEVEL_REWARD_BASE + max(level - 2, 0) * LEVEL_REWARD_STEP

def user_inventory(data):
    return list(data.get("inventory") or [])

def item_count(data, item_id):
    return user_inventory(data).count(item_id)

def has_item(data, item_id):
    return item_count(data, item_id) > 0

def item_bonus(data, item_id, per_item, max_qty=None):
    qty = item_count(data, item_id)
    if max_qty is not None:
        qty = min(qty, max_qty)
    return qty * per_item

def payout_multiplier(data, streak):
    return (
        1
        + (streak * STREAK_MULTIPLIER)
        + item_bonus(data, "lucky_charm", 0.01, 10)
        + item_bonus(data, "streak_polish", 0.005, 8)
    )

def payout_multiplier_after_win(data, new_streak):
    return (
        1
        + (new_streak * STREAK_MULTIPLIER)
        + item_bonus(data, "lucky_charm", 0.01, 10)
        + item_bonus(data, "streak_polish", 0.005, 8)
    )

def xp_multiplier(data):
    return 1 + item_bonus(data, "xp_tonic", 0.05, 5)

def level_reward_multiplier(data):
    return 1 + item_bonus(data, "queso_magnet", 0.05, 5)

def claim_reward_multiplier(data):
    return 1 + item_bonus(data, "daily_spice", 0.02, 10)

def item_display_name(item):
    emoji = (item.get("emoji") or "").strip()
    return f"{emoji} {item['name']}" if emoji else item["name"]

def item_select_emoji(item):
    emoji = (item.get("emoji") or "").strip()
    if not emoji:
        return None
    try:
        return discord.PartialEmoji.from_str(emoji)
    except Exception:
        return emoji

def owned_item_lines(data):
    inventory = user_inventory(data)
    lines = []
    for item_id, item in SHOP_ITEMS.items():
        qty = inventory.count(item_id)
        if qty:
            suffix = f" x{qty}" if item.get("max_qty", 1) > 1 else ""
            lines.append(f"{item_display_name(item)}{suffix}")
    return lines

def user_mention(user_id):
    return f"<@{user_id}>"

def build_profile_embed(user, data):
    level = data.get("level") or 1
    xp = data.get("xp") or 0
    needed = xp_needed_for_level(level)
    items = owned_item_lines(data)
    title = "Royal High Roller" if has_item(data, "royal_crown") else ("High Roller" if has_item(data, "high_roller") else "Queso Collector")
    if has_item(data, "velvet_frame"):
        title = f"Velvet {title}"
    net = (data.get("total_won") or 0) - (data.get("total_lost") or 0)

    embed = discord.Embed(
        title=f"{Q_XP} Profile",
        description=f"{user_mention(user.id)}\n**{title}**",
        color=discord.Color.gold()
    )
    embed.set_thumbnail(url=user.display_avatar.url)
    embed.add_field(name=f"{QASH_EMOJI} Balance", value=format_balance(data["balance"]), inline=True)
    embed.add_field(name=f"{Q_LEVEL_UP} Level", value=f"{level}", inline=True)
    embed.add_field(name=f"{Q_XP} XP", value=f"{xp:,}/{needed:,}", inline=True)
    embed.add_field(name="Messages", value=f"{data.get('messages_sent') or 0:,}", inline=True)
    embed.add_field(name="Net Gambling", value=format_balance(net), inline=True)
    embed.add_field(name="Items", value=", ".join(items) if items else "None", inline=False)
    return embed

def build_quests_embed(user, data):
    embed = discord.Embed(
        title=f"{Q_QUEST} Quests",
        description=f"{user_mention(user.id)}\nClaim completed quests with the button below.",
        color=discord.Color.blurple()
    )

    main_lines = []
    achievements = achievement_ids(data)
    for quest_id, quest in MAIN_QUESTS.items():
        progress = min(int(data.get(quest["field"]) or 0), quest["target"])
        status = "Claimed" if quest_id in achievements else f"{progress}/{quest['target']}"
        main_lines.append(
            f"**{quest['name']}** - {status}\n"
            f"{quest['description']}\n"
            f"Reward: **{format_balance(quest['reward'])}**"
        )
    embed.add_field(name="Main Quests", value="\n\n".join(main_lines), inline=False)

    claims = quest_claim_ids(data)
    for period in ["daily", "weekly", "monthly"]:
        lines = []
        for quest_name, description, metric, target, reward in selected_period_quests(user.id, period):
            progress = min(quest_progress(data, metric), target)
            claim_id = quest_claim_id(period, quest_name)
            status = "Claimed" if claim_id in claims else f"{progress}/{target}"
            lines.append(
                f"**{quest_name}** - {status}\n"
                f"{description}\n"
                f"Reward: **{format_balance(reward)}**"
            )
        embed.add_field(name=f"{period.title()} Quests", value="\n\n".join(lines), inline=False)
    return embed

def format_duration(seconds):
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs and not parts:
        parts.append(f"{secs}s")
    return " ".join(parts) or "ready"

def claim_cooldown_text(last_claim, cooldown_seconds):
    if not last_claim:
        return "Ready"
    last_claim = last_claim.replace(tzinfo=timezone.utc) if last_claim.tzinfo is None else last_claim
    remaining = cooldown_seconds - (datetime.now(timezone.utc) - last_claim).total_seconds()
    return "Ready" if remaining <= 0 else format_duration(remaining)

def command_cooldown_text(user_id, command):
    last_used = _cooldowns.get((user_id, "quewo"))
    if not last_used:
        return "Ready"
    cooldown = COOLDOWN_SECS
    try:
        data = get_user(user_id) if db_ready else None
        if data:
            cooldown *= max(0.5, 1 - item_bonus(data, "cooldown_clock", 0.04, 5))
    except Exception:
        pass
    remaining = cooldown - (time.time() - last_used)
    return "Ready" if remaining <= 0 else format_duration(remaining)

async def quewo_command_cooldown_check(ctx):
    command_name = ctx.command.name if ctx.command else ""
    if command_name in QUEWO_COOLDOWN_EXEMPT or has_economy_owner_power(ctx.author.id, ctx.guild):
        return True
    now = time.time()
    last_used = _command_cooldowns.get(ctx.author.id)
    cooldown = COOLDOWN_SECS
    if last_used and now - last_used < cooldown:
        ctx.quewo_cooldown_blocked = True
        await ctx.send(f"{Q_TIMER_TICK} Chill for **{cooldown - (now - last_used):.1f}s** before using another Quewo command.")
        return False
    _command_cooldowns[ctx.author.id] = now
    return True

async def send_economy_log(ctx, title, fields, color=discord.Color.gold()):
    if economy_log_callback is None:
        return
    embed = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
    embed.add_field(name="By", value=f"<@{ctx.author.id}> ({ctx.author.id})", inline=False)
    for name, value, inline in fields:
        embed.add_field(name=name, value=value, inline=inline)
    try:
        await economy_log_callback(embed, ctx.guild)
    except Exception as e:
        print(f"Quewo log failed: {type(e).__name__} - {e}")

def is_super_owner(user_id, guild=None):
    super_owner_id = 885548126365171824
    return int(user_id) == super_owner_id

def has_guild_admin_power(user_id, guild=None):
    if guild is None:
        return False
    member = guild.get_member(int(user_id))
    permissions = getattr(member, "guild_permissions", None)
    return bool(permissions and permissions.administrator)

def has_economy_owner_power(user_id, guild=None):
    if is_super_owner(user_id, guild):
        return True
    return guild is not None and (guild.owner_id == int(user_id) or has_guild_admin_power(user_id, guild))

def economy_permission_rank(user_id, guild=None):
    user_id = int(user_id)
    if is_super_owner(user_id, guild):
        return 4
    if guild is not None and guild.owner_id == user_id:
        return 3
    if has_guild_admin_power(user_id, guild):
        return 2
    return 0

def can_economy_act_on(actor_id, target_id, guild=None):
    if int(actor_id) == int(target_id):
        return economy_permission_rank(actor_id, guild) > 0
    return economy_permission_rank(actor_id, guild) > economy_permission_rank(target_id, guild)

def is_superowner_id(user_id):
    return int(user_id) == 885548126365171824

async def resolve_admin_targets(ctx, target):
    if ctx.guild is None:
        raise commands.BadArgument("Bulk economy targets only work in servers.")

    target_key = target.strip()
    is_everyone = target_key in {"@everyone", "@here", str(ctx.guild.default_role.id)}
    if is_everyone:
        members = [member for member in ctx.guild.members if not member.bot]
        return {
            "kind": "everyone",
            "label": "@everyone",
            "log_label": "@everyone",
            "user_ids": [member.id for member in members],
            "member": None,
            "role": None,
        }

    try:
        role = await commands.RoleConverter().convert(ctx, target_key)
    except commands.BadArgument:
        role = None
    if role is not None:
        members = [member for member in role.members if not member.bot]
        return {
            "kind": "role",
            "label": f"members with **{role.name}**",
            "log_label": f"{role.mention} ({role.id})",
            "user_ids": [member.id for member in members],
            "member": None,
            "role": role,
        }

    member = await commands.MemberConverter().convert(ctx, target_key)
    return {
        "kind": "member",
        "label": user_mention(member.id),
        "log_label": f"{user_mention(member.id)} ({member.id})",
        "user_ids": [member.id],
        "member": member,
        "role": None,
    }

def check_cooldown(user_id, command):
    key = (user_id, "quewo")
    now = time.time()
    cooldown = COOLDOWN_SECS
    try:
        data = get_user(user_id) if db_ready else None
        if data:
            cooldown *= max(0.5, 1 - item_bonus(data, "cooldown_clock", 0.04, 5))
    except Exception:
        pass
    if key in _cooldowns:
        elapsed = now - _cooldowns[key]
        if elapsed < cooldown:
            return cooldown - elapsed
    _cooldowns[key] = now
    return 0

def parse_amount(raw, user_id=None, guild=None, balance=None):
    raw_text = str(raw).strip().lower().replace(",", "").replace("_", "")
    if raw_text == "all":
        if balance is None:
            return MAX_BET
        balance = max(0, int(balance))
        return min(balance, MAX_BET)
    multipliers = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000, "bn": 1_000_000_000}
    try:
        suffix = ""
        number = raw_text
        for candidate in sorted(multipliers, key=len, reverse=True):
            if raw_text.endswith(candidate):
                suffix = candidate
                number = raw_text[:-len(candidate)]
                break
        val = int(float(number) * multipliers.get(suffix, 1))
        if user_id is not None and has_economy_owner_power(user_id, guild):
            return val
        return min(val, MAX_BET)
    except (TypeError, ValueError):
        return None

def parse_whole_number(raw):
    raw_text = str(raw).strip().lower().replace(",", "").replace("_", "")
    multipliers = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000, "bn": 1_000_000_000}
    try:
        suffix = ""
        number = raw_text
        for candidate in sorted(multipliers, key=len, reverse=True):
            if raw_text.endswith(candidate):
                suffix = candidate
                number = raw_text[:-len(candidate)]
                break
        return int(float(number) * multipliers.get(suffix, 1))
    except (TypeError, ValueError):
        return None

async def send_nonpositive_amount_error(ctx, raw_amount):
    if str(raw_amount).lower() == "all":
        await ctx.send(f"{Q_DENIED} You don't have any {CURRENCY_EMOJI} to use.")
        return

    await ctx.send(f"{Q_DENIED} Amount must be positive.")

# --- Helpers ---
async def send_error(ctx, text):
    if text == "Database unavailable. Try again shortly.":
        await ensure_db_ready(ctx, force=True)
        return

    try:
        await ctx.send(f"{Q_DENIED} {text}")
    except:
        pass

async def ensure_db_ready(ctx, force=False):
    global db_initializing, db_init_task
    if db_ready and not force:
        return True

    msg = await ctx.send("Loading database")
    frames = ["Loading database.", "Loading database..", "Loading database..."]
    frame_index = 0

    if force or db_init_task is None or db_init_task.done():
        db_initializing = True
        db_init_task = asyncio.create_task(asyncio.to_thread(init_db))

    while not db_init_task.done():
        await asyncio.sleep(1)
        try:
            await msg.edit(content=frames[frame_index % len(frames)])
        except:
            pass
        frame_index += 1

    await db_init_task
    db_initializing = False

    if db_ready:
        await msg.edit(content="Database loaded")
        return True

    await msg.edit(content="Database still loading. Try again shortly.")
    return False

# =====================
# BALANCE + STREAKS
# =====================
@commands.command(aliases=["balance", "wallet", "cash"])
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
            mult = payout_multiplier(data, streak)
            streak_lines += f"\n`{game}` {streak} wins → ×{mult:.2f} payout"

    embed = discord.Embed(
        title=f"{QASH_EMOJI} Balance",
        description=user_mention(user.id),
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

    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command(aliases=["prof", "level", "lvl"])
async def profile(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return

    user = member or ctx.author
    try:
        data = get_user(user.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(embed=build_profile_embed(user, data), allowed_mentions=discord.AllowedMentions.none())

@commands.command()
async def quests(ctx):
    if not await ensure_db_ready(ctx):
        return

    try:
        data = get_user(ctx.author.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    class QuestView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=120)

        async def interaction_check(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Open your own quests with `.quests`.", ephemeral=True)
                return False
            return True

        @discord.ui.button(label="Claim Completed", style=discord.ButtonStyle.success)
        async def claim_completed(self, interaction, button):
            try:
                current = get_user(interaction.user.id)
                total_reward = 0
                achievements = achievement_ids(current)
                claims = quest_claim_ids(current)

                for quest_id, quest in MAIN_QUESTS.items():
                    if quest_id not in achievements and int(current.get(quest["field"]) or 0) >= quest["target"]:
                        achievements.append(quest_id)
                        total_reward += quest["reward"]
                        log_transaction(interaction.user.id, "achievement", quest["reward"], quest["name"])

                for period in ["daily", "weekly", "monthly"]:
                    for quest_name, _, metric, target, reward in selected_period_quests(interaction.user.id, period):
                        claim_id = quest_claim_id(period, quest_name)
                        if claim_id not in claims and quest_progress(current, metric) >= target:
                            claims.append(claim_id)
                            total_reward += reward
                            log_transaction(interaction.user.id, f"{period}_quest", reward, quest_name)

                if total_reward <= 0:
                    await interaction.response.send_message("No completed quests to claim yet.", ephemeral=True)
                    return

                update_user(
                    interaction.user.id,
                    achievements=achievements,
                    quest_claims=claims,
                    balance=current["balance"] + total_reward,
                    total_earned=current["total_earned"] + total_reward
                )
                updated = get_user(interaction.user.id)
            except Exception:
                await interaction.response.send_message(f"{Q_DENIED} Database unavailable. Try again shortly.", ephemeral=True)
                return

            await interaction.response.edit_message(
                content=f"{Q_SUCCESS} Claimed **{format_balance(total_reward)}** in quest rewards.",
                embed=build_quests_embed(interaction.user, updated),
                view=self,
                allowed_mentions=discord.AllowedMentions.none()
            )

        @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary)
        async def refresh(self, interaction, button):
            await interaction.response.edit_message(
                embed=build_quests_embed(interaction.user, get_user(interaction.user.id)),
                view=self,
                allowed_mentions=discord.AllowedMentions.none()
            )

    await ctx.send(embed=build_quests_embed(ctx.author, data), view=QuestView(), allowed_mentions=discord.AllowedMentions.none())

@commands.command()
async def shop(ctx):
    if not await ensure_db_ready(ctx):
        return

    def catalog_embed(selected_item_id=None):
        try:
            data = get_user(ctx.author.id)
        except Exception:
            data = {"balance": 0, "inventory": []}

        embed = discord.Embed(
            title=f"{Q_SHOP} Quewo Shop",
            description=(
                f"{user_mention(ctx.author.id)}\n"
                f"Balance: **{format_balance(data['balance'])}**\n"
                "Select an item below, then press Buy."
            ),
            color=discord.Color.gold()
        )
        for category in sorted({item["category"] for item in SHOP_ITEMS.values()}):
            lines = []
            for item_id, item in SHOP_ITEMS.items():
                if item["category"] != category:
                    continue
                owned = item_count(data, item_id)
                max_qty = item.get("max_qty", 1)
                owned_text = f"{owned}/{max_qty}" if max_qty > 1 else ("owned" if owned else "not owned")
                marker = "→ " if item_id == selected_item_id else ""
                lines.append(
                    f"{marker}**{item_display_name(item)}** - {format_balance(item['cost'])}\n"
                    f"{item['description']}\n"
                    f"Owned: **{owned_text}**"
                )
            embed.add_field(name=category, value="\n\n".join(lines), inline=False)
        return embed

    class ShopQuantityModal(discord.ui.Modal):
        def __init__(self, item_id):
            super().__init__(title=f"Buy {SHOP_ITEMS[item_id]['name']}")
            self.item_id = item_id
            self.quantity = discord.ui.TextInput(
                label="Quantity",
                placeholder="Example: 1",
                default="1",
                min_length=1,
                max_length=6
            )
            self.add_item(self.quantity)

        async def on_submit(self, interaction):
            await interaction.response.defer(ephemeral=True, thinking=True)
            raw_quantity = str(self.quantity.value).strip()
            if not raw_quantity.isdigit():
                await interaction.followup.send(f"{Q_DENIED} Quantity must be a positive whole number.", ephemeral=True)
                return

            quantity = int(raw_quantity)
            if quantity <= 0:
                await interaction.followup.send(f"{Q_DENIED} Quantity must be positive.", ephemeral=True)
                return

            item = SHOP_ITEMS[self.item_id]
            item_name = item_display_name(item)
            try:
                data = get_user(interaction.user.id)
                inventory = user_inventory(data)
                owned = inventory.count(self.item_id)
                max_qty = item.get("max_qty", 1)
                remaining_allowed = max_qty - owned
                if remaining_allowed <= 0:
                    await interaction.followup.send(f"{Q_DENIED} You already own the max amount of **{item_name}**.", ephemeral=True)
                    return
                if quantity > remaining_allowed:
                    await interaction.followup.send(
                        f"{Q_DENIED} You can only buy **{remaining_allowed}** more **{item_name}**.",
                        ephemeral=True
                    )
                    return

                total_cost = item["cost"] * quantity
                if data["balance"] < total_cost:
                    affordable = data["balance"] // item["cost"]
                    await interaction.followup.send(
                        f"{Q_DENIED} That costs **{format_balance(total_cost)}**. You can afford **{affordable}**.",
                        ephemeral=True
                    )
                    return

                inventory.extend([self.item_id] * quantity)
                new_balance = data["balance"] - total_cost
                update_user(interaction.user.id, balance=new_balance, inventory=inventory)
                log_transaction(interaction.user.id, "shop_purchase", -total_cost, f"{quantity}x {item['name']}")
            except Exception:
                await interaction.followup.send(f"{Q_DENIED} Database unavailable. Try again shortly.", ephemeral=True)
                return

            await interaction.followup.send(
                f"{Q_SUCCESS} Bought **{quantity}x {item_name}** for **{format_balance(total_cost)}**.\n"
                f"New Balance: **{format_balance(new_balance)}**",
                ephemeral=True
            )

    class ShopView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=120)
            self.selected_item_id = next(iter(SHOP_ITEMS))
            options = [
                discord.SelectOption(
                    label=item["name"],
                    value=item_id,
                    description=f"{item['category']} | {format_balance(item['cost'])} | max {item.get('max_qty', 1)}",
                    emoji=item_select_emoji(item)
                )
                for item_id, item in SHOP_ITEMS.items()
            ]
            self.item_select = discord.ui.Select(
                placeholder="Choose an item",
                options=options,
                min_values=1,
                max_values=1
            )
            self.item_select.callback = self.select_item
            self.add_item(self.item_select)

        async def interaction_check(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Open your own shop with `.shop`.", ephemeral=True)
                return False
            return True

        async def select_item(self, interaction):
            self.selected_item_id = self.item_select.values[0]
            await interaction.response.edit_message(
                embed=catalog_embed(self.selected_item_id),
                view=self,
                allowed_mentions=discord.AllowedMentions.none()
            )

        @discord.ui.button(label="Buy", style=discord.ButtonStyle.success)
        async def buy_button(self, interaction, button):
            await interaction.response.send_modal(ShopQuantityModal(self.selected_item_id))

    view = ShopView()
    await ctx.send(embed=catalog_embed(view.selected_item_id), view=view, allowed_mentions=discord.AllowedMentions.none())

@commands.command(aliases=["cd", "cooldown"])
async def cooldowns(ctx):
    if not await ensure_db_ready(ctx):
        return

    try:
        data = get_user(ctx.author.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    embed = discord.Embed(title=f"{Q_TIMER} Cooldowns", color=discord.Color.blurple())
    embed.add_field(name="Daily", value=claim_cooldown_text(data.get("last_daily"), 86400), inline=True)
    embed.add_field(name="Weekly", value=claim_cooldown_text(data.get("last_weekly"), 604800), inline=True)
    embed.add_field(name="Monthly", value=claim_cooldown_text(data.get("last_monthly"), 2592000), inline=True)
    for command in ["cf", "roulette", "slots", "blackjack", "scratch", "ms", "wheel"]:
        embed.add_field(name=command, value=command_cooldown_text(ctx.author.id, command), inline=True)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command(aliases=["tx"])
async def transactions(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return

    user = member or ctx.author
    try:
        rows = get_recent_transactions(user.id, 12)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    embed = discord.Embed(
        title=f"{QOIN_TRANSFER} Transactions",
        description=user_mention(user.id),
        color=discord.Color.blurple()
    )
    if not rows:
        embed.add_field(name="Recent", value="No transactions yet.", inline=False)
    for row in rows:
        amount = row["amount"]
        sign = "+" if amount >= 0 else ""
        ts = row["created_at"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        embed.add_field(
            name=f"{row['kind']} | <t:{int(ts.timestamp())}:R>",
            value=f"`{sign}{amount:,}` {CURRENCY_EMOJI}\n{row['note'] or ''}",
            inline=False
        )
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command()
async def lottery(ctx, action: str = None, amount: str = None):
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} Lottery only works in servers.")
        return
    if not await ensure_db_ready(ctx):
        return

    config = get_lottery_config(ctx.guild.id)
    if config is None:
        if not has_economy_owner_power(ctx.author.id, ctx.guild):
            await ctx.send("Lottery is not set up yet. Ask the server owner or an admin to run `.lottery`.")
            return

        await ctx.send("Lottery setup: mention the lottery channel or send its channel ID.")

        def check(message):
            return message.author.id == ctx.author.id and message.channel.id == ctx.channel.id

        try:
            channel_msg = await bot.wait_for("message", check=check, timeout=60)
        except asyncio.TimeoutError:
            await ctx.send("Lottery setup timed out.")
            return

        channel = channel_msg.channel_mentions[0] if channel_msg.channel_mentions else None
        if channel is None and channel_msg.content.strip().isdigit():
            channel = ctx.guild.get_channel(int(channel_msg.content.strip()))
        if channel is None:
            await ctx.send(f"{Q_DENIED} Channel not found. Run `.lottery` again.")
            return
        if not isinstance(channel, discord.TextChannel):
            await ctx.send(f"{Q_DENIED} Pick a normal text channel for lottery setup.")
            return

        await ctx.send("How often should the lottery draw? Example: `1d`, `12h`, `30m`.")
        try:
            period_msg = await bot.wait_for("message", check=check, timeout=60)
        except asyncio.TimeoutError:
            await ctx.send("Lottery setup timed out.")
            return

        period_seconds = period_seconds_from_text(period_msg.content)
        if period_seconds is None:
            await ctx.send(f"{Q_DENIED} Invalid period. Use at least 5 minutes, like `30m`, `12h`, or `1d`.")
            return

        next_draw = datetime.now(timezone.utc) + timedelta(seconds=period_seconds)
        try:
            await prepare_lottery_channel(ctx.guild, channel, period_seconds, LOTTERY_TICKET_COST, LOTTERY_HOUSE_CUT)
            role = await recreate_lottery_role(ctx.guild)
        except Exception as e:
            await ctx.send(f"{Q_DENIED} I couldn't prepare the lottery channel: `{type(e).__name__}`")
            print(f"Lottery setup failed: {type(e).__name__} - {e}")
            return

        save_lottery_config(ctx.guild.id, channel.id, period_seconds, next_draw, thread_id=None, role_id=role.id if role else None, message_id=None)
        panel = await refresh_lottery_message(ctx.guild)
        await ctx.send(
            f"{Q_SUCCESS} Lottery set for {channel.mention}. Draws every **{format_duration(period_seconds)}**.\n"
            f"Prize: **the current pot**. Starting pot: **{format_balance(0)}**.\n"
            f"Tickets cost **{format_balance(LOTTERY_TICKET_COST)}**. House cut: **{LOTTERY_HOUSE_CUT * 100:.1f}%**.\n"
            f"Users can buy tickets with the buttons on {panel.jump_url if panel else 'the lottery panel'}.",
            allowed_mentions=discord.AllowedMentions.none()
        )
        return

    if action and action.casefold() in {"setup", "config"}:
        if not has_economy_owner_power(ctx.author.id, ctx.guild):
            await ctx.send(f"{Q_DENIED} Server owner or admin only.")
            return
        await ctx.send("To reconfigure, delete the current lottery config from the database or ask me to add a reset flow.")
        return

    if action and action.casefold() == "buy":
        await refresh_lottery_message(ctx.guild, config)
        await ctx.send("Use the ticket buttons on the lottery panel to buy lottery tickets.", allowed_mentions=discord.AllowedMentions.none())
        return

    panel = await refresh_lottery_message(ctx.guild, config)
    embed = await asyncio.to_thread(build_lottery_embed, ctx.guild, get_lottery_config(ctx.guild.id))
    if panel:
        embed.add_field(name="Lottery Panel", value=f"[Open Panel]({panel.jump_url})", inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command()
async def editlottery(ctx, setting: str = None, *, value: str = None):
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} Lottery editing only works in servers.")
        return
    if not await ensure_db_ready(ctx):
        return
    if not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} Server owner or admin only.")
        return

    config = get_lottery_config(ctx.guild.id)
    if config is None:
        await ctx.send("Lottery is not set up yet. Run `.lottery` first.")
        return

    if not setting or not value:
        await ctx.send(
            "Use `.editlottery <setting> <value>`.\n"
            "Settings: `price`, `duration`, `cut`, `channel`.\n"
            "Examples: `.editlottery price 250000`, `.editlottery duration 12h`, `.editlottery cut 5`, `.editlottery channel #lottery`"
        )
        return

    setting = setting.casefold()
    updates = {}
    message = ""

    if setting in {"price", "ticket", "ticketprice"}:
        parsed = parse_amount(value, ctx.author.id, ctx.guild, None)
        if parsed is None or parsed <= 0:
            await ctx.send(f"{Q_DENIED} Ticket price must be a positive number.")
            return
        updates["ticket_cost"] = parsed
        message = f"Ticket price set to **{format_balance(parsed)}**."

    elif setting in {"duration", "period", "time"}:
        seconds = period_seconds_from_text(value)
        if seconds is None:
            await ctx.send(f"{Q_DENIED} Invalid duration. Use at least 5 minutes, like `30m`, `12h`, or `1d`.")
            return
        updates["period_seconds"] = seconds
        updates["next_draw"] = datetime.now(timezone.utc) + timedelta(seconds=seconds)
        message = f"Lottery duration set to **{format_duration(seconds)}**. Next draw was reset."

    elif setting in {"cut", "housecut", "tax", "burn"}:
        try:
            cut_percent = float(value.strip().replace("%", ""))
        except ValueError:
            await ctx.send(f"{Q_DENIED} House cut must be a number from 0 to 90.")
            return
        if cut_percent < 0 or cut_percent > 90:
            await ctx.send(f"{Q_DENIED} House cut must be from 0 to 90.")
            return
        updates["house_cut"] = cut_percent / 100
        message = f"House cut set to **{cut_percent:.1f}%**."

    elif setting in {"channel", "chan"}:
        channel = ctx.message.channel_mentions[0] if ctx.message.channel_mentions else None
        if channel is None and value.strip().isdigit():
            channel = ctx.guild.get_channel(int(value.strip()))
        if channel is None or not isinstance(channel, discord.TextChannel):
            await ctx.send(f"{Q_DENIED} Mention a normal text channel or send its channel ID.")
            return
        try:
            await prepare_lottery_channel(
                ctx.guild,
                channel,
                int(config["period_seconds"]),
                lottery_ticket_cost(config),
                lottery_house_cut(config)
            )
        except Exception as e:
            await ctx.send(f"{Q_DENIED} I couldn't prepare that channel: `{type(e).__name__}`")
            return
        updates["channel_id"] = channel.id
        updates["thread_id"] = None
        updates["message_id"] = None
        message = f"Lottery channel moved to {channel.mention}; a fresh ticket panel was posted."

    else:
        await ctx.send(f"{Q_DENIED} Unknown setting. Use `price`, `duration`, `cut`, or `channel`.")
        return

    try:
        update_lottery_config(ctx.guild.id, **updates)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    updated = get_lottery_config(ctx.guild.id)
    if updated:
        await announce_lottery_update(ctx.guild, updated, message)

    await ctx.send(f"{Q_SUCCESS} {message}", allowed_mentions=discord.AllowedMentions.none())

@commands.command()
async def stoplottery(ctx):
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} Lottery stopping only works in servers.")
        return
    if not await ensure_db_ready(ctx):
        return
    if not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} Server owner or admin only.")
        return

    config = get_lottery_config(ctx.guild.id)
    if config is None:
        await ctx.send("Lottery is not set up in this server.")
        return

    rows = lottery_ticket_rows(ctx.guild.id)
    total_tickets = sum(row["tickets"] for row in rows)
    pot = int(config.get("pot") or 0)
    role_deleted = False
    role_note = "No participant role was saved."

    role_id = config.get("role_id")
    if role_id:
        role = ctx.guild.get_role(role_id)
        if role:
            try:
                await role.delete(reason=f"Lottery stopped by {ctx.author.id}")
                role_deleted = True
                role_note = "Participant role deleted."
            except Exception:
                role_note = "Participant role could not be deleted, so it was left alone."
        else:
            role_note = "Participant role was already gone."

    try:
        delete_lottery_config(ctx.guild.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(
        f"{Q_SUCCESS} Lottery stopped for this server.\n"
        f"Cleared Prize Pot: **{format_balance(pot)}**\n"
        f"Cleared {Q_TICKET} Tickets: **{total_tickets:,}** from **{len(rows):,}** players\n"
        f"{Q_SUCCESS if role_deleted else Q_WARNING} {role_note}\n"
        "The lottery channel/panel messages were left in place.",
        allowed_mentions=discord.AllowedMentions.none()
    )

def build_lottery_stats_embed(ctx, config, rows, panel_value, page=0, per_page=10):
    total_tickets = sum(row["tickets"] for row in rows)
    unique_players = len(rows)
    ticket_cost = lottery_ticket_cost(config)
    house_cut = lottery_house_cut(config)
    next_draw = config["next_draw"]
    if next_draw.tzinfo is None:
        next_draw = next_draw.replace(tzinfo=timezone.utc)

    channel = ctx.guild.get_channel(config["channel_id"])
    embed = discord.Embed(title=f"{QOIN_CHEST} Lottery Stats", color=discord.Color.gold())
    embed.add_field(name="Prize / Current Pot", value=format_balance(config["pot"]), inline=True)
    embed.add_field(name=f"{Q_TICKET} Total Tickets", value=f"{total_tickets:,}", inline=True)
    embed.add_field(name="Players", value=f"{unique_players:,}", inline=True)
    embed.add_field(name=f"{Q_TICKET} Ticket Price", value=format_balance(ticket_cost), inline=True)
    embed.add_field(name="House Cut", value=f"{house_cut * 100:.1f}%", inline=True)
    if config.get("role_id"):
        embed.add_field(name="Participant Role", value=f"<@&{config['role_id']}>", inline=True)
    embed.add_field(name="Next Draw", value=f"<t:{int(next_draw.timestamp())}:R>", inline=True)
    embed.add_field(name="Lottery Channel", value=channel.mention if channel else f"`{config['channel_id']}`", inline=True)
    embed.add_field(name=f"{Q_TICKET} Ticket Panel", value=panel_value, inline=True)

    if rows:
        page_count = max(1, ((len(rows) - 1) // per_page) + 1)
        page = max(0, min(page, page_count - 1))
        start = page * per_page
        page_rows = rows[start:start + per_page]
        leaders = []
        for index, row in enumerate(page_rows, start + 1):
            odds = (row["tickets"] / total_tickets * 100) if total_tickets else 0
            leaders.append(f"{index}. {user_mention(row['user_id'])} - **{row['tickets']:,}** tickets ({odds:.1f}%)")
        embed.add_field(
            name=f"{Q_TICKET} Ticket Holders #{start + 1}-{start + len(page_rows)}",
            value="\n".join(leaders),
            inline=False
        )
        embed.set_footer(text=f"Ticket holder page {page + 1}/{page_count}")
    else:
        embed.add_field(name=f"{Q_TICKET} Ticket Holders", value="No tickets bought this round.", inline=False)
        embed.set_footer(text="Ticket holder page 1/1")

    embed.add_field(
        name="How To Enter",
        value=f"Use the buttons on the lottery panel. Each ticket costs **{format_balance(ticket_cost)}**.",
        inline=False
    )
    return embed

class LotteryStatsView(discord.ui.View):
    def __init__(self, ctx, config, rows, panel_value):
        super().__init__(timeout=180)
        self.ctx = ctx
        self.config = config
        self.rows = rows
        self.panel_value = panel_value
        self.page = 0
        self.per_page = 10
        self.message = None
        self.update_buttons()

    async def interaction_check(self, interaction):
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message("Use your own lottery stats command.", ephemeral=True)
            return False
        return True

    def update_buttons(self):
        max_page = max(0, (len(self.rows) - 1) // self.per_page)
        self.prev_page.disabled = self.page <= 0
        self.next_page.disabled = self.page >= max_page

    def embed(self):
        self.update_buttons()
        return build_lottery_stats_embed(
            self.ctx,
            self.config,
            self.rows,
            self.panel_value,
            self.page,
            self.per_page
        )

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction, button):
        self.page = max(0, self.page - 1)
        await interaction.response.edit_message(embed=self.embed(), view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction, button):
        self.page = min(max(0, (len(self.rows) - 1) // self.per_page), self.page + 1)
        await interaction.response.edit_message(embed=self.embed(), view=self)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

@commands.command()
async def lotterystats(ctx):
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} Lottery stats only work in servers.")
        return
    if not await ensure_db_ready(ctx):
        return

    config = get_lottery_config(ctx.guild.id)
    if config is None:
        await ctx.send("Lottery is not set up yet.")
        return

    rows = sorted(lottery_ticket_rows(ctx.guild.id), key=lambda row: row["tickets"], reverse=True)
    panel = await refresh_lottery_message(ctx.guild, config)
    panel_value = f"[Open Panel]({panel.jump_url})" if panel else "panel unavailable"
    view = LotteryStatsView(ctx, config, rows, panel_value)
    view.message = await ctx.send(embed=view.embed(), view=view, allowed_mentions=discord.AllowedMentions.none())

@commands.command()
async def buytick(ctx, amount: str = "1"):
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} Lottery tickets only work in servers.")
        return
    if not await ensure_db_ready(ctx):
        return

    config = get_lottery_config(ctx.guild.id)
    if config is None:
        await ctx.send("Lottery is not set up yet.")
        return

    try:
        tickets = int(amount)
    except ValueError:
        await ctx.send(f"{Q_DENIED} Use `.buytick <ticket amount>`.")
        return
    if tickets <= 0:
        await ctx.send(f"{Q_DENIED} Ticket amount must be positive.")
        return

    try:
        result = await asyncio.to_thread(buy_lottery_tickets_sync, ctx.guild.id, ctx.author.id, tickets)
        if not result.get("ok"):
            await ctx.send(result["message"], allowed_mentions=discord.AllowedMentions.none())
            return
        await assign_lottery_role(ctx.guild, ctx.author.id, result["config"].get("role_id"))
        await refresh_lottery_message(ctx.guild, result["config"])
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(
        lottery_purchase_message(result),
        allowed_mentions=discord.AllowedMentions.none()
    )

async def process_lottery_draw(config):
    guild = bot.get_guild(config["guild_id"]) if bot else None
    if guild is None:
        return

    next_draw = datetime.now(timezone.utc) + timedelta(seconds=int(config["period_seconds"]))
    rows = lottery_ticket_rows(guild.id)
    channel = guild.get_channel(config["channel_id"])
    if channel is None:
        try:
            channel = await bot.fetch_channel(config["channel_id"])
        except Exception:
            channel = None

    unique_players = len(rows)
    if unique_players < 5:
        ticket_cost = lottery_ticket_cost(config)
        refunds = refund_lottery_round(guild.id, rows, ticket_cost)
        refunded_total = sum(amount for _, amount in refunds)
        reset_lottery_round(guild.id, next_draw)
        role = await recreate_lottery_role(guild, config.get("role_id"))
        set_lottery_role(guild.id, role.id if role else None)
        updated_config = await asyncio.to_thread(get_lottery_config, guild.id)
        await refresh_lottery_message(guild, updated_config)
        if channel:
            refund_lines = [
                f"- {user_mention(user_id)} refunded **{format_balance(amount)}**"
                for user_id, amount in refunds[:10]
            ]
            extra = "\n".join(refund_lines) if refund_lines else "No paid tickets needed a refund."
            if len(refunds) > 10:
                extra += f"\n...and **{len(refunds) - 10}** more refunds."
            await channel.send(
                f"{QOIN_CHEST} Lottery draw cancelled: **{unique_players}/5** players joined.\n"
                f"{Q_TICKET} Refunded **{format_balance(refunded_total)}** across **{len(refunds)}** users.\n"
                f"{extra}\n"
                "No winner this round. The lottery has restarted with a fresh participant role.",
                allowed_mentions=discord.AllowedMentions.none()
            )
        return

    weighted = []
    for row in rows:
        weighted.extend([row["user_id"]] * int(row["tickets"]))
    winner_id = random.choice(weighted)
    pot = int(config["pot"])
    try:
        data = get_user(winner_id)
        update_user(winner_id, balance=data["balance"] + pot, total_earned=data["total_earned"] + pot)
        log_transaction(winner_id, "lottery_win", pot, f"Guild {guild.id}")
        reset_lottery_round(guild.id, next_draw)
        role = await recreate_lottery_role(guild, config.get("role_id"))
        set_lottery_role(guild.id, role.id if role else None)
        updated_config = await asyncio.to_thread(get_lottery_config, guild.id)
    except Exception as e:
        print(f"Lottery draw failed: {type(e).__name__} - {e}")
        return

    if channel:
        deleted_count = await clear_lottery_channel(channel)
        await channel.send(
            f"{Q_CONFETTI} Lottery winner: {user_mention(winner_id)} won the prize: **{format_balance(pot)}**!\n"
            f"Next draw: <t:{int(next_draw.timestamp())}:R>\n"
            f"Cleared **{deleted_count:,}** old lottery messages.\n"
            "A fresh participant role has been created for the new round.",
            allowed_mentions=discord.AllowedMentions(users=True)
        )
        try:
            await asyncio.to_thread(update_lottery_config, guild.id, message_id=None)
            updated_config["message_id"] = None
        except Exception as e:
            print(f"Lottery message id reset failed: {type(e).__name__} - {e}")
    await refresh_lottery_message(guild, updated_config)

async def lottery_draw_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        if db_ready:
            try:
                for config in await asyncio.to_thread(all_lotteries_due):
                    await process_lottery_draw(config)
            except Exception as e:
                print(f"Lottery loop error: {type(e).__name__} - {e}")
        await asyncio.sleep(60)

async def db_keepalive_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            await asyncio.to_thread(ping_db)
        except Exception as e:
            print(f"Database keep-alive loop error: {type(e).__name__} - {e}")
        await asyncio.sleep(240)

class BalanceRankView(discord.ui.View):
    def __init__(self, ctx, order, title, icon):
        super().__init__(timeout=180)
        self.ctx = ctx
        self.order = order
        self.title = title
        self.icon = icon
        self.page = 0
        self.per_page = 10
        self.total = 0
        self.message = None
        self.update_buttons()

    async def interaction_check(self, interaction):
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message("Use your own leaderboard command.", ephemeral=True)
            return False
        return True

    def update_buttons(self):
        max_page = max(0, (self.total - 1) // self.per_page)
        self.first_page.disabled = self.page <= 0
        self.prev_page.disabled = self.page <= 0
        self.next_page.disabled = self.page >= max_page
        self.last_page.disabled = self.page >= max_page

    def fetch_page(self):
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS count FROM economy")
        self.total = cur.fetchone()["count"]
        cur.execute(
            f"SELECT user_id, balance FROM economy ORDER BY balance {self.order}, user_id ASC LIMIT %s OFFSET %s",
            (self.per_page, self.page * self.per_page)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    def build_embed(self, rows):
        max_page = max(1, ((self.total - 1) // self.per_page) + 1)
        start_rank = self.page * self.per_page + 1
        end_rank = min(self.total, self.page * self.per_page + len(rows))
        embed = discord.Embed(title=f"{self.icon} {self.title}", color=discord.Color.gold())
        if not rows:
            embed.description = "No balances found yet."
        else:
            embed.description = f"Showing **#{start_rank}-{end_rank}** of **{self.total}**."
            for i, row in enumerate(rows, start_rank):
                embed.add_field(
                    name=f"{i}. {user_mention(row['user_id'])}",
                    value=format_balance(row["balance"]),
                    inline=False
                )
        embed.set_footer(text=f"Page {self.page + 1}/{max_page}")
        return embed

    async def render(self):
        try:
            rows = await asyncio.to_thread(self.fetch_page)
        except Exception:
            return None
        self.update_buttons()
        return self.build_embed(rows)

    @discord.ui.button(label="First", style=discord.ButtonStyle.secondary)
    async def first_page(self, interaction, button):
        self.page = 0
        embed = await self.render()
        if embed is None:
            return await interaction.response.send_message("Database unavailable. Try again shortly.", ephemeral=True)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction, button):
        self.page = max(0, self.page - 1)
        embed = await self.render()
        if embed is None:
            return await interaction.response.send_message("Database unavailable. Try again shortly.", ephemeral=True)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction, button):
        self.page += 1
        embed = await self.render()
        if embed is None:
            return await interaction.response.send_message("Database unavailable. Try again shortly.", ephemeral=True)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Last", style=discord.ButtonStyle.secondary)
    async def last_page(self, interaction, button):
        self.page = max(0, (self.total - 1) // self.per_page)
        embed = await self.render()
        if embed is None:
            return await interaction.response.send_message("Database unavailable. Try again shortly.", ephemeral=True)
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

async def send_balance_rank(ctx, order, title, icon=QOIN_CHEST):
    if not await ensure_db_ready(ctx):
        return

    view = BalanceRankView(ctx, order, title, icon)
    embed = await view.render()
    if embed is None:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    view.message = await ctx.send(embed=embed, view=view, allowed_mentions=discord.AllowedMentions.none())

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
            await ctx.send(f"{Q_TIMER} You can claim daily in **{hours_left}h {minutes_left}m**")
            return

    streak = data['daily_streak'] + 1
    base_reward = random.randint(10_000, 15_000)
    streak_bonus = min(max(streak - 1, 0) * 10, 200)
    reward = base_reward + streak_bonus
    reward = int(reward * claim_reward_multiplier(data))

    try:
        update_user(
            user_id,
            balance=data['balance'] + reward,
            daily_streak=streak,
            last_daily=now,
            total_earned=data['total_earned'] + reward
        )
        log_transaction(user_id, "daily", reward, f"Streak {streak}")
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    updated = get_user(user_id)
    achievement_reward = maybe_award_main_quest(user_id, updated, "daily_30")
    extra = f"\n{Q_QUEST} Main quest complete: **{format_balance(achievement_reward)}**!" if achievement_reward else ""
    await ctx.send(f"{QOIN_BAG} You claimed **{format_balance(reward)}**!\nStreak: **{streak}** days (+{streak_bonus} bonus){extra}")

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
            await ctx.send(f"{Q_TIMER} You can claim weekly in **{days_left}** days")
            return

    streak = data['weekly_streak'] + 1
    base_reward = random.randint(20_000, 30_000)
    streak_bonus = min(max(streak - 1, 0) * 50, 500)
    reward = base_reward + streak_bonus
    reward = int(reward * claim_reward_multiplier(data))

    try:
        update_user(
            user_id,
            balance=data['balance'] + reward,
            weekly_streak=streak,
            last_weekly=now,
            total_earned=data['total_earned'] + reward
        )
        log_transaction(user_id, "weekly", reward, f"Streak {streak}")
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    updated = get_user(user_id)
    achievement_reward = maybe_award_main_quest(user_id, updated, "weekly_8")
    extra = f"\n{Q_QUEST} Main quest complete: **{format_balance(achievement_reward)}**!" if achievement_reward else ""
    await ctx.send(f"{QOIN_BAG} You claimed **{format_balance(reward)}**!\nWeekly streak: **{streak}** weeks (+{streak_bonus} bonus){extra}")

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
            await ctx.send(f"{Q_TIMER} You can claim monthly in **{days_left}** days")
            return

    streak = data['monthly_streak'] + 1
    base_reward = random.randint(40_000, 60_000)
    streak_bonus = min(max(streak - 1, 0) * 500, 5000)
    reward = base_reward + streak_bonus
    reward = int(reward * claim_reward_multiplier(data))

    try:
        update_user(
            user_id,
            balance=data['balance'] + reward,
            monthly_streak=streak,
            last_monthly=now,
            total_earned=data['total_earned'] + reward
        )
        log_transaction(user_id, "monthly", reward, f"Streak {streak}")
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    updated = get_user(user_id)
    achievement_reward = maybe_award_main_quest(user_id, updated, "monthly_5")
    extra = f"\n{Q_QUEST} Main quest complete: **{format_balance(achievement_reward)}**!" if achievement_reward else ""
    await ctx.send(f"{QOIN_BAG} You claimed **{format_balance(reward)}**!\nMonthly streak: **{streak}** months (+{streak_bonus} bonus){extra}")

# =====================
# COIN FLIP
# =====================
@commands.command(name="cf", aliases=["flip", "coinflip"])
async def gamble(ctx, amount: str, choice: str = None):
    """Coin flip. Use `.cf <amount> h/t`, `.cf <amount> heads/tails`, or `.cf <amount>`."""
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "cf")
    if cd > 0:
        await ctx.send(f"{Q_TIMER_TICK} Chill for **{cd:.1f}s** before flipping again.")
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    raw_amount = amount
    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send(f"{Q_DENIED} Use `.cf all`, `.cf <amount>`, or `.flip <amount>` (max {MAX_BET:,} {CURRENCY_EMOJI})")
        return

    amount = parsed

    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return

    if amount > data['balance'] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return

    if choice:
        choice = choice.lower()
        if choice in ("h", "heads"):
            chosen_side = "HEADS"
        elif choice in ("t", "tails"):
            chosen_side = "TAILS"
        else:
            await ctx.send(f"{Q_DENIED} Pick `h`, `heads`, `t`, `tails`, or leave it blank.")
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
                await interaction.response.edit_message(content=f"{Q_FLIP} You picked **{selected}**.", view=self)
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
        choice_msg = await ctx.send(f"{Q_FLIP} Pick heads, tails, or random:", view=choice_view)
        await choice_view.wait()
        if choice_view.choice is None:
            await choice_msg.edit(content=f"{Q_TIMER} Coin flip cancelled.", view=None)
            return
        chosen_side = choice_view.choice

    streak = data.get('gamble_streak', 0)
    mult = payout_multiplier(data, streak)
    coin_result = random.choice(["HEADS", "TAILS"])
    win = coin_result == chosen_side
    flip_msg = await ctx.send(
        f"{Q_FLIP_SPIN} **COIN FLIP**\n"
        f"─────────────────\n"
        f"Pick: **{chosen_side}**\n"
        f"`[ spinning ]`  Bet: **{format_balance(amount)}**"
    )
    for face in ["HEADS", "TAILS", "HEADS", coin_result]:
        await asyncio.sleep(0.8)
        await flip_msg.edit(
            content=(
                f"{Q_FLIP_SPIN} **COIN FLIP**\n"
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
            streak_msg = f" {Q_STREAK_FIRE} {streak + 1} in a row! ×{payout_multiplier_after_win(data, streak + 1):.2f} payout" if streak > 0 else ""
            await flip_msg.edit(
                content=(
                f"{Q_FLIP} **COIN FLIP**\n"
                f"─────────────────\n"
                f"Pick: **{chosen_side}**\n"
                f">>> {Q_SUCCESS} **{coin_result} — YOU WIN!**\n"
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
                f"{Q_FLIP} **COIN FLIP**\n"
                f"─────────────────\n"
                f"Pick: **{chosen_side}**\n"
                f">>> {Q_DENIED} **{coin_result} — YOU LOSE**\n"
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
@commands.command(aliases=["roul", "rl"])
async def roulette(ctx, amount: str, color: str = None):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "roulette")
    if cd > 0:
        await ctx.send(f"{Q_TIMER_TICK} Chill for **{cd:.1f}s** before roulette again.")
        return

    user_id = ctx.author.id
    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    raw_amount = amount
    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send(f"{Q_DENIED} Use `.roulette all <red|black|green>` or `.roulette <amount> <red|black|green>`")
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
                    content=f"{Q_WHEEL} Roulette color: **{selected_color.upper()}**",
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
        color_msg = await ctx.send(f"{Q_WHEEL} Pick a roulette color:", view=color_view)
        await color_view.wait()
        if color_view.color is None:
            await color_msg.edit(content=f"{Q_TIMER} Roulette cancelled.", view=None)
            return
        color = color_view.color
    else:
        color = color.lower()

    if color not in ['red', 'black', 'green']:
        await ctx.send(f"{Q_DENIED} Use `.roulette all <red|black|green>` or `.roulette <amount> <red|black|green>`")
        return

    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return

    if amount > data['balance'] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return

    outcomes = ['red'] * 18 + ['black'] * 18 + ['green'] * 2
    result = random.choice(outcomes)
    multipliers = {'red': 2, 'black': 2, 'green': 10}
    emoji_map = {'red': Q_ROULETTE_RED, 'black': Q_ROULETTE_BLACK, 'green': Q_ROULETTE_GREEN}
    streak = data.get('roulette_streak', 0)
    mult = payout_multiplier(data, streak)
    roulette_msg = await ctx.send(
        f"{Q_WHEEL_SPIN} **ROULETTE**\n"
        f"─────────────────\n"
        f"{Q_TARGET} Pick: **{emoji_map[color]} {color.upper()}**\n"
        f"[ spinning... ]"
    )
    spin_frames = [
        f"{Q_ROULETTE_RED} {Q_ROULETTE_BLACK} {Q_ROULETTE_GREEN}",
        f"{Q_ROULETTE_BLACK} {Q_ROULETTE_GREEN} {Q_ROULETTE_RED}",
        f"{Q_ROULETTE_GREEN} {Q_ROULETTE_RED} {Q_ROULETTE_BLACK}",
        f"{Q_ROULETTE_BLACK} {Q_ROULETTE_RED} {Q_ROULETTE_BLACK}",
    ]
    for frame in spin_frames:
        await asyncio.sleep(0.8)
        await roulette_msg.edit(
            content=(
                f"{Q_WHEEL_SPIN} **ROULETTE**\n"
                f"─────────────────\n"
                f"{Q_TARGET} Pick: **{emoji_map[color]} {color.upper()}**\n"
                f"[ {frame} ]"
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
            streak_msg = f" {Q_STREAK_FIRE} {streak + 1} in a row! ×{payout_multiplier_after_win(data, streak + 1):.2f} payout" if streak > 0 else ""
            await roulette_msg.edit(
                content=(
                f"{Q_WHEEL} **ROULETTE**\n"
                f"─────────────────\n"
                f"{Q_TARGET} You picked: **{emoji_map[color]} {color.upper()}**\n"
                f"─────────────────\n"
                f">>> {Q_SUCCESS} **{color.upper()}!**\n"
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
                f"{Q_WHEEL} **ROULETTE**\n"
                f"─────────────────\n"
                f"{Q_TARGET} You picked: **{emoji_map[color]} {color.upper()}**\n"
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
@commands.command(aliases=["slot"])
async def slots(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "slots")
    if cd > 0:
        await ctx.send(f"{Q_TIMER_TICK} Chill for **{cd:.1f}s** before slots again.")
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    raw_amount = amount
    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send(f"{Q_DENIED} Use `.slots all` or `.slots <amount>` (max {MAX_BET:,} {CURRENCY_EMOJI})")
        return

    amount = parsed

    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return

    if amount > data['balance'] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return

    slot_symbols = [
        (Q_SLOT_STAR, 2),
        (Q_SLOT_DIAMOND, 3),
        (Q_SLOT_CROWN, 4),
        (Q_SLOT_JACKPOT, 5),
    ]
    symbol_weights = [40, 30, 20, 10]
    if random.random() < 0.18:
        chosen_symbol, result_multiplier = random.choices(slot_symbols, weights=symbol_weights)[0]
        final_reels = [chosen_symbol, chosen_symbol, chosen_symbol]
    else:
        symbols = [symbol for symbol, _ in slot_symbols]
        final_reels = [random.choices(symbols, weights=symbol_weights)[0] for _ in range(3)]
        while final_reels[0] == final_reels[1] == final_reels[2]:
            final_reels[random.randrange(3)] = random.choice([symbol for symbol in symbols if symbol != final_reels[0]])
        result_multiplier = 0

    slots_msg = await ctx.send(
        f"{Q_SLOTS} **SPINNING...**\n"
        f"─────────────────\n"
        f"| {Q_SLOTS} | {Q_SLOTS} | {Q_SLOTS} |\n"
        f"─────────────────"
    )

    await asyncio.sleep(1.1)
    r1 = final_reels[0]
    await slots_msg.edit(
        content=(
            f"{Q_SLOTS} **SPINNING...**\n"
            f"─────────────────\n"
            f"| {r1} | {Q_SLOTS} | {Q_SLOTS} |\n"
            f"─────────────────\n"
            f"_First reel locked..._"
        )
    )
    await asyncio.sleep(1.1)
    r2 = final_reels[1]
    await slots_msg.edit(
        content=(
            f"{Q_SLOTS} **SPINNING...**\n"
            f"─────────────────\n"
            f"| {r1} | {r2} | {Q_SLOTS} |\n"
            f"─────────────────\n"
            f"_One reel left..._"
        )
    )
    await asyncio.sleep(1.0)
    r3 = final_reels[2]

    streak = data.get('slots_streak', 0)
    mult = payout_multiplier(data, streak)

    try:
        if result_multiplier > 0:
            winnings = int(amount * mult * result_multiplier)
            new_balance = data['balance'] + winnings - amount
            update_user(
                user_id,
                balance=new_balance,
                slots_streak=streak + 1,
                total_won=data['total_won'] + winnings - amount
            )
            streak_msg = f" {Q_STREAK_FIRE} {streak + 1} in a row! ×{payout_multiplier_after_win(data, streak + 1):.2f} payout" if streak > 0 else ""
            await slots_msg.edit(
                content=(
                    f"{Q_SLOTS} **RESULTS**\n"
                    f"─────────────────\n"
                    f"| {r1} | {r2} | {r3} |\n"
                    f"─────────────────\n"
                    f">>> {QOIN_CHEST} **THREE MATCH!** ×{result_multiplier}\n"
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
                    f"{Q_SLOTS} **RESULTS**\n"
                    f"─────────────────\n"
                    f"| {r1} | {r2} | {r3} |\n"
                    f"─────────────────\n"
                    f">>> {Q_DENIED} **NO MATCH**\n"
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
    suits = [Q_CARD_SPADE, Q_CARD_HEART, Q_CARD_DIAMOND, Q_CARD_CLUB]
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

@commands.command(aliases=["bj"])
async def blackjack(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "blackjack")
    if cd > 0:
        await ctx.send(f"{Q_TIMER_TICK} Chill for **{cd:.1f}s** before blackjack again.")
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    raw_amount = amount
    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send(f"{Q_DENIED} Use `.blackjack all` or `.blackjack <amount>` (max {MAX_BET:,} {CURRENCY_EMOJI})")
        return

    amount = parsed

    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return

    if amount > data['balance'] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return

    deck = shuffle_deck()

    player_hand = [deck.pop(), deck.pop()]
    dealer_hand = [deck.pop(), deck.pop()]

    player_val = hand_value(player_hand)
    dealer_val = hand_value(dealer_hand)

    streak = data.get('blackjack_streak', 0)
    mult = payout_multiplier(data, streak)

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
                streak_msg = f" {Q_STREAK_FIRE} {new_streak} in a row! ×{payout_multiplier_after_win(data, new_streak):.2f} payout" if new_streak > 1 else ""
                await msg.edit(
                    content=(
                        f"{Q_CARDS} **BLACKJACK**\n"
                        f"─────────────────\n"
                        f"**Your hand:** {format_hand(player_hand)}  →  **{player_final}**\n"
                        f"**Dealer:**     {format_hand(dealer_hand)}  →  **{dealer_final}**\n"
                        f"─────────────────\n"
                        f">>> {Q_SUCCESS} **{win_type}!**\n"
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
                        f"{Q_CARDS} **BLACKJACK**\n"
                        f"─────────────────\n"
                        f"**Your hand:** {format_hand(player_hand)}  →  **{player_final}**\n"
                        f"**Dealer:**     {format_hand(dealer_hand)}  →  **{dealer_final}**\n"
                        f"─────────────────\n"
                        f">>> {Q_DENIED} **{win_type}**\n"
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
                        f"{Q_CARDS} **BLACKJACK**\n"
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
        f"{Q_CARDS} **BLACKJACK**\n"
        f"─────────────────\n"
        f"**Your hand:** {format_hand(player_hand)}  →  **{player_val}**\n"
        f"**Dealer:**     [{dealer_hand[0][0]}{dealer_hand[0][1]}]  [?]\n"
        f"─────────────────",
        view=BJView()
    )

# =====================
# GIVE
# =====================
@commands.command(aliases=["pay"])
async def give(ctx, member: discord.Member, amount: str):
    if not await ensure_db_ready(ctx):
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    if str(amount).lower() == "all" and has_economy_owner_power(ctx.author.id, ctx.guild):
        raw_amount = amount
        amount = max(0, int(data['balance']))
    else:
        raw_amount = amount
        parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
        if parsed is None:
            await ctx.send(f"{Q_DENIED} Use `.give @user all` or `.give @user <amount>`")
            return
        amount = parsed

    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return

    if member.id == ctx.author.id:
        await ctx.send(f"{Q_DENIED} Can't transfer to yourself.")
        return

    if amount > data['balance'] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return

    try:
        old_sender_balance = data['balance']
        new_sender_balance = max(0, old_sender_balance - amount)
        update_user(user_id, balance=new_sender_balance)
        receiver_data = get_user(member.id)
        old_receiver_balance = receiver_data['balance']
        tax = 0 if has_economy_owner_power(ctx.author.id, ctx.guild) else int(amount * TRANSFER_TAX_RATE)
        received_amount = amount - tax
        new_receiver_balance = old_receiver_balance + received_amount
        update_user(member.id, balance=new_receiver_balance)
        log_transaction(user_id, "give_sent", -amount, f"Sent to {member.id}; tax {tax}")
        log_transaction(member.id, "give_received", received_amount, f"Received from {ctx.author.id}")
        if tax:
            log_transaction(user_id, "transfer_tax", -tax, "3% transfer tax burned")
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(
        f"{QOIN_TRANSFER} You sent **{format_balance(amount)}** to **{user_mention(member.id)}**\n"
        f"Tax Burned: **{format_balance(tax)}**\n"
        f"Received: **{format_balance(received_amount)}**\n"
        f"Your Balance: **{format_balance(old_sender_balance)}** → **{format_balance(new_sender_balance)}**\n"
        f"{user_mention(member.id)}'s Balance: **{format_balance(old_receiver_balance)}** → **{format_balance(new_receiver_balance)}**",
        allowed_mentions=discord.AllowedMentions.none()
    )
    if has_economy_owner_power(ctx.author.id, ctx.guild):
        await send_economy_log(ctx, "Quewo Transfer", [
            ("Recipient", f"{user_mention(member.id)} ({member.id})", False),
            ("Amount", format_balance(amount), True),
            ("Tax", format_balance(tax), True),
            ("Sender Balance", f"{format_balance(old_sender_balance)} → {format_balance(new_sender_balance)}", False),
            ("Recipient Balance", f"{format_balance(old_receiver_balance)} → {format_balance(new_receiver_balance)}", False),
        ])

# =====================
# LEADERBOARD
# =====================
@commands.command(name="lb", aliases=["leaderboard", "rank"])
async def lb(ctx):
    await send_balance_rank(ctx, "DESC", "Leaderboard", QOIN_CHEST)

# =====================
# ADD / REMOVE (OWNER)
# =====================
@commands.command()
async def add(ctx, target: str, amount: str):
    if not await ensure_db_ready(ctx):
        return

    if not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} Server owner or admin only.")
        return

    amount = parse_whole_number(amount)
    if amount is None:
        await ctx.send(f"{Q_DENIED} Use `.add @user <amount>`, `.add @role <amount>`, or `.add @everyone <amount>`.")
        return
    if amount <= 0:
        await ctx.send(f"{Q_DENIED} Amount must be positive.")
        return

    target_key = target.strip()
    is_everyone = target_key in {"@everyone", "@here"} or target_key == str(ctx.guild.default_role.id) if ctx.guild else False
    role = None
    member = None

    if ctx.guild and is_everyone:
        if ctx.author.id != 885548126365171824:
            await ctx.send(f"{Q_DENIED} Bulk `.add @everyone` is superowner only.")
            return
        members = list(ctx.guild.members)
        try:
            count = bulk_add_users([m.id for m in members], amount, ctx.author.id, "@everyone")
        except Exception:
            await send_error(ctx, "Database unavailable. Try again shortly.")
            return

        await ctx.send(
            f"{Q_SUCCESS} Added **{format_balance(amount)}** to **{count}** server members.",
            allowed_mentions=discord.AllowedMentions.none()
        )
        await send_economy_log(ctx, "Quewo Bulk Add", [
            ("Target", "@everyone", False),
            ("Recipients", f"{count:,}", True),
            ("Amount Each", format_balance(amount), True),
            ("Total Created", format_balance(amount * count), True),
        ], color=discord.Color.green())
        return

    if ctx.guild:
        try:
            role = await commands.RoleConverter().convert(ctx, target_key)
        except commands.BadArgument:
            role = None

    if role is not None:
        if ctx.author.id != 885548126365171824:
            await ctx.send(f"{Q_DENIED} Bulk `.add @role` is superowner only.")
            return
        members = list(role.members)
        if not members:
            await ctx.send(f"{Q_DENIED} That role has no members.")
            return
        try:
            count = bulk_add_users([m.id for m in members], amount, ctx.author.id, f"role {role.id}")
        except Exception:
            await send_error(ctx, "Database unavailable. Try again shortly.")
            return

        await ctx.send(
            f"{Q_SUCCESS} Added **{format_balance(amount)}** to **{count}** members with **{role.name}**.",
            allowed_mentions=discord.AllowedMentions.none()
        )
        await send_economy_log(ctx, "Quewo Bulk Add", [
            ("Target", f"{role.mention} ({role.id})", False),
            ("Recipients", f"{count:,}", True),
            ("Amount Each", format_balance(amount), True),
            ("Total Created", format_balance(amount * count), True),
        ], color=discord.Color.green())
        return

    try:
        member = await commands.MemberConverter().convert(ctx, target_key)
    except commands.BadArgument:
        await ctx.send(f"{Q_DENIED} Use `.add @user <amount>`, `.add @role <amount>`, or `.add @everyone <amount>`.")
        return
    if not can_economy_act_on(ctx.author.id, member.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You can't edit that user's Quewo balance.")
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
        log_transaction(member.id, "owner_add", amount, f"By {ctx.author.id}")
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(
        f"{Q_SUCCESS} Added **{format_balance(amount)}** to **{user_mention(member.id)}**\n"
        f"Balance: **{format_balance(old_balance)}** → **{format_balance(new_balance)}**",
        allowed_mentions=discord.AllowedMentions.none()
    )
    await send_economy_log(ctx, "Quewo Add", [
        ("Target", f"{user_mention(member.id)} ({member.id})", False),
        ("Amount", format_balance(amount), True),
        ("Balance", f"{format_balance(old_balance)} → {format_balance(new_balance)}", False),
    ], color=discord.Color.green())

@commands.command()
async def remove(ctx, member: discord.Member, amount: str):
    if not await ensure_db_ready(ctx):
        return

    if not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} Server owner or admin only.")
        return
    if not can_economy_act_on(ctx.author.id, member.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You can't edit that user's Quewo balance.")
        return

    try:
        target_data = get_user(member.id)
        old_balance = target_data['balance']
        raw_amount = amount
        if str(amount).lower() == "all":
            amount = old_balance
        else:
            amount = parse_whole_number(amount)
            if amount is None:
                raise ValueError
        if amount <= 0:
            await send_nonpositive_amount_error(ctx, raw_amount)
            return
        new_balance = max(0, old_balance - amount)
        update_user(member.id, balance=new_balance)
        log_transaction(member.id, "owner_remove", -amount, f"By {ctx.author.id}")
    except ValueError:
        await ctx.send(f"{Q_DENIED} Use `.remove @user all` or `.remove @user <amount>`")
        return
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(
        f"{Q_SUCCESS} Removed **{format_balance(amount)}** from **{user_mention(member.id)}**\n"
        f"Balance: **{format_balance(old_balance)}** → **{format_balance(new_balance)}**",
        allowed_mentions=discord.AllowedMentions.none()
    )
    await send_economy_log(ctx, "Quewo Remove", [
        ("Target", f"{user_mention(member.id)} ({member.id})", False),
        ("Amount", format_balance(amount), True),
        ("Balance", f"{format_balance(old_balance)} → {format_balance(new_balance)}", False),
    ], color=discord.Color.red())

@commands.command()
async def addtick(ctx, target: str, amount: str):
    """Superowner only. Adds free lottery tickets to a user, role, or everyone."""
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} `.addtick` only works in servers.")
        return
    if not await ensure_db_ready(ctx):
        return
    if not is_superowner_id(ctx.author.id):
        await ctx.send(f"{Q_DENIED} Superowner only.")
        return
    amount = parse_whole_number(amount)
    if amount is None:
        await ctx.send(f"{Q_DENIED} Use `.addtick @user <tickets>`, `.addtick @role <tickets>`, or `.addtick @everyone <tickets>`.")
        return
    if amount <= 0:
        await ctx.send(f"{Q_DENIED} Ticket amount must be positive.")
        return

    config = get_lottery_config(ctx.guild.id)
    if config is None:
        await ctx.send("Lottery is not set up yet.")
        return

    try:
        targets = await resolve_admin_targets(ctx, target)
    except commands.BadArgument:
        await ctx.send(f"{Q_DENIED} Use `.addtick @user <tickets>`, `.addtick @role <tickets>`, or `.addtick @everyone <tickets>`.")
        return
    if not targets["user_ids"]:
        await ctx.send(f"{Q_DENIED} No users matched that target.")
        return

    try:
        count = bulk_adjust_lottery_tickets(ctx.guild.id, targets["user_ids"], amount, "add", ctx.author.id)
        updated = get_lottery_config(ctx.guild.id)
        await refresh_lottery_message(ctx.guild, updated)
        if count == 1 and targets["member"] is not None:
            await assign_lottery_role(ctx.guild, targets["member"].id, updated.get("role_id") if updated else None)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    total_added = amount * count
    await ctx.send(
        f"{Q_SUCCESS} Added **{amount:,}** free {Q_TICKET} tickets to **{count:,}** target(s): {targets['label']}.\n"
        f"Total Entries Added: **{total_added:,}**",
        allowed_mentions=discord.AllowedMentions.none()
    )
    await send_economy_log(ctx, "Lottery Tickets Added", [
        ("Target", targets["log_label"], False),
        ("Recipients", f"{count:,}", True),
        ("Tickets Each", f"{amount:,}", True),
        ("Total Tickets", f"{total_added:,}", True),
    ], color=discord.Color.green())

@commands.command()
async def settick(ctx, target: str, amount: str):
    """Superowner only. Sets lottery tickets for a user, role, or everyone."""
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} `.settick` only works in servers.")
        return
    if not await ensure_db_ready(ctx):
        return
    if not is_superowner_id(ctx.author.id):
        await ctx.send(f"{Q_DENIED} Superowner only.")
        return
    amount = parse_whole_number(amount)
    if amount is None:
        await ctx.send(f"{Q_DENIED} Use `.settick @user <tickets>`, `.settick @role <tickets>`, or `.settick @everyone <tickets>`.")
        return
    if amount < 0:
        await ctx.send(f"{Q_DENIED} Ticket amount cannot be negative.")
        return

    config = get_lottery_config(ctx.guild.id)
    if config is None:
        await ctx.send("Lottery is not set up yet.")
        return

    try:
        targets = await resolve_admin_targets(ctx, target)
    except commands.BadArgument:
        await ctx.send(f"{Q_DENIED} Use `.settick @user <tickets>`, `.settick @role <tickets>`, or `.settick @everyone <tickets>`.")
        return
    if not targets["user_ids"]:
        await ctx.send(f"{Q_DENIED} No users matched that target.")
        return

    try:
        count = bulk_adjust_lottery_tickets(ctx.guild.id, targets["user_ids"], amount, "set", ctx.author.id)
        updated = get_lottery_config(ctx.guild.id)
        await refresh_lottery_message(ctx.guild, updated)
        if count == 1 and amount > 0 and targets["member"] is not None:
            await assign_lottery_role(ctx.guild, targets["member"].id, updated.get("role_id") if updated else None)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(
        f"{Q_SUCCESS} Set {Q_TICKET} tickets to **{amount:,}** for **{count:,}** target(s): {targets['label']}.",
        allowed_mentions=discord.AllowedMentions.none()
    )
    await send_economy_log(ctx, "Lottery Tickets Set", [
        ("Target", targets["log_label"], False),
        ("Recipients", f"{count:,}", True),
        ("Tickets Each", f"{amount:,}", True),
    ], color=discord.Color.gold())

@commands.command()
async def setquesos(ctx, target: str, amount: str):
    """Superowner only. Sets balances for a user, role, or everyone."""
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} `.setquesos` only works in servers.")
        return
    if not await ensure_db_ready(ctx):
        return
    if not is_superowner_id(ctx.author.id):
        await ctx.send(f"{Q_DENIED} Superowner only.")
        return
    amount = parse_whole_number(amount)
    if amount is None:
        await ctx.send(f"{Q_DENIED} Use `.setquesos @user <amount>`, `.setquesos @role <amount>`, or `.setquesos @everyone <amount>`.")
        return
    if amount < 0:
        await ctx.send(f"{Q_DENIED} Balance cannot be negative.")
        return

    try:
        targets = await resolve_admin_targets(ctx, target)
    except commands.BadArgument:
        await ctx.send(f"{Q_DENIED} Use `.setquesos @user <amount>`, `.setquesos @role <amount>`, or `.setquesos @everyone <amount>`.")
        return
    if not targets["user_ids"]:
        await ctx.send(f"{Q_DENIED} No users matched that target.")
        return

    try:
        count = bulk_set_balances(targets["user_ids"], amount, ctx.author.id, targets["log_label"])
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(
        f"{Q_SUCCESS} Set balance to **{format_balance(amount)}** for **{count:,}** target(s): {targets['label']}.",
        allowed_mentions=discord.AllowedMentions.none()
    )
    await send_economy_log(ctx, "Quewo Balance Set", [
        ("Target", targets["log_label"], False),
        ("Recipients", f"{count:,}", True),
        ("New Balance", format_balance(amount), True),
    ], color=discord.Color.gold())

# =====================
# SCRATCH CARD
# =====================
# Design: horizontal ticket with 5 hidden cells, animated one-by-one reveal
# Win: all 5 symbols match = ×10 payout

SCRATCH_TIERS = [
    (5, 10),
]
SCRATCH_SYMBOLS = [Q_SCRATCH_MARK, Q_SLOT_STAR, Q_SLOT_DIAMOND, Q_SLOT_CROWN, Q_SLOT_JACKPOT]
SCRATCH_WIN_CHANCE = 0.08

@commands.command(aliases=["scratchcard"])
async def scratch(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "scratch")
    if cd > 0:
        await ctx.send(f"{Q_TIMER_TICK} Chill for **{cd:.1f}s** before scratching again.")
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    raw_amount = amount
    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send(f"{Q_DENIED} Use `.scratch all` or `.scratch <amount>` (max {MAX_BET:,} {CURRENCY_EMOJI})")
        return

    amount = parsed

    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return

    if amount > data['balance'] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return

    if random.random() < SCRATCH_WIN_CHANCE:
        win_symbol = random.choice(SCRATCH_SYMBOLS)
        ticket = [win_symbol] * 5
    else:
        ticket = [random.choice(SCRATCH_SYMBOLS) for _ in range(5)]
        while len(set(ticket)) == 1:
            ticket[random.randrange(5)] = random.choice([symbol for symbol in SCRATCH_SYMBOLS if symbol != ticket[0]])

    best_symbol, match_count = max(
        ((symbol, ticket.count(symbol)) for symbol in set(ticket)),
        key=lambda item: item[1]
    )
    if match_count == 5:
        multiplier = 10
    else:
        multiplier = 0

    cell_states = ['[????]'] * 5
    hidden = list(range(5))
    random.shuffle(hidden)

    async def scratch_msg(states, extra=None):
        cells_line = '  '.join(states)
        return (
            f"{QOIN_CHEST} **SCRATCH CARD**\n"
            f"─────────────────\n"
            f"{cells_line}\n"
            f"─────────────────\n"
            f"{extra or f'Bet: **{format_balance(amount)}**\n_Revealing cells..._'}"
        )

    msg = await ctx.send(await scratch_msg(cell_states))

    for idx in hidden:
        await asyncio.sleep(0.55)
        cell_states[idx] = f"[{ticket[idx]}]"
        await msg.edit(content=await scratch_msg(cell_states))

    await asyncio.sleep(0.4)

    try:
        data = get_user(user_id)
        streak = data.get('scratch_streak', 0)
        mult = payout_multiplier(data, streak)
        if multiplier > 0:
            winnings = int(amount * mult * multiplier)
            new_balance = data['balance'] + winnings - amount
            update_user(
                user_id,
                balance=new_balance,
                scratch_streak=streak + 1,
                total_won=data['total_won'] + winnings - amount
            )
            streak_msg = f" {Q_STREAK_FIRE} {streak + 1} streak! ×{payout_multiplier_after_win(data, streak + 1):.2f}" if streak > 0 else ""
            await msg.edit(
                content=(
                    f"{QOIN_CHEST} **SCRATCH CARD — WIN!**\n"
                    f"─────────────────\n"
                    f"{'  '.join(cell_states)}\n"
                    f"─────────────────\n"
                    f">>> {Q_SUCCESS} **{match_count}/5 {best_symbol} matched!**\n"
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
                    f"{QOIN_CHEST} **SCRATCH CARD**\n"
                    f"─────────────────\n"
                    f"{'  '.join(cell_states)}\n"
                    f"─────────────────\n"
                    f">>> {Q_DENIED} **{match_count}/5 matched** — no prize\n"
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
    'gem': Q_XP,
    'bomb': Q_MINE,
    'hidden': Q_MS_HIDDEN,
    'cursor': Q_MS_CURSOR,
}

@commands.command(name="ms", aliases=["minesweeper", "minesweepeer"])
async def minesweeper(ctx, amount: str):
    """Play minesweeper. Use `.ms all` or `.ms 500`."""
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "ms")
    if cd > 0:
        await ctx.send(f"{Q_TIMER_TICK} Chill for **{cd:.1f}s** before minesweeper again.")
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    raw_amount = amount
    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send(f"{Q_DENIED} Use `.ms all` or `.ms <amount>`")
        return

    amount = parsed

    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return

    if amount > data['balance'] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
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
                content=f"{Q_MINE} **MINE HUNT** `{grid_size}x{grid_size}` selected.",
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
    size_msg = await ctx.send(f"{Q_MINE} Choose your minefield size:", view=size_view)
    await size_view.wait()
    if size_view.grid is None:
        await size_msg.edit(content=f"{Q_TIMER} Mine hunt cancelled.", view=None)
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
        header = f"{Q_MINE_SPARK} **MINE HUNT** `{rows}x{cols}` | {Q_MINE} {bomb_count} | Bet **{format_balance(amount)}**"
        if game_over:
            if game_won:
                header += f"\n> {Q_SUCCESS} All gems found! ×{multiplier:.2f} multiplier!"
            else:
                header += f"\n> {Q_DENIED} BOOM! Game over."
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
                    f"\n> {Q_TIMER} Timed out! Lost **{format_balance(amount)}**\n"
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
    ('×0.5', 0xCC0000, Q_WHEEL_RED),
    ('×1',   0x1E90FF, Q_WHEEL_BLUE),
    ('×1',   0x228B22, Q_WHEEL_GREEN),
    ('×2',   0xFF8C00, Q_WHEEL_ORANGE),
    ('×2',   0x9932CC, Q_WHEEL_PURPLE),
    ('×3',   0xFFD700, Q_WHEEL_GOLD),
    ('BLANK', 0x555555, Q_WHEEL_BLANK),
    ('×5',   0xFF1493, Q_WHEEL_PINK),
]
WHEEL_WEIGHTS = [15, 25, 25, 15, 10, 5, 3, 2]  # sum = 100

@commands.command(aliases=["spin"])
async def wheel(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "wheel")
    if cd > 0:
        await ctx.send(f"{Q_TIMER_TICK} Chill for **{cd:.1f}s** before wheel again.")
        return

    user_id = ctx.author.id

    try:
        data = get_user(user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    raw_amount = amount
    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data['balance'])
    if parsed is None:
        await ctx.send(f"{Q_DENIED} Use `.wheel all` or `.wheel <amount>` (max {MAX_BET:,} {CURRENCY_EMOJI})")
        return

    amount = parsed

    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return

    if amount > data['balance'] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
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
        header = f"{Q_WHEEL_SPIN if spinning else Q_WHEEL} **FORTUNE WHEEL**  |  Bet **{format_balance(amount)}**"
        if not spinning:
            if label == 'BLANK':
                header += f"\n> {emoji} **{label}** — nothing lost, nothing won"
            else:
                mult_val = float(label.replace('×', ''))
                header += f"\n> {Q_WHEEL} Landed on: **{emoji} {label}**"
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
    mult = payout_multiplier(data, streak)

    try:
        if label == 'BLANK':
            update_user(user_id, wheel_streak=0)
            await msg.edit(
                content=(
                    render_wheel(spinning=False, offset=final_offset, landed_idx=segment_idx) +
                    "\n" +
                f">>> {emoji} **{label}**\n"
                f"Nothing lost, nothing won.\n"
                f"New Balance: **{format_balance(data['balance'])}**"
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
                streak_msg = f" {Q_STREAK_FIRE} {streak + 1} in a row! ×{payout_multiplier_after_win(data, streak + 1):.2f}" if streak > 0 else ""
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
    "admin": "Admin power means superowner, actual server owner, or Discord Administrator. Server owner outranks admins, and superowner is highest.",
    "bal": "Shows balance, streaks, and total earned/won/lost. Use `.bal` or `.bal @user`.",
    "balance": "Alias for `.bal`. Shows balance, streaks, and total earned/won/lost.",
    "cash": "Alias for `.bal`. Shows balance, streaks, and total earned/won/lost.",
    "profile": "Shows level, XP, balance, stats, and owned items. Use `.profile` or `.profile @user`.",
    "level": "Alias for `.profile`. Shows level, XP, balance, stats, and owned items.",
    "lvl": "Alias for `.profile`. Shows level, XP, balance, stats, and owned items.",
    "quests": "Opens your quests UI with main, daily, weekly, and monthly quests plus claim/refresh buttons.",
    "shop": "Opens the categorized Quewo shop UI. Select an item, press Buy, then enter quantity.",
    "cooldowns": "Shows daily, weekly, monthly, and gambling cooldowns. Use `.cooldowns` or `.cd`.",
    "transactions": "Shows recent Quewo transactions. Use `.transactions` or `.transactions @user`.",
    "lottery": "Shows and refreshes the lottery ticket panel. First server run sets channel and draw period.",
    "editlottery": "Server owner/admin command. Edits lottery price, duration, house cut, or channel, then refreshes the panel.",
    "stoplottery": "Server owner/admin command. Stops this server's lottery and clears its tickets/config.",
    "lotterystats": "Shows lottery prize, tickets, players, next draw, paginated ticket holders, and panel link.",
    "buytick": "Legacy text command for buying lottery tickets. The lottery panel buttons are preferred.",
    "daily": "Claim a daily reward. Higher daily streak means a small bonus.",
    "weekly": "Claim a weekly reward. Higher weekly streak means a bigger bonus.",
    "monthly": "Claim a monthly reward. Higher monthly streak means a bigger bonus.",
    "cf": "Bet on heads or tails. Use `.cf <amount> h` or `.cf <amount> tails`.",
    "flip": "Bet on heads or tails. Use `.cf <amount> h` or `.cf <amount> tails`.",
    "roulette": "Bet on red, black, or green. Matching color wins.",
    "slots": "Slot machine. All 3 reels must match to win.",
    "blackjack": "Blackjack with Hit and Stand buttons. Beat the dealer without busting.",
    "scratch": "Scratch card. All 5 symbols must match to win.",
    "ms": "Pick a grid size, reveal safe tiles, avoid bombs.",
    "minesweeper": "Pick a grid size, reveal safe tiles, avoid bombs.",
    "minesweepeer": "Pick a grid size, reveal safe tiles, avoid bombs.",
    "wheel": "Spin the wheel for a multiplier or blank result.",
    "give": "Transfers quesos to another user. Normal transfers burn 3% tax.",
    "lb": "Shows a paginated balance leaderboard.",
    "leaderboard": "Shows a paginated balance leaderboard.",
    "add": "Admin-power command. Adds quesos to a user. Superowner can bulk add to @everyone or a role.",
    "remove": "Admin-power command. Removes quesos from a user.",
    "addtick": "Superowner command. Adds free lottery tickets to a user, role, or @everyone.",
    "settick": "Superowner command. Sets lottery tickets for a user, role, or @everyone.",
    "setquesos": "Superowner command. Sets balances for a user, role, or @everyone.",
    "disable": "Admin-power command. Disables one bot command. Superowner can still bypass disabled commands.",
    "enable": "Admin-power command. Enables one disabled command.",
    "disableall": "Admin-power command. Disables all commands except enableall.",
    "enableall": "Admin-power command. Enables all commands again.",
    "dclist": "Shows currently disabled commands.",
    "dsnipe": "Shows a recently deleted message in this channel.",
    "editsnipe": "Shows a recently edited message in this channel.",
    "rsnipe": "Shows a recently removed reaction.",
    "setnick": "Admin-power command. Changes a member's nickname if they are below you in the permission order.",
    "shut": "Admin-power command. Silences a user's messages if they are below you in the permission order.",
    "unshut": "Admin-power command. Unsilences a user's messages.",
    "rshut": "Admin-power command. Silences a user's reactions if they are below you in the permission order.",
    "unrshut": "Admin-power command. Unsilences a user's reactions.",
    "lockdown": "Admin-power command. Locks this channel so only admin-power users can speak.",
    "unlock": "Admin-power command. Unlocks this channel.",
    "rlockdown": "Admin-power command. Disables reactions in this channel.",
    "runlock": "Admin-power command. Enables reactions in this channel.",
    "purge": "Admin-power command. Deletes messages.",
    "reactcount": "Admin-power command. Counts reactions on a message.",
    "sleep": "Marks you as sleeping until you send a message.",
    "fsleep": "Superowner command. Marks members as sleeping.",
    "wake": "Superowner command. Removes sleep mode from members.",
    "afk": "Marks you AFK until you send a message.",
    "setbday": "Saves your birthday.",
    "removebday": "Removes your birthday.",
    "setbdaychannel": "Sets the server's birthday announcement channel.",
    "bdaychannel": "Alias for `.setbdaychannel`. Sets the server's birthday announcement channel.",
    "birthdaychannel": "Alias for `.setbdaychannel`. Sets the server's birthday announcement channel.",
    "away": "Shows AFK and sleeping users.",
    "listbans": "Admin-power command. Lists blacklisted users.",
    "calc": "Calculates a math expression.",
    "poll": "Creates a reaction poll. Use `.poll question`, `.poll question | option | option`, or add a final `| 10m`/`| 2h`/`| 1d` timer.",
    "epoll": "Ends one of your active polls. Admin-power users can end any active poll in the server.",
    "giveaway": "Admin-power command. Starts a timed giveaway with a custom reaction entry.",
    "steal": "Admin-power command. Copies a custom emoji or sticker into this server if the bot has permission.",
    "ask": "Asks the AI a question.",
    "generate": "Generates text with AI.",
    "analyse": "Analyzes provided text or content.",
    "translate": "Translates text.",
    "econhelp": "Shows Quewo commands, aliases, and short explanations.",
    "economyhelp": "Alias for `.econhelp`. Shows Quewo commands, aliases, and short explanations.",
    "quewohelp": "Alias for `.econhelp`. Shows Quewo commands, aliases, and short explanations.",
    "ehelp": "Alias for `.econhelp`. Shows Quewo commands, aliases, and short explanations.",
    "prefix": "Shows or changes this server's command prefix.",
    "preifx": "Typo alias for `.prefix`. Shows or changes this server's command prefix.",
    "ttt": "Starts Tic Tac Toe against another user. If the challenger sets a bet, the opponent must accept that bet too.",
    "c4": "Starts Connect 4 against another user. If the challenger sets a bet, the opponent must accept that bet too.",
    "chess": "Starts a chess game against another user with a clickable move UI.",
    "move": "Fallback chess command. Makes a chess move with notation like `.move e2e4` or `.move Nf3`.",
    "chessmove": "Alias for `.move`. Makes a chess move with notation like `.move e2e4` or `.move Nf3`.",
    "resign": "Resigns the active chess game in this channel.",
}

DETAILED_EXPLANATIONS = {
    "daily": f"Gives a reward once every 24 hours. Base reward is 10,000-15,000 {CURRENCY_EMOJI}. Your daily streak adds a small bonus after day 1.",
    "profile": f"Shows level, current XP toward the next level, balance, net gambling result, message count, and shop items. Chat XP can level you up and level rewards start at {format_balance(LEVEL_REWARD_BASE)}.",
    "level": f"Alias for `.profile`. Shows level, current XP toward the next level, balance, net gambling result, message count, and shop items. Level rewards start at {format_balance(LEVEL_REWARD_BASE)}.",
    "lvl": f"Alias for `.profile`. Shows level, current XP toward the next level, balance, net gambling result, message count, and shop items. Level rewards start at {format_balance(LEVEL_REWARD_BASE)}.",
    "quests": "Main quests track long streak achievements: 30 daily claims, 8 weekly claims, and 5 monthly claims. Daily, weekly, and monthly random quests rotate by period and can be claimed from the `.quests` UI.",
    "shop": "Opens an interactive categorized Quewo shop. Select an item, press Buy, then enter the quantity. The bot checks your balance, item limit, and total price before purchasing.",
    "cooldowns": "Shows daily, weekly, monthly, and active gambling command cooldowns in one place.",
    "transactions": "Shows recent money movement including shop purchases, quest rewards, level rewards, transfer tax, admin changes, and lottery activity.",
    "lottery": f"Server lottery. First run asks the server owner or an admin for a channel and draw period, locks the channel, and posts a persistent ticket panel with buy buttons. Existing active lottery data is preserved when the panel is refreshed. The prize is the full current pot. Tickets cost {format_balance(LOTTERY_TICKET_COST)} and {int(LOTTERY_HOUSE_CUT * 100)}% is burned as a money sink.",
    "editlottery": "Server owner/admin command. Use `.editlottery price 250000`, `.editlottery duration 12h`, `.editlottery cut 5`, or `.editlottery channel #lottery`. Duration resets the next draw timer. Channel posts a fresh lottery panel. Updates ping the lottery participant role.",
    "stoplottery": "Server owner/admin command. Use `.stoplottery` to remove the lottery setup for this server, clear the current pot/tickets, and delete the participant role if the bot can. It leaves channels and panel messages alone.",
    "lotterystats": "Shows the current lottery prize pot, total ticket count, number of players, participant role, next draw time, panel link, and paginated ticket holders with approximate odds.",
    "buytick": f"Legacy text command for buying tickets for the configured server lottery. The lottery panel buttons are preferred because they send private confirmations and update the panel automatically. Each ticket costs {format_balance(LOTTERY_TICKET_COST)}. The prize is the full current lottery pot; every ticket is one entry.",
    "weekly": f"Gives a reward once every 7 days. Base reward is 20,000-30,000 {CURRENCY_EMOJI}. Your weekly streak adds a bonus after week 1.",
    "monthly": f"Gives a reward once every 30 days. Base reward is 40,000-60,000 {CURRENCY_EMOJI}. Your monthly streak adds a bigger bonus after month 1.",
    "cf": "Pick heads or tails with `.cf <amount> h`, `.cf <amount> t`, `.flip <amount> heads`, or `.flip <amount> tails`. If you do not pick, the bot asks you. Winning pays ×2 before streak bonus, so betting 100 wins 200 total and gives +100 profit. Losing removes the bet. Consecutive coinflip wins add +1.5% payout each win and reset on loss.",
    "flip": "Pick heads or tails with `.cf <amount> h`, `.cf <amount> t`, `.flip <amount> heads`, or `.flip <amount> tails`. If you do not pick, the bot asks you. Winning pays ×2 before streak bonus, so betting 100 wins 200 total and gives +100 profit. Losing removes the bet. Consecutive coinflip wins add +1.5% payout each win and reset on loss.",
    "roulette": "Pick red, black, green, or use the button menu if you leave the color blank. Red and black pay ×2. Green pays ×10 because it is rarer. The bet is removed from the payout result, so a 100 bet on red winning gives 200 total and +100 profit. Consecutive roulette wins add +1.5% payout each win and reset on loss.",
    "slots": "The bot spins 3 reels with 4 custom symbols. All 3 reels must match to win: first symbol pays ×2, second pays ×3, third pays ×4, and fourth pays ×5. Non-perfect results lose the bet. Consecutive slots wins add +1.5% payout each win and reset on loss.",
    "blackjack": "You get cards against the dealer and use Hit or Stand buttons. Try to get closer to 21 than the dealer without going over. A normal win pays +1x your bet as profit. Losing removes the bet. A push changes nothing. Consecutive blackjack wins add +1.5% payout each win and reset on loss.",
    "scratch": "The ticket reveals 5 symbols one by one. All 5 symbols must match to win ×10. The base win chance is intentionally low at about 8%. Consecutive scratch wins add +1.5% payout each win and reset on loss.",
    "ms": "Choose 3x3, 4x4, or 5x5, then reveal tiles. 3x3 has 1 bomb, 4x4 has 3 bombs, and 5x5 has 5 bombs. Reveal every safe tile to win. Each safe reveal raises the final multiplier by +0.15. Hitting a bomb or timing out loses the bet.",
    "minesweeper": "Choose 3x3, 4x4, or 5x5, then reveal tiles. 3x3 has 1 bomb, 4x4 has 3 bombs, and 5x5 has 5 bombs. Reveal every safe tile to win. Each safe reveal raises the final multiplier by +0.15. Hitting a bomb or timing out loses the bet.",
    "minesweepeer": "Choose 3x3, 4x4, or 5x5, then reveal tiles. 3x3 has 1 bomb, 4x4 has 3 bombs, and 5x5 has 5 bombs. Reveal every safe tile to win. Each safe reveal raises the final multiplier by +0.15. Hitting a bomb or timing out loses the bet.",
    "wheel": "The wheel lands on a segment. ×2, ×3, and ×5 are wins. ×1 gives your stake back, ×0.5 loses half the bet, and BLANK changes nothing. Consecutive wheel wins add +1.5% payout each win and reset on loss or partial loss.",
    "give": f"Moves quesos from you to another user. Normal transfers burn {int(TRANSFER_TAX_RATE * 100)}% as tax. Admin-power users can use `.give @user all`. The message shows sent amount, tax, received amount, and balances.",
    "add": "Admin-power command. `.add @user <amount>` adds new quesos to one user. Superowner can use `.add @everyone <amount>` or `.add @role <amount>` to add that amount to every server member or every member with that role. It does not support `all`.",
    "remove": "Admin-power command. Removes quesos from a user. `.remove @user all` removes their full balance. The balance cannot go below 0, and the message shows old and new balance.",
    "addtick": "Superowner-only lottery admin command. Use `.addtick @user <tickets>`, `.addtick @role <tickets>`, or `.addtick @everyone <tickets>` to add free entries to the current lottery without changing the prize pot or charging users. The lottery panel refreshes after the change.",
    "settick": "Superowner-only lottery admin command. Use `.settick @user <tickets>`, `.settick @role <tickets>`, or `.settick @everyone <tickets>` to set current lottery entries to an exact number. Setting tickets does not charge users or change the prize pot. The lottery panel refreshes after the change.",
    "setquesos": f"Superowner-only Quewo admin command. Use `.setquesos @user <amount>`, `.setquesos @role <amount>`, or `.setquesos @everyone <amount>` to set balances to an exact {CURRENCY_EMOJI} amount. This sets balance directly instead of adding or removing a delta.",
    "prefix": "Changes the command prefix for this server. Use `.prefix !` or `.preifx !`. If the superowner is in the server, only the superowner can change it. If not, the server owner or admins can change it.",
    "preifx": "Typo alias for `.prefix`. Changes the command prefix for this server.",
    "setbdaychannel": "Sets the birthday announcement channel for this server. Users keep one birthday date globally, and the bot announces it in every server where they are still a member and a birthday channel is configured.",
    "ttt": "Challenge a user to Tic Tac Toe. The opponent accepts the game first. If the challenger enables a bet and enters an amount, the opponent gets a second accept/decline prompt for that exact bet before the game starts.",
    "c4": "Challenge a user to Connect 4. The opponent accepts the game first. If the challenger enables a bet and enters an amount, the opponent gets a second accept/decline prompt for that exact bet before the game starts. The board shows column numbers below the grid.",
    "chess": "Challenge a user to chess. The opponent accepts first, then the board message uses dropdown UI controls: choose one of your pieces, then choose one of that piece's legal moves. Movement legality, check, checkmate, stalemate, and draw detection come from python-chess.",
    "move": "Fallback chess command for manual notation. Use UCI like `.move e2e4` or SAN like `.move Nf3`. The clickable chess UI is preferred.",
    "chessmove": "Fallback chess command for manual notation. Use UCI like `.chessmove e2e4` or SAN like `.chessmove Nf3`. The clickable chess UI is preferred.",
    "resign": "Ends the active chess game in this channel and awards the win to the other player.",
}

ECONHELP_COMMANDS = [
    ("Core", ["bal", "profile", "quests", "shop", "cooldowns", "transactions", "lb"]),
    ("Claims", ["daily", "weekly", "monthly"]),
    ("Lottery", ["lottery", "buytick", "lotterystats", "editlottery", "stoplottery"]),
    ("Gambling", ["cf", "roulette", "slots", "blackjack", "scratch", "ms", "wheel"]),
    ("Transfers", ["give"]),
    ("Admin Quewo", ["add", "remove", "addtick", "settick", "setquesos"]),
    ("Help", ["econhelp", "explain"]),
]

def apply_prefix_to_help_text(text, prefix):
    return text.replace("`.", f"`{prefix}")

def command_help_line(command_name, prefix="."):
    command = bot.get_command(command_name) if bot else None
    usage_name = command.qualified_name if command else command_name
    aliases = command.aliases if command else []
    alias_text = f" aliases: `{', '.join(aliases)}`" if aliases else ""
    text = EXPLANATIONS.get(command_name)
    if command and not text:
        text = EXPLANATIONS.get(command.name)
    if not text:
        text = (command.help or "").strip().splitlines()[0] if command and command.help else "Runs this Quewo command."
    text = apply_prefix_to_help_text(text, prefix)
    return f"`{prefix}{usage_name}`{alias_text}\n{text}"

@commands.command(name="econhelp", aliases=["economyhelp", "quewohelp", "ehelp"])
async def econhelp(ctx):
    """Shows Quewo commands, aliases, and short explanations."""
    prefix = getattr(ctx, "prefix", ".")
    embed = discord.Embed(
        title=f"{Q_BOOK} Quewo Help",
        description=(
            f"Quewo commands only. Use `{prefix}explain <command>` for detailed help, "
            f"or `{prefix}help` for the full bot command list if it is enabled."
        ),
        color=discord.Color.gold()
    )
    for category, commands_ in ECONHELP_COMMANDS:
        lines = [command_help_line(command_name, prefix) for command_name in commands_]
        embed.add_field(name=category, value="\n\n".join(lines), inline=False)
    embed.set_footer(text=f"Tip: examples: {prefix}explain lottery, {prefix}explain shop, {prefix}explain cf")
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command()
async def explain(ctx, command_name: str = None):
    prefix = getattr(ctx, "prefix", ".")
    if not command_name:
        command_names = sorted({command.name for command in bot.commands})
        names = ", ".join(command_names)
        await ctx.send(f"Use `{prefix}explain <command>`. Commands: {names}, admin")
        return

    key = command_name.casefold().removeprefix(prefix.casefold()).lstrip(".")
    text = EXPLANATIONS.get(key)
    command = next(
        (
            command
            for command in bot.walk_commands()
            if command.qualified_name.casefold() == key
            or command.name.casefold() == key
            or key in {alias.casefold() for alias in command.aliases}
        ),
        None
    ) if bot else None
    if command and not text:
        text = EXPLANATIONS.get(command.name)
    if command and not text:
        text = (command.help or "").strip().splitlines()[0] if command.help else "Runs this bot command."
    if not text and key != "admin":
        await ctx.send("I don't have a short explanation for that command.", delete_after=30)
        return
    detail = DETAILED_EXPLANATIONS.get(key)
    if command and not detail:
        detail = DETAILED_EXPLANATIONS.get(command.name)
    if detail:
        text = f"{text}\n\nDetails: {detail}"
    text = apply_prefix_to_help_text(text, prefix)

    if command:
        usage = f"{prefix}{command.qualified_name}"
        if command.signature:
            usage += f" {command.signature}"
        aliases = f"\nAliases: {', '.join(command.aliases)}" if command.aliases else ""
        await ctx.send(f"**{usage}** — {text}{aliases}")
    else:
        await ctx.send(f"**{key}** — {text}")


# =====================
# SETUP
# =====================
async def setup(bot_ref, log_callback=None):
    global bot, economy_log_callback, lottery_task, db_keepalive_task
    bot = bot_ref
    economy_log_callback = log_callback
    print("Initializing Quewo system...")
    await asyncio.to_thread(init_db)
    print(f"Quewo db_ready = {db_ready}")

    economy_commands = [
        bal, profile, quests, shop, cooldowns, transactions, lottery, editlottery, stoplottery, lotterystats, buytick,
        daily, weekly, monthly, gamble, roulette, slots, blackjack,
        scratch, minesweeper, wheel, give, lb, add, remove, addtick, settick, setquesos, econhelp, explain
    ]
    for command in economy_commands:
        if bot.get_command(command.name):
            continue
        command.add_check(quewo_command_cooldown_check)
        bot.add_command(command)

    await restore_lottery_panels()

    if lottery_task is None or lottery_task.done():
        lottery_task = asyncio.create_task(lottery_draw_loop())
    if db_keepalive_task is None or db_keepalive_task.done():
        db_keepalive_task = asyncio.create_task(db_keepalive_loop())
