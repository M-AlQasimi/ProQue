import asyncio
import random
import os
import re
import shlex
import time
import math
import psycopg2
from psycopg2.extras import RealDictCursor
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
import discord
from discord.ext import commands
try:
    from discord.ext.commands.view import StringView
except Exception:
    StringView = None

db_ready = False
db_initializing = False
db_init_task = None

bot = None

# --- Config ---
MAX_BET = 200_000
COOLDOWN_SECS = 10
LONG_HELP_VIEW_TIMEOUT = 24 * 60 * 60
LONG_SETUP_VIEW_TIMEOUT = 60 * 60
DAILY_LOSS_WARNING_RATIO = 0.70
DAILY_LOSS_HARD_RATIO = 0.85
STREAK_BASE_BONUS = 0.01
STREAK_STEP_BONUS = 0.0025
BLACKJACK_DEALER_STAND_ON_16_CHANCE = 0.20
COINFLIP_WIN_CHANCE = 0.49
ROULETTE_WIN_CHANCE = 0.32
SLOTS_WIN_CHANCE = 0.25
SUPER_OWNER_ID = 885548126365171824
QUE_OWNER_DISPLAY = f"𝚀𝚞𝚎 (<@{SUPER_OWNER_ID}>)"
SUPEROWNER_LUCK_BONUS = 0.05
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
Q_TICKET_MINUS = "<:QTicketMinus:1504183251823235113>"
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
Q_FORTUNE_VIAL = "<:QFortuneVial:1501257715823935548>"
Q_ACTIVITY = "<:QActivity:1501257801996042430>"
Q_ACCEPT = "<:QAccept:1500516711114477709>"
Q_ALARM = "<:QAlarm:1500516713094054008>"
Q_ATTACHMENT = "<:QAttachment:1500516714641887402>"
Q_BELL = "<:QBell:1500516716344639618>"
Q_BIRTHDAY = "<:QBirthday:1500516717976097004>"
Q_BIRTHDAY_BALLOONS = "<:QBirthdayBalloons:1504932237584629781>"
Q_BIRTHDAY_CAKE = "<:QBirthdayCake:1504932239094579441>"
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
Q_TOWER = "<:QTower:1501489978033574008>"
Q_TOWER_DOOR = "<:QTowerDoor:1501489979862417540>"
Q_TOWER_TRAP = "<:QTowerTrap:1501489981649064146>"
Q_VAULT = "<:QVault:1501490432801247344>"
Q_VAULT_DIAL = "<:QVaultDial:1501490434499940352>"
Q_MEMORY = "<:QMemory:1501490610421629109>"
Q_MEMORY_TILE = "<:QMemoryTile:1501490612510134272>"
Q_CARD_LADDER = Q_CARDS
Q_LOCKPICK = Q_LOCK
Q_HEIST = "<:QHeist:1501999656643723425>"
Q_DICE_DUEL = "<:QDiceDuel:1501999780459450368>"
Q_CASES = "<:QCases:1501999740429271040>"
Q_PLINKO = "<a:QPlinkoDrop:1502002567427915866>"
Q_LUCKY_NUMBER = "<:QLuckyNumber:1501999889150775488>"
Q_JACKPOT_SPIN = "<a:QJackpotSpin:1502002724261330955>"
Q_DOUBLE_NOTHING = "<:QDoubleNothing:1503820795745534062>"
Q_GAME_STATS = "<:QGameStats:1502000517201661962>"
Q_BADGE = "<:QBadge:1502000221977055372>"
Q_AUDIT = "<:QAudit:1503820565599748308>"
Q_LIMITS = "<:QLimits:1503820580267495605>"
Q_HISTORY = "<:QHistory:1503820805853806824>"
Q_ACHIEVEMENT_LOCKED = "<:QAchievementLocked:1503820788560826589>"
Q_ACHIEVEMENT_UNLOCKED = "<:QAchievementUnlocked:1503820790339080282>"
Q_RISK_LOW = "<:QRiskLow:1503820811822301274>"
Q_RISK_MEDIUM = "<:QRiskMedium:1503820813491638434>"
Q_RISK_HIGH = "<:QRiskHigh:1503820809293135954>"
Q_RISK_EXTREME = "<:QRiskExtreme:1503820807691046912>"
Q_PERF = "<:QPerf:1503820589121671199>"
Q_FILTER = "<:QFilter:1503820570553356408>"
Q_WATCH = "<:QWatch:1505602276650385578>"
Q_TRUST = "<:QTrust:1505602273504399523>"
Q_SETUP = "<:QSetup:1505602260015648848>"
Q_QUOTE = "<:QQuote:1505602256467132426>"
Q_GAME_AUDIT = "<:QGameAudit:1505602251723509870>"
Q_EVENT = "<:QEvent:1505602249546530836>"
Q_COMMAND_CHECK = "<:QCommandCheck:1505602244538536128>"
Q_BALANCE = "<:QBalance:1505602235080380426>"
Q_ARCHIVE = "<:QArchive:1505602232085643446>"
Q_AI_HISTORY = "<:QAIHistory:1505602228466221117>"
Q_DUNGEON = "<:QDungeon:1503459478690074644>"
Q_DUNGEON_HEART = "<:QDungeonHeart:1503459480766255104>"
Q_DUNGEON_KEY = "<:QDungeonKey:1503459483030917270>"
Q_DUNGEON_RELIC = "<:QDungeonRelic:1503459486633955450>"
Q_DUNGEON_MONSTER = "<:QDungeonMonster:1503459485081927701>"
Q_BANK = "<:QBank:1507457909745782844>"
Q_STREAK_FREEZE = "<:QStreakFreeze:1507457952821415976>"
Q_ROB = "<:QRob:1507458102977364020>"
Q_TUTORIAL = "<:QTutorial:1507458236968865993>"
Q_RECOMMEND = "<:QRecommend:1507458216437747833>"
Q_SEASON_PASS = "<:QSeasonPass:1507457946756321482>"
INTERNAL_SUPEROWNER_TRANSACTION_KINDS = {"transfer_tax", "lottery_house_cut", "shop_payment"}
SLOT_SYMBOL_PAYOUTS = [
    (Q_SLOT_STAR, 2),
    (Q_SLOT_DIAMOND, 3),
    (Q_SLOT_CROWN, 4),
    (Q_SLOT_JACKPOT, 5),
]
CHAT_XP_COOLDOWN_SECS = 60
LEVEL_REWARD_BASE = 300_000
LEVEL_REWARD_STEP = 50_000
TRANSFER_TAX_RATE = 0.05
LOTTERY_TICKET_COST = 100_000
LOTTERY_HOUSE_CUT = 0.10
LOTTERY_MAX_BALANCE_SPEND_RATIO = 0.60
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
        "rarity": "Rare",
        "name": "Lucky Charm",
        "emoji": Q_LUCKY_CHARM,
        "cost": 750_000,
        "max_qty": 10,
        "description": "Passive: +1% gambling payout on wins per charm. Max 10.",
    },
    "xp_tonic": {
        "category": "Leveling",
        "rarity": "Common",
        "name": "XP Tonic",
        "emoji": Q_XP_TONIC,
        "cost": 300_000,
        "max_qty": 5,
        "description": "Passive: +5% chat XP per tonic. Max 5.",
    },
    "queso_magnet": {
        "category": "Leveling",
        "rarity": "Epic",
        "name": "Queso Magnet",
        "emoji": Q_QUESO_MAGNET,
        "cost": 1_250_000,
        "max_qty": 5,
        "description": "Passive: +5% level-up queso rewards per magnet. Max 5.",
    },
    "daily_spice": {
        "category": "Claims",
        "rarity": "Common",
        "name": "Daily Spice",
        "emoji": Q_DAILY_SPICE,
        "cost": 325_000,
        "max_qty": 10,
        "description": "Passive: +2% daily, weekly, and monthly claim rewards per spice. Max 10.",
    },
    "streak_polish": {
        "category": "Gambling",
        "rarity": "Common",
        "name": "Streak Polish",
        "emoji": Q_STREAK_POLISH,
        "cost": 375_000,
        "max_qty": 8,
        "description": "Passive: +0.5% gambling payout on wins per polish. Max 8.",
    },
    "gold_badge": {
        "category": "Cosmetics",
        "rarity": "Rare",
        "name": "Gold Badge",
        "emoji": Q_GOLD_BADGE,
        "cost": 500_000,
        "max_qty": 1,
        "description": "Cosmetic: adds a gold badge to your profile items.",
    },
    "high_roller": {
        "category": "Cosmetics",
        "rarity": "Epic",
        "name": "High Roller Title",
        "emoji": Q_HIGH_ROLLER,
        "cost": 1_000_000,
        "max_qty": 1,
        "description": "Cosmetic: changes your profile title to High Roller.",
    },
    "velvet_frame": {
        "category": "Cosmetics",
        "rarity": "Rare",
        "name": "Velvet Profile Frame",
        "emoji": Q_VELVET_FRAME,
        "cost": 750_000,
        "max_qty": 1,
        "description": "Cosmetic: adds Velvet before your profile title.",
    },
    "ticket_charm": {
        "category": "Lottery",
        "rarity": "Epic",
        "name": "Ticket Charm",
        "emoji": Q_TICKET_CHARM,
        "cost": 1_500_000,
        "max_qty": 5,
        "description": "Passive: +2% bonus lottery entries per charm when buying tickets. Max 5.",
    },
    "cooldown_clock": {
        "category": "Utility",
        "rarity": "Epic",
        "name": "Cooldown Clock",
        "emoji": Q_COOLDOWN_CLOCK,
        "cost": 1_000_000,
        "max_qty": 5,
        "description": "Passive: -4% 𝚀𝚞𝚎wo gambling cooldown per clock. Max 5.",
    },
    "fortune_vial": {
        "category": "Gambling",
        "rarity": "Epic",
        "name": "Fortune Vial",
        "emoji": Q_FORTUNE_VIAL,
        "cost": 1_000_000,
        "max_qty": 99,
        "duration_hours": 4,
        "luck_bonus": 0.07,
        "description": "Temporary: +7% win chance for 4 hours per vial. Time stacks.",
    },
    "streak_freeze": {
        "category": "Claims",
        "rarity": "Rare",
        "name": "Streak Freeze",
        "emoji": Q_STREAK_FREEZE,
        "cost": 850_000,
        "max_qty": 25,
        "description": "Consumable: protects one missed daily, weekly, or monthly streak.",
    },
    "royal_crown": {
        "category": "Cosmetics",
        "rarity": "Royal",
        "name": "Royal Q Crown",
        "emoji": Q_ROYAL_CROWN,
        "cost": 2_000_000,
        "max_qty": 1,
        "description": "Cosmetic: upgrades your profile title to Royal High Roller.",
    },
}

economy_log_callback = None
lottery_task = None
db_keepalive_task = None
lottery_view_registered = False
lottery_status_messages = {}

# --- Cooldown tracking ---
_cooldowns = {}  # {(user_id, command): timestamp}
_command_cooldowns = {}  # {user_id: timestamp}
_cooldown_multiplier_cache = {}  # {user_id: (expires_at, multiplier)}
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
        print("⚠️ DATABASE_URL not set - 𝚀𝚞𝚎wo system disabled")
        return

    for attempt in range(1, 11):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS economy (
                    user_id BIGINT PRIMARY KEY,
                    balance BIGINT DEFAULT 0,
                    bank_balance BIGINT DEFAULT 0,
                    last_bank_interest TIMESTAMP,
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
                    equipped_badges TEXT[] DEFAULT '{}',
                    profile_theme TEXT DEFAULT 'default',
                    daily_challenge_streak INTEGER DEFAULT 0,
                    last_daily_challenge_claim DATE,
                    quest_claims TEXT[] DEFAULT '{}',
                    steal_blacklist BIGINT[] DEFAULT '{}',
                    luck_boost_until TIMESTAMP,
                    personal_bet_limit BIGINT,
                    gambling_pause_until TIMESTAMP,
                    tutorial_mode BOOLEAN DEFAULT TRUE,
                    tutorial_uses INTEGER DEFAULT 0
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
                CREATE TABLE IF NOT EXISTS economy_guild_settings (
                    guild_id BIGINT PRIMARY KEY,
                    robbing_enabled BOOLEAN NOT NULL DEFAULT FALSE
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS economy_game_stats (
                    user_id BIGINT NOT NULL,
                    game_key TEXT NOT NULL,
                    played BIGINT NOT NULL DEFAULT 0,
                    wins BIGINT NOT NULL DEFAULT 0,
                    losses BIGINT NOT NULL DEFAULT 0,
                    profit BIGINT NOT NULL DEFAULT 0,
                    biggest_win BIGINT NOT NULL DEFAULT 0,
                    PRIMARY KEY (user_id, game_key)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS economy_game_history (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    game_key TEXT NOT NULL,
                    won BOOLEAN,
                    net_amount BIGINT NOT NULL DEFAULT 0,
                    payout BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_economy_game_history_user_id_created_at ON economy_game_history (user_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_economy_game_history_game_key_created_at ON economy_game_history (game_key, created_at DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS economy_daily_losses (
                    user_id BIGINT NOT NULL,
                    loss_date DATE NOT NULL,
                    amount BIGINT NOT NULL DEFAULT 0,
                    PRIMARY KEY (user_id, loss_date)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS economy_daily_challenges (
                    user_id BIGINT NOT NULL,
                    challenge_date DATE NOT NULL,
                    challenge_id TEXT NOT NULL,
                    progress BIGINT NOT NULL DEFAULT 0,
                    claimed BOOLEAN NOT NULL DEFAULT FALSE,
                    PRIMARY KEY (user_id, challenge_date, challenge_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS economy_season_scores (
                    season_key TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    played BIGINT NOT NULL DEFAULT 0,
                    wins BIGINT NOT NULL DEFAULT 0,
                    profit BIGINT NOT NULL DEFAULT 0,
                    PRIMARY KEY (season_key, user_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS economy_season_rewards (
                    season_key TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    rank INTEGER NOT NULL,
                    reward BIGINT NOT NULL,
                    rewarded_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (season_key, user_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS economy_events (
                    guild_id BIGINT PRIMARY KEY,
                    event_key TEXT NOT NULL,
                    started_by BIGINT NOT NULL,
                    channel_id BIGINT,
                    pot BIGINT NOT NULL DEFAULT 0,
                    ends_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_receipts (
                    receipt_id TEXT PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    channel_id BIGINT,
                    actor_id BIGINT NOT NULL,
                    target_ids BIGINT[] NOT NULL DEFAULT '{}',
                    action TEXT NOT NULL,
                    amount BIGINT,
                    details TEXT,
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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lottery_status_messages (
                    guild_id BIGINT NOT NULL,
                    channel_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (guild_id, channel_id, message_id)
                )
            """)
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS balance BIGINT DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS bank_balance BIGINT DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS last_bank_interest TIMESTAMP")
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
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS equipped_badges TEXT[] DEFAULT '{}'")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS profile_theme TEXT DEFAULT 'default'")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS daily_challenge_streak INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS last_daily_challenge_claim DATE")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS quest_claims TEXT[] DEFAULT '{}'")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS steal_blacklist BIGINT[] DEFAULT '{}'")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS luck_boost_until TIMESTAMP")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS personal_bet_limit BIGINT")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS gambling_pause_until TIMESTAMP")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS tutorial_mode BOOLEAN DEFAULT TRUE")
            cur.execute("ALTER TABLE economy ADD COLUMN IF NOT EXISTS tutorial_uses INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE economy_events ADD COLUMN IF NOT EXISTS channel_id BIGINT")
            cur.execute("ALTER TABLE economy_events ADD COLUMN IF NOT EXISTS pot BIGINT NOT NULL DEFAULT 0")
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
            print(f"✅ 𝚀𝚞𝚎wo DB initialized (PostgreSQL) on attempt {attempt}")
            return
        except psycopg2.OperationalError as e:
            if attempt < 10:
                print(f"⏳ 𝚀𝚞𝚎wo DB attempt {attempt}/10 failed (DB starting up), retrying in 5s...")
                time.sleep(5)
            else:
                print(f"❌ 𝚀𝚞𝚎wo DB init failed after 10 attempts: {e}")
                db_ready = False
        except Exception as e:
            print(f"❌ 𝚀𝚞𝚎wo DB init failed: {e}")
            db_ready = False
            return

def get_user(user_id):
    for attempt in range(3):
        conn = None
        cur = None
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
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass
            if attempt < 2:
                continue
            raise

def update_user(user_id, **kwargs):
    if not kwargs:
        return get_user(user_id)

    for attempt in range(3):
        conn = None
        cur = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO economy (user_id, balance) VALUES (%s, 0) ON CONFLICT (user_id) DO NOTHING",
                (user_id,)
            )
            old_total_lost = None
            if "total_lost" in kwargs:
                cur.execute("SELECT total_lost FROM economy WHERE user_id = %s FOR UPDATE", (user_id,))
                row = cur.fetchone()
                old_total_lost = int((row or {}).get("total_lost", 0) or 0)

            set_clauses = []
            values = []
            for key, value in kwargs.items():
                set_clauses.append(f"{key} = %s")
                values.append(value)

            values.append(user_id)
            query = f"UPDATE economy SET {', '.join(set_clauses)} WHERE user_id = %s RETURNING *"
            cur.execute(query, values)
            updated = cur.fetchone()
            if updated is None:
                raise RuntimeError(f"Economy update affected no rows for user {user_id}")
            if old_total_lost is not None:
                new_total_lost = int(updated["total_lost"] or 0)
                loss_delta = new_total_lost - old_total_lost
                if loss_delta > 0:
                    cur.execute(
                        """
                        INSERT INTO economy_daily_losses (user_id, loss_date, amount)
                        VALUES (%s, CURRENT_DATE, %s)
                        ON CONFLICT (user_id, loss_date) DO UPDATE SET
                            amount = economy_daily_losses.amount + EXCLUDED.amount
                        """,
                        (user_id, loss_delta)
                    )
            conn.commit()
            cur.close()
            conn.close()
            return updated
        except psycopg2.OperationalError:
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass
            if attempt < 2:
                continue
            raise
        except Exception:
            try:
                if conn:
                    conn.rollback()
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass
            raise

def add_user_balance(user_id, amount, earned_delta=0):
    for attempt in range(3):
        conn = None
        cur = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO economy (user_id, balance) VALUES (%s, 0) ON CONFLICT (user_id) DO NOTHING",
                (user_id,)
            )
            cur.execute(
                "SELECT balance, total_earned FROM economy WHERE user_id = %s FOR UPDATE",
                (user_id,)
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError(f"Economy row missing after insert for user {user_id}")

            old_balance = int(row["balance"])
            old_total_earned = int(row["total_earned"])
            new_balance = old_balance + int(amount)
            new_total_earned = old_total_earned + int(earned_delta)
            cur.execute(
                """
                UPDATE economy
                SET balance = %s,
                    total_earned = %s
                WHERE user_id = %s
                RETURNING balance, total_earned
                """,
                (new_balance, new_total_earned, user_id)
            )
            updated = cur.fetchone()
            if updated is None:
                raise RuntimeError(f"Economy balance update affected no rows for user {user_id}")
            conn.commit()
            cur.close()
            conn.close()
            return old_balance, int(updated["balance"])
        except psycopg2.OperationalError:
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass
            if attempt < 2:
                continue
            raise
        except Exception:
            try:
                if conn:
                    conn.rollback()
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass
            raise

def transfer_user_balance(sender_id, receiver_id, amount, tax=0, allow_overdraft=False):
    sender_id = int(sender_id)
    receiver_id = int(receiver_id)
    amount = int(amount)
    tax = int(tax)
    received_amount = amount - tax

    for attempt in range(3):
        conn = None
        cur = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            user_ids = sorted({sender_id, receiver_id, SUPER_OWNER_ID} if tax > 0 else {sender_id, receiver_id})
            cur.execute(
                "INSERT INTO economy (user_id, balance) SELECT user_id, 0 FROM unnest(%s::bigint[]) AS user_id ON CONFLICT (user_id) DO NOTHING",
                (user_ids,)
            )
            cur.execute(
                "SELECT user_id, balance FROM economy WHERE user_id = ANY(%s::bigint[]) ORDER BY user_id FOR UPDATE",
                (user_ids,)
            )
            rows = {int(row["user_id"]): row for row in cur.fetchall()}
            if sender_id not in rows or receiver_id not in rows:
                raise RuntimeError("Transfer row lock failed")

            old_sender_balance = int(rows[sender_id]["balance"])
            old_receiver_balance = int(rows[receiver_id]["balance"])
            if old_sender_balance < amount and not allow_overdraft:
                raise ValueError("insufficient_balance")

            new_sender_balance = max(0, old_sender_balance - amount)
            new_receiver_balance = old_receiver_balance + received_amount
            cur.execute(
                "UPDATE economy SET balance = %s WHERE user_id = %s RETURNING balance",
                (new_sender_balance, sender_id)
            )
            if cur.fetchone() is None:
                raise RuntimeError(f"Sender balance update affected no rows for user {sender_id}")
            cur.execute(
                "UPDATE economy SET balance = %s WHERE user_id = %s RETURNING balance",
                (new_receiver_balance, receiver_id)
            )
            if cur.fetchone() is None:
                raise RuntimeError(f"Receiver balance update affected no rows for user {receiver_id}")
            if tax > 0:
                cur.execute(
                    "UPDATE economy SET balance = balance + %s WHERE user_id = %s RETURNING balance",
                    (tax, SUPER_OWNER_ID)
                )
                if cur.fetchone() is None:
                    raise RuntimeError("𝚀𝚞𝚎 owner tax credit affected no rows")
                if sender_id == SUPER_OWNER_ID:
                    new_sender_balance += tax
                if receiver_id == SUPER_OWNER_ID:
                    new_receiver_balance += tax
            conn.commit()
            cur.close()
            conn.close()
            return {
                "old_sender_balance": old_sender_balance,
                "new_sender_balance": new_sender_balance,
                "old_receiver_balance": old_receiver_balance,
                "new_receiver_balance": new_receiver_balance,
                "received_amount": received_amount,
                "receiver_credited_amount": new_receiver_balance - old_receiver_balance,
                "tax": tax,
            }
        except psycopg2.OperationalError:
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass
            if attempt < 2:
                continue
            raise
        except Exception:
            try:
                if conn:
                    conn.rollback()
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass
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

def apply_shop_purchase(user_id, item_id, total_cost, new_balance, inventory=None, luck_boost_until=None, note=""):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO economy (user_id, balance) VALUES (%s, 0) ON CONFLICT (user_id) DO NOTHING",
            (user_id,)
        )
        cur.execute(
            "INSERT INTO economy (user_id, balance) VALUES (%s, 0) ON CONFLICT (user_id) DO NOTHING",
            (SUPER_OWNER_ID,)
        )
        cur.execute("SELECT balance FROM economy WHERE user_id = %s FOR UPDATE", (user_id,))
        buyer = cur.fetchone()
        if buyer is None or int(buyer["balance"]) < int(total_cost):
            raise RuntimeError("Insufficient balance during shop purchase")
        new_balance = int(buyer["balance"]) - int(total_cost)
        if luck_boost_until is not None:
            cur.execute(
                "UPDATE economy SET balance = %s, luck_boost_until = %s WHERE user_id = %s",
                (new_balance, luck_boost_until, user_id)
            )
        else:
            cur.execute(
                "UPDATE economy SET balance = %s, inventory = %s WHERE user_id = %s",
                (new_balance, inventory, user_id)
            )
        cur.execute(
            "UPDATE economy SET balance = balance + %s WHERE user_id = %s",
            (total_cost, SUPER_OWNER_ID)
        )
        cur.execute(
            "INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, %s, %s, %s)",
            (user_id, "shop_purchase", -total_cost, note)
        )
        cur.execute(
            "INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, %s, %s, %s)",
            (SUPER_OWNER_ID, "shop_payment", total_cost, f"Shop payment from {user_id}: {note}")
        )
        conn.commit()
        _cooldown_multiplier_cache.pop(int(user_id), None)
        return new_balance
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

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
        RETURNING user_id
        """,
        (amount, amount, unique_ids)
    )
    updated_ids = [int(row["user_id"]) for row in cur.fetchall()]
    if len(updated_ids) != len(unique_ids):
        conn.rollback()
        cur.close()
        conn.close()
        raise RuntimeError(f"Bulk add updated {len(updated_ids)} of {len(unique_ids)} economy rows")
    cur.execute(
        """
        INSERT INTO economy_transactions (user_id, kind, amount, note)
        SELECT user_id, 'owner_add', %s, %s FROM unnest(%s::bigint[]) AS user_id
        """,
        (amount, f"Bulk by {actor_id}: {note}", updated_ids)
    )
    conn.commit()
    cur.close()
    conn.close()
    return len(updated_ids)

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
        RETURNING user_id
        """,
        (amount, unique_ids)
    )
    updated_ids = [int(row["user_id"]) for row in cur.fetchall()]
    if len(updated_ids) != len(unique_ids):
        conn.rollback()
        cur.close()
        conn.close()
        raise RuntimeError(f"Bulk set updated {len(updated_ids)} of {len(unique_ids)} economy rows")
    cur.execute(
        """
        INSERT INTO economy_transactions (user_id, kind, amount, note)
        SELECT user_id, 'owner_set', %s, %s FROM unnest(%s::bigint[]) AS user_id
        """,
        (amount, f"Set by {actor_id}: {note}", updated_ids)
    )
    conn.commit()
    cur.close()
    conn.close()
    return len(updated_ids)

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

def record_game_result(user_id, game_key, won, net_amount, payout=0):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO economy_game_stats (user_id, game_key, played, wins, losses, profit, biggest_win)
            VALUES (%s, %s, 1, %s, %s, %s, %s)
            ON CONFLICT (user_id, game_key) DO UPDATE SET
                played = economy_game_stats.played + 1,
                wins = economy_game_stats.wins + EXCLUDED.wins,
                losses = economy_game_stats.losses + EXCLUDED.losses,
                profit = economy_game_stats.profit + EXCLUDED.profit,
                biggest_win = GREATEST(economy_game_stats.biggest_win, EXCLUDED.biggest_win)
            RETURNING *
            """,
            (user_id, game_key, 1 if won is True else 0, 1 if won is False else 0, int(net_amount), int(payout) if won is True else 0)
        )
        row = cur.fetchone()
        cur.execute(
            """
            INSERT INTO economy_game_history (user_id, game_key, won, net_amount, payout)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (user_id, game_key, won, int(net_amount), int(payout))
        )
        cur.execute(
            """
            INSERT INTO economy_season_scores (season_key, user_id, played, wins, profit)
            VALUES (%s, %s, 1, %s, %s)
            ON CONFLICT (season_key, user_id) DO UPDATE SET
                played = economy_season_scores.played + 1,
                wins = economy_season_scores.wins + EXCLUDED.wins,
                profit = economy_season_scores.profit + EXCLUDED.profit
            """,
            (current_season_key(), user_id, 1 if won is True else 0, int(net_amount))
        )
        conn.commit()
        if won is True:
            try:
                track_daily_challenge_progress(user_id, game_key, True, 1)
            except Exception as e:
                print(f"Daily challenge progress failed: {type(e).__name__} - {e}")
        return row
    finally:
        cur.close()
        conn.close()

def get_game_stats(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM economy_game_stats WHERE user_id = %s ORDER BY played DESC, game_key ASC",
        (user_id,)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_game_stat(user_id, game_key):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM economy_game_stats WHERE user_id = %s AND game_key = %s",
        (user_id, game_key)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def get_game_history(user_id, limit=12):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT game_key, won, net_amount, payout, created_at
        FROM economy_game_history
        WHERE user_id = %s
        ORDER BY id DESC
        LIMIT %s
        """,
        (user_id, limit)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_game_audit_rows(days=7):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT game_key,
               COUNT(*) AS plays,
               COUNT(*) FILTER (WHERE won IS TRUE) AS wins,
               COUNT(*) FILTER (WHERE won IS FALSE) AS losses,
               COALESCE(SUM(net_amount), 0) AS profit,
               COALESCE(AVG(net_amount), 0) AS avg_net,
               COALESCE(MAX(payout), 0) AS biggest_payout
        FROM economy_game_history
        WHERE created_at > NOW() - (%s * INTERVAL '1 day')
        GROUP BY game_key
        ORDER BY plays DESC, game_key ASC
        """,
        (int(days),)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def set_user_safety_settings(user_id, personal_bet_limit=None, gambling_pause_until=None, clear_limit=False, clear_pause=False):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO economy (user_id, balance) VALUES (%s, 0) ON CONFLICT (user_id) DO NOTHING", (int(user_id),))
    updates = []
    params = []
    if clear_limit:
        updates.append("personal_bet_limit = NULL")
    elif personal_bet_limit is not None:
        updates.append("personal_bet_limit = %s")
        params.append(max(0, int(personal_bet_limit)))
    if clear_pause:
        updates.append("gambling_pause_until = NULL")
    elif gambling_pause_until is not None:
        updates.append("gambling_pause_until = %s")
        pause_value = gambling_pause_until
        if getattr(pause_value, "tzinfo", None) is not None:
            pause_value = pause_value.astimezone(timezone.utc).replace(tzinfo=None)
        params.append(pause_value)
    if updates:
        params.append(int(user_id))
        cur.execute(f"UPDATE economy SET {', '.join(updates)} WHERE user_id = %s RETURNING *", params)
        row = cur.fetchone()
    else:
        cur.execute("SELECT * FROM economy WHERE user_id = %s", (int(user_id),))
        row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row

EVENT_TYPES = {
    "jackpot": "Community sink jackpot. Users donate quesos into a public pot.",
    "shopdiscount": "Shop discount event marker for the server.",
    "doublexp": "Double XP event marker for the server.",
    "taxfree": "Tax-free event marker for the server.",
}

def parse_event_duration(raw):
    text = str(raw or "").strip().casefold()
    match = re.fullmatch(r"(\d{1,3})([smhd])", text)
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2)
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    seconds = amount * multipliers[unit]
    return max(60, min(seconds, 14 * 86400))

def start_economy_event(guild_id, event_key, started_by, channel_id, seconds):
    event_key = str(event_key or "").casefold()
    if event_key not in EVENT_TYPES:
        raise ValueError("unknown_event")
    ends_at = datetime.now(timezone.utc) + timedelta(seconds=int(seconds))
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO economy_events (guild_id, event_key, started_by, channel_id, ends_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (guild_id) DO UPDATE SET
            event_key = EXCLUDED.event_key,
            started_by = EXCLUDED.started_by,
            channel_id = EXCLUDED.channel_id,
            ends_at = EXCLUDED.ends_at,
            created_at = NOW()
        RETURNING *
        """,
        (guild_id, event_key, started_by, channel_id, ends_at.replace(tzinfo=None))
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row

def get_economy_event(guild_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM economy_events WHERE guild_id = %s AND ends_at <= NOW()", (guild_id,))
    cur.execute("SELECT * FROM economy_events WHERE guild_id = %s", (guild_id,))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row

def active_event_key(guild_id):
    if not guild_id:
        return None
    row = get_economy_event(guild_id)
    return str(row["event_key"]).casefold() if row else None

def event_shop_discount_rate(guild_id):
    return 0.15 if active_event_key(guild_id) == "shopdiscount" else 0.0

def event_transfer_tax_rate(guild_id):
    return 0.0 if active_event_key(guild_id) == "taxfree" else TRANSFER_TAX_RATE

def stop_economy_event(guild_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM economy_events WHERE guild_id = %s RETURNING *", (guild_id,))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row

def donate_to_economy_event(guild_id, user_id, amount):
    amount = int(amount)
    if amount <= 0:
        raise ValueError("amount_positive")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM economy_events WHERE guild_id = %s AND ends_at > NOW() FOR UPDATE", (guild_id,))
    event = cur.fetchone()
    if not event:
        conn.rollback()
        cur.close()
        conn.close()
        raise ValueError("no_event")
    cur.execute("INSERT INTO economy (user_id, balance) VALUES (%s, 0) ON CONFLICT (user_id) DO NOTHING", (user_id,))
    cur.execute("SELECT balance FROM economy WHERE user_id = %s FOR UPDATE", (user_id,))
    row = cur.fetchone()
    balance = int(row["balance"] or 0)
    if balance < amount:
        conn.rollback()
        cur.close()
        conn.close()
        raise ValueError("insufficient_balance")
    new_balance = balance - amount
    cur.execute("UPDATE economy SET balance = %s WHERE user_id = %s", (new_balance, user_id))
    cur.execute("UPDATE economy_events SET pot = pot + %s WHERE guild_id = %s RETURNING *", (amount, guild_id))
    event = cur.fetchone()
    cur.execute(
        "INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, 'event_sink', %s, %s)",
        (user_id, -amount, f"Event donation: {event['event_key']} in guild {guild_id}")
    )
    conn.commit()
    cur.close()
    conn.close()
    return balance, new_balance, event

def create_receipt(guild_id, channel_id, actor_id, target_ids, action, amount=None, details=None):
    receipt_id = f"QTX-{int(time.time())}-{random.randint(1000, 9999)}"
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO bot_receipts (receipt_id, guild_id, channel_id, actor_id, target_ids, action, amount, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (receipt_id) DO NOTHING
        """,
        (
            receipt_id,
            int(guild_id or 0),
            int(channel_id) if channel_id else None,
            int(actor_id),
            [int(x) for x in (target_ids or [])],
            str(action or "")[:120],
            int(amount) if amount is not None else None,
            str(details or "")[:1500],
        )
    )
    conn.commit()
    cur.close()
    conn.close()
    return receipt_id

def receipt_line(receipt_id):
    return f"\n-# {Q_ARCHIVE} ||Receipt|| `{receipt_id}`" if receipt_id else ""

def get_receipt(receipt_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT receipt_id, guild_id, channel_id, actor_id, target_ids, action, amount, details, created_at FROM bot_receipts WHERE receipt_id = %s",
        (str(receipt_id),)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def get_receipts_for_user(user_id=None, guild_id=None, limit=10):
    limit = max(1, min(int(limit or 10), 25))
    conn = get_db_connection()
    cur = conn.cursor()
    clauses = []
    params = []
    if guild_id is not None:
        clauses.append("guild_id = %s")
        params.append(int(guild_id))
    if user_id is not None:
        clauses.append("(actor_id = %s OR %s = ANY(target_ids))")
        params.extend([int(user_id), int(user_id)])
    where_sql = "WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(limit)
    cur.execute(
        f"""
        SELECT receipt_id, guild_id, channel_id, actor_id, target_ids, action, amount, details, created_at
        FROM bot_receipts
        {where_sql}
        ORDER BY created_at DESC
        LIMIT %s
        """,
        params,
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def current_season_key(now=None):
    now = now or datetime.now(timezone.utc)
    return now.strftime("%Y-%m")

def season_end_timestamp(now=None):
    now = now or datetime.now(timezone.utc)
    year = now.year + (1 if now.month == 12 else 0)
    month = 1 if now.month == 12 else now.month + 1
    end = datetime(year, month, 1, tzinfo=timezone.utc)
    return int(end.timestamp())

SEASON_REWARDS = [100_000_000, 80_000_000, 50_000_000]

def get_season_leaderboard(season_key=None, limit=10):
    season_key = season_key or current_season_key()
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM economy_season_scores
        WHERE season_key = %s
        ORDER BY profit DESC, wins DESC, played DESC, user_id ASC
        LIMIT %s
        """,
        (season_key, int(limit))
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def reward_previous_season(season_key):
    rows = get_season_leaderboard(season_key, len(SEASON_REWARDS))
    if not rows:
        return []
    conn = get_db_connection()
    cur = conn.cursor()
    rewarded = []
    try:
        for index, row in enumerate(rows, 1):
            reward = SEASON_REWARDS[index - 1]
            user_id = int(row["user_id"])
            cur.execute(
                """
                INSERT INTO economy_season_rewards (season_key, user_id, rank, reward)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (season_key, user_id) DO NOTHING
                RETURNING user_id
                """,
                (season_key, user_id, index, reward)
            )
            if cur.fetchone() is None:
                continue
            cur.execute(
                "INSERT INTO economy (user_id, balance) VALUES (%s, 0) ON CONFLICT (user_id) DO NOTHING",
                (user_id,)
            )
            cur.execute(
                "UPDATE economy SET balance = balance + %s, total_earned = total_earned + %s WHERE user_id = %s",
                (reward, reward, user_id)
            )
            cur.execute(
                "INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, %s, %s, %s)",
                (user_id, "season_reward", reward, f"{season_key} rank #{index}")
            )
            rewarded.append((index, user_id, reward))
        conn.commit()
        return rewarded
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def get_economy_audit():
    stats = get_economy_stats()
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            game_key,
            SUM(played) AS played,
            SUM(wins) AS wins,
            SUM(losses) AS losses,
            SUM(profit) AS profit,
            MAX(biggest_win) AS biggest_win
        FROM economy_game_stats
        GROUP BY game_key
        ORDER BY ABS(SUM(profit)) DESC, SUM(played) DESC
        LIMIT 12
        """
    )
    games = cur.fetchall()
    cur.execute(
        """
        SELECT COALESCE(SUM(amount), 0) AS lost_today
        FROM economy_daily_losses
        WHERE loss_date = CURRENT_DATE
        """
    )
    daily_losses = cur.fetchone()
    cur.execute(
        """
        SELECT COUNT(*) AS rows, COALESCE(SUM(amount), 0) AS amount
        FROM economy_transactions
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        """
    )
    tx_24h = cur.fetchone()
    cur.close()
    conn.close()
    stats["games"] = games
    stats["lost_today"] = int(daily_losses["lost_today"] or 0)
    stats["transactions_24h"] = int(tx_24h["rows"] or 0)
    stats["transaction_amount_24h"] = int(tx_24h["amount"] or 0)
    return stats

def get_lottery_user_spend(guild_id, user_id):
    if not guild_id:
        return None
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT tickets, spent FROM lottery_tickets WHERE guild_id = %s AND user_id = %s",
        (guild_id, user_id)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def get_daily_loss_amount(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT amount FROM economy_daily_losses WHERE user_id = %s AND loss_date = CURRENT_DATE",
        (user_id,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return int((row or {}).get("amount", 0) or 0)

def daily_loss_status(user_id, balance, proposed_loss=0):
    lost_today = get_daily_loss_amount(user_id)
    balance = max(0, int(balance or 0))
    proposed_loss = max(0, int(proposed_loss or 0))
    bankroll = max(1, balance + lost_today)
    hard_limit = int(bankroll * DAILY_LOSS_HARD_RATIO)
    warning_limit = int(bankroll * DAILY_LOSS_WARNING_RATIO)
    after_loss = lost_today + proposed_loss
    remaining = max(0, hard_limit - lost_today)
    return {
        "lost_today": lost_today,
        "bankroll": bankroll,
        "hard_limit": hard_limit,
        "warning_limit": warning_limit,
        "after_loss": after_loss,
        "remaining": remaining,
        "blocked": after_loss > hard_limit,
        "warn": after_loss >= warning_limit,
    }

async def check_daily_loss_limit(ctx, data, amount):
    status = await asyncio.to_thread(daily_loss_status, ctx.author.id, int(data["balance"]), int(amount))
    if status["blocked"]:
        await ctx.send(
            f"{Q_WARNING} Daily safety limit reached. You can risk up to **{format_balance(status['remaining'])}** more today.\n"
            f"Lost today: **{format_balance(status['lost_today'])}** / limit **{format_balance(status['hard_limit'])}**."
        )
        return False
    if status["warn"]:
        await ctx.send(
            f"{Q_WARNING} Warning: this could put your daily gambling losses near the safety limit "
            f"(**{format_balance(status['after_loss'])}** / **{format_balance(status['hard_limit'])}**)."
        )
    return True

def get_economy_stats():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            COUNT(*) AS users,
            COALESCE(SUM(balance), 0) AS total_balance,
            COALESCE(SUM(total_earned), 0) AS total_earned,
            COALESCE(SUM(total_won), 0) AS total_won,
            COALESCE(SUM(total_lost), 0) AS total_lost,
            COALESCE(SUM(messages_sent), 0) AS messages_sent,
            COALESCE(MAX(balance), 0) AS richest_balance
        FROM economy
        """
    )
    totals = cur.fetchone()
    cur.execute(
        """
        SELECT user_id, balance
        FROM economy
        ORDER BY balance DESC, user_id ASC
        LIMIT 1
        """
    )
    richest = cur.fetchone()
    cur.execute("SELECT COUNT(*) AS count, COALESCE(SUM(pot), 0) AS total_pot FROM lottery_config")
    lottery = cur.fetchone()
    cur.execute("SELECT COALESCE(SUM(tickets), 0) AS tickets, COALESCE(SUM(spent), 0) AS spent FROM lottery_tickets")
    tickets = cur.fetchone()
    cur.execute(
        """
        SELECT
            kind,
            COALESCE(SUM(
                CASE
                    WHEN kind = 'transfer_tax' AND user_id != %s THEN 0
                    ELSE amount
                END
            ), 0) AS total
        FROM economy_transactions
        WHERE kind IN ('transfer_tax', 'lottery_house_cut', 'shop_payment', 'shop_purchase')
        GROUP BY kind
        """,
        (SUPER_OWNER_ID,)
    )
    transaction_totals = {row["kind"]: int(row["total"] or 0) for row in cur.fetchall()}
    cur.close()
    conn.close()
    return {
        "users": int(totals["users"] or 0),
        "total_balance": int(totals["total_balance"] or 0),
        "total_earned": int(totals["total_earned"] or 0),
        "total_won": int(totals["total_won"] or 0),
        "total_lost": int(totals["total_lost"] or 0),
        "messages_sent": int(totals["messages_sent"] or 0),
        "richest_user_id": int(richest["user_id"]) if richest else None,
        "richest_balance": int(richest["balance"] or 0) if richest else 0,
        "active_lotteries": int(lottery["count"] or 0),
        "lottery_pots": int(lottery["total_pot"] or 0),
        "lottery_tickets": int(tickets["tickets"] or 0),
        "lottery_spent": int(tickets["spent"] or 0),
        "transaction_totals": transaction_totals,
    }

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

def equipped_badge_ids(data):
    return [badge for badge in list(data.get("equipped_badges") or []) if badge in achievement_ids(data)]

def achievement_display(achievement_id):
    achievement = GAME_ACHIEVEMENTS.get(achievement_id)
    if not achievement:
        return achievement_id
    tier = achievement.get("tier")
    return f"{achievement['name']} ({tier})" if tier else achievement["name"]

def quest_claim_ids(data):
    return list(data.get("quest_claims") or [])

def claim_completed_quests_sync(user_id):
    current = get_user(user_id)
    total_reward = 0
    achievements = achievement_ids(current)
    claims = quest_claim_ids(current)

    for quest_id, quest in MAIN_QUESTS.items():
        if quest_id not in achievements and int(current.get(quest["field"]) or 0) >= quest["target"]:
            achievements.append(quest_id)
            total_reward += quest["reward"]
            log_transaction(user_id, "achievement", quest["reward"], quest["name"])

    for period in ["daily", "weekly", "monthly"]:
        for quest_name, _, metric, target, reward in selected_period_quests(user_id, period):
            claim_id = quest_claim_id(period, quest_name)
            if claim_id not in claims and quest_progress(current, metric) >= target:
                claims.append(claim_id)
                total_reward += reward
                log_transaction(user_id, f"{period}_quest", reward, quest_name)

    if total_reward <= 0:
        return current, total_reward, current

    update_user(
        user_id,
        achievements=achievements,
        quest_claims=claims,
        balance=current["balance"] + total_reward,
        total_earned=current["total_earned"] + total_reward
    )
    return current, total_reward, get_user(user_id)

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

def save_lottery_status_message(guild_id, channel_id, message_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO lottery_status_messages (guild_id, channel_id, message_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (guild_id, channel_id, message_id) DO NOTHING
        """,
        (int(guild_id), int(channel_id), int(message_id))
    )
    conn.commit()
    cur.close()
    conn.close()

def delete_lottery_status_message(guild_id, channel_id, message_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM lottery_status_messages WHERE guild_id = %s AND channel_id = %s AND message_id = %s",
        (int(guild_id), int(channel_id), int(message_id))
    )
    conn.commit()
    cur.close()
    conn.close()

def lottery_status_message_rows(guild_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT channel_id, message_id FROM lottery_status_messages WHERE guild_id = %s",
        (int(guild_id),)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

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
    if mode not in {"add", "remove", "set"}:
        raise ValueError("mode must be add, remove, or set")

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
    elif mode == "remove":
        cur.execute(
            """
            INSERT INTO lottery_tickets (guild_id, user_id, tickets, spent, pot_add)
            SELECT %s, user_id, 0, 0, 0 FROM unnest(%s::bigint[]) AS user_id
            ON CONFLICT (guild_id, user_id) DO UPDATE SET
                tickets = GREATEST(lottery_tickets.tickets - %s, 0)
            """,
            (guild_id, unique_ids, tickets)
        )
        kind = "lottery_admin_remove"
        note = f"Removed {tickets} tickets by {actor_id}"
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
        (kind, -tickets if mode == "remove" else tickets, note, unique_ids)
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

def remember_lottery_status_message(guild_id, message):
    guild_id = int(guild_id)
    lottery_status_messages.setdefault(guild_id, set()).add((message.channel.id, message.id))
    try:
        save_lottery_status_message(guild_id, message.channel.id, message.id)
    except Exception as e:
        print(f"Lottery status message save failed: {type(e).__name__} - {e}")

def forget_lottery_status_message(guild_id, channel_id, message_id):
    guild_id = int(guild_id)
    messages = lottery_status_messages.get(int(guild_id))
    if messages:
        messages.discard((int(channel_id), int(message_id)))
        if not messages:
            lottery_status_messages.pop(guild_id, None)
    try:
        delete_lottery_status_message(guild_id, channel_id, message_id)
    except Exception as e:
        print(f"Lottery status message delete failed: {type(e).__name__} - {e}")

def load_lottery_status_messages(guild_id):
    try:
        rows = lottery_status_message_rows(guild_id)
    except Exception as e:
        print(f"Lottery status message load failed: {type(e).__name__} - {e}")
        return set()
    return {(int(row["channel_id"]), int(row["message_id"])) for row in rows}

async def build_lottery_status_embed(guild, config, panel_message=None):
    embed = await asyncio.to_thread(build_lottery_embed, guild, config)
    if panel_message:
        panel_url = panel_message if isinstance(panel_message, str) else panel_message.jump_url
        embed.add_field(name="Lottery Panel", value=f"[Open Panel]({panel_url})", inline=False)
    return embed

def lottery_panel_url(guild, config):
    if not guild or not config or not config.get("channel_id") or not config.get("message_id"):
        return None
    return f"https://discord.com/channels/{guild.id}/{int(config['channel_id'])}/{int(config['message_id'])}"

def schedule_lottery_refresh(guild, config=None):
    if not guild:
        return
    async def runner():
        try:
            await refresh_lottery_message(guild, config)
        except Exception as e:
            print(f"Lottery background refresh failed: {type(e).__name__} - {e}")
    asyncio.create_task(runner())

async def refresh_lottery_status_messages(guild, config=None, panel_message=None):
    if not guild:
        return
    guild_id = int(guild.id)
    saved = load_lottery_status_messages(guild_id)
    if saved:
        lottery_status_messages.setdefault(guild_id, set()).update(saved)
    tracked = list(lottery_status_messages.get(guild_id, set()))
    if not tracked:
        return
    if config is None:
        config = await asyncio.to_thread(get_lottery_config, guild_id)
    if not config:
        lottery_status_messages.pop(guild_id, None)
        return

    embed = await build_lottery_status_embed(guild, config, panel_message)
    view = LotteryPanelView()
    for channel_id, message_id in tracked:
        channel = guild.get_channel(channel_id)
        if channel is None and bot:
            try:
                channel = await bot.fetch_channel(channel_id)
            except Exception:
                channel = None
        if channel is None:
            forget_lottery_status_message(guild_id, channel_id, message_id)
            continue
        try:
            message = await channel.fetch_message(message_id)
            await message.edit(embed=embed, view=view, allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            forget_lottery_status_message(guild_id, channel_id, message_id)

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
        cur.execute(
            "INSERT INTO economy (user_id, balance) VALUES (%s, 0) "
            "ON CONFLICT (user_id) DO NOTHING",
            (SUPER_OWNER_ID,)
        )
        cur.execute("SELECT * FROM economy WHERE user_id = %s FOR UPDATE", (user_id,))
        data = cur.fetchone()
        if data["balance"] < total_cost:
            conn.rollback()
            return {
                "ok": False,
                "message": f"{Q_DENIED} You need {format_balance(total_cost)}, but you only have {format_balance(data['balance'])}.",
            }

        cur.execute(
            "SELECT spent FROM lottery_tickets WHERE guild_id = %s AND user_id = %s FOR UPDATE",
            (guild_id, user_id)
        )
        ticket_row = cur.fetchone()
        round_spent = int(ticket_row["spent"] or 0) if ticket_row else 0
        spend_base = int(data["balance"]) + round_spent
        max_round_spend = int(spend_base * LOTTERY_MAX_BALANCE_SPEND_RATIO)
        remaining_lottery_spend = max(0, max_round_spend - round_spent)
        if total_cost > remaining_lottery_spend:
            max_more_tickets = remaining_lottery_spend // ticket_cost
            conn.rollback()
            return {
                "ok": False,
                "message": (
                    f"{Q_DENIED} Lottery safety limit: you can spend at most "
                    f"**{format_balance(max_round_spend)}** this round (**{int(LOTTERY_MAX_BALANCE_SPEND_RATIO * 100)}%** of your lottery-adjusted balance).\n"
                    f"Already spent: **{format_balance(round_spent)}** | Remaining allowed: **{format_balance(remaining_lottery_spend)}**\n"
                    f"Max tickets you can buy now: **{max_more_tickets:,}**"
                ),
            }

        bonus_tickets = int(tickets * item_bonus(data, "ticket_charm", 0.02, 5))
        total_entries = tickets + bonus_tickets
        new_balance = data["balance"] - total_cost
        new_pot = int(config["pot"] or 0) + pot_add

        cur.execute("UPDATE economy SET balance = %s WHERE user_id = %s RETURNING balance", (new_balance, user_id))
        updated = cur.fetchone()
        if updated is None:
            conn.rollback()
            return {"ok": False, "message": f"{Q_DENIED} Ticket purchase failed because your balance row could not be updated."}
        cur.execute(
            """
            INSERT INTO lottery_tickets (guild_id, user_id, tickets, spent, pot_add)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET
                tickets = lottery_tickets.tickets + EXCLUDED.tickets,
                spent = lottery_tickets.spent + EXCLUDED.spent,
                pot_add = lottery_tickets.pot_add + EXCLUDED.pot_add
            RETURNING tickets
            """,
            (guild_id, user_id, total_entries, total_cost, pot_add)
        )
        user_total_entries = int(cur.fetchone()["tickets"])
        cur.execute("SELECT COALESCE(SUM(tickets), 0) AS total FROM lottery_tickets WHERE guild_id = %s", (guild_id,))
        round_total_entries = int(cur.fetchone()["total"] or 0)
        cur.execute("UPDATE lottery_config SET pot = %s WHERE guild_id = %s", (new_pot, guild_id))
        if burned > 0:
            cur.execute(
                "UPDATE economy SET balance = balance + %s WHERE user_id = %s",
                (burned, SUPER_OWNER_ID)
            )
        cur.execute(
            "INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, %s, %s, %s)",
            (user_id, "lottery_tickets", -total_cost, f"{tickets} tickets; {bonus_tickets} bonus; {burned} burned")
        )
        if burned > 0:
            cur.execute(
                "INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, %s, %s, %s)",
                (SUPER_OWNER_ID, "lottery_house_cut", burned, f"Guild {guild_id}; buyer {user_id}; {tickets} tickets")
            )
        conn.commit()
        config["pot"] = new_pot
        return {
            "ok": True,
            "config": config,
            "tickets": tickets,
            "bonus_tickets": bonus_tickets,
            "total_entries": total_entries,
            "user_total_entries": user_total_entries,
            "round_total_entries": round_total_entries,
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
    user_total_entries = result.get("user_total_entries", result["total_entries"])
    round_total_entries = max(1, int(result.get("round_total_entries") or user_total_entries))
    odds = (user_total_entries / round_total_entries) * 100
    return (
        f"{Q_TICKET} Bought **{result['tickets']:,}** lottery tickets for **{format_balance(result['total_cost'])}**.\n"
        f"Bonus Tickets: **+{result['bonus_tickets']:,}** | Your Total Entries: **{user_total_entries:,}**\n"
        f"{Q_TARGET} Odds Now: **{odds:.2f}%** ({user_total_entries:,}/{round_total_entries:,} entries)\n"
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
            await refresh_lottery_status_messages(guild, config, message)
            return message
        except Exception as e:
            print(f"Lottery panel refresh will recreate message: {type(e).__name__} - {e}")

    if not create_if_missing:
        return None
    message = await channel.send(embed=embed, view=view, allowed_mentions=discord.AllowedMentions.none())
    await asyncio.to_thread(update_lottery_config, guild.id, message_id=message.id)
    config["message_id"] = message.id
    await refresh_lottery_status_messages(guild, config, message)
    return message

async def clear_lottery_channel(channel):
    if channel is None:
        return 0
    try:
        deleted_count = 0
        while True:
            deleted = await channel.purge(limit=100, reason="Lottery round finished")
            deleted_count += len(deleted)
            if len(deleted) < 100:
                break
            await asyncio.sleep(1)
        return deleted_count
    except Exception as e:
        print(f"Lottery channel purge failed, trying manual delete: {type(e).__name__} - {e}")

    deleted_count = 0
    try:
        async for message in channel.history(limit=500):
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
        await interaction.followup.send(lottery_purchase_message(result), ephemeral=True)
        schedule_lottery_refresh(interaction.guild, result["config"])
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
        data = await asyncio.to_thread(get_user, interaction.user.id)
        ticket_cost = lottery_ticket_cost(config)
        round_spent = int(own["spent"] or 0) if own else 0
        spend_base = int(data["balance"] or 0) + round_spent
        max_round_spend = int(spend_base * LOTTERY_MAX_BALANCE_SPEND_RATIO)
        remaining_lottery_spend = max(0, max_round_spend - round_spent)
        max_more_tickets = remaining_lottery_spend // max(1, ticket_cost)
        total_tickets = sum(int(row["tickets"]) for row in rows)
        own_tickets = int(own["tickets"]) if own else 0
        odds = (own_tickets / total_tickets * 100) if total_tickets else 0
        own_text = (
            f"{Q_TICKET} Your Entries: **{own_tickets:,}** ({odds:.2f}% current chance)\n"
            f"Spent This Round: **{format_balance(round_spent)}**\n"
            f"Max More Tickets Now: **{max_more_tickets:,}**"
        )
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

def resolve_lottery_text_channel(guild, raw, mentioned_channels=None):
    for channel in mentioned_channels or []:
        if getattr(channel, "guild", None) == guild and isinstance(channel, discord.TextChannel):
            return channel
    raw_text = str(raw or "").strip()
    name_text = raw_text[1:] if raw_text.startswith("#") else raw_text
    if name_text:
        lowered = name_text.casefold()
        for channel in getattr(guild, "text_channels", []) or []:
            if channel.name.casefold() == lowered:
                return channel
    match = re.search(r"\d{15,25}", str(raw or ""))
    if not match:
        return None
    channel = guild.get_channel(int(match.group(0)))
    if isinstance(channel, discord.TextChannel):
        return channel
    return None

async def apply_lottery_edit(guild, author, setting, value, send, channel_mentions=None):
    config = await asyncio.to_thread(get_lottery_config, guild.id)
    if config is None:
        await send("Lottery is not set up yet. Run `.lottery` first.")
        return False

    setting = str(setting or "").casefold()
    updates = {}
    message = ""

    if setting in {"price", "ticket", "ticketprice"}:
        parsed = parse_amount(value, author.id, guild, None)
        if parsed is None or parsed <= 0:
            await send(f"{Q_DENIED} Ticket price must be a positive number.")
            return False
        updates["ticket_cost"] = parsed
        message = f"Ticket price set to **{format_balance(parsed)}**."

    elif setting in {"duration", "period", "time"}:
        seconds = period_seconds_from_text(value)
        if seconds is None:
            await send(f"{Q_DENIED} Invalid duration. Use at least 5 minutes, like `30m`, `12h`, or `1d`.")
            return False
        updates["period_seconds"] = seconds
        updates["next_draw"] = datetime.now(timezone.utc) + timedelta(seconds=seconds)
        message = f"Lottery duration set to **{format_duration(seconds)}**. Next draw was reset."

    elif setting in {"cut", "housecut", "tax", "burn"}:
        try:
            cut_percent = float(str(value).strip().replace("%", ""))
        except ValueError:
            await send(f"{Q_DENIED} House cut must be a number from 0 to 90.")
            return False
        if cut_percent < 0 or cut_percent > 90:
            await send(f"{Q_DENIED} House cut must be from 0 to 90.")
            return False
        updates["house_cut"] = cut_percent / 100
        message = f"House cut set to **{cut_percent:.1f}%**."

    elif setting in {"channel", "chan"}:
        channel = resolve_lottery_text_channel(guild, value, channel_mentions)
        if channel is None:
            await send(f"{Q_DENIED} Mention a normal text channel or send its channel ID.")
            return False
        try:
            await prepare_lottery_channel(
                guild,
                channel,
                int(config["period_seconds"]),
                lottery_ticket_cost(config),
                lottery_house_cut(config)
            )
        except Exception as e:
            await send(f"{Q_DENIED} I couldn't prepare that channel: {public_error_text(e)}")
            return False
        updates["channel_id"] = channel.id
        updates["thread_id"] = None
        updates["message_id"] = None
        message = f"Lottery channel moved to {channel.mention}; a fresh ticket panel was posted."

    else:
        await send(f"{Q_DENIED} Unknown setting. Use `price`, `duration`, `cut`, or `channel`.")
        return False

    try:
        await asyncio.to_thread(update_lottery_config, guild.id, **updates)
    except Exception:
        await send(f"{Q_DENIED} Database unavailable. Try again shortly.")
        return False

    updated = await asyncio.to_thread(get_lottery_config, guild.id)
    if updated:
        await announce_lottery_update(guild, updated, message)

    await send(f"{Q_SUCCESS} {message}", allowed_mentions=discord.AllowedMentions.none())
    return True

class LotteryEditValueModal(discord.ui.Modal):
    def __init__(self, author_id, setting, label, placeholder):
        super().__init__(title=f"Edit lottery {label.lower()}")
        self.author_id = author_id
        self.setting = setting
        self.value_input = discord.ui.TextInput(
            label=label,
            placeholder=placeholder,
            min_length=1,
            max_length=100,
        )
        self.add_item(self.value_input)

    async def on_submit(self, interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)

        async def send(content=None, **kwargs):
            kwargs.setdefault("ephemeral", True)
            return await interaction.followup.send(content, **kwargs)

        await apply_lottery_edit(
            interaction.guild,
            interaction.user,
            self.setting,
            str(self.value_input.value).strip(),
            send,
        )

class LotteryEditSettingSelect(discord.ui.Select):
    def __init__(self):
        super().__init__(
            placeholder="Choose what to edit",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label="Ticket price", value="price", description="Example: 250000"),
                discord.SelectOption(label="Duration", value="duration", description="Example: 12h"),
                discord.SelectOption(label="House cut", value="cut", description="Example: 5"),
                discord.SelectOption(label="Channel", value="channel", description="Paste #channel or its ID"),
            ],
        )

    async def callback(self, interaction):
        if interaction.user.id != self.view.author_id:
            await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
            return
        setting = self.values[0]
        modal_specs = {
            "price": ("Ticket price", "250000"),
            "duration": ("Duration", "12h"),
            "cut": ("House cut percent", "5"),
            "channel": ("Channel mention or ID", "#lottery or 123456789012345678"),
        }
        label, placeholder = modal_specs[setting]
        await interaction.response.send_modal(LotteryEditValueModal(self.view.author_id, setting, label, placeholder))

class LotteryEditView(discord.ui.View):
    def __init__(self, author_id):
        super().__init__(timeout=LONG_SETUP_VIEW_TIMEOUT)
        self.author_id = author_id
        self.add_item(LotteryEditSettingSelect())

async def send_lottery_edit_ui(ctx, selected_setting=None):
    embed = discord.Embed(
        title=f"{Q_EDIT} Edit Lottery",
        description="Choose a setting, then enter the new value.",
        color=discord.Color.blue(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Ticket Price", value="Amount per ticket, like `250000`.", inline=False)
    embed.add_field(name="Duration", value="Time between draws, like `12h` or `1d`.", inline=False)
    embed.add_field(name="House Cut", value="Percent taken from purchases, like `5`.", inline=False)
    embed.add_field(name="Channel", value="Paste a channel mention or ID.", inline=False)
    await ctx.send(embed=embed, view=LotteryEditView(ctx.author.id), allowed_mentions=discord.AllowedMentions.none())

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
    schedule_lottery_refresh(guild, config)
    await channel.send(
        f"{role_mention} {QOIN_CHEST} **Lottery Update**\n{message}".strip(),
        allowed_mentions=discord.AllowedMentions(roles=True)
    )

def award_chat_xp(user_id, event_multiplier=1):
    data = get_user(user_id)
    now = datetime.now(timezone.utc)
    last_xp = data.get("last_xp")
    if last_xp:
        last_xp = last_xp.replace(tzinfo=timezone.utc) if last_xp.tzinfo is None else last_xp
        if (now - last_xp).total_seconds() < CHAT_XP_COOLDOWN_SECS:
            return None

    gained_xp = max(1, int(random.randint(15, 25) * xp_multiplier(data) * max(1, event_multiplier or 1)))
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

def fit_discord_content(content, limit=2000):
    content = str(content or "")
    if len(content) <= limit:
        return content
    return content[:limit - 20].rstrip() + "\n…"

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

def consume_inventory_item(user_id, item_id, quantity=1):
    data = get_user(user_id)
    inventory = user_inventory(data)
    quantity = max(1, int(quantity or 1))
    removed = 0
    remaining = []
    for owned in inventory:
        if owned == item_id and removed < quantity:
            removed += 1
            continue
        remaining.append(owned)
    if removed <= 0:
        return False, data
    updated = update_user(user_id, inventory=remaining)
    return True, updated

def robbing_enabled(guild_id):
    if guild_id is None:
        return False
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT robbing_enabled FROM economy_guild_settings WHERE guild_id = %s", (int(guild_id),))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return bool(row and row["robbing_enabled"])

def set_robbing_enabled(guild_id, enabled):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO economy_guild_settings (guild_id, robbing_enabled)
        VALUES (%s, %s)
        ON CONFLICT (guild_id) DO UPDATE SET robbing_enabled = EXCLUDED.robbing_enabled
        """,
        (int(guild_id), bool(enabled)),
    )
    conn.commit()
    cur.close()
    conn.close()
    return bool(enabled)

def transfer_to_bank(user_id, amount, mode):
    amount = int(amount)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO economy (user_id, balance, bank_balance) VALUES (%s, 0, 0) ON CONFLICT (user_id) DO NOTHING", (int(user_id),))
        cur.execute("SELECT balance, bank_balance FROM economy WHERE user_id = %s FOR UPDATE", (int(user_id),))
        row = cur.fetchone()
        balance = int(row["balance"] or 0)
        bank = int(row["bank_balance"] or 0)
        if mode == "deposit":
            if amount <= 0 or amount > balance:
                raise ValueError("deposit")
            new_balance = balance - amount
            new_bank = bank + amount
            kind = "bank_deposit"
            note = "Bank deposit"
        else:
            if amount <= 0 or amount > bank:
                raise ValueError("withdraw")
            new_balance = balance + amount
            new_bank = bank - amount
            kind = "bank_withdraw"
            note = "Bank withdraw"
        cur.execute("UPDATE economy SET balance = %s, bank_balance = %s WHERE user_id = %s", (new_balance, new_bank, int(user_id)))
        cur.execute("INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, %s, %s, %s)", (int(user_id), kind, amount if mode == "withdraw" else -amount, note))
        conn.commit()
        return {"old_balance": balance, "old_bank": bank, "balance": new_balance, "bank": new_bank, "amount": amount}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def claim_bank_interest(user_id):
    current = get_user(user_id)
    bank = int(current.get("bank_balance") or 0)
    if bank <= 0:
        return {"ok": False, "message": f"{Q_DENIED} You need banked money before claiming interest."}
    last = current.get("last_bank_interest")
    now = datetime.now(timezone.utc)
    if last:
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        next_claim = last + timedelta(hours=24)
        if now < next_claim:
            return {"ok": False, "message": f"{Q_TIMER_TICK} Bank interest is ready {discord_relative_time(next_claim)}."}
    interest = max(1_000, min(100_000, int(bank * 0.005)))
    updated = update_user(
        user_id,
        balance=int(current["balance"] or 0) + interest,
        total_earned=int(current["total_earned"] or 0) + interest,
        last_bank_interest=now.replace(tzinfo=None)
    )
    log_transaction(user_id, "bank_interest", interest, "daily bank interest")
    return {"ok": True, "amount": interest, "balance": int(updated["balance"] or 0), "bank": bank}

def rob_user_sync(guild_id, robber_id, target_id):
    if not robbing_enabled(guild_id):
        return {"ok": False, "message": f"{Q_DENIED} Robbing is disabled in this server."}
    if int(robber_id) == int(target_id):
        return {"ok": False, "message": f"{Q_DENIED} Robbing yourself is just moving money with extra drama."}
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for uid in (int(robber_id), int(target_id)):
            cur.execute("INSERT INTO economy (user_id, balance, bank_balance) VALUES (%s, 0, 0) ON CONFLICT (user_id) DO NOTHING", (uid,))
        first, second = sorted([int(robber_id), int(target_id)])
        cur.execute("SELECT * FROM economy WHERE user_id = %s FOR UPDATE", (first,))
        first_row = cur.fetchone()
        cur.execute("SELECT * FROM economy WHERE user_id = %s FOR UPDATE", (second,))
        second_row = cur.fetchone()
        rows = {int(first_row["user_id"]): first_row, int(second_row["user_id"]): second_row}
        robber = rows[int(robber_id)]
        target = rows[int(target_id)]
        target_cash = int(target["balance"] or 0)
        robber_cash = int(robber["balance"] or 0)
        if target_cash < 25_000:
            return {"ok": False, "message": f"{Q_DENIED} They do not have enough cash out to rob. Banked money is protected."}
        success = random.random() < 0.34
        if success:
            amount = min(200_000, max(10_000, int(target_cash * random.uniform(0.05, 0.12))))
            new_robber = robber_cash + amount
            new_target = target_cash - amount
            cur.execute("UPDATE economy SET balance = %s, total_earned = total_earned + %s WHERE user_id = %s", (new_robber, amount, int(robber_id)))
            cur.execute("UPDATE economy SET balance = %s WHERE user_id = %s", (new_target, int(target_id)))
            cur.execute("INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, 'rob_success', %s, %s)", (int(robber_id), amount, f"Robbed {target_id}"))
            cur.execute("INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, 'robbed', %s, %s)", (int(target_id), -amount, f"Robbed by {robber_id}"))
            conn.commit()
            return {"ok": True, "success": True, "amount": amount, "robber_balance": new_robber, "target_balance": new_target}
        fine = min(max(15_000, int(robber_cash * 0.08)), 75_000, robber_cash)
        if fine > 0:
            cur.execute("UPDATE economy SET balance = balance - %s WHERE user_id = %s", (fine, int(robber_id)))
            cur.execute("UPDATE economy SET balance = balance + %s WHERE user_id = %s", (fine, int(target_id)))
            cur.execute("INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, 'rob_fail_fine', %s, %s)", (int(robber_id), -fine, f"Failed rob against {target_id}"))
            cur.execute("INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, 'rob_fail_paid', %s, %s)", (int(target_id), fine, f"Rob attempt by {robber_id} failed"))
        conn.commit()
        return {"ok": True, "success": False, "fine": fine, "robber_balance": robber_cash - fine, "target_balance": target_cash + fine}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def item_bonus(data, item_id, per_item, max_qty=None):
    qty = item_count(data, item_id)
    if max_qty is not None:
        qty = min(qty, max_qty)
    return qty * per_item

def item_rarity(item):
    return item.get("rarity", "Common")

def item_rarity_label(item):
    rarity = item_rarity(item)
    markers = {
        "Common": Q_RISK_LOW,
        "Rare": Q_RISK_MEDIUM,
        "Epic": Q_RISK_HIGH,
        "Royal": Q_RISK_EXTREME,
    }
    return f"{markers.get(rarity, Q_RISK_LOW)} {rarity}"

def streak_bonus_for_wins(streak):
    streak = max(0, int(streak or 0))
    if streak <= 0:
        return 0
    return STREAK_BASE_BONUS + ((streak - 1) * STREAK_STEP_BONUS)

def payout_multiplier(data, streak):
    return (
        1
        + streak_bonus_for_wins(streak)
        + item_bonus(data, "lucky_charm", 0.01, 10)
        + item_bonus(data, "streak_polish", 0.005, 8)
    )

def payout_multiplier_after_win(data, new_streak):
    return payout_multiplier(data, new_streak)

def next_gambling_streak(data):
    return int(data.get("gamble_streak", 0) or 0) + 1

def gambling_streak_text(data, new_streak):
    bonus = streak_bonus_for_wins(new_streak) * 100
    return f" {Q_STREAK_FIRE} {new_streak} in a row! (+{bonus:.2f}% streak)"

GAME_DISPLAY_NAMES = {
    "heist": "Heist",
    "diceduel": "Dice Duel",
    "cases": "Q Cases",
    "plinko": "Plinko",
    "luckynumber": "Lucky Number",
    "jackpotspin": "Jackpot Spin",
    "dungeon": "Dungeon",
    "flagquiz": "Flag Quiz",
    "memory": "Memory",
    "tower": "Tower",
    "vault": "Vault",
    "cardladder": "Card Ladder",
    "lockpick": "Lockpick",
    "ms": "Mine Hunt",
    "wheel": "Wheel",
    "slots": "Slots",
    "scratch": "Scratch",
    "blackjack": "Blackjack",
    "roulette": "Roulette",
    "cf": "Coin Flip",
}

GAME_RISK_LABELS = {
    "heist": "Medium/High",
    "diceduel": "Medium",
    "cases": "Low/High",
    "plinko": "Medium",
    "luckynumber": "Chosen by range",
    "jackpotspin": "Extreme",
    "dungeon": "Free/Solo",
    "flagquiz": "Free/Knowledge",
    "memory": "Skill",
    "tower": "High",
    "vault": "Skill",
    "cardladder": "Medium",
    "lockpick": "Skill",
    "ms": "Skill/High",
    "wheel": "Medium",
    "slots": "High",
    "scratch": "High",
    "blackjack": "Medium",
    "roulette": "Medium",
    "cf": "Medium",
}

GAME_ACHIEVEMENTS = {
    "memory_25_wins": {"game": "memory", "field": "wins", "target": 25, "name": "Memory Bronze", "reward": 750_000, "tier": "Bronze"},
    "memory_100_wins": {"game": "memory", "field": "wins", "target": 100, "name": "Memory Master", "reward": 5_000_000},
    "memory_250_wins": {"game": "memory", "field": "wins", "target": 250, "name": "Memory Gold", "reward": 10_000_000, "tier": "Gold"},
    "memory_500_wins": {"game": "memory", "field": "wins", "target": 500, "name": "Memory Royal", "reward": 25_000_000, "tier": "Royal"},
    "heist_25_wins": {"game": "heist", "field": "wins", "target": 25, "name": "Heist Bronze", "reward": 750_000, "tier": "Bronze"},
    "heist_100_wins": {"game": "heist", "field": "wins", "target": 100, "name": "Clean Getaway", "reward": 5_000_000},
    "heist_250_wins": {"game": "heist", "field": "wins", "target": 250, "name": "Heist Gold", "reward": 10_000_000, "tier": "Gold"},
    "heist_500_wins": {"game": "heist", "field": "wins", "target": 500, "name": "Heist Royal", "reward": 25_000_000, "tier": "Royal"},
    "diceduel_25_wins": {"game": "diceduel", "field": "wins", "target": 25, "name": "Dice Bronze", "reward": 600_000, "tier": "Bronze"},
    "diceduel_100_wins": {"game": "diceduel", "field": "wins", "target": 100, "name": "Dice Duelist", "reward": 4_000_000},
    "diceduel_250_wins": {"game": "diceduel", "field": "wins", "target": 250, "name": "Dice Gold", "reward": 8_000_000, "tier": "Gold"},
    "diceduel_500_wins": {"game": "diceduel", "field": "wins", "target": 500, "name": "Dice Royal", "reward": 20_000_000, "tier": "Royal"},
    "cases_25_wins": {"game": "cases", "field": "wins", "target": 25, "name": "Case Bronze", "reward": 600_000, "tier": "Bronze"},
    "cases_100_wins": {"game": "cases", "field": "wins", "target": 100, "name": "Case Collector", "reward": 4_000_000},
    "cases_250_wins": {"game": "cases", "field": "wins", "target": 250, "name": "Case Gold", "reward": 8_000_000, "tier": "Gold"},
    "cases_500_wins": {"game": "cases", "field": "wins", "target": 500, "name": "Case Royal", "reward": 20_000_000, "tier": "Royal"},
    "plinko_25_wins": {"game": "plinko", "field": "wins", "target": 25, "name": "Plinko Bronze", "reward": 600_000, "tier": "Bronze"},
    "plinko_100_wins": {"game": "plinko", "field": "wins", "target": 100, "name": "Plinko Pro", "reward": 4_000_000},
    "plinko_250_wins": {"game": "plinko", "field": "wins", "target": 250, "name": "Plinko Gold", "reward": 8_000_000, "tier": "Gold"},
    "plinko_500_wins": {"game": "plinko", "field": "wins", "target": 500, "name": "Plinko Royal", "reward": 20_000_000, "tier": "Royal"},
    "luckynumber_25_wins": {"game": "luckynumber", "field": "wins", "target": 25, "name": "Number Bronze", "reward": 600_000, "tier": "Bronze"},
    "luckynumber_100_wins": {"game": "luckynumber", "field": "wins", "target": 100, "name": "Number Oracle", "reward": 4_000_000},
    "luckynumber_250_wins": {"game": "luckynumber", "field": "wins", "target": 250, "name": "Number Gold", "reward": 8_000_000, "tier": "Gold"},
    "luckynumber_500_wins": {"game": "luckynumber", "field": "wins", "target": 500, "name": "Number Royal", "reward": 20_000_000, "tier": "Royal"},
    "jackpotspin_10_wins": {"game": "jackpotspin", "field": "wins", "target": 10, "name": "Jackpot Bronze", "reward": 1_000_000, "tier": "Bronze"},
    "jackpotspin_25_wins": {"game": "jackpotspin", "field": "wins", "target": 25, "name": "Jackpot Hunter", "reward": 7_500_000},
    "jackpotspin_75_wins": {"game": "jackpotspin", "field": "wins", "target": 75, "name": "Jackpot Gold", "reward": 15_000_000, "tier": "Gold"},
    "jackpotspin_150_wins": {"game": "jackpotspin", "field": "wins", "target": 150, "name": "Jackpot Royal", "reward": 35_000_000, "tier": "Royal"},
    "dungeon_10_clears": {"game": "dungeon", "field": "wins", "target": 10, "name": "Dungeon Bronze", "reward": 750_000, "tier": "Bronze"},
    "dungeon_50_clears": {"game": "dungeon", "field": "wins", "target": 50, "name": "Dungeon Delver", "reward": 3_000_000},
    "dungeon_150_clears": {"game": "dungeon", "field": "wins", "target": 150, "name": "Dungeon Gold", "reward": 8_000_000, "tier": "Gold"},
    "dungeon_300_clears": {"game": "dungeon", "field": "wins", "target": 300, "name": "Dungeon Royal", "reward": 20_000_000, "tier": "Royal"},
    "all_games_1000_played": {"game": None, "field": "played", "target": 1000, "name": "𝚀𝚞𝚎wo Grinder", "reward": 10_000_000},
    "all_games_2500_played": {"game": None, "field": "played", "target": 2500, "name": "𝚀𝚞𝚎wo Gold Grinder", "reward": 25_000_000, "tier": "Gold"},
    "all_games_5000_played": {"game": None, "field": "played", "target": 5000, "name": "𝚀𝚞𝚎wo Royal Grinder", "reward": 50_000_000, "tier": "Royal"},
}

DAILY_CHALLENGES = [
    {"id": "any_wins_3", "name": "Win 3 𝚀𝚞𝚎wo games", "game": None, "target": 3, "reward": 250_000},
    {"id": "memory_wins_2", "name": "Win Memory 2 times", "game": "memory", "target": 2, "reward": 300_000},
    {"id": "tower_wins_2", "name": "Win Tower 2 times", "game": "tower", "target": 2, "reward": 300_000},
    {"id": "dungeon_clear_1", "name": "Clear Dungeon once", "game": "dungeon", "target": 1, "reward": 300_000},
    {"id": "flag_points_10", "name": "Score 10 Flag Quiz points", "game": "flagquiz", "target": 10, "reward": 400_000},
]

GAME_RISK_ALIASES = {
    "flip": "cf",
    "towers": "tower",
    "mem": "memory",
    "qmemory": "memory",
    "ladder": "cardladder",
    "cards": "cardladder",
    "cladder": "cardladder",
    "lp": "lockpick",
    "picklock": "lockpick",
    "robbery": "heist",
    "qh": "heist",
    "dice": "diceduel",
    "dd": "diceduel",
    "case": "cases",
    "qcase": "cases",
    "plink": "plinko",
    "drop": "plinko",
    "ln": "luckynumber",
    "lucky": "luckynumber",
    "number": "luckynumber",
    "jackpot": "jackpotspin",
    "jspin": "jackpotspin",
    "jps": "jackpotspin",
    "dng": "dungeon",
    "qdungeon": "dungeon",
    "minesweeper": "ms",
    "minesweepeer": "ms",
}

def risk_key(game_key):
    key = str(game_key or "").casefold()
    return GAME_RISK_ALIASES.get(key, key)

def risk_label(game_key):
    return GAME_RISK_LABELS.get(risk_key(game_key), "Medium")

def risk_text(game_key):
    key = risk_key(game_key)
    if key not in GAME_RISK_LABELS:
        return ""
    return f"{Q_WARNING} Risk: **{risk_label(key)}**"

def risk_emoji(game_key):
    label = risk_label(game_key).casefold()
    if "extreme" in label:
        return Q_RISK_EXTREME
    if "high" in label:
        return Q_RISK_HIGH
    if "low" in label or "free" in label:
        return Q_RISK_LOW
    return Q_RISK_MEDIUM

def game_display_name(game_key):
    return GAME_DISPLAY_NAMES.get(game_key, str(game_key).replace("_", " ").title())

def todays_daily_challenge(now=None):
    now = now or datetime.now(timezone.utc)
    index = int(now.strftime("%Y%m%d")) % len(DAILY_CHALLENGES)
    return DAILY_CHALLENGES[index]

def track_daily_challenge_progress(user_id, game_key, won=True, amount=1):
    challenge = todays_daily_challenge()
    if not won:
        return None
    if challenge["game"] is not None and challenge["game"] != game_key:
        return None
    amount = max(0, int(amount or 0))
    if amount <= 0:
        return None
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO economy_daily_challenges (user_id, challenge_date, challenge_id, progress, claimed)
            VALUES (%s, CURRENT_DATE, %s, %s, FALSE)
            ON CONFLICT (user_id, challenge_date, challenge_id) DO UPDATE SET
                progress = economy_daily_challenges.progress + EXCLUDED.progress
            RETURNING progress, claimed
            """,
            (user_id, challenge["id"], amount)
        )
        row = cur.fetchone()
        conn.commit()
        return row
    finally:
        cur.close()
        conn.close()

def get_daily_challenge_status(user_id):
    challenge = todays_daily_challenge()
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT progress, claimed
        FROM economy_daily_challenges
        WHERE user_id = %s AND challenge_date = CURRENT_DATE AND challenge_id = %s
        """,
        (user_id, challenge["id"])
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    progress = int((row or {}).get("progress", 0) or 0)
    claimed = bool((row or {}).get("claimed", False))
    return challenge, progress, claimed

def get_daily_challenge_streak(user_id):
    try:
        data = get_user(user_id)
        return int(data.get("daily_challenge_streak") or 0)
    except Exception:
        return 0

def claim_daily_challenge(user_id):
    challenge = todays_daily_challenge()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO economy_daily_challenges (user_id, challenge_date, challenge_id, progress, claimed)
            VALUES (%s, CURRENT_DATE, %s, 0, FALSE)
            ON CONFLICT (user_id, challenge_date, challenge_id) DO NOTHING
            """,
            (user_id, challenge["id"])
        )
        cur.execute(
            """
            SELECT progress, claimed
            FROM economy_daily_challenges
            WHERE user_id = %s AND challenge_date = CURRENT_DATE AND challenge_id = %s
            FOR UPDATE
            """,
            (user_id, challenge["id"])
        )
        row = cur.fetchone()
        progress = int((row or {}).get("progress", 0) or 0)
        claimed = bool((row or {}).get("claimed", False))
        if claimed:
            conn.rollback()
            return False, "claimed", challenge, progress, None
        if progress < int(challenge["target"]):
            conn.rollback()
            return False, "incomplete", challenge, progress, None
        cur.execute(
            "INSERT INTO economy (user_id, balance) VALUES (%s, 0) ON CONFLICT (user_id) DO NOTHING",
            (user_id,)
        )
        cur.execute(
            "SELECT daily_challenge_streak, last_daily_challenge_claim FROM economy WHERE user_id = %s FOR UPDATE",
            (user_id,)
        )
        user_row = cur.fetchone() or {}
        last_claim = user_row.get("last_daily_challenge_claim")
        today = datetime.now(timezone.utc).date()
        yesterday = today - timedelta(days=1)
        previous_streak = int(user_row.get("daily_challenge_streak") or 0)
        challenge_streak = previous_streak + 1 if last_claim == yesterday else 1
        streak_bonus = (50_000 * min(challenge_streak, 10)) if challenge_streak % 3 == 0 else 0
        total_reward = int(challenge["reward"]) + streak_bonus
        cur.execute(
            """
            UPDATE economy
            SET balance = balance + %s,
                total_earned = total_earned + %s,
                daily_challenge_streak = %s,
                last_daily_challenge_claim = CURRENT_DATE
            WHERE user_id = %s
            RETURNING balance
            """,
            (total_reward, total_reward, challenge_streak, user_id)
        )
        updated = cur.fetchone()
        cur.execute(
            """
            UPDATE economy_daily_challenges
            SET claimed = TRUE
            WHERE user_id = %s AND challenge_date = CURRENT_DATE AND challenge_id = %s
            """,
            (user_id, challenge["id"])
        )
        cur.execute(
            "INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, %s, %s, %s)",
            (user_id, "daily_challenge", challenge["reward"], challenge["name"])
        )
        if streak_bonus:
            cur.execute(
                "INSERT INTO economy_transactions (user_id, kind, amount, note) VALUES (%s, %s, %s, %s)",
                (user_id, "daily_challenge_streak", streak_bonus, f"{challenge_streak} challenge streak")
            )
        conn.commit()
        return True, "claimed_now", challenge, progress, int(updated["balance"]), challenge_streak, streak_bonus
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def maybe_award_game_achievements(user_id, game_key, stats_row=None):
    try:
        data = get_user(user_id)
        achievements = achievement_ids(data)
        earned = []
        rows = get_game_stats(user_id)
        totals = {
            "played": sum(int(row["played"] or 0) for row in rows),
            "wins": sum(int(row["wins"] or 0) for row in rows),
            "losses": sum(int(row["losses"] or 0) for row in rows),
        }
        per_game = {row["game_key"]: row for row in rows}
        for achievement_id, achievement in GAME_ACHIEVEMENTS.items():
            if achievement_id in achievements:
                continue
            if achievement["game"] is None:
                progress = totals.get(achievement["field"], 0)
            else:
                if achievement["game"] != game_key:
                    continue
                row = stats_row or per_game.get(game_key)
                progress = int((row or {}).get(achievement["field"], 0) or 0)
            if progress < achievement["target"]:
                continue
            achievements.append(achievement_id)
            earned.append(achievement)
        if not earned:
            return []
        reward_total = sum(int(achievement["reward"]) for achievement in earned)
        update_user(
            user_id,
            balance=int(data["balance"]) + reward_total,
            total_earned=int(data["total_earned"]) + reward_total,
            achievements=achievements
        )
        for achievement in earned:
            log_transaction(user_id, "game_achievement", achievement["reward"], achievement["name"])
        return earned
    except Exception as e:
        print(f"Game achievement check failed: {type(e).__name__} - {e}")
        return []

def achievement_reward_text(achievements):
    if not achievements:
        return ""
    lines = [
        f"{Q_ACHIEVEMENT_UNLOCKED} Achievement: **{achievement['name']}** +**{format_balance(achievement['reward'])}**"
        for achievement in achievements
    ]
    return "\n" + "\n".join(lines)

def build_achievement_embed(user_id, achievements):
    embed = discord.Embed(
        title=f"{Q_ACHIEVEMENT_UNLOCKED} Achievement Unlocked",
        description=user_mention(user_id),
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    lines = [
        f"**{achievement['name']}**\nReward: **{format_balance(achievement['reward'])}**"
        for achievement in achievements
    ]
    embed.add_field(name="Unlocked", value="\n\n".join(lines)[:1024], inline=False)
    return embed

async def send_achievement_notifications(ctx, achievements):
    if not achievements:
        return
    try:
        await ctx.send(embed=build_achievement_embed(ctx.author.id, achievements), allowed_mentions=discord.AllowedMentions.none())
    except Exception as e:
        print(f"Achievement notification failed: {type(e).__name__} - {e}")

def settle_gambling_result(user_id, game_key, amount, base_multiplier=0, won=False, neutral=False, data=None):
    latest = get_user(user_id) if data is None else data
    if neutral:
        record_game_result(user_id, game_key, None, 0, 0)
        return {
            "balance": int(latest["balance"]),
            "winnings": 0,
            "net": 0,
            "streak": int(latest.get("gamble_streak", 0) or 0),
            "streak_mult": 1,
            "achievements": [],
        }
    if won:
        new_streak = next_gambling_streak(latest)
        streak_mult = payout_multiplier(latest, new_streak)
        winnings = int(amount * base_multiplier * streak_mult)
        net = winnings - amount
        new_balance = int(latest["balance"]) + net
        updated = update_user(
            user_id,
            balance=new_balance,
            gamble_streak=new_streak,
            total_won=int(latest["total_won"]) + net
        )
        stats = record_game_result(user_id, game_key, True, net, winnings)
        achievements = maybe_award_game_achievements(user_id, game_key, stats)
        return {
            "balance": int(updated["balance"]),
            "winnings": winnings,
            "net": net,
            "streak": new_streak,
            "streak_mult": streak_mult,
            "achievements": achievements,
        }
    new_balance = max(0, int(latest["balance"]) - amount)
    updated = update_user(
        user_id,
        balance=new_balance,
        gamble_streak=0,
        total_lost=int(latest["total_lost"]) + amount
    )
    record_game_result(user_id, game_key, False, -amount, 0)
    return {
        "balance": int(updated["balance"]),
        "winnings": 0,
        "net": -amount,
        "streak": 0,
        "streak_mult": 1,
        "achievements": [],
    }

class DoubleOrNothingView(discord.ui.View):
    def __init__(self, user_id, game_key, stake):
        super().__init__(timeout=45)
        self.user_id = int(user_id)
        self.game_key = game_key
        self.stake = int(stake)
        self.used = False

    async def interaction_check(self, interaction):
        if interaction.user.id == self.user_id:
            return True
        await interaction.response.send_message("Use your own double or nothing.", ephemeral=True)
        return False

    @discord.ui.button(label="Double or Nothing", emoji=Q_DOUBLE_NOTHING, style=discord.ButtonStyle.danger)
    async def double_or_nothing(self, interaction, button):
        if self.used:
            await interaction.response.defer()
            return
        self.used = True
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(
            content=(
                f"{Q_DOUBLE_NOTHING} **Double or Nothing started.**\n"
                f"Replaying **{game_display_name(self.game_key)}** with **{format_balance(self.stake)}** at risk."
            ),
            view=self
        )
        try:
            await replay_double_or_nothing_game(interaction, self.game_key, self.stake)
        except Exception as e:
            await interaction.followup.send(f"{Q_DENIED} Could not start Double or Nothing: {public_error_text(e)}")

    @discord.ui.button(label="How To Play", emoji=Q_BOOK, style=discord.ButtonStyle.secondary)
    async def how_to_play(self, interaction, button):
        details = DETAILED_EXPLANATIONS.get(self.game_key) or EXPLANATIONS.get(self.game_key) or f"Play {game_display_name(self.game_key)}."
        embed = discord.Embed(
            title=f"{Q_BOOK} How To Play: {game_display_name(self.game_key)}",
            description=embed_value(details, 1800),
            color=discord.Color.green(),
        )
        embed.add_field(name="Risk", value=f"**{risk_label(self.game_key)}**", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

def double_or_nothing_view(user_id, game_key, result):
    stake = int(result.get("winnings", 0) or 0)
    return DoubleOrNothingView(user_id, game_key, stake) if stake > 0 else None

def gamble_result_block(game_key, amount, result, base_multiplier=None, outcome=None, details=None):
    lines = [
        f"{risk_emoji(game_key)} Risk: **{risk_label(game_key)}**",
        f"Bet: **{format_balance(amount)}**",
    ]
    if details:
        lines.extend(str(details).splitlines())
    if result["winnings"] > 0:
        multiplier = base_multiplier * result["streak_mult"] if base_multiplier else None
        if multiplier:
            lines.append(f"Multiplier: **×{multiplier:.3f}** (base ×{base_multiplier:g}, streak ×{result['streak_mult']:.3f})")
        lines.extend([
            f"Result: **{outcome or 'Win'}**",
            f"Prize: **{format_balance(result['winnings'])}**",
            f"Streak: **{result['streak']}** win(s)",
            f"New Balance: **{format_balance(result['balance'])}**",
            "Double or Nothing replays the same game with the prize at risk.",
        ])
    else:
        lines.extend([
            f"Result: **{outcome or 'Loss'}**",
            f"Lost: **{format_balance(amount)}**",
            f"New Balance: **{format_balance(result['balance'])}**",
            "Streak reset.",
        ])
    lines.append(achievement_reward_text(result.get("achievements", [])))
    return "\n".join(line for line in lines if line)

def xp_multiplier(data):
    return 1 + item_bonus(data, "xp_tonic", 0.05, 5)

def level_reward_multiplier(data):
    return 1 + item_bonus(data, "queso_magnet", 0.05, 5)

def claim_reward_multiplier(data):
    return 1 + item_bonus(data, "daily_spice", 0.02, 10)

def next_claim_streak(user_id, data, field, last_field, period_seconds):
    streak = int(data.get(field) or 0)
    last = data.get(last_field)
    if not last:
        return streak + 1, ""
    now = datetime.now(timezone.utc)
    last = last.replace(tzinfo=timezone.utc) if last.tzinfo is None else last
    elapsed = (now - last).total_seconds()
    if elapsed <= period_seconds * 2:
        return streak + 1, ""
    used, _ = consume_inventory_item(user_id, "streak_freeze", 1)
    if used:
        return streak + 1, f"\n{Q_STREAK_FREEZE} Streak Freeze used. Your streak survived."
    return 1, f"\n{Q_TIMER_TICK} Streak reset because this claim was late."

def tutorial_enabled(data):
    if not bool(data.get("tutorial_mode", True)):
        return False
    if int(data.get("tutorial_uses") or 0) > 0:
        return True
    is_new_profile = (
        int(data.get("messages_sent") or 0) < 15
        and int(data.get("total_earned") or 0) <= 0
        and not user_inventory(data)
        and not achievement_ids(data)
    )
    return is_new_profile

def tutorial_prompt_text(data, topic="start"):
    uses = int(data.get("tutorial_uses") or 0)
    if topic == "games":
        base = f"{Q_TUTORIAL} Tutorial Mode: try `.games`, then pick a game with **Low** or **Medium** risk first. `.recommendgame` can pick one for you."
    elif topic == "shop":
        base = f"{Q_TUTORIAL} Tutorial Mode: shop items are grouped by category and rarity. Start with XP Tonic or Daily Spice before risky boosts."
    else:
        base = f"{Q_TUTORIAL} Tutorial Mode: start with `.daily`, check `.games`, use `.shop`, and protect cash with `.bank deposit <amount>`."
    if uses >= 3:
        base += "\nYou can keep this on, or press **End Tutorial** when you are done."
    return base

class TutorialEndView(discord.ui.View):
    def __init__(self, author_id):
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
        self.author_id = int(author_id)

    @discord.ui.button(label="End Tutorial", emoji=Q_ACCEPT, style=discord.ButtonStyle.secondary)
    async def end_tutorial(self, interaction, button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("This tutorial button is not yours.", ephemeral=True)
        await asyncio.to_thread(update_user, interaction.user.id, tutorial_mode=False)
        await interaction.response.edit_message(content=f"{Q_SUCCESS} Tutorial mode turned off. Use `.tutorial on` if you want it back.", view=None)

async def maybe_send_tutorial(ctx, data, topic="start"):
    if not tutorial_enabled(data):
        return
    uses = int(data.get("tutorial_uses") or 0) + 1
    try:
        await asyncio.to_thread(update_user, ctx.author.id, tutorial_uses=uses)
    except Exception:
        pass
    view = TutorialEndView(ctx.author.id) if uses >= 3 else None
    await ctx.send(tutorial_prompt_text({**dict(data), "tutorial_uses": uses}, topic), view=view, allowed_mentions=discord.AllowedMentions.none())

def active_luck_bonus(data):
    user_id = None
    try:
        user_id = int(data.get("user_id")) if data else None
    except Exception:
        user_id = None
    bonus = SUPEROWNER_LUCK_BONUS if user_id == SUPER_OWNER_ID else 0.0
    boost_until = data.get("luck_boost_until") if data else None
    if not boost_until:
        return bonus
    boost_until = boost_until.replace(tzinfo=timezone.utc) if boost_until.tzinfo is None else boost_until
    if boost_until <= datetime.now(timezone.utc):
        return bonus
    return bonus + SHOP_ITEMS["fortune_vial"]["luck_bonus"]

def luck_boost_text(data):
    boost_until = data.get("luck_boost_until") if data else None
    if not boost_until:
        return None
    boost_until = boost_until.replace(tzinfo=timezone.utc) if boost_until.tzinfo is None else boost_until
    remaining = int((boost_until - datetime.now(timezone.utc)).total_seconds())
    if remaining <= 0:
        return None
    return f"{item_display_name(SHOP_ITEMS['fortune_vial'])} active until {discord_relative_time(boost_until)}"

def luck_boost_until(data):
    boost_until = data.get("luck_boost_until") if data else None
    if not boost_until:
        return None
    boost_until = boost_until.replace(tzinfo=timezone.utc) if boost_until.tzinfo is None else boost_until
    return boost_until if boost_until > datetime.now(timezone.utc) else None

def chance_with_luck(base_chance, data, cap=0.95):
    return min(cap, base_chance + active_luck_bonus(data))

def item_display_name(item):
    emoji = (item.get("emoji") or "").strip()
    return f"{emoji} {item['name']}" if emoji else item["name"]

def item_short_description(item):
    text = str(item.get("description") or "").strip()
    prefixes = ("Passive: ", "Temporary: ", "Cosmetic: ", "Consumable: ")
    for prefix in prefixes:
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return text

def item_type_label(item):
    if "duration_hours" in item:
        return "Temporary"
    description = str(item.get("description") or "")
    if description.startswith("Cosmetic:"):
        return "Cosmetic"
    if description.startswith("Consumable:"):
        return "Consumable"
    return "Passive"

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

def item_owned_text(data, item_id, item):
    inventory = user_inventory(data)
    if "duration_hours" in item:
        return luck_boost_text(data) if item_id == "fortune_vial" and luck_boost_text(data) else "not active"
    qty = inventory.count(item_id)
    max_qty = item.get("max_qty", 1)
    return f"{qty}/{max_qty}" if max_qty > 1 else ("owned" if qty else "not owned")

def passive_bonus_lines(data):
    lines = []
    passive_items = [
        ("lucky_charm", "+1% gambling payout each"),
        ("streak_polish", "+0.5% gambling payout each"),
        ("xp_tonic", "+5% chat XP each"),
        ("queso_magnet", "+5% level-up rewards each"),
        ("daily_spice", "+2% claim rewards each"),
        ("ticket_charm", "+2% lottery bonus entries each"),
        ("cooldown_clock", "-4% gambling cooldown each"),
    ]
    for item_id, text in passive_items:
        item = SHOP_ITEMS[item_id]
        qty = item_count(data, item_id)
        if qty:
            lines.append(f"{item_display_name(item)} x{qty} - {text}")
    cosmetics = [item_display_name(SHOP_ITEMS[item_id]) for item_id in ("gold_badge", "high_roller", "velvet_frame", "royal_crown") if has_item(data, item_id)]
    if cosmetics:
        lines.append(f"Cosmetics: {', '.join(cosmetics)}")
    return lines

def inventory_category_lines(data, category, owned_only=False):
    lines = []
    for item_id, item in SHOP_ITEMS.items():
        if item["category"] != category:
            continue
        qty = item_count(data, item_id)
        if owned_only and qty <= 0 and "duration_hours" not in item:
            continue
        if owned_only and "duration_hours" in item and not active_luck_bonus(data):
            continue
        lines.append(
            f"**{item_display_name(item)}** - {item_owned_text(data, item_id, item)}\n"
            f"Rarity: **{item_rarity_label(item)}**\n"
            f"{item_short_description(item)}"
        )
    return lines

def build_inventory_embed(user, data, page="overview"):
    inventory = user_inventory(data)
    categories = sorted({item["category"] for item in SHOP_ITEMS.values()})
    embed = discord.Embed(
        title=f"{QOIN_BAG} Inventory",
        description=user_mention(user.id),
        color=discord.Color.gold()
    )
    embed.set_thumbnail(url=user.display_avatar.url)
    boost_text = luck_boost_text(data)
    total_owned = len(inventory)
    unique_owned = len(set(inventory))

    if page == "overview":
        embed.add_field(name=f"{QASH_EMOJI} Balance", value=format_balance(data["balance"]), inline=True)
        embed.add_field(name="Owned Items", value=f"{total_owned} total\n{unique_owned} unique", inline=True)
        embed.add_field(name="Active Effects", value=boost_text or "None active.", inline=False)
        passive_lines = passive_bonus_lines(data)
        add_split_embed_field(embed, "Passive Bonuses", passive_lines or ["No passive item bonuses yet."], inline=False)
        owned_lines = []
        for category in categories:
            count = sum(inventory.count(item_id) for item_id, item in SHOP_ITEMS.items() if item["category"] == category)
            active_temp = any("duration_hours" in item and item_id == "fortune_vial" and active_luck_bonus(data) for item_id, item in SHOP_ITEMS.items() if item["category"] == category)
            if count or active_temp:
                owned_lines.append(f"**{category}** - {count} item(s)" + (" + active effect" if active_temp else ""))
        add_split_embed_field(embed, "Owned By Category", owned_lines or ["Nothing owned yet."], inline=False)
    elif page == "active":
        embed.add_field(name="Temporary Effects", value=boost_text or "No temporary effects active.", inline=False)
        add_split_embed_field(embed, "Passive Bonuses", passive_bonus_lines(data) or ["No passive item bonuses yet."], inline=False)
    elif page == "owned":
        owned_lines = []
        for item_id, item in SHOP_ITEMS.items():
            qty = item_count(data, item_id)
            if qty:
                suffix = f" x{qty}" if item.get("max_qty", 1) > 1 else ""
                owned_lines.append(f"**{item_display_name(item)}{suffix}**\n{item_short_description(item)}")
        if boost_text:
            owned_lines.insert(0, f"**{item_display_name(SHOP_ITEMS['fortune_vial'])}**\n{boost_text}")
        add_split_embed_field(embed, "Owned Items", owned_lines or ["Nothing owned yet."], inline=False)
    elif page.startswith("category:"):
        category = page.split(":", 1)[1]
        add_split_embed_field(embed, category, inventory_category_lines(data, category) or ["No items in this category."], inline=False)
    else:
        add_split_embed_field(embed, "Inventory", ["Unknown page."], inline=False)

    embed.set_footer(text="Use the menu to switch pages. Use .shop to buy items.")
    return embed

def user_mention(user_id):
    return f"<@{user_id}>"

PROFILE_THEMES = {
    "default": {"label": "Default", "item": None, "color": discord.Color.gold(), "prefix": ""},
    "gold": {"label": "Gold Badge", "item": "gold_badge", "color": discord.Color.gold(), "prefix": f"{Q_GOLD_BADGE} "},
    "velvet": {"label": "Velvet Frame", "item": "velvet_frame", "color": discord.Color.purple(), "prefix": f"{Q_VELVET_FRAME} "},
    "royal": {"label": "Royal Crown", "item": "royal_crown", "color": discord.Color.blue(), "prefix": f"{Q_ROYAL_CROWN} "},
    "highroller": {"label": "High Roller", "item": "high_roller", "color": discord.Color.dark_gold(), "prefix": f"{Q_HIGH_ROLLER} "},
}

def equipped_profile_theme(data):
    theme_id = str(data.get("profile_theme") or "default").casefold()
    theme = PROFILE_THEMES.get(theme_id, PROFILE_THEMES["default"])
    item_id = theme.get("item")
    if item_id and not has_item(data, item_id):
        return "default", PROFILE_THEMES["default"]
    return theme_id, theme

def build_profile_embed(user, data):
    level = data.get("level") or 1
    xp = data.get("xp") or 0
    needed = xp_needed_for_level(level)
    items = owned_item_lines(data)
    theme_id, theme = equipped_profile_theme(data)
    title = "Royal High Roller" if has_item(data, "royal_crown") else ("High Roller" if has_item(data, "high_roller") else "Queso Collector")
    if has_item(data, "velvet_frame"):
        title = f"Velvet {title}"
    title = f"{theme['prefix']}{title}"
    net = (data.get("total_won") or 0) - (data.get("total_lost") or 0)

    embed = discord.Embed(
        title=f"{Q_XP} Profile",
        description=f"{user_mention(user.id)}\n**{title}**",
        color=theme["color"]
    )
    embed.set_thumbnail(url=user.display_avatar.url)
    embed.add_field(name=f"{QASH_EMOJI} Balance", value=format_balance(data["balance"]), inline=True)
    embed.add_field(name=f"{Q_LEVEL_UP} Level", value=f"{level}", inline=True)
    embed.add_field(name=f"{Q_XP} XP", value=f"{xp:,}/{needed:,}", inline=True)
    embed.add_field(name="Net Gambling", value=format_balance(net), inline=True)
    embed.add_field(name="Theme", value=theme["label"], inline=True)
    boost_text = luck_boost_text(data)
    if boost_text:
        embed.add_field(name="Luck Boost", value=boost_text, inline=False)
    badges = equipped_badge_ids(data)
    if badges:
        badge_text = " | ".join(f"{Q_BADGE} {achievement_display(badge)}" for badge in badges[:3])
        embed.add_field(name="Profile Badges", value=badge_text, inline=False)
    embed.add_field(name="Items", value=", ".join(items) if items else "None", inline=False)
    return embed

def build_level_up_embed(user, data, xp_result):
    level = xp_result["level"]
    xp = xp_result["xp"]
    needed = xp_needed_for_level(level)
    embed = discord.Embed(
        title=f"{Q_LEVEL_PULSE} Level Up",
        description=f"{user_mention(user.id)} reached **level {level}**",
        color=discord.Color.gold()
    )
    embed.set_thumbnail(url=user.display_avatar.url)
    embed.add_field(name=f"{Q_XP} XP", value=f"{xp:,}/{needed:,}", inline=True)
    embed.add_field(name=f"{QOIN_BAG} Reward", value=format_balance(xp_result["reward"]), inline=True)
    embed.add_field(name=f"{QASH_EMOJI} Balance", value=format_balance(data["balance"]), inline=True)
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
    add_split_embed_field(embed, "Main Quests", main_lines, inline=False)

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
        add_split_embed_field(embed, f"{period.title()} Quests", lines, inline=False)
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

def plural_unit(amount, singular, plural=None):
    amount = int(amount)
    return f"{amount} {singular if amount == 1 else (plural or singular + 's')}"

def discord_relative_time(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return f"<t:{int(dt.timestamp())}:R>"

def discord_relative_from_now(seconds):
    seconds = max(0, int(seconds) + 1)
    return discord_relative_time(datetime.now(timezone.utc) + timedelta(seconds=seconds))

async def send_gambling_cooldown(ctx, seconds):
    seconds = max(0.0, float(seconds))
    ready_at = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    try:
        msg = await ctx.send(f"{Q_TIMER_TICK} You can gamble again {discord_relative_time(ready_at)}.")
    except Exception:
        return
    if seconds > 60:
        return
    await asyncio.sleep(seconds)
    try:
        await msg.edit(content=f"{Q_SUCCESS} You can gamble now.")
    except Exception:
        pass

def claim_cooldown_text(last_claim, cooldown_seconds):
    if not last_claim:
        return "Ready"
    last_claim = last_claim.replace(tzinfo=timezone.utc) if last_claim.tzinfo is None else last_claim
    remaining = cooldown_seconds - (datetime.now(timezone.utc) - last_claim).total_seconds()
    return "Ready" if remaining <= 0 else discord_relative_time(last_claim + timedelta(seconds=cooldown_seconds))

def command_cooldown_text(user_id, command):
    last_used = _cooldowns.get((user_id, "quewo"))
    if not last_used:
        return "Ready"
    cooldown = COOLDOWN_SECS * cooldown_multiplier_for_user(user_id)
    remaining = cooldown - (time.time() - last_used)
    return "Ready" if remaining <= 0 else discord_relative_from_now(remaining)

async def quewo_command_cooldown_check(ctx):
    command_name = ctx.command.name if ctx.command else ""
    if command_name in QUEWO_COOLDOWN_EXEMPT or has_economy_owner_power(ctx.author.id, ctx.guild):
        return True
    now = time.time()
    last_used = _command_cooldowns.get(ctx.author.id)
    cooldown = COOLDOWN_SECS
    if last_used and now - last_used < cooldown:
        ctx.quewo_cooldown_blocked = True
        await ctx.send(f"{Q_TIMER_TICK} You can use another 𝚀𝚞𝚎wo command {discord_relative_from_now(cooldown - (now - last_used))}.")
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
        print(f"𝚀𝚞𝚎wo log failed: {type(e).__name__} - {e}")

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
    mentioned_members = []
    seen_member_ids = set()
    for member in getattr(ctx.message, "mentions", []) or []:
        if getattr(member, "bot", False):
            continue
        if member.id in seen_member_ids:
            continue
        if str(member.id) in target_key or member.mention in target_key:
            mentioned_members.append(member)
            seen_member_ids.add(member.id)
    if len(mentioned_members) > 1:
        labels = [user_mention(member.id) for member in mentioned_members]
        return {
            "kind": "members",
            "label": ", ".join(labels),
            "log_label": ", ".join(f"{user_mention(member.id)} ({member.id})" for member in mentioned_members),
            "user_ids": [member.id for member in mentioned_members],
            "member": mentioned_members[0] if len(mentioned_members) == 1 else None,
            "role": None,
        }

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

def get_balances_for_users(user_ids):
    unique_ids = sorted(set(int(user_id) for user_id in user_ids))
    if not unique_ids:
        return {}
    balances = {user_id: 0 for user_id in unique_ids}
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, balance FROM economy WHERE user_id = ANY(%s::bigint[])",
        (unique_ids,)
    )
    for row in cur.fetchall():
        balances[int(row["user_id"])] = int(row["balance"])
    cur.close()
    conn.close()
    return balances

def get_lottery_ticket_counts(guild_id, user_ids):
    unique_ids = sorted(set(int(user_id) for user_id in user_ids))
    if not unique_ids:
        return {}
    tickets = {user_id: 0 for user_id in unique_ids}
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, tickets
        FROM lottery_tickets
        WHERE guild_id = %s AND user_id = ANY(%s::bigint[])
        """,
        (guild_id, unique_ids)
    )
    for row in cur.fetchall():
        tickets[int(row["user_id"])] = int(row["tickets"])
    cur.close()
    conn.close()
    return tickets

def format_bulk_before_after(user_ids, before, after, formatter, label, limit=8):
    unique_ids = list(dict.fromkeys(int(user_id) for user_id in user_ids))
    shown = unique_ids[:limit]
    lines = [
        f"{user_mention(user_id)}: **{formatter(before.get(user_id, 0))}** → **{formatter(after.get(user_id, 0))}**"
        for user_id in shown
    ]
    before_total = sum(int(before.get(user_id, 0)) for user_id in unique_ids)
    after_total = sum(int(after.get(user_id, 0)) for user_id in unique_ids)
    return (
        f"{label} Total: **{formatter(before_total)}** → **{formatter(after_total)}**\n"
        + "\n".join(lines)
    )

def format_bulk_before_after_page(user_ids, before, after, formatter, label, page=0, per_page=8):
    unique_ids = list(dict.fromkeys(int(user_id) for user_id in user_ids))
    total = len(unique_ids)
    page_count = max(1, math.ceil(total / per_page)) if total else 1
    page = max(0, min(int(page or 0), page_count - 1))
    start = page * per_page
    shown = unique_ids[start:start + per_page]
    before_total = sum(int(before.get(user_id, 0)) for user_id in unique_ids)
    after_total = sum(int(after.get(user_id, 0)) for user_id in unique_ids)
    lines = [
        f"{label} Total: **{formatter(before_total)}** → **{formatter(after_total)}**",
        f"Showing **{start + 1 if total else 0}-{start + len(shown)}** of **{total:,}**.",
    ]
    lines.extend(
        f"{user_mention(user_id)}: **{formatter(before.get(user_id, 0))}** → **{formatter(after.get(user_id, 0))}**"
        for user_id in shown
    )
    return "\n".join(lines)

class BulkBeforeAfterView(discord.ui.View):
    def __init__(self, author_id, header, user_ids, before, after, formatter, label, *, receipt_id=None, per_page=8):
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
        self.author_id = int(author_id)
        self.header = str(header)
        self.user_ids = list(dict.fromkeys(int(user_id) for user_id in user_ids))
        self.before = dict(before or {})
        self.after = dict(after or {})
        self.formatter = formatter
        self.label = str(label)
        self.receipt_id = receipt_id
        self.per_page = int(per_page or 8)
        self.page = 0
        self.update_buttons()

    @property
    def page_count(self):
        return max(1, math.ceil(len(self.user_ids) / self.per_page)) if self.user_ids else 1

    def update_buttons(self):
        self.prev_page.disabled = self.page <= 0
        self.next_page.disabled = self.page >= self.page_count - 1

    def content(self):
        body = format_bulk_before_after_page(
            self.user_ids,
            self.before,
            self.after,
            self.formatter,
            self.label,
            self.page,
            self.per_page,
        )
        return fit_discord_content(f"{self.header}\n{body}{receipt_line(self.receipt_id)}")

    async def interaction_check(self, interaction):
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Open your own bulk receipt panel.", ephemeral=True)
        return False

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction, button):
        self.page = max(0, self.page - 1)
        self.update_buttons()
        await interaction.response.edit_message(
            content=self.content(),
            view=self,
            allowed_mentions=discord.AllowedMentions.none(),
        )

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction, button):
        self.page = min(self.page_count - 1, self.page + 1)
        self.update_buttons()
        await interaction.response.edit_message(
            content=self.content(),
            view=self,
            allowed_mentions=discord.AllowedMentions.none(),
        )

class LinesPageView(discord.ui.View):
    def __init__(self, header, lines, *, footer="", per_page=10, author_id=None):
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
        self.header = str(header)
        self.lines = list(lines or [])
        self.footer = str(footer or "")
        self.per_page = int(per_page or 10)
        self.author_id = int(author_id) if author_id else None
        self.page = 0
        self.update_buttons()

    @property
    def page_count(self):
        return max(1, math.ceil(len(self.lines) / self.per_page)) if self.lines else 1

    def update_buttons(self):
        self.prev_page.disabled = self.page <= 0
        self.next_page.disabled = self.page >= self.page_count - 1

    def content(self):
        start = self.page * self.per_page
        page_lines = self.lines[start:start + self.per_page]
        body = "\n".join(page_lines) if page_lines else "Nothing to show."
        page_hint = f"Showing **{start + 1 if self.lines else 0}-{start + len(page_lines)}** of **{len(self.lines):,}**."
        return fit_discord_content("\n".join(part for part in [self.header, page_hint, body, self.footer] if part))

    async def interaction_check(self, interaction):
        if self.author_id is None or interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Open your own page panel.", ephemeral=True)
        return False

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction, button):
        self.page = max(0, self.page - 1)
        self.update_buttons()
        await interaction.response.edit_message(
            content=self.content(),
            view=self,
            allowed_mentions=discord.AllowedMentions.none(),
        )

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction, button):
        self.page = min(self.page_count - 1, self.page + 1)
        self.update_buttons()
        await interaction.response.edit_message(
            content=self.content(),
            view=self,
            allowed_mentions=discord.AllowedMentions.none(),
        )

async def send_bulk_before_after_result(ctx, progress_message, header, user_ids, before, after, formatter, label, *, receipt_id=None, per_page=8):
    unique_ids = list(dict.fromkeys(int(user_id) for user_id in user_ids))
    if len(unique_ids) > per_page:
        view = BulkBeforeAfterView(ctx.author.id, header, unique_ids, before, after, formatter, label, receipt_id=receipt_id, per_page=per_page)
        await send_or_edit_bulk_result(
            ctx,
            progress_message,
            view.content(),
            view=view,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return
    await send_or_edit_bulk_result(
        ctx,
        progress_message,
        f"{header}\n{format_bulk_before_after(unique_ids, before, after, formatter, label, limit=per_page)}{receipt_line(receipt_id)}",
        allowed_mentions=discord.AllowedMentions.none(),
    )

def cooldown_multiplier_for_user(user_id, data=None):
    user_id = int(user_id)
    now = time.time()
    if data is None:
        cached = _cooldown_multiplier_cache.get(user_id)
        if cached and cached[0] > now:
            return cached[1]
    try:
        latest = data
        multiplier = max(0.5, 1 - item_bonus(latest, "cooldown_clock", 0.04, 5)) if latest else 1
    except Exception:
        multiplier = 1
    _cooldown_multiplier_cache[user_id] = (now + 60, multiplier)
    return multiplier

def check_cooldown(user_id, command, data=None):
    if is_super_owner(user_id):
        return 0
    try:
        latest = data
        pause_until = latest.get("gambling_pause_until") if latest else None
        if pause_until:
            if pause_until.tzinfo is None:
                pause_until = pause_until.replace(tzinfo=timezone.utc)
            remaining_pause = (pause_until - datetime.now(timezone.utc)).total_seconds()
            if remaining_pause > 0:
                return remaining_pause
    except Exception:
        pass
    key = (user_id, "quewo")
    now = time.time()
    cooldown = COOLDOWN_SECS * cooldown_multiplier_for_user(user_id, data)
    if key in _cooldowns:
        elapsed = now - _cooldowns[key]
        if elapsed < cooldown:
            return cooldown - elapsed
    _cooldowns[key] = now
    return 0

def parse_amount(raw, user_id=None, guild=None, balance=None):
    raw_text = str(raw).strip().lower().replace(",", "").replace("_", "")
    personal_limit = None
    if user_id is not None and balance is None and not has_economy_owner_power(user_id, guild) and db_ready:
        try:
            data = get_user(user_id)
            stored_limit = data.get("personal_bet_limit")
            if stored_limit is not None and int(stored_limit) > 0:
                personal_limit = int(stored_limit)
        except Exception:
            personal_limit = None
    if raw_text == "all":
        if balance is None:
            return min(MAX_BET, personal_limit) if personal_limit else MAX_BET
        balance = max(0, int(balance))
        cap = min(balance, MAX_BET)
        return min(cap, personal_limit) if personal_limit else cap
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
        cap = min(val, MAX_BET)
        return min(cap, personal_limit) if personal_limit else cap
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

def parse_target_amount_args(raw_args, *, allow_all=False):
    try:
        tokens = shlex.split(str(raw_args or ""))
    except ValueError:
        return None, None
    if len(tokens) < 2:
        return None, None

    amount_indexes = []
    for index, token in enumerate(tokens):
        if allow_all and token.casefold() == "all":
            amount_indexes.append(index)
            continue
        if parse_whole_number(token) is not None:
            amount_indexes.append(index)
    if not amount_indexes:
        return None, None

    amount_index = len(tokens) - 1 if len(tokens) - 1 in amount_indexes else amount_indexes[0]
    amount = tokens[amount_index]
    target = " ".join(tokens[:amount_index] + tokens[amount_index + 1:]).strip()
    if not target:
        return None, None
    return target, amount

async def send_nonpositive_amount_error(ctx, raw_amount):
    if str(raw_amount).lower() == "all":
        await ctx.send(f"{Q_DENIED} You don't have any {CURRENCY_EMOJI} to use.")
        return

    await ctx.send(f"{Q_DENIED} Amount must be positive.")

# --- Helpers ---
async def reply_to_command(ctx, *args, **kwargs):
    kwargs.setdefault("mention_author", False)
    return await ctx.reply(*args, **kwargs)

async def send_error(ctx, text):
    if text == "Database unavailable. Try again shortly.":
        await ensure_db_ready(ctx, force=True)
        return

    try:
        await ctx.send(f"{Q_DENIED} {text}")
    except:
        pass

async def send_owner_only(ctx):
    await ctx.send(f"{Q_DENIED} Only {QUE_OWNER_DISPLAY} can use this.", allowed_mentions=discord.AllowedMentions.none())

async def send_bulk_progress(ctx, action, count):
    if int(count or 0) < 20:
        return None
    return await ctx.send(
        f"{Q_TIMER_TICK} Working on **{int(count):,}** target(s) for **{action}**...",
        allowed_mentions=discord.AllowedMentions.none(),
    )

async def send_or_edit_bulk_result(ctx, progress_message, content, **kwargs):
    content = fit_discord_content(content)
    if progress_message:
        try:
            await progress_message.edit(content=content, **kwargs)
            return
        except Exception:
            pass
    await reply_to_command(ctx, content, **kwargs)

async def send_or_edit_bulk_error(ctx, progress_message, text):
    if progress_message:
        try:
            await progress_message.edit(content=fit_discord_content(f"{Q_DENIED} {text}"))
            return
        except Exception:
            pass
    await send_error(ctx, text)

def strip_bulk_confirm(args):
    text = str(args or "")
    confirmed = bool(re.search(r"(?i)(?:^|\s)--confirm(?:\s|$)", text))
    return confirmed, re.sub(r"(?i)(?:^|\s)--confirm(?:\s|$)", " ", text).strip()

async def require_bulk_confirmation(ctx, action, count):
    if int(count or 0) < 100:
        return False
    await ctx.send(
        f"{Q_WARNING} **Large {action}** affects **{int(count):,}** target(s).\n"
        f"Run the same command again with `--confirm` at the end if that is intentional.",
        allowed_mentions=discord.AllowedMentions.none(),
    )
    return True

def public_error_text(error, fallback="That action failed."):
    text = str(error or "").strip()
    if not text:
        return fallback
    text = re.sub(r"^Error:\s*", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"^\d{3}\s+[A-Za-z ]+:\s*", "", text).strip()
    text = re.sub(r"^[A-Za-z_][\w.]*Error:\s*", "", text).strip()
    text = re.sub(r"\s*\([0-9a-f]{8}-[0-9a-f-]{27,}\)\s*$", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"\s+", " ", text).strip()
    lowered = text.casefold()
    if "missing permissions" in lowered or "forbidden" in lowered:
        return "I do not have permission to do that."
    if "unknown message" in lowered:
        return "That message no longer exists."
    if "must be 2000 or fewer" in lowered or "maximum size" in lowered:
        return "That response was too long for Discord."
    return text[:220] if text else fallback

ECONOMY_INPUT_UI_COMMANDS = {"buytick", "give", "add", "remove", "addtick", "removetick", "settick", "setquesos"}

ECONOMY_INPUT_PLACEHOLDERS = {
    "buytick": "30",
    "give": "@user 10k",
    "add": "@user 1m",
    "remove": "@user 10k",
    "addtick": "@user 10",
    "removetick": "@user 10",
    "settick": "@user 10",
    "setquesos": "@user 1m",
}

async def invoke_economy_command_from_interaction(interaction, command_name, args):
    if bot is None:
        return await interaction.followup.send(f"{Q_DENIED} Bot is not ready.", ephemeral=True)
    command = bot.get_command(command_name)
    if command is None:
        return await interaction.followup.send(f"{Q_DENIED} Command `{command_name}` was not found.", ephemeral=True)
    if StringView is None or not hasattr(commands.Context, "from_interaction"):
        return await interaction.followup.send(f"{Q_DENIED} Setup UI forwarding is not available in this Discord library version.", ephemeral=True)
    ctx = await commands.Context.from_interaction(interaction)
    ctx.command = command
    ctx.invoked_with = command.name
    ctx.prefix = "/run "
    ctx.view = StringView(str(args or "").strip())
    ctx.view.skip_ws()
    try:
        await command.invoke(ctx)
    except commands.CommandError as e:
        await ctx.send(f"{Q_DENIED} {public_error_text(e)}")
    except Exception as e:
        print(f"Economy setup UI failed for {command_name}: {type(e).__name__} - {e}")
        await interaction.followup.send(f"{Q_DENIED} That command failed.", ephemeral=True)

class EconomyCommandInputModal(discord.ui.Modal):
    def __init__(self, author_id, command_name):
        super().__init__(title=f"Run {command_name}")
        self.author_id = author_id
        self.command_name = command_name
        self.command_input = discord.ui.TextInput(
            label="Command input",
            placeholder=ECONOMY_INPUT_PLACEHOLDERS.get(command_name, "Enter what comes after the command"),
            min_length=1,
            max_length=500,
        )
        self.add_item(self.command_input)

    async def on_submit(self, interaction):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.defer(thinking=True)
        await invoke_economy_command_from_interaction(interaction, self.command_name, str(self.command_input.value).strip())

class EconomyCommandInputView(discord.ui.View):
    def __init__(self, author_id, command_name):
        super().__init__(timeout=LONG_SETUP_VIEW_TIMEOUT)
        self.author_id = author_id
        self.command_name = command_name

    @discord.ui.button(label="Enter Input", emoji=Q_EDIT, style=discord.ButtonStyle.primary)
    async def enter_input(self, interaction, button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("Use your own setup UI.", ephemeral=True)
        await interaction.response.send_modal(EconomyCommandInputModal(self.author_id, self.command_name))

async def send_economy_command_input_ui(ctx, command_name, note=None):
    prefix = getattr(ctx, "prefix", ".")
    placeholder = ECONOMY_INPUT_PLACEHOLDERS.get(command_name, "")
    example = f"{prefix}{command_name} {placeholder}".strip()
    embed = discord.Embed(
        title=f"{Q_EDIT} {prefix}{command_name}",
        description=note or "This command needs input. Press the button and enter what should come after the command.",
        color=discord.Color.blurple(),
    )
    embed.add_field(name="Example", value=f"`{example}`", inline=False)
    await ctx.send(embed=embed, view=EconomyCommandInputView(ctx.author.id, command_name), allowed_mentions=discord.AllowedMentions.none())

async def ensure_db_ready(ctx, force=False):
    global db_initializing, db_init_task
    if db_ready and not force:
        return True

    if force or db_init_task is None or db_init_task.done():
        db_initializing = True
        db_init_task = asyncio.create_task(asyncio.to_thread(init_db))

    while not db_init_task.done():
        await asyncio.sleep(1)

    await db_init_task
    db_initializing = False

    return bool(db_ready)

# =====================
# BALANCE + STREAKS
# =====================
@commands.command(aliases=["balance", "wallet", "cash"])
async def bal(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return

    user = ctx.author if not member else member
    try:
        data = await asyncio.to_thread(get_user, user.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    streak = int(data.get("gamble_streak", 0) or 0)
    streak_lines = ""
    if streak > 0:
        mult = payout_multiplier(data, streak)
        streak_lines = f"`gambling` {streak} wins → ×{mult:.3f} payout"

    embed = discord.Embed(
        title=f"{QASH_EMOJI} Balance",
        description=user_mention(user.id),
        color=discord.Color.gold()
    )
    embed.add_field(name="Cash", value=format_balance(data['balance']), inline=True)
    embed.add_field(name="Bank", value=format_balance(data.get('bank_balance', 0)), inline=True)
    embed.add_field(name="Total", value=format_balance(int(data['balance'] or 0) + int(data.get('bank_balance') or 0)), inline=True)
    if streak_lines:
        embed.add_field(name="Streaks", value=streak_lines.strip(), inline=False)
    embed.add_field(name="Daily Streak", value=plural_unit(data["daily_streak"], "day"), inline=True)
    embed.add_field(name="Weekly Streak", value=plural_unit(data["weekly_streak"], "week"), inline=True)
    embed.add_field(name="Monthly Streak", value=plural_unit(data["monthly_streak"], "month"), inline=True)
    embed.add_field(name="Total Earned", value=format_balance(data['total_earned']), inline=True)
    embed.add_field(name="Total Won", value=format_balance(data['total_won']), inline=True)
    embed.add_field(name="Total Lost", value=format_balance(data['total_lost']), inline=True)

    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    if user.id == ctx.author.id:
        await maybe_send_tutorial(ctx, data, "start")

@commands.command(name="bank", aliases=["safe", "vaultcash", "deposit", "withdraw"])
async def bank(ctx, action: str = None, *, raw_amount: str = None):
    if not await ensure_db_ready(ctx):
        return
    try:
        data = await asyncio.to_thread(get_user, ctx.author.id)
    except Exception:
        return await send_error(ctx, "Database unavailable. Try again shortly.")
    invoked = ctx.invoked_with.casefold()
    if invoked in {"deposit", "withdraw"} and action is not None and raw_amount is None:
        raw_amount = action
        action = invoked
    action = (action or "status").casefold()
    if action in {"status", "bal", "balance", "show"}:
        last_interest = data.get("last_bank_interest")
        next_interest = None
        if last_interest:
            if last_interest.tzinfo is None:
                last_interest = last_interest.replace(tzinfo=timezone.utc)
            next_interest = last_interest + timedelta(hours=24)
        embed = discord.Embed(
            title=f"{Q_BANK} Bank",
            description=f"{user_mention(ctx.author.id)}\nBanked money is protected from robbery.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Cash", value=format_balance(int(data["balance"] or 0)), inline=True)
        embed.add_field(name="Bank", value=format_balance(int(data.get("bank_balance") or 0)), inline=True)
        embed.add_field(name="Total", value=format_balance(int(data["balance"] or 0) + int(data.get("bank_balance") or 0)), inline=True)
        embed.add_field(name="Daily Interest", value=(discord_relative_time(next_interest) if next_interest and datetime.now(timezone.utc) < next_interest else "Ready with `.bank interest`"), inline=False)
        embed.add_field(name="Use", value="`.bank deposit 100k`\n`.bank withdraw 50k`\n`.bank deposit all`\n`.bank interest`", inline=False)
        return await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    if action in {"interest", "claim", "collect"}:
        try:
            result = await asyncio.to_thread(claim_bank_interest, ctx.author.id)
        except Exception:
            return await send_error(ctx, "Database unavailable. Try again shortly.")
        if not result.get("ok"):
            return await ctx.send(result["message"], allowed_mentions=discord.AllowedMentions.none())
        return await ctx.send(
            f"{Q_BANK} Claimed **{format_balance(result['amount'])}** bank interest.\n"
            f"Cash: **{format_balance(result['balance'])}**\n"
            f"Bank: **{format_balance(result['bank'])}**",
            allowed_mentions=discord.AllowedMentions.none(),
        )
    if action not in {"deposit", "dep", "withdraw", "with", "take"}:
        return await ctx.send(f"{Q_DENIED} Use `.bank deposit <amount>`, `.bank withdraw <amount>`, or `.bank interest`.")
    mode = "deposit" if action in {"deposit", "dep"} else "withdraw"
    source_balance = int(data["balance"] or 0) if mode == "deposit" else int(data.get("bank_balance") or 0)
    amount = parse_amount(raw_amount, ctx.author.id, ctx.guild, source_balance)
    if amount is None or amount <= 0:
        return await ctx.send(f"{Q_DENIED} Use an amount like `50k`, `1m`, or `all`.")
    try:
        result = await asyncio.to_thread(transfer_to_bank, ctx.author.id, amount, mode)
    except ValueError:
        return await ctx.send(f"{Q_DENIED} You do not have that much {'cash' if mode == 'deposit' else 'banked money'}.")
    except Exception:
        return await send_error(ctx, "Database unavailable. Try again shortly.")
    verb = "Deposited" if mode == "deposit" else "Withdrew"
    await ctx.send(
        f"{Q_BANK} {verb} **{format_balance(result['amount'])}**.\n"
        f"Cash: **{format_balance(result['old_balance'])}** → **{format_balance(result['balance'])}**\n"
        f"Bank: **{format_balance(result['old_bank'])}** → **{format_balance(result['bank'])}**",
        allowed_mentions=discord.AllowedMentions.none(),
    )

@commands.command(name="tutorial", aliases=["tutorialmode", "tips"])
async def tutorial(ctx, setting: str = None):
    if not await ensure_db_ready(ctx):
        return
    setting_key = str(setting or "status").casefold()
    if setting_key in {"off", "end", "stop", "disable"}:
        updated = await asyncio.to_thread(update_user, ctx.author.id, tutorial_mode=False)
        return await ctx.send(f"{Q_SUCCESS} Tutorial mode is now **off**.")
    if setting_key in {"on", "start", "enable"}:
        updated = await asyncio.to_thread(update_user, ctx.author.id, tutorial_mode=True, tutorial_uses=1)
        return await ctx.send(f"{Q_TUTORIAL} Tutorial mode is now **on**.")
    data = await asyncio.to_thread(get_user, ctx.author.id)
    await ctx.send(
        f"{Q_TUTORIAL} Tutorial mode is **{'on' if tutorial_enabled(data) else 'off'}**.\n"
        "Use `.tutorial off` when you do not want starter tips anymore.",
        view=TutorialEndView(ctx.author.id) if tutorial_enabled(data) else None,
        allowed_mentions=discord.AllowedMentions.none(),
    )

@commands.command(name="recommendgame", aliases=["recgame", "whatgame", "suggestgame"])
async def recommendgame(ctx):
    if not await ensure_db_ready(ctx):
        return
    try:
        data = await asyncio.to_thread(get_user, ctx.author.id)
        stats = await asyncio.to_thread(get_game_stats, ctx.author.id)
    except Exception:
        return await send_error(ctx, "Database unavailable. Try again shortly.")
    balance = int(data["balance"] or 0)
    played = {row["game_key"]: int(row["played"] or 0) for row in stats}
    profit = {row["game_key"]: int(row["profit"] or 0) for row in stats}
    if balance < 50_000:
        pick = "daily"
        reason = "Build a little cash first. Claim rewards, use `.flagquiz`, or try free `.dungeon`."
        command = ".daily"
    elif balance < 250_000:
        pick = "dungeon"
        reason = "Free, interactive, and good for learning the bot without risking cash."
        command = ".dungeon"
    elif profit.get("memory", 0) >= 0 and played.get("memory", 0) < 20:
        pick = "memory"
        reason = "Skill-based, not pure luck, and good if you want thinking without huge swings."
        command = ".memory 50k"
    elif balance >= 1_000_000:
        pick = "tower"
        reason = "Good control: you can cash out early instead of letting the game decide everything."
        command = ".tower 100k"
    else:
        pick = "cardladder"
        reason = "Simple skill/luck mix, clear cashout points, and not too chaotic."
        command = ".cardladder 50k"
    embed = discord.Embed(
        title=f"{Q_RECOMMEND} Recommended Game",
        description=f"Try **{game_display_name(pick)}**\n`{command}`",
        color=discord.Color.green(),
    )
    embed.add_field(name="Why", value=reason, inline=False)
    embed.add_field(name="Your Cash", value=format_balance(balance), inline=True)
    embed.add_field(name="Safer Move", value="Bank extra money with `.bank deposit <amount>` before gambling.", inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="robsettings", aliases=["robbing", "setrob", "robconfig"])
async def robsettings(ctx, setting: str = None):
    if not await ensure_db_ready(ctx):
        return
    if ctx.guild is None:
        return await ctx.send(f"{Q_DENIED} Robbing settings only work in servers.")
    if not has_economy_owner_power(ctx.author.id, ctx.guild):
        return await ctx.send(f"{Q_DENIED} Server owner or admin only.")
    setting_key = str(setting or "status").casefold()
    if setting_key in {"on", "enable", "enabled", "true"}:
        await asyncio.to_thread(set_robbing_enabled, ctx.guild.id, True)
        return await ctx.send(f"{Q_ROB} Robbing is now **enabled**. Banked money stays protected.")
    if setting_key in {"off", "disable", "disabled", "false"}:
        await asyncio.to_thread(set_robbing_enabled, ctx.guild.id, False)
        return await ctx.send(f"{Q_ROB} Robbing is now **disabled**.")
    enabled = await asyncio.to_thread(robbing_enabled, ctx.guild.id)
    await ctx.send(f"{Q_ROB} Robbing is **{'enabled' if enabled else 'disabled'}**.\nUse `.robsettings on` or `.robsettings off`.")

@commands.command(name="rob", aliases=["stealqs", "mug"])
async def rob(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return
    if ctx.guild is None:
        return await ctx.send(f"{Q_DENIED} Robbing only works in servers.")
    if member is None:
        return await ctx.send(f"{Q_DENIED} Use `.rob @user`.")
    try:
        result = await asyncio.to_thread(rob_user_sync, ctx.guild.id, ctx.author.id, member.id)
    except Exception:
        return await send_error(ctx, "Database unavailable. Try again shortly.")
    if not result.get("ok"):
        return await ctx.send(result["message"], allowed_mentions=discord.AllowedMentions.none())
    if result.get("success"):
        msg = (
            f"{Q_ROB} {user_mention(ctx.author.id)} robbed **{format_balance(result['amount'])}** from {user_mention(member.id)}.\n"
            f"Your Cash: **{format_balance(result['robber_balance'])}**\n"
            f"Their Cash: **{format_balance(result['target_balance'])}**"
        )
    else:
        msg = (
            f"{Q_DENIED} Rob failed. You paid {user_mention(member.id)} **{format_balance(result['fine'])}**.\n"
            f"Your Cash: **{format_balance(result['robber_balance'])}**\n"
            f"Their Cash: **{format_balance(result['target_balance'])}**"
        )
    await ctx.send(msg, allowed_mentions=discord.AllowedMentions(users=True))

@commands.command(aliases=["prof", "level", "lvl"])
async def profile(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return

    user = member or ctx.author
    try:
        data = await asyncio.to_thread(get_user, user.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    embed = build_profile_embed(user, data)
    try:
        rows = await asyncio.to_thread(get_game_stats, user.id)
    except Exception:
        rows = []
    if rows:
        favorite = max(rows, key=lambda row: int(row["played"] or 0))
        best = max(rows, key=lambda row: int(row["profit"] or 0))
        embed.add_field(
            name=f"{Q_GAME_STATS} Games",
            value=(
                f"Favorite: **{game_display_name(favorite['game_key'])}** ({int(favorite['played'] or 0):,} played)\n"
                f"Best Profit: **{game_display_name(best['game_key'])}** ({format_balance(int(best['profit'] or 0))})"
            ),
            inline=False,
        )
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="settheme", aliases=["theme", "profiletheme"])
async def settheme(ctx, theme: str = None):
    """Equips a profile theme from owned decorative shop items."""
    if not await ensure_db_ready(ctx):
        return
    try:
        data = await asyncio.to_thread(get_user, ctx.author.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    if theme is None:
        lines = []
        for theme_id, info in PROFILE_THEMES.items():
            item_id = info.get("item")
            owned = item_id is None or has_item(data, item_id)
            status = "owned" if owned else f"requires {item_display_name(SHOP_ITEMS[item_id])}"
            lines.append(f"`{theme_id}` - {info['label']} ({status})")
        return await ctx.send(f"{Q_VELVET_FRAME} Profile themes:\n" + "\n".join(lines))

    theme_id = theme.strip().casefold()
    if theme_id not in PROFILE_THEMES:
        return await ctx.send(f"{Q_DENIED} Use one of: `{', '.join(PROFILE_THEMES)}`")
    item_id = PROFILE_THEMES[theme_id].get("item")
    if item_id and not has_item(data, item_id):
        return await ctx.send(f"{Q_DENIED} You need **{item_display_name(SHOP_ITEMS[item_id])}** to use that theme.")
    await asyncio.to_thread(update_user, ctx.author.id, profile_theme=theme_id)
    await ctx.send(f"{Q_SUCCESS} Profile theme equipped: **{PROFILE_THEMES[theme_id]['label']}**")

@commands.command(name="inventory", aliases=["inv", "items"])
async def inventory(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return

    user = member or ctx.author
    try:
        data = await asyncio.to_thread(get_user, user.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    class InventoryView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
            self.page = "overview"
            self.message = None
            self.page_select = discord.ui.Select(
                placeholder="Inventory page",
                options=[
                    discord.SelectOption(label="Overview", value="overview", description="Balance, active effects, and bonus summary"),
                    discord.SelectOption(label="Active Effects", value="active", description="Temporary effects and passive bonuses"),
                    discord.SelectOption(label="Owned Items", value="owned", description="Only items this user owns"),
                    *[
                        discord.SelectOption(label=category, value=f"category:{category}", description=f"All {category.lower()} shop items")
                        for category in sorted({item["category"] for item in SHOP_ITEMS.values()})
                    ],
                ],
                min_values=1,
                max_values=1
            )
            self.page_select.callback = self.select_page
            self.add_item(self.page_select)

        async def interaction_check(self, interaction):
            if interaction.user.id == ctx.author.id:
                return True
            await interaction.response.send_message("Open your own inventory with `.inventory`.", ephemeral=True)
            return False

        async def refresh(self, interaction=None):
            try:
                updated = await asyncio.to_thread(get_user, user.id)
            except Exception:
                if interaction:
                    await interaction.followup.send(f"{Q_DENIED} Database unavailable. Try again shortly.", ephemeral=True)
                return
            embed = build_inventory_embed(user, updated, self.page)
            if interaction:
                await interaction.edit_original_response(embed=embed, view=self, allowed_mentions=discord.AllowedMentions.none())
            elif self.message:
                await self.message.edit(embed=embed, view=self, allowed_mentions=discord.AllowedMentions.none())

        async def select_page(self, interaction):
            self.page = self.page_select.values[0]
            await interaction.response.defer()
            await self.refresh(interaction)

        @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary)
        async def refresh_button(self, interaction, button):
            await interaction.response.defer()
            await self.refresh(interaction)

        @discord.ui.button(label="Shop", emoji=Q_SHOP, style=discord.ButtonStyle.success)
        async def shop_hint_button(self, interaction, button):
            await interaction.response.send_message("Use `.shop` to buy boosts, cosmetics, and active effects.", ephemeral=True)

        @discord.ui.button(label="Profile", emoji=Q_XP, style=discord.ButtonStyle.secondary)
        async def profile_hint_button(self, interaction, button):
            await interaction.response.send_message("Use `.profile` to view your equipped badges, level, balance, and theme.", ephemeral=True)

        @discord.ui.button(label="Equip Theme", style=discord.ButtonStyle.primary)
        async def equip_theme_button(self, interaction, button):
            try:
                updated = await asyncio.to_thread(get_user, interaction.user.id)
            except Exception:
                return await interaction.response.send_message(f"{Q_DENIED} Database unavailable. Try again shortly.", ephemeral=True)
            options = []
            for theme_id, info in PROFILE_THEMES.items():
                item_id = info.get("item")
                if item_id is not None and not has_item(updated, item_id):
                    continue
                options.append(discord.SelectOption(label=info["label"], value=theme_id, description=f"Equip {info['label']} theme"))
            parent = self

            class ThemeEquipView(discord.ui.View):
                def __init__(self):
                    super().__init__(timeout=60)
                    select = discord.ui.Select(placeholder="Choose profile theme", options=options, min_values=1, max_values=1)
                    select.callback = self.select_theme
                    self.add_item(select)

                async def select_theme(self, select_interaction):
                    theme_id = select_interaction.data["values"][0]
                    await asyncio.to_thread(update_user, select_interaction.user.id, profile_theme=theme_id)
                    await select_interaction.response.send_message(f"{Q_SUCCESS} Equipped **{PROFILE_THEMES[theme_id]['label']}**.", ephemeral=True)
                    await parent.refresh()

            await interaction.response.send_message("Pick a theme to equip.", view=ThemeEquipView(), ephemeral=True)

        @discord.ui.button(label="Equip Badges", style=discord.ButtonStyle.secondary)
        async def equip_badges_button(self, interaction, button):
            try:
                updated = await asyncio.to_thread(get_user, interaction.user.id)
            except Exception:
                return await interaction.response.send_message(f"{Q_DENIED} Database unavailable. Try again shortly.", ephemeral=True)
            earned = [badge for badge in achievement_ids(updated) if badge in GAME_ACHIEVEMENTS]
            if not earned:
                return await interaction.response.send_message(f"{Q_DENIED} You have no earned badges yet.", ephemeral=True)
            options = [
                discord.SelectOption(label=achievement_display(badge)[:100], value=badge)
                for badge in earned[:25]
            ]
            parent = self

            class BadgeEquipView(discord.ui.View):
                def __init__(self):
                    super().__init__(timeout=60)
                    select = discord.ui.Select(
                        placeholder="Choose up to 3 badges",
                        options=options,
                        min_values=0,
                        max_values=min(3, len(options)),
                    )
                    select.callback = self.select_badges
                    self.add_item(select)

                async def select_badges(self, select_interaction):
                    selected = select_interaction.data.get("values", [])[:3]
                    await asyncio.to_thread(update_user, select_interaction.user.id, equipped_badges=selected)
                    await select_interaction.response.send_message(f"{Q_SUCCESS} Equipped **{len(selected)}** badge(s).", ephemeral=True)
                    await parent.refresh()

            await interaction.response.send_message("Pick up to 3 badges for your profile.", view=BadgeEquipView(), ephemeral=True)

        async def on_timeout(self):
            for item in self.children:
                item.disabled = True
            await self.refresh()

    view = InventoryView()
    view.message = await ctx.send(embed=build_inventory_embed(user, data), view=view, allowed_mentions=discord.AllowedMentions.none())

@commands.command()
async def quests(ctx):
    if not await ensure_db_ready(ctx):
        return

    try:
        data = await asyncio.to_thread(get_user, ctx.author.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    class QuestView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)

        async def interaction_check(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Open your own quests with `.quests`.", ephemeral=True)
                return False
            return True

        @discord.ui.button(label="Claim Completed", style=discord.ButtonStyle.success)
        async def claim_completed(self, interaction, button):
            await interaction.response.defer()
            try:
                _, total_reward, updated = await asyncio.to_thread(claim_completed_quests_sync, interaction.user.id)
            except Exception as e:
                print(f"Quest claim UI error: {type(e).__name__} - {e}")
                await interaction.followup.send(f"{Q_DENIED} Database unavailable. Try again shortly.", ephemeral=True)
                return

            if total_reward <= 0:
                await interaction.followup.send("No completed quests to claim yet.", ephemeral=True)
                return

            await interaction.edit_original_response(
                content=f"{Q_SUCCESS} Claimed **{format_balance(total_reward)}** in quest rewards.",
                embed=build_quests_embed(interaction.user, updated),
                view=self,
                allowed_mentions=discord.AllowedMentions.none()
            )

        @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary)
        async def refresh(self, interaction, button):
            await interaction.response.defer()
            try:
                updated = await asyncio.to_thread(get_user, interaction.user.id)
            except Exception as e:
                print(f"Quest refresh UI error: {type(e).__name__} - {e}")
                await interaction.followup.send(f"{Q_DENIED} Database unavailable. Try again shortly.", ephemeral=True)
                return
            await interaction.edit_original_response(
                embed=build_quests_embed(interaction.user, updated),
                view=self,
                allowed_mentions=discord.AllowedMentions.none()
            )

    await ctx.send(embed=build_quests_embed(ctx.author, data), view=QuestView(), allowed_mentions=discord.AllowedMentions.none())

@commands.command()
async def shop(ctx):
    if not await ensure_db_ready(ctx):
        return

    categories = sorted({item["category"] for item in SHOP_ITEMS.values()})

    def items_for_category(category):
        return [(item_id, item) for item_id, item in SHOP_ITEMS.items() if item["category"] == category]

    def selected_item_for_category(category, current=None):
        item_ids = [item_id for item_id, _ in items_for_category(category)]
        return current if current in item_ids else (item_ids[0] if item_ids else next(iter(SHOP_ITEMS)))

    async def get_shop_data(user_id):
        try:
            return await asyncio.to_thread(get_user, user_id)
        except Exception:
            return {"balance": 0, "inventory": []}

    async def get_shop_discount():
        return await asyncio.to_thread(event_shop_discount_rate, ctx.guild.id if ctx.guild else None)

    def catalog_embed(data, selected_category, selected_item_id, discount=0.0):
        selected_category = selected_category if selected_category in categories else categories[0]
        selected_item_id = selected_item_for_category(selected_category, selected_item_id)
        discount = float(discount or 0.0)
        category_items = items_for_category(selected_category)
        selected_item = SHOP_ITEMS[selected_item_id]
        balance = int(data.get("balance", 0) or 0)
        boost_text = luck_boost_text(data)
        try:
            inventory = user_inventory(data)
        except Exception:
            inventory = []

        embed = discord.Embed(
            title=f"{Q_SHOP} 𝚀𝚞𝚎wo Shop",
            description=(
                f"{user_mention(ctx.author.id)}\n"
                f"{QASH_EMOJI} Balance: **{format_balance(balance)}**\n"
                f"{Q_FILTER} Category: **{selected_category}**"
            ),
            color=discord.Color.gold()
        )
        if boost_text:
            embed.add_field(name="Active Effect", value=boost_text, inline=False)
        lines = []
        for item_id, item in category_items:
            marker = "→ " if item_id == selected_item_id else ""
            display_cost = int(item["cost"] * (1 - discount))
            price_text = format_balance(display_cost)
            if discount:
                price_text += f" ~~{format_balance(item['cost'])}~~"
            owned = inventory.count(item_id)
            max_qty = item.get("max_qty", 1)
            owned_text = "active" if "duration_hours" in item and item_id == "fortune_vial" and boost_text else (
                f"{owned}/{max_qty}" if max_qty > 1 else ("owned" if owned else "not owned")
            )
            lines.append(
                f"{marker}**{item_display_name(item)}** — **{price_text}**\n"
                f"{item_short_description(item)}\n"
                f"`{item_type_label(item)}` • {item_rarity_label(item)} • Owned: **{owned_text}**"
            )
        add_split_embed_field(embed, "Catalog", lines or ["No items in this category."], inline=False)

        selected_cost = int(selected_item["cost"] * (1 - discount))
        selected_owned = inventory.count(selected_item_id)
        selected_max = selected_item.get("max_qty", 1)
        selected_owned_text = "active" if "duration_hours" in selected_item and selected_item_id == "fortune_vial" and boost_text else (
            f"{selected_owned}/{selected_max}" if selected_max > 1 else ("owned" if selected_owned else "not owned")
        )
        embed.add_field(
            name=f"{Q_COMMAND_CHECK} Selected Item",
            value=(
                f"**{item_display_name(selected_item)}**\n"
                f"{item_short_description(selected_item)}\n\n"
                f"Price: **{format_balance(selected_cost)}**"
                + (f" ~~{format_balance(selected_item['cost'])}~~" if discount else "")
                + f"\nType: **{item_type_label(selected_item)}**"
                f"\nRarity: **{item_rarity_label(selected_item)}**"
                f"\nOwned: **{selected_owned_text}**"
            ),
            inline=False,
        )
        embed.set_footer(text=f"{categories.index(selected_category) + 1}/{len(categories)} categories • Pick an item, then Buy.")
        return embed

    def purchase_summary_embed(data, purchase, closed=False):
        item = SHOP_ITEMS[purchase["item_id"]]
        embed = discord.Embed(
            title=f"{Q_SHOP} Shop Summary",
            description=(
                f"{user_mention(ctx.author.id)}\n"
                f"{Q_SUCCESS} Bought **{purchase['quantity']}x {item_display_name(item)}**."
            ),
            color=discord.Color.green()
        )
        embed.add_field(name="Spent", value=format_balance(purchase["total_cost"]), inline=True)
        embed.add_field(name="Balance", value=format_balance(purchase["new_balance"]), inline=True)
        embed.add_field(name="Item", value=item_short_description(item), inline=False)
        if purchase.get("discount"):
            embed.add_field(name="Discount", value=f"-{int(purchase['discount'] * 100)}%", inline=True)
        if purchase.get("effect_until"):
            embed.add_field(name="Effect", value=f"Active until {discord_relative_time(purchase['effect_until'])}", inline=False)
        embed.set_footer(text="Shopping closed." if closed else "Continue shopping or press Done to keep this compact summary.")
        return embed

    def buy_item_sync(user_id, guild_id, item_id, quantity):
        if not str(quantity).isdigit():
            return {"ok": False, "message": f"{Q_DENIED} Quantity must be a positive whole number."}

        quantity = int(quantity)
        if quantity <= 0:
            return {"ok": False, "message": f"{Q_DENIED} Quantity must be positive."}

        item = SHOP_ITEMS[item_id]
        item_name = item_display_name(item)
        data = get_user(user_id)
        inventory = user_inventory(data)
        if "duration_hours" in item:
            max_qty = item.get("max_qty", 99)
            if quantity > max_qty:
                return {"ok": False, "message": f"{Q_DENIED} You can buy at most **{max_qty}** **{item_name}** at once."}
        else:
            owned = inventory.count(item_id)
            max_qty = item.get("max_qty", 1)
            remaining_allowed = max_qty - owned
            if remaining_allowed <= 0:
                return {"ok": False, "message": f"{Q_DENIED} You already own the max amount of **{item_name}**."}
            if quantity > remaining_allowed:
                return {"ok": False, "message": f"{Q_DENIED} You can only buy **{remaining_allowed}** more **{item_name}**."}

        discount = event_shop_discount_rate(guild_id)
        unit_cost = int(item["cost"] * (1 - discount))
        total_cost = unit_cost * quantity
        if int(data["balance"] or 0) < total_cost:
            affordable = int(data["balance"] or 0) // max(1, unit_cost)
            return {"ok": False, "message": f"{Q_DENIED} That costs **{format_balance(total_cost)}**. You can afford **{affordable}**."}

        new_balance_value = int(data["balance"] or 0) - total_cost
        effect_text = ""
        effect_until = None
        if "duration_hours" in item:
            now = datetime.now(timezone.utc)
            current_until = data.get("luck_boost_until") if item_id == "fortune_vial" else None
            if current_until:
                current_until = current_until.replace(tzinfo=timezone.utc) if current_until.tzinfo is None else current_until
            start = max(now, current_until) if current_until else now
            boost_until = start + timedelta(hours=item["duration_hours"] * quantity)
            effect_until = boost_until
            new_balance = apply_shop_purchase(
                user_id,
                item_id,
                total_cost,
                new_balance_value,
                luck_boost_until=boost_until,
                note=f"{quantity}x {item['name']}",
            )
            effect_text = f"\nEffect active until **{discord_relative_time(boost_until)}**."
        else:
            inventory.extend([item_id] * quantity)
            new_balance = apply_shop_purchase(
                user_id,
                item_id,
                total_cost,
                new_balance_value,
                inventory=inventory,
                note=f"{quantity}x {item['name']}",
            )
        discount_text = f"\n{Q_EVENT} Shop Discount: **-{int(discount * 100)}%**" if discount else ""
        return {
            "ok": True,
            "message": (
                f"{Q_SUCCESS} Bought **{quantity}x {item_name}** for **{format_balance(total_cost)}**.\n"
                f"New Balance: **{format_balance(new_balance)}**{discount_text}{effect_text}"
            ),
            "purchase": {
                "item_id": item_id,
                "quantity": quantity,
                "total_cost": total_cost,
                "new_balance": new_balance,
                "discount": discount,
                "effect_until": effect_until,
            },
        }

    class ShopQuantityModal(discord.ui.Modal):
        def __init__(self, item_id, shop_view):
            super().__init__(title=f"Buy {SHOP_ITEMS[item_id]['name']}")
            self.item_id = item_id
            self.shop_view = shop_view
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
            try:
                result = await asyncio.to_thread(
                    buy_item_sync,
                    interaction.user.id,
                    interaction.guild.id if interaction.guild else None,
                    self.item_id,
                    str(self.quantity.value).strip(),
                )
            except Exception:
                await interaction.followup.send(f"{Q_DENIED} Database unavailable. Try again shortly.", ephemeral=True)
                return

            await interaction.followup.send(result["message"], ephemeral=True)
            if result.get("ok") and result.get("purchase"):
                await self.shop_view.show_purchase_summary(result["purchase"])
            else:
                await self.shop_view.refresh_message()

    class ShopView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
            self.selected_category = categories[0]
            self.selected_item_id = selected_item_for_category(self.selected_category)
            self.message = None
            self.refresh_task = None
            self.mode = "catalog"
            self.last_purchase = None
            self.rebuild_components()

        def rebuild_components(self):
            self.clear_items()
            if self.mode == "summary":
                self.continue_button = discord.ui.Button(label="Continue shopping", style=discord.ButtonStyle.primary, row=0)
                self.continue_button.callback = self.continue_shopping
                self.add_item(self.continue_button)
                self.done_button = discord.ui.Button(label="Done", style=discord.ButtonStyle.secondary, row=0)
                self.done_button.callback = self.finish_shopping
                self.add_item(self.done_button)
                return
            category_options = [
                discord.SelectOption(
                    label=category,
                    value=category,
                    description=f"{len(items_for_category(category))} item(s)",
                    default=category == self.selected_category,
                )
                for category in categories
            ]
            self.category_select = discord.ui.Select(
                placeholder="Choose a category",
                options=category_options,
                min_values=1,
                max_values=1,
                row=0,
            )
            self.category_select.callback = self.select_category
            self.add_item(self.category_select)

            item_options = []
            for item_id, item in items_for_category(self.selected_category):
                item_options.append(
                    discord.SelectOption(
                        label=item["name"],
                        value=item_id,
                        description=f"{format_balance(item['cost'])} | {item_rarity_label(item)} | max {item.get('max_qty', 1)}",
                        emoji=item_select_emoji(item),
                        default=item_id == self.selected_item_id,
                    )
                )
            self.item_select = discord.ui.Select(
                placeholder="Choose an item",
                options=item_options,
                min_values=1,
                max_values=1,
                row=1,
            )
            self.item_select.callback = self.select_item
            self.add_item(self.item_select)
            self.buy_button = discord.ui.Button(label="Buy", style=discord.ButtonStyle.success, row=2)
            self.buy_button.callback = self.buy_selected
            self.add_item(self.buy_button)

        async def start_live_refresh(self):
            if self.refresh_task is None or self.refresh_task.done():
                self.refresh_task = asyncio.create_task(self.live_refresh_loop())

        async def live_refresh_loop(self):
            while not self.is_finished():
                await asyncio.sleep(30)
                await self.refresh_message()

        async def refresh_message(self):
            if not self.message:
                return
            try:
                data, discount = await asyncio.gather(
                    get_shop_data(ctx.author.id),
                    asyncio.to_thread(event_shop_discount_rate, ctx.guild.id if ctx.guild else None),
                )
                if self.mode == "summary" and self.last_purchase:
                    await self.message.edit(
                        embed=purchase_summary_embed(data, self.last_purchase),
                        view=self,
                        allowed_mentions=discord.AllowedMentions.none()
                    )
                else:
                    await self.message.edit(
                        embed=catalog_embed(data, self.selected_category, self.selected_item_id, discount),
                        view=self,
                        allowed_mentions=discord.AllowedMentions.none()
                    )
            except Exception:
                self.stop()

        async def on_timeout(self):
            if self.refresh_task and not self.refresh_task.done():
                self.refresh_task.cancel()
            for item in self.children:
                item.disabled = True
            await self.refresh_message()

        async def interaction_check(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Open your own shop with `.shop`.", ephemeral=True)
                return False
            return True

        async def select_item(self, interaction):
            self.mode = "catalog"
            self.selected_item_id = self.item_select.values[0]
            data, discount = await asyncio.gather(
                get_shop_data(interaction.user.id),
                get_shop_discount(),
            )
            await interaction.response.edit_message(
                embed=catalog_embed(data, self.selected_category, self.selected_item_id, discount),
                view=self,
                allowed_mentions=discord.AllowedMentions.none()
            )

        async def select_category(self, interaction):
            self.mode = "catalog"
            self.selected_category = self.category_select.values[0]
            self.selected_item_id = selected_item_for_category(self.selected_category, self.selected_item_id)
            self.rebuild_components()
            data, discount = await asyncio.gather(
                get_shop_data(interaction.user.id),
                get_shop_discount(),
            )
            await interaction.response.edit_message(
                embed=catalog_embed(data, self.selected_category, self.selected_item_id, discount),
                view=self,
                allowed_mentions=discord.AllowedMentions.none()
            )

        async def buy_selected(self, interaction):
            await interaction.response.send_modal(ShopQuantityModal(self.selected_item_id, self))

        async def show_purchase_summary(self, purchase):
            self.mode = "summary"
            self.last_purchase = purchase
            self.rebuild_components()
            if not self.message:
                return
            data = await get_shop_data(ctx.author.id)
            await self.message.edit(
                embed=purchase_summary_embed(data, purchase),
                view=self,
                allowed_mentions=discord.AllowedMentions.none()
            )

        async def continue_shopping(self, interaction):
            self.mode = "catalog"
            self.rebuild_components()
            data, discount = await asyncio.gather(
                get_shop_data(interaction.user.id),
                get_shop_discount(),
            )
            await interaction.response.edit_message(
                embed=catalog_embed(data, self.selected_category, self.selected_item_id, discount),
                view=self,
                allowed_mentions=discord.AllowedMentions.none()
            )

        async def finish_shopping(self, interaction):
            if not self.last_purchase:
                await interaction.response.defer()
                return
            self.mode = "summary"
            for item in self.children:
                item.disabled = True
            data = await get_shop_data(interaction.user.id)
            await interaction.response.edit_message(
                embed=purchase_summary_embed(data, self.last_purchase, closed=True),
                view=self,
                allowed_mentions=discord.AllowedMentions.none()
            )
            self.stop()

    view = ShopView()
    data, discount = await asyncio.gather(
        get_shop_data(ctx.author.id),
        get_shop_discount(),
    )
    view.message = await ctx.send(embed=catalog_embed(data, view.selected_category, view.selected_item_id, discount), view=view, allowed_mentions=discord.AllowedMentions.none())
    await view.start_live_refresh()
    try:
        await maybe_send_tutorial(ctx, await asyncio.to_thread(get_user, ctx.author.id), "shop")
    except Exception:
        pass

@commands.command(aliases=["cd", "cooldown"])
async def cooldowns(ctx):
    if not await ensure_db_ready(ctx):
        return

    def build_embed(data):
        embed = discord.Embed(title=f"{Q_TIMER} Cooldowns", color=discord.Color.blurple())
        embed.add_field(name="Daily", value=claim_cooldown_text(data.get("last_daily"), 86400), inline=True)
        embed.add_field(name="Weekly", value=claim_cooldown_text(data.get("last_weekly"), 604800), inline=True)
        embed.add_field(name="Monthly", value=claim_cooldown_text(data.get("last_monthly"), 2592000), inline=True)
        for command in ["cf", "roulette", "slots", "blackjack", "scratch", "tower", "vault", "memory", "cardladder", "lockpick", "heist", "diceduel", "cases", "plinko", "luckynumber", "jackpotspin", "ms", "wheel"]:
            embed.add_field(name=command, value=command_cooldown_text(ctx.author.id, command), inline=True)
        embed.set_footer(text="Refresh updates live timestamps. Gambling uses one shared 𝚀𝚞𝚎wo cooldown.")
        return embed

    class CooldownsView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)

        async def interaction_check(self, interaction):
            if interaction.user.id == ctx.author.id:
                return True
            await interaction.response.send_message("Open your own cooldowns with `.cooldowns`.", ephemeral=True)
            return False

        @discord.ui.button(label="Refresh", style=discord.ButtonStyle.primary)
        async def refresh_button(self, interaction, button):
            await interaction.response.defer()
            try:
                updated = await asyncio.to_thread(get_user, interaction.user.id)
            except Exception:
                return await interaction.followup.send(f"{Q_DENIED} Database unavailable. Try again shortly.", ephemeral=True)
            await interaction.edit_original_response(embed=build_embed(updated), view=self, allowed_mentions=discord.AllowedMentions.none())

        @discord.ui.button(label="Guide", style=discord.ButtonStyle.secondary)
        async def guide_button(self, interaction, button):
            await interaction.response.send_message(
                f"{Q_BOOK} Daily, weekly, and monthly have separate claim timers. Gambling commands share one universal cooldown, so any gambling game starts the same timer.",
                ephemeral=True,
            )

    try:
        data = await asyncio.to_thread(get_user, ctx.author.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await ctx.send(embed=build_embed(data), view=CooldownsView(), allowed_mentions=discord.AllowedMentions.none())

@commands.command(aliases=["tx"])
async def transactions(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return

    user = member or ctx.author
    try:
        rows = await asyncio.to_thread(get_recent_transactions, user.id, 12)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    if user.id == SUPER_OWNER_ID and ctx.author.id != SUPER_OWNER_ID:
        rows = [
            row for row in rows
            if row["kind"] not in INTERNAL_SUPEROWNER_TRANSACTION_KINDS
        ]

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

    config = await asyncio.to_thread(get_lottery_config, ctx.guild.id)
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
        if channel is None:
            match = re.search(r"\d{15,25}", channel_msg.content or "")
            if match:
                channel = ctx.guild.get_channel(int(match.group(0)))
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
            await ctx.send(f"{Q_DENIED} I couldn't prepare the lottery channel: {public_error_text(e)}")
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
        if not is_superowner_id(ctx.author.id):
            await send_owner_only(ctx)
            return
        await ctx.send("To reconfigure, delete the current lottery config from the database or ask me to add a reset flow.")
        return

    if action and action.casefold() == "buy":
        schedule_lottery_refresh(ctx.guild, config)
        await ctx.send("Use the ticket buttons on the lottery panel to buy lottery tickets.", allowed_mentions=discord.AllowedMentions.none())
        return

    panel_url = lottery_panel_url(ctx.guild, config)
    embed = await build_lottery_status_embed(ctx.guild, config, panel_url)
    status_message = await ctx.send(embed=embed, view=LotteryPanelView(), allowed_mentions=discord.AllowedMentions.none())
    remember_lottery_status_message(ctx.guild.id, status_message)
    schedule_lottery_refresh(ctx.guild, config)

@commands.command()
async def editlottery(ctx, setting: str = None, *, value: str = None):
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} Lottery editing only works in servers.")
        return
    if not await ensure_db_ready(ctx):
        return
    if not is_superowner_id(ctx.author.id):
        await send_owner_only(ctx)
        return

    config = await asyncio.to_thread(get_lottery_config, ctx.guild.id)
    if config is None:
        await ctx.send("Lottery is not set up yet. Run `.lottery` first.")
        return

    if not setting or not value:
        await send_lottery_edit_ui(ctx, setting)
        return

    async def send(content=None, **kwargs):
        return await ctx.send(content, **kwargs)

    await apply_lottery_edit(ctx.guild, ctx.author, setting, value, send, ctx.message.channel_mentions)

@commands.command()
async def stoplottery(ctx):
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} Lottery stopping only works in servers.")
        return
    if not await ensure_db_ready(ctx):
        return
    if not is_superowner_id(ctx.author.id):
        await send_owner_only(ctx)
        return

    config = await asyncio.to_thread(get_lottery_config, ctx.guild.id)
    if config is None:
        await ctx.send("Lottery is not set up in this server.")
        return

    rows = await asyncio.to_thread(lottery_ticket_rows, ctx.guild.id)
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
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
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

    config = await asyncio.to_thread(get_lottery_config, ctx.guild.id)
    if config is None:
        await ctx.send("Lottery is not set up yet.")
        return

    rows = sorted(await asyncio.to_thread(lottery_ticket_rows, ctx.guild.id), key=lambda row: row["tickets"], reverse=True)
    panel_url = lottery_panel_url(ctx.guild, config)
    panel_value = f"[Open Panel]({panel_url})" if panel_url else "panel unavailable"
    view = LotteryStatsView(ctx, config, rows, panel_value)
    view.message = await ctx.send(embed=view.embed(), view=view, allowed_mentions=discord.AllowedMentions.none())
    schedule_lottery_refresh(ctx.guild, config)

@commands.command()
async def buytick(ctx, amount: str = None):
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} Lottery tickets only work in servers.")
        return
    if not await ensure_db_ready(ctx):
        return

    config = await asyncio.to_thread(get_lottery_config, ctx.guild.id)
    if config is None:
        await ctx.send("Lottery is not set up yet.")
        return

    if amount is None:
        await send_economy_command_input_ui(ctx, "buytick", "Enter how many lottery tickets you want to buy.")
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
        schedule_lottery_refresh(ctx.guild, result["config"])
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await reply_to_command(
        ctx,
        lottery_purchase_message(result),
        allowed_mentions=discord.AllowedMentions.none()
    )

async def process_lottery_draw(config):
    guild = bot.get_guild(config["guild_id"]) if bot else None
    if guild is None:
        return

    next_draw = datetime.now(timezone.utc) + timedelta(seconds=int(config["period_seconds"]))
    rows = await asyncio.to_thread(lottery_ticket_rows, guild.id)
    channel = guild.get_channel(config["channel_id"])
    if channel is None:
        try:
            channel = await bot.fetch_channel(config["channel_id"])
        except Exception:
            channel = None

    unique_players = len(rows)
    if unique_players < 5:
        ticket_cost = lottery_ticket_cost(config)
        def cancel_lottery_sync():
            refunds = refund_lottery_round(guild.id, rows, ticket_cost)
            reset_lottery_round(guild.id, next_draw)
            return refunds
        refunds = await asyncio.to_thread(cancel_lottery_sync)
        refunded_total = sum(amount for _, amount in refunds)
        role = await recreate_lottery_role(guild, config.get("role_id"))
        await asyncio.to_thread(set_lottery_role, guild.id, role.id if role else None)
        updated_config = await asyncio.to_thread(get_lottery_config, guild.id)
        await refresh_lottery_message(guild, updated_config)
        if channel:
            refund_lines = [
                f"- {user_mention(user_id)} refunded **{format_balance(amount)}**"
                for user_id, amount in refunds
            ]
            header = (
                f"{QOIN_CHEST} Lottery draw cancelled: **{unique_players}/5** players joined.\n"
                f"{Q_TICKET} Refunded **{format_balance(refunded_total)}** across **{len(refunds)}** users."
            )
            footer = "No winner this round. The lottery has restarted with a fresh participant role."
            if len(refund_lines) > 10:
                view = LinesPageView(header, refund_lines, footer=footer, per_page=10)
                await channel.send(
                    view.content(),
                    view=view,
                    allowed_mentions=discord.AllowedMentions.none()
                )
            else:
                extra = "\n".join(refund_lines) if refund_lines else "No paid tickets needed a refund."
                await channel.send(
                    f"{header}\n{extra}\n{footer}",
                    allowed_mentions=discord.AllowedMentions.none()
                )
        return

    total_tickets = sum(max(0, int(row["tickets"] or 0)) for row in rows)
    if total_tickets <= 0:
        await asyncio.to_thread(reset_lottery_round, guild.id, next_draw)
        await refresh_lottery_message(guild, await asyncio.to_thread(get_lottery_config, guild.id))
        return
    pick = random.randint(1, total_tickets)
    running = 0
    winner_id = int(rows[-1]["user_id"])
    for row in rows:
        running += max(0, int(row["tickets"] or 0))
        if running >= pick:
            winner_id = int(row["user_id"])
            break
    pot = int(config["pot"])
    try:
        def pay_lottery_winner_sync():
            data = get_user(winner_id)
            update_user(winner_id, balance=data["balance"] + pot, total_earned=data["total_earned"] + pot)
            log_transaction(winner_id, "lottery_win", pot, f"Guild {guild.id}")
            reset_lottery_round(guild.id, next_draw)

        await asyncio.to_thread(pay_lottery_winner_sync)
        role = await recreate_lottery_role(guild, config.get("role_id"))
        await asyncio.to_thread(set_lottery_role, guild.id, role.id if role else None)
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

LEADERBOARD_RANK_TYPES = {
    "quesos": {
        "label": "Quesos",
        "description": "Rank by current balance.",
        "order": "balance DESC, user_id ASC",
        "title": "Quesos Leaderboard",
        "formatter": lambda row: format_balance(row["balance"]),
    },
    "level": {
        "label": "Level",
        "description": "Rank by level, then XP.",
        "order": "level DESC, xp DESC, user_id ASC",
        "title": "Level Leaderboard",
        "formatter": lambda row: f"Level **{int(row['level'] or 1):,}** - XP **{int(row['xp'] or 0):,}**",
    },
    "earned": {
        "label": "Earnings",
        "description": "Rank by total earned.",
        "order": "total_earned DESC, user_id ASC",
        "title": "Earnings Leaderboard",
        "formatter": lambda row: format_balance(row["total_earned"]),
    },
    "won": {
        "label": "Total Won",
        "description": "Rank by gambling winnings.",
        "order": "total_won DESC, user_id ASC",
        "title": "Wins Leaderboard",
        "formatter": lambda row: format_balance(row["total_won"]),
    },
    "lost": {
        "label": "Total Lost",
        "description": "Rank by gambling losses.",
        "order": "total_lost DESC, user_id ASC",
        "title": "Losses Leaderboard",
        "formatter": lambda row: format_balance(row["total_lost"]),
    },
    "net": {
        "label": "Net Gambling",
        "description": "Rank by won minus lost.",
        "order": "(total_won - total_lost) DESC, user_id ASC",
        "title": "Net Gambling Leaderboard",
        "formatter": lambda row: format_balance((row["total_won"] or 0) - (row["total_lost"] or 0)),
    },
    "messages": {
        "label": "Messages",
        "description": "Rank by 𝚀𝚞𝚎wo-tracked messages.",
        "order": "messages_sent DESC, user_id ASC",
        "title": "Messages Leaderboard",
        "formatter": lambda row: f"**{int(row['messages_sent'] or 0):,}** messages",
    },
}

def get_leaderboard_user_ids(rank_type="quesos", limit=5, local_ids=None):
    rank_config = LEADERBOARD_RANK_TYPES.get(str(rank_type or "quesos").casefold(), LEADERBOARD_RANK_TYPES["quesos"])
    order_clause = rank_config["order"]
    where = ""
    params = []
    if local_ids is not None:
        where = "WHERE user_id = ANY(%s)"
        params.append([int(user_id) for user_id in local_ids])
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        f"SELECT user_id FROM economy {where} ORDER BY {order_clause} LIMIT %s",
        params + [int(limit)]
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [int(row["user_id"]) for row in rows]

class BalanceRankView(discord.ui.View):
    def __init__(self, ctx, order, title, icon):
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
        self.ctx = ctx
        self.order = order
        self.title = title
        self.icon = icon
        self.scope = "local"
        self.rank_type = "quesos"
        self.page = 0
        self.per_page = 10
        self.total = 0
        self.author_rank = None
        self.author_data = None
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
        self.local_scope.disabled = self.scope == "local"
        self.global_scope.disabled = self.scope == "global"
        self.local_scope.style = discord.ButtonStyle.primary if self.scope == "local" else discord.ButtonStyle.secondary
        self.global_scope.style = discord.ButtonStyle.primary if self.scope == "global" else discord.ButtonStyle.secondary
        for item in self.children:
            if isinstance(item, discord.ui.Select) and getattr(item, "custom_id", None) == "leaderboard_rank_type":
                for option in item.options:
                    option.default = option.value == self.rank_type

    async def local_user_ids(self):
        if not self.ctx.guild:
            return [self.ctx.author.id]
        ids = {member.id for member in self.ctx.guild.members if not member.bot}
        ids.add(self.ctx.author.id)
        return list(ids)

    def fetch_page(self, local_ids=None):
        conn = get_db_connection()
        cur = conn.cursor()
        where = ""
        params = []
        if local_ids is not None:
            where = "WHERE user_id = ANY(%s)"
            params.append(local_ids)

        self.author_data = get_user(self.ctx.author.id)
        rank_config = LEADERBOARD_RANK_TYPES[self.rank_type]
        order_clause = rank_config["order"]

        cur.execute(f"SELECT COUNT(*) AS count FROM economy {where}", params)
        self.total = cur.fetchone()["count"]

        if local_ids is not None and self.ctx.author.id not in local_ids:
            self.author_rank = None
        else:
            rank_where = "WHERE user_id = %s"
            rank_params = [self.ctx.author.id]
            cte_params = list(params)
            cur.execute(
                f"""
                WITH ranked AS (
                    SELECT
                        user_id,
                        ROW_NUMBER() OVER (ORDER BY {order_clause}) AS rank
                    FROM economy
                    {where}
                )
                SELECT rank FROM ranked {rank_where}
                """,
                cte_params + rank_params
            )
            rank_row = cur.fetchone()
            self.author_rank = int(rank_row["rank"]) if rank_row else None

        cur.execute(
            f"SELECT * FROM economy {where} ORDER BY {order_clause} LIMIT %s OFFSET %s",
            params + [self.per_page, self.page * self.per_page]
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    def build_embed(self, rows):
        max_page = max(1, ((self.total - 1) // self.per_page) + 1)
        start_rank = self.page * self.per_page + 1
        end_rank = min(self.total, self.page * self.per_page + len(rows))
        scope_label = "Local" if self.scope == "local" else "Global"
        rank_config = LEADERBOARD_RANK_TYPES[self.rank_type]
        embed = discord.Embed(title=f"{self.icon} {scope_label} {rank_config['title']}", color=discord.Color.gold())
        if not rows:
            embed.description = "No balances found yet."
        else:
            lines = [f"Showing **#{start_rank}-{end_rank}** of **{self.total}**."]
            if self.author_rank is not None and self.author_data:
                lines.append(f"Your rank: **#{self.author_rank}** - {rank_config['formatter'](self.author_data)}")
            lines.append("")
            for i, row in enumerate(rows, start_rank):
                lines.append(f"**{i}.** {user_mention(row['user_id'])} - {rank_config['formatter'](row)}")
            embed.description = "\n".join(lines)
        embed.set_footer(text=f"Page {self.page + 1}/{max_page}")
        return embed

    async def render(self):
        try:
            local_ids = await self.local_user_ids() if self.scope == "local" else None
            rows = await asyncio.to_thread(self.fetch_page, local_ids)
            self.update_buttons()
            return self.build_embed(rows)
        except Exception as e:
            print(f"Leaderboard render error: {type(e).__name__} - {e}")
            return None

    async def refresh_interaction(self, interaction):
        await interaction.response.defer()
        embed = await self.render()
        if embed is None:
            return await interaction.followup.send("Database unavailable. Try again shortly.", ephemeral=True)
        await interaction.edit_original_response(embed=embed, view=self)

    @discord.ui.button(label="Local", style=discord.ButtonStyle.primary, row=0)
    async def local_scope(self, interaction, button):
        self.scope = "local"
        self.page = 0
        await self.refresh_interaction(interaction)

    @discord.ui.button(label="Global", style=discord.ButtonStyle.secondary, row=0)
    async def global_scope(self, interaction, button):
        self.scope = "global"
        self.page = 0
        await self.refresh_interaction(interaction)

    @discord.ui.select(
        custom_id="leaderboard_rank_type",
        placeholder="Ranking type",
        min_values=1,
        max_values=1,
        row=2,
        options=[
            discord.SelectOption(label=config["label"], value=key, description=config["description"])
            for key, config in LEADERBOARD_RANK_TYPES.items()
        ],
    )
    async def rank_type_select(self, interaction, select):
        self.rank_type = select.values[0]
        self.page = 0
        await self.refresh_interaction(interaction)

    @discord.ui.button(label="First", style=discord.ButtonStyle.secondary, row=1)
    async def first_page(self, interaction, button):
        self.page = 0
        await self.refresh_interaction(interaction)

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary, row=1)
    async def prev_page(self, interaction, button):
        self.page = max(0, self.page - 1)
        await self.refresh_interaction(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, row=1)
    async def next_page(self, interaction, button):
        self.page += 1
        await self.refresh_interaction(interaction)

    @discord.ui.button(label="Last", style=discord.ButtonStyle.secondary, row=1)
    async def last_page(self, interaction, button):
        self.page = max(0, (self.total - 1) // self.per_page)
        await self.refresh_interaction(interaction)

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
        data = await asyncio.to_thread(get_user, user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    now = datetime.now(timezone.utc)

    if data['last_daily']:
        last_daily = data['last_daily'].replace(tzinfo=timezone.utc) if data['last_daily'].tzinfo is None else data['last_daily']
        elapsed = (now - last_daily).total_seconds()

        if elapsed < 86400:
            next_claim = last_daily + timedelta(seconds=86400)
            await reply_to_command(ctx, f"{Q_TIMER} You can claim daily {discord_relative_time(next_claim)}")
            return

    streak, freeze_note = next_claim_streak(user_id, data, "daily_streak", "last_daily", 86400)
    base_reward = random.randint(10_000, 15_000)
    streak_bonus = min(max(streak - 1, 0) * 10, 200)
    reward = base_reward + streak_bonus
    reward = int(reward * claim_reward_multiplier(data))

    try:
        def claim_daily_sync():
            update_user(
                user_id,
                balance=data['balance'] + reward,
                daily_streak=streak,
                last_daily=now,
                total_earned=data['total_earned'] + reward
            )
            log_transaction(user_id, "daily", reward, f"Streak {streak}")
            updated_user = get_user(user_id)
            achievement = maybe_award_main_quest(user_id, updated_user, "daily_30")
            return updated_user, achievement
        updated, achievement_reward = await asyncio.to_thread(claim_daily_sync)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    extra = f"\n{Q_QUEST} Main quest complete: **{format_balance(achievement_reward)}**!" if achievement_reward else ""
    await reply_to_command(ctx, f"{QOIN_BAG} You claimed **{format_balance(reward)}**!\nStreak: **{plural_unit(streak, 'day')}** (+{streak_bonus} bonus){freeze_note}{extra}")
    await maybe_send_tutorial(ctx, updated, "start")

@commands.command()
async def weekly(ctx):
    if not await ensure_db_ready(ctx):
        return

    user_id = ctx.author.id
    try:
        data = await asyncio.to_thread(get_user, user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    now = datetime.now(timezone.utc)

    if data['last_weekly']:
        last_weekly = data['last_weekly'].replace(tzinfo=timezone.utc) if data['last_weekly'].tzinfo is None else data['last_weekly']
        elapsed = (now - last_weekly).total_seconds()

        if elapsed < 604800:
            next_claim = last_weekly + timedelta(seconds=604800)
            await reply_to_command(ctx, f"{Q_TIMER} You can claim weekly {discord_relative_time(next_claim)}")
            return

    streak, freeze_note = next_claim_streak(user_id, data, "weekly_streak", "last_weekly", 604800)
    base_reward = random.randint(20_000, 30_000)
    streak_bonus = min(max(streak - 1, 0) * 50, 500)
    reward = base_reward + streak_bonus
    reward = int(reward * claim_reward_multiplier(data))

    try:
        def claim_weekly_sync():
            update_user(
                user_id,
                balance=data['balance'] + reward,
                weekly_streak=streak,
                last_weekly=now,
                total_earned=data['total_earned'] + reward
            )
            log_transaction(user_id, "weekly", reward, f"Streak {streak}")
            updated_user = get_user(user_id)
            achievement = maybe_award_main_quest(user_id, updated_user, "weekly_8")
            return updated_user, achievement
        updated, achievement_reward = await asyncio.to_thread(claim_weekly_sync)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    extra = f"\n{Q_QUEST} Main quest complete: **{format_balance(achievement_reward)}**!" if achievement_reward else ""
    await reply_to_command(ctx, f"{QOIN_BAG} You claimed **{format_balance(reward)}**!\nWeekly streak: **{plural_unit(streak, 'week')}** (+{streak_bonus} bonus){freeze_note}{extra}")

@commands.command()
async def monthly(ctx):
    if not await ensure_db_ready(ctx):
        return

    user_id = ctx.author.id
    try:
        data = await asyncio.to_thread(get_user, user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    now = datetime.now(timezone.utc)

    if data['last_monthly']:
        last_monthly = data['last_monthly'].replace(tzinfo=timezone.utc) if data['last_monthly'].tzinfo is None else data['last_monthly']
        elapsed = (now - last_monthly).total_seconds()

        if elapsed < 2592000:
            next_claim = last_monthly + timedelta(seconds=2592000)
            await reply_to_command(ctx, f"{Q_TIMER} You can claim monthly {discord_relative_time(next_claim)}")
            return

    streak, freeze_note = next_claim_streak(user_id, data, "monthly_streak", "last_monthly", 2592000)
    base_reward = random.randint(40_000, 60_000)
    streak_bonus = min(max(streak - 1, 0) * 500, 5000)
    reward = base_reward + streak_bonus
    reward = int(reward * claim_reward_multiplier(data))

    try:
        def claim_monthly_sync():
            update_user(
                user_id,
                balance=data['balance'] + reward,
                monthly_streak=streak,
                last_monthly=now,
                total_earned=data['total_earned'] + reward
            )
            log_transaction(user_id, "monthly", reward, f"Streak {streak}")
            updated_user = get_user(user_id)
            achievement = maybe_award_main_quest(user_id, updated_user, "monthly_5")
            return updated_user, achievement
        updated, achievement_reward = await asyncio.to_thread(claim_monthly_sync)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    extra = f"\n{Q_QUEST} Main quest complete: **{format_balance(achievement_reward)}**!" if achievement_reward else ""
    await reply_to_command(ctx, f"{QOIN_BAG} You claimed **{format_balance(reward)}**!\nMonthly streak: **{plural_unit(streak, 'month')}** (+{streak_bonus} bonus){freeze_note}{extra}")

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
        await send_gambling_cooldown(ctx, cd)
        return

    user_id = ctx.author.id

    try:
        data = await asyncio.to_thread(get_user, user_id)
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

    if not await check_daily_loss_limit(ctx, data, amount):
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
                if interaction.user.id != ctx.author.id:
                    await interaction.response.send_message("Use your own coin flip prompt.", ephemeral=True)
                    return False
                return True

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

    if random.random() < chance_with_luck(COINFLIP_WIN_CHANCE, data):
        coin_result = chosen_side
    else:
        coin_result = "TAILS" if chosen_side == "HEADS" else "HEADS"
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
        data = await asyncio.to_thread(get_user, user_id)
        if win:
            new_streak = next_gambling_streak(data)
            mult = payout_multiplier(data, new_streak)
            winnings = int(amount * mult * 2)
            new_balance = data['balance'] + winnings - amount
            await asyncio.to_thread(update_user,
                user_id,
                balance=new_balance,
                gamble_streak=new_streak,
                total_won=data['total_won'] + winnings - amount
            )
            result_block = gamble_result_block(
                "cf",
                amount,
                {"winnings": winnings, "balance": new_balance, "streak": new_streak, "streak_mult": mult},
                2,
                outcome=f"{coin_result} - You Win",
            )
            await flip_msg.edit(
                content=(
                f"{Q_FLIP} **COIN FLIP**\n"
                f"─────────────────\n"
                f"Pick: **{chosen_side}**\n"
                f">>> {Q_SUCCESS} **{coin_result} - YOU WIN!**\n"
                f"{result_block}"
                ),
                view=double_or_nothing_view(user_id, "cf", {"winnings": winnings})
            )
        else:
            new_balance = max(0, data['balance'] - amount)
            await asyncio.to_thread(update_user,
                user_id,
                balance=new_balance,
                gamble_streak=0,
                total_lost=data['total_lost'] + amount
            )
            result_block = gamble_result_block(
                "cf",
                amount,
                {"winnings": 0, "balance": new_balance},
                outcome=f"{coin_result} - You Lose",
            )
            await flip_msg.edit(
                content=(
                f"{Q_FLIP} **COIN FLIP**\n"
                f"─────────────────\n"
                f"Pick: **{chosen_side}**\n"
                f">>> {Q_DENIED} **{coin_result} - YOU LOSE**\n"
                f"{result_block}"
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
        await send_gambling_cooldown(ctx, cd)
        return

    user_id = ctx.author.id
    try:
        data = await asyncio.to_thread(get_user, user_id)
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
                if interaction.user.id != ctx.author.id:
                    await interaction.response.send_message("Use your own roulette prompt.", ephemeral=True)
                    return False
                return True

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

    if not await check_daily_loss_limit(ctx, data, amount):
        return

    colors = ["red", "black", "green"]
    win_chance = chance_with_luck(ROULETTE_WIN_CHANCE, data, cap=0.45)
    result = color if random.random() < win_chance else random.choice([entry for entry in colors if entry != color])
    multipliers = {'red': 3, 'black': 3, 'green': 3}
    emoji_map = {'red': Q_ROULETTE_RED, 'black': Q_ROULETTE_BLACK, 'green': Q_ROULETTE_GREEN}
    roulette_msg = await ctx.send(
        f"{Q_WHEEL_SPIN} **ROULETTE**\n"
        f"─────────────────\n"
        f"{Q_TARGET} Pick: **{emoji_map[color]} {color.upper()}**\n"
        f"[ spinning... ]"
    )
    side_colors = [entry for entry in colors if entry != result]
    spin_frames = [
        f"{Q_ROULETTE_RED} {Q_ROULETTE_BLACK} {Q_ROULETTE_GREEN}",
        f"{Q_ROULETTE_BLACK} {Q_ROULETTE_GREEN} {Q_ROULETTE_RED}",
        f"{Q_ROULETTE_GREEN} {Q_ROULETTE_RED} {Q_ROULETTE_BLACK}",
        f"{emoji_map[side_colors[0]]} {emoji_map[result]} {emoji_map[side_colors[1]]}",
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
        data = await asyncio.to_thread(get_user, user_id)
        if result == color:
            new_streak = next_gambling_streak(data)
            mult = payout_multiplier(data, new_streak)
            winnings = int(amount * mult * multipliers[color])
            new_balance = data['balance'] + winnings - amount
            await asyncio.to_thread(update_user,
                user_id,
                balance=new_balance,
                gamble_streak=new_streak,
                total_won=data['total_won'] + winnings - amount
            )
            result_block = gamble_result_block(
                "roulette",
                amount,
                {"winnings": winnings, "balance": new_balance, "streak": new_streak, "streak_mult": mult},
                multipliers[color],
                outcome=f"{color.upper()}",
            )
            await roulette_msg.edit(
                content=(
                f"{Q_WHEEL} **ROULETTE**\n"
                f"─────────────────\n"
                f"{Q_TARGET} You picked: **{emoji_map[color]} {color.upper()}**\n"
                f"─────────────────\n"
                f">>> {Q_SUCCESS} **{color.upper()}!**\n"
                f"{result_block}"
                ),
                view=double_or_nothing_view(user_id, "roulette", {"winnings": winnings})
            )
        else:
            new_balance = max(0, data['balance'] - amount)
            await asyncio.to_thread(update_user,
                user_id,
                balance=new_balance,
                gamble_streak=0,
                total_lost=data['total_lost'] + amount
            )
            result_block = gamble_result_block(
                "roulette",
                amount,
                {"winnings": 0, "balance": new_balance},
                outcome=f"{result.upper()}",
            )
            await roulette_msg.edit(
                content=(
                f"{Q_WHEEL} **ROULETTE**\n"
                f"─────────────────\n"
                f"{Q_TARGET} You picked: **{emoji_map[color]} {color.upper()}**\n"
                f"─────────────────\n"
                f">>> {emoji_map[result]} **{result.upper()}!**\n"
                f"{result_block}"
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
        await send_gambling_cooldown(ctx, cd)
        return

    user_id = ctx.author.id

    try:
        data = await asyncio.to_thread(get_user, user_id)
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

    if not await check_daily_loss_limit(ctx, data, amount):
        return

    slot_symbols = SLOT_SYMBOL_PAYOUTS
    symbol_weights = [40, 30, 20, 10]
    if random.random() < chance_with_luck(SLOTS_WIN_CHANCE, data):
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

    try:
        data = await asyncio.to_thread(get_user, user_id)
        if result_multiplier > 0:
            new_streak = next_gambling_streak(data)
            mult = payout_multiplier(data, new_streak)
            winnings = int(amount * mult * result_multiplier)
            new_balance = data['balance'] + winnings - amount
            await asyncio.to_thread(update_user,
                user_id,
                balance=new_balance,
                gamble_streak=new_streak,
                total_won=data['total_won'] + winnings - amount
            )
            result_block = gamble_result_block(
                "slots",
                amount,
                {"winnings": winnings, "balance": new_balance, "streak": new_streak, "streak_mult": mult},
                result_multiplier,
                outcome=f"Three Match x{result_multiplier}",
            )
            await slots_msg.edit(
                content=(
                    f"{Q_SLOTS} **RESULTS**\n"
                    f"─────────────────\n"
                    f"| {r1} | {r2} | {r3} |\n"
                    f"─────────────────\n"
                    f">>> {QOIN_CHEST} **THREE MATCH!** ×{result_multiplier}\n"
                    f"{result_block}"
                ),
                view=double_or_nothing_view(user_id, "slots", {"winnings": winnings})
            )
        else:
            new_balance = max(0, data['balance'] - amount)
            await asyncio.to_thread(update_user,
                user_id,
                balance=new_balance,
                gamble_streak=0,
                total_lost=data['total_lost'] + amount
            )
            result_block = gamble_result_block(
                "slots",
                amount,
                {"winnings": 0, "balance": new_balance},
                outcome="No Match",
            )
            await slots_msg.edit(
                content=(
                    f"{Q_SLOTS} **RESULTS**\n"
                    f"─────────────────\n"
                    f"| {r1} | {r2} | {r3} |\n"
                    f"─────────────────\n"
                    f">>> {Q_DENIED} **NO MATCH**\n"
                    f"{result_block}"
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

def blackjack_dealer_should_hit(dealer_total):
    if dealer_total < 16:
        return True
    if dealer_total > 16:
        return False
    return random.random() >= BLACKJACK_DEALER_STAND_ON_16_CHANCE

@commands.command(aliases=["bj"])
async def blackjack(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "blackjack")
    if cd > 0:
        await send_gambling_cooldown(ctx, cd)
        return

    user_id = ctx.author.id

    try:
        data = await asyncio.to_thread(get_user, user_id)
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

    if not await check_daily_loss_limit(ctx, data, amount):
        return

    deck = shuffle_deck()

    player_hand = [deck.pop(), deck.pop()]
    dealer_hand = [deck.pop(), deck.pop()]

    player_val = hand_value(player_hand)
    dealer_val = hand_value(dealer_hand)

    async def final_outcome(player_final, dealer_final, win_type, amount_delta, new_streak):
        if amount_delta < 0 and random.random() < active_luck_bonus(data):
            win_type = "Fortune Reversal"
            amount_delta = amount
            new_streak = next_gambling_streak(data)
        try:
            if amount_delta > 0:
                if new_streak is None:
                    new_streak = next_gambling_streak(data)
                mult = payout_multiplier(data, new_streak)
                winnings = int(amount_delta * mult)
                prize = amount + winnings
                await asyncio.to_thread(update_user,
                    user_id,
                    balance=data['balance'] + winnings,
                    gamble_streak=new_streak,
                    total_won=data['total_won'] + winnings
                )
                result_block = gamble_result_block(
                    "blackjack",
                    amount,
                    {"winnings": prize, "balance": data["balance"] + winnings, "streak": new_streak, "streak_mult": mult},
                    None,
                    outcome=win_type,
                )
                await msg.edit(
                    content=(
                        f"{Q_CARDS} **BLACKJACK**\n"
                        f"─────────────────\n"
                        f"**Your hand:** {format_hand(player_hand)}  →  **{player_final}**\n"
                        f"**Dealer:**     {format_hand(dealer_hand)}  →  **{dealer_final}**\n"
                        f"─────────────────\n"
                        f">>> {Q_SUCCESS} **{win_type}!**\n"
                        f"{result_block}"
                    ),
                    view=double_or_nothing_view(user_id, "blackjack", {"winnings": prize})
                )
            elif amount_delta == 0:
                await msg.edit(
                    content=(
                        f"{Q_CARDS} **BLACKJACK**\n"
                        f"─────────────────\n"
                        f"**Your hand:** {format_hand(player_hand)}  →  **{player_final}**\n"
                        f"**Dealer:**     {format_hand(dealer_hand)}  →  **{dealer_final}**\n"
                        f"─────────────────\n"
                        f">>> {Q_TIMER} **{win_type}**\n"
                        f"Nothing lost, nothing won.\n"
                        f"New Balance: **{format_balance(data['balance'])}**"
                    )
                )
            else:
                new_balance = max(0, data['balance'] + amount_delta)
                await asyncio.to_thread(update_user,
                    user_id,
                    balance=new_balance,
                    gamble_streak=0,
                    total_lost=data['total_lost'] + abs(amount_delta)
                )
                result_block = gamble_result_block(
                    "blackjack",
                    abs(amount_delta),
                    {"winnings": 0, "balance": new_balance},
                    outcome=win_type,
                )
                await msg.edit(
                    content=(
                        f"{Q_CARDS} **BLACKJACK**\n"
                        f"─────────────────\n"
                        f"**Your hand:** {format_hand(player_hand)}  →  **{player_final}**\n"
                        f"**Dealer:**     {format_hand(dealer_hand)}  →  **{dealer_final}**\n"
                        f"─────────────────\n"
                        f">>> {Q_DENIED} **{win_type}**\n"
                        f"{result_block}"
                    )
                )
        except Exception:
            await send_error(ctx, "Database unavailable. Try again shortly.")

    class BJView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=30)
            self.done = False

        async def interaction_check(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own blackjack game.", ephemeral=True)
                return False
            return True

        async def on_timeout(self):
            if not self.done:
                self.done = True
                player_val_now = hand_value(player_hand)
                dealer_val_now = hand_value(dealer_hand)
                if player_val_now <= 21:
                    while blackjack_dealer_should_hit(dealer_val_now):
                        dealer_hand.append(deck.pop())
                        dealer_val_now = hand_value(dealer_hand)
                    if dealer_val_now > 21:
                        await final_outcome(player_val_now, "BUST!", "Dealer Busted!", amount, None)
                    elif player_val_now > dealer_val_now:
                        await final_outcome(player_val_now, dealer_val_now, "You Win!", amount, None)
                    elif player_val_now < dealer_val_now:
                        await final_outcome(player_val_now, dealer_val_now, "Dealer Wins", -amount, 0)
                    else:
                        await final_outcome(player_val_now, dealer_val_now, "Push", 0, None)

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

            while blackjack_dealer_should_hit(dealer_val):
                dealer_hand.append(deck.pop())
                dealer_val = hand_value(dealer_hand)

            if dealer_val > 21:
                await final_outcome(player_val, "BUST!", "Dealer Busted!", amount, None)
            elif dealer_val > player_val:
                await final_outcome(player_val, dealer_val, "Dealer Wins", -amount, 0)
            elif dealer_val < player_val:
                await final_outcome(player_val, dealer_val, "You Win!", amount, None)
            else:
                await final_outcome(player_val, dealer_val, "Push", 0, None)

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
async def give(ctx, *, args: str = None):
    if not await ensure_db_ready(ctx):
        return

    target, amount = parse_target_amount_args(args, allow_all=True)
    if not target:
        await send_economy_command_input_ui(ctx, "give", "Enter the user and amount. Either order works.")
        return
    multi_targets = None
    if len(getattr(ctx.message, "mentions", []) or []) > 1:
        try:
            resolved = await resolve_admin_targets(ctx, target)
        except commands.BadArgument:
            resolved = None
        if resolved and resolved["kind"] == "members":
            multi_targets = resolved

    try:
        member = None if multi_targets else await commands.MemberConverter().convert(ctx, target)
    except commands.BadArgument:
        await ctx.send(f"{Q_DENIED} Mention a user or paste their ID. Example: `.give @user 10k`.")
        return

    user_id = ctx.author.id

    try:
        data = await asyncio.to_thread(get_user, user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    if str(amount).lower() == "all" and multi_targets:
        await ctx.send(f"{Q_DENIED} Use a number when sending to multiple users, like `.give @user1 @user2 10k`.")
        return

    if str(amount).lower() == "all":
        raw_amount = amount
        amount = max(0, int(data['balance']))
    else:
        raw_amount = amount
        parsed = parse_whole_number(amount)
        if parsed is None:
            await ctx.send(f"{Q_DENIED} Use `.give @user all`, `.give all @user`, or a number like `10k`.")
            return
        amount = parsed

    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return

    if multi_targets:
        recipient_ids = [target_id for target_id in multi_targets["user_ids"] if target_id != ctx.author.id]
        if not recipient_ids:
            await ctx.send(f"{Q_DENIED} Can't transfer to yourself.")
            return
        total_amount = amount * len(recipient_ids)
        if total_amount > data['balance'] and not has_economy_owner_power(ctx.author.id, ctx.guild):
            await ctx.send(f"{Q_DENIED} You need {format_balance(total_amount)} to send {format_balance(amount)} to **{len(recipient_ids)}** users.")
            return
        if not await check_daily_loss_limit(ctx, data, total_amount):
            return

        old_sender_balance = data["balance"]
        new_sender_balance = old_sender_balance
        total_tax = 0
        total_received = 0
        try:
            def run_multi_give():
                before = get_balances_for_users(recipient_ids)
                local_new_sender_balance = old_sender_balance
                local_total_tax = 0
                local_total_received = 0
                tax_rate = event_transfer_tax_rate(ctx.guild.id if ctx.guild else None)
                for recipient_id in recipient_ids:
                    tax = int(amount * tax_rate)
                    transfer = transfer_user_balance(
                        user_id,
                        recipient_id,
                        amount,
                        tax=tax,
                        allow_overdraft=has_economy_owner_power(ctx.author.id, ctx.guild),
                    )
                    local_new_sender_balance = transfer["new_sender_balance"]
                    receiver_credited_amount = transfer["receiver_credited_amount"]
                    local_total_tax += tax
                    local_total_received += receiver_credited_amount
                    log_transaction(user_id, "give_sent", -amount, f"Sent to {recipient_id}; tax {tax}")
                    log_transaction(recipient_id, "give_received", transfer["received_amount"], f"Received from {ctx.author.id}")
                    if tax:
                        log_transaction(user_id, "transfer_tax_paid", -tax, f"{int(TRANSFER_TAX_RATE * 100)}% transfer tax burned")
                        log_transaction(SUPER_OWNER_ID, "transfer_tax", tax, f"Transfer tax from {user_id} to {recipient_id}")
                after = get_balances_for_users(recipient_ids)
                receipt = create_receipt(
                    ctx.guild.id if ctx.guild else 0,
                    ctx.channel.id,
                    ctx.author.id,
                    recipient_ids,
                    "give_multi",
                    amount,
                    f"amount_each={amount}; total_tax={local_total_tax}; recipients={len(recipient_ids)}",
                )
                return before, after, local_new_sender_balance, local_total_tax, local_total_received, receipt, tax_rate

            before_recipient_balances, after_recipient_balances, new_sender_balance, total_tax, total_received, receipt_id, tax_rate = await asyncio.to_thread(run_multi_give)
        except ValueError:
            latest = await asyncio.to_thread(get_user, user_id)
            await ctx.send(f"{Q_DENIED} You only have {format_balance(latest['balance'])}")
            return
        except Exception:
            await send_error(ctx, "Database unavailable. Try again shortly.")
            return

        await send_bulk_before_after_result(
            ctx,
            None,
            f"{QOIN_TRANSFER} You sent **{format_balance(amount)}** each to **{len(recipient_ids)}** users.\n"
            f"Tax Burned: **{format_balance(total_tax)}**{' (tax-free event)' if tax_rate == 0 else ''}\n"
            f"Total Received: **{format_balance(total_received)}**\n"
            f"Your Balance: **{format_balance(old_sender_balance)}** → **{format_balance(new_sender_balance)}**",
            recipient_ids,
            before_recipient_balances,
            after_recipient_balances,
            format_balance,
            "Recipient Balance",
            receipt_id=receipt_id,
        )
        if has_economy_owner_power(ctx.author.id, ctx.guild):
            await send_economy_log(ctx, "𝚀𝚞𝚎wo Multi Transfer", [
                ("Recipients", multi_targets["log_label"], False),
                ("Amount Each", format_balance(amount), True),
                ("Recipients Count", f"{len(recipient_ids):,}", True),
                ("Tax", format_balance(total_tax), True),
                ("Sender Balance", f"{format_balance(old_sender_balance)} → {format_balance(new_sender_balance)}", False),
            ])
        return

    if member.id == ctx.author.id:
        await ctx.send(f"{Q_DENIED} Can't transfer to yourself.")
        return

    if amount > data['balance'] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return

    if not await check_daily_loss_limit(ctx, data, amount):
        return

    try:
        def run_single_give():
            tax_rate = event_transfer_tax_rate(ctx.guild.id if ctx.guild else None)
            tax = int(amount * tax_rate)
            transfer = transfer_user_balance(
                user_id,
                member.id,
                amount,
                tax=tax,
                allow_overdraft=has_economy_owner_power(ctx.author.id, ctx.guild),
            )
            log_transaction(user_id, "give_sent", -amount, f"Sent to {member.id}; tax {tax}")
            log_transaction(member.id, "give_received", transfer["received_amount"], f"Received from {ctx.author.id}")
            if tax:
                log_transaction(user_id, "transfer_tax_paid", -tax, f"{int(TRANSFER_TAX_RATE * 100)}% transfer tax burned")
                log_transaction(SUPER_OWNER_ID, "transfer_tax", tax, f"Transfer tax from {user_id} to {member.id}")
            receipt = create_receipt(
                ctx.guild.id if ctx.guild else 0,
                ctx.channel.id,
                ctx.author.id,
                [member.id],
                "give",
                amount,
                f"tax={tax}; received={transfer['receiver_credited_amount']}",
            )
            return tax_rate, tax, transfer, receipt

        tax_rate, tax, transfer, receipt_id = await asyncio.to_thread(run_single_give)
        old_sender_balance = transfer["old_sender_balance"]
        new_sender_balance = transfer["new_sender_balance"]
        old_receiver_balance = transfer["old_receiver_balance"]
        new_receiver_balance = transfer["new_receiver_balance"]
        receiver_credited_amount = transfer["receiver_credited_amount"]
    except ValueError:
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await reply_to_command(
        ctx,
        f"{QOIN_TRANSFER} You sent **{format_balance(amount)}** to **{user_mention(member.id)}**\n"
        f"Tax Burned: **{format_balance(tax)}**{' (tax-free event)' if tax_rate == 0 else ''}\n"
        f"Received: **{format_balance(receiver_credited_amount)}**\n"
        f"Your Balance: **{format_balance(old_sender_balance)}** → **{format_balance(new_sender_balance)}**\n"
        f"{user_mention(member.id)}'s Balance: **{format_balance(old_receiver_balance)}** → **{format_balance(new_receiver_balance)}**"
        f"{receipt_line(receipt_id)}",
        allowed_mentions=discord.AllowedMentions.none()
    )
    if has_economy_owner_power(ctx.author.id, ctx.guild):
        await send_economy_log(ctx, "𝚀𝚞𝚎wo Transfer", [
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

@commands.command(name="qstats", aliases=["economystats", "qstatus"])
async def qstats(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return
    if not is_superowner_id(ctx.author.id):
        await send_owner_only(ctx)
        return
    if member is not None:
        command = bot.get_command("economyaudit") if bot else None
        if command:
            await ctx.invoke(command, member=member)
        else:
            await ctx.send(f"{Q_DENIED} Use `.economyaudit @user`.")
        return
    try:
        stats = await asyncio.to_thread(get_economy_stats)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    tx = stats["transaction_totals"]
    net_gambling = stats["total_won"] - stats["total_lost"]
    embed = discord.Embed(
        title=f"{QOIN_CHEST} 𝚀𝚞𝚎wo Economy Stats",
        description="Global economy health across all servers.",
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="Users", value=f"{stats['users']:,}", inline=True)
    embed.add_field(name="Money Supply", value=format_balance(stats["total_balance"]), inline=True)
    embed.add_field(name="Total Earned", value=format_balance(stats["total_earned"]), inline=True)
    embed.add_field(name="Gambling Won", value=format_balance(stats["total_won"]), inline=True)
    embed.add_field(name="Gambling Lost", value=format_balance(stats["total_lost"]), inline=True)
    embed.add_field(name="Net Gambling", value=format_balance(net_gambling), inline=True)
    embed.add_field(name="Active Lotteries", value=f"{stats['active_lotteries']:,}", inline=True)
    embed.add_field(name="Lottery Pots", value=format_balance(stats["lottery_pots"]), inline=True)
    embed.add_field(name="Lottery Tickets", value=f"{stats['lottery_tickets']:,}", inline=True)
    burned = abs(tx.get("shop_purchase", 0)) + tx.get("transfer_tax", 0) + tx.get("lottery_house_cut", 0)
    embed.add_field(name="Tracked Taxes / Payments", value=format_balance(burned), inline=True)
    if stats["richest_user_id"]:
        embed.add_field(
            name="Richest",
            value=f"{user_mention(stats['richest_user_id'])}\n{format_balance(stats['richest_balance'])}",
            inline=True
        )
    embed.add_field(name="Messages Tracked", value=f"{stats['messages_sent']:,}", inline=True)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="balancedashboard", aliases=["ecodashboard", "moneydashboard", "sinkdashboard"])
async def balancedashboard(ctx, days: int = 14):
    if not await ensure_db_ready(ctx):
        return
    if not is_superowner_id(ctx.author.id):
        await send_owner_only(ctx)
        return
    days = max(1, min(int(days or 14), 30))
    try:
        stats = await asyncio.to_thread(get_economy_stats)
        rows = await asyncio.to_thread(get_game_audit_rows, days)
        games, transactions, losses, lottery_watch = await asyncio.to_thread(get_abuse_audit_rows)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    tx = stats["transaction_totals"]
    net_gambling = stats["total_won"] - stats["total_lost"]
    tracked_flow = abs(tx.get("shop_purchase", 0)) + tx.get("transfer_tax", 0) + tx.get("lottery_house_cut", 0)
    supply = max(1, int(stats["total_balance"] or 0))
    embed = discord.Embed(
        title=f"{Q_BALANCE} Economy Balance Dashboard",
        description=f"Global money flow, balance signals, and anti-abuse watch for the last **{days} day(s)**.",
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(
        name="Supply",
        value=(
            f"Users: **{stats['users']:,}**\n"
            f"Money: **{format_balance(stats['total_balance'])}**\n"
            f"Richest: **{format_balance(stats['richest_balance'])}**"
        ),
        inline=True,
    )
    embed.add_field(
        name="Flow",
        value=(
            f"Net gambling: **{format_balance(net_gambling)}**\n"
            f"Tracked taxes/payments: **{format_balance(tracked_flow)}**\n"
            f"Flow vs supply: **{tracked_flow / supply * 100:.1f}%**"
        ),
        inline=True,
    )
    embed.add_field(
        name="Lottery",
        value=(
            f"Active: **{stats['active_lotteries']:,}**\n"
            f"Pots: **{format_balance(stats['lottery_pots'])}**\n"
            f"Tickets: **{stats['lottery_tickets']:,}**"
        ),
        inline=True,
    )

    game_lines = []
    for row in rows[:8]:
        plays = int(row["plays"] or 0)
        wins = int(row["wins"] or 0)
        losses_count = int(row["losses"] or 0)
        win_rate = wins / max(1, wins + losses_count) * 100
        user_profit = int(row["profit"] or 0)
        verdict = "watch" if plays >= 15 and (win_rate >= 60 or win_rate <= 10 or abs(user_profit) >= 2_000_000) else "ok"
        game_lines.append(
            f"{risk_emoji(row['game_key'])} **{game_display_name(row['game_key'])}** - "
            f"{plays:,} plays, {win_rate:.1f}% win, users {format_balance(user_profit)} | {verdict}"
        )
    add_split_embed_field(embed, "Game Balance", game_lines or ["No recent game data."], inline=False)

    watch_lines = []
    for row in games[:4]:
        watch_lines.append(f"{Q_WARNING} {user_mention(row['user_id'])}: {int(row['plays']):,} plays, profit {format_balance(int(row['profit'] or 0))}")
    for row in transactions[:4]:
        watch_lines.append(f"{Q_AUDIT} {user_mention(row['user_id'])}: {int(row['tx_count']):,} tx, volume {format_balance(int(row['volume'] or 0))}")
    for row in lottery_watch[:4]:
        watch_lines.append(f"{Q_TICKET} {user_mention(row['user_id'])}: {int(row['tickets']):,} tickets, spent {format_balance(int(row['spent'] or 0))}")
    add_split_embed_field(embed, "Watchlist", watch_lines or [f"{Q_SUCCESS} No major watch signals."], inline=False)

    notes = []
    if tracked_flow / supply < 0.02:
        notes.append("Tracked sinks are light compared to money supply; watch inflation if balances climb fast.")
    if net_gambling > 0:
        notes.append("Users are ahead globally on gambling; review recent payout changes.")
    if stats["lottery_pots"] > supply * 0.20:
        notes.append("Lottery pots are large compared to supply; make sure current panels are healthy.")
    add_split_embed_field(embed, "Tuning Notes", notes or ["Current signals look balanced."], inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="economyhealth", aliases=["ecohealth", "moneyhealth", "supply"])
async def economyhealth(ctx):
    await qstats.callback(ctx)

# =====================
# ADD / REMOVE (OWNER)
# =====================
@commands.command()
async def add(ctx, *, args: str = None):
    if not await ensure_db_ready(ctx):
        return

    if not is_superowner_id(ctx.author.id):
        await send_owner_only(ctx)
        return

    bulk_confirmed, args = strip_bulk_confirm(args)
    target, amount = parse_target_amount_args(args)
    if not target:
        await send_economy_command_input_ui(ctx, "add", "Enter the target and amount. Users, roles, and @everyone work where allowed.")
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
            await ctx.send(f"{Q_DENIED} Bulk `.add @everyone` is only for {QUE_OWNER_DISPLAY}.", allowed_mentions=discord.AllowedMentions.none())
            return
        members = list(ctx.guild.members)
        user_ids = [m.id for m in members]
        if not bulk_confirmed and await require_bulk_confirmation(ctx, "add", len(user_ids)):
            return
        progress_msg = await send_bulk_progress(ctx, "add quesos", len(user_ids))
        try:
            def run_bulk_add():
                before = get_balances_for_users(user_ids)
                count = bulk_add_users(user_ids, amount, ctx.author.id, "@everyone")
                after = get_balances_for_users(user_ids)
                receipt = create_receipt(ctx.guild.id if ctx.guild else 0, ctx.channel.id, ctx.author.id, user_ids, "add_everyone", amount, f"count={count}")
                return before, count, after, receipt
            before_balances, count, after_balances, receipt_id = await asyncio.to_thread(run_bulk_add)
        except Exception:
            await send_or_edit_bulk_error(ctx, progress_msg, "Database unavailable. Try again shortly.")
            return
        await send_bulk_before_after_result(
            ctx,
            progress_msg,
            f"{Q_SUCCESS} Added **{format_balance(amount)}** to **{count}** server members.",
            user_ids,
            before_balances,
            after_balances,
            format_balance,
            "Balance",
            receipt_id=receipt_id,
        )
        await send_economy_log(ctx, "𝚀𝚞𝚎wo Bulk Add", [
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
            await ctx.send(f"{Q_DENIED} Bulk `.add @role` is only for {QUE_OWNER_DISPLAY}.", allowed_mentions=discord.AllowedMentions.none())
            return
        members = list(role.members)
        if not members:
            await ctx.send(f"{Q_DENIED} That role has no members.")
            return
        user_ids = [m.id for m in members]
        if not bulk_confirmed and await require_bulk_confirmation(ctx, "add", len(user_ids)):
            return
        progress_msg = await send_bulk_progress(ctx, "add quesos", len(user_ids))
        try:
            def run_bulk_add():
                before = get_balances_for_users(user_ids)
                count = bulk_add_users(user_ids, amount, ctx.author.id, f"role {role.id}")
                after = get_balances_for_users(user_ids)
                receipt = create_receipt(ctx.guild.id if ctx.guild else 0, ctx.channel.id, ctx.author.id, user_ids, "add_role", amount, f"role={role.id}; count={count}")
                return before, count, after, receipt
            before_balances, count, after_balances, receipt_id = await asyncio.to_thread(run_bulk_add)
        except Exception:
            await send_or_edit_bulk_error(ctx, progress_msg, "Database unavailable. Try again shortly.")
            return
        await send_bulk_before_after_result(
            ctx,
            progress_msg,
            f"{Q_SUCCESS} Added **{format_balance(amount)}** to **{count}** members with **{role.name}**.",
            user_ids,
            before_balances,
            after_balances,
            format_balance,
            "Balance",
            receipt_id=receipt_id,
        )
        await send_economy_log(ctx, "𝚀𝚞𝚎wo Bulk Add", [
            ("Target", f"{role.mention} ({role.id})", False),
            ("Recipients", f"{count:,}", True),
            ("Amount Each", format_balance(amount), True),
            ("Total Created", format_balance(amount * count), True),
        ], color=discord.Color.green())
        return

    if len(getattr(ctx.message, "mentions", []) or []) > 1:
        try:
            targets = await resolve_admin_targets(ctx, target_key)
        except commands.BadArgument:
            targets = None
        if targets and targets["kind"] == "members":
            blocked = [user_id for user_id in targets["user_ids"] if not can_economy_act_on(ctx.author.id, user_id, ctx.guild)]
            if blocked:
                await ctx.send(f"{Q_DENIED} You can't edit one or more of those users' 𝚀𝚞𝚎wo balances.")
                return
            if not bulk_confirmed and await require_bulk_confirmation(ctx, "add", len(targets["user_ids"])):
                return
            progress_msg = await send_bulk_progress(ctx, "add quesos", len(targets["user_ids"]))
            try:
                def run_bulk_add():
                    before = get_balances_for_users(targets["user_ids"])
                    count = bulk_add_users(targets["user_ids"], amount, ctx.author.id, "multiple users")
                    after = get_balances_for_users(targets["user_ids"])
                    receipt = create_receipt(ctx.guild.id if ctx.guild else 0, ctx.channel.id, ctx.author.id, targets["user_ids"], "add_multi", amount, f"count={count}")
                    return before, count, after, receipt
                before_balances, count, after_balances, receipt_id = await asyncio.to_thread(run_bulk_add)
            except Exception:
                await send_or_edit_bulk_error(ctx, progress_msg, "Database unavailable. Try again shortly.")
                return
            await send_bulk_before_after_result(
                ctx,
                progress_msg,
                f"{Q_SUCCESS} Added **{format_balance(amount)}** each to **{count:,}** users.",
                targets["user_ids"],
                before_balances,
                after_balances,
                format_balance,
                "Balance",
                receipt_id=receipt_id,
            )
            await send_economy_log(ctx, "𝚀𝚞𝚎wo Multi Add", [
                ("Targets", targets["log_label"], False),
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
        await ctx.send(f"{Q_DENIED} You can't edit that user's 𝚀𝚞𝚎wo balance.")
        return

    try:
        def run_single_add():
            old, new = add_user_balance(member.id, amount, earned_delta=amount)
            log_transaction(member.id, "owner_add", amount, f"By {ctx.author.id}")
            receipt = create_receipt(ctx.guild.id if ctx.guild else 0, ctx.channel.id, ctx.author.id, [member.id], "add", amount, f"{old}->{new}")
            return old, new, receipt
        old_balance, new_balance, receipt_id = await asyncio.to_thread(run_single_add)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await reply_to_command(
        ctx,
        f"{Q_SUCCESS} Added **{format_balance(amount)}** to **{user_mention(member.id)}**\n"
        f"Balance: **{format_balance(old_balance)}** → **{format_balance(new_balance)}**"
        f"{receipt_line(receipt_id)}",
        allowed_mentions=discord.AllowedMentions.none()
    )
    await send_economy_log(ctx, "𝚀𝚞𝚎wo Add", [
        ("Target", f"{user_mention(member.id)} ({member.id})", False),
        ("Amount", format_balance(amount), True),
        ("Balance", f"{format_balance(old_balance)} → {format_balance(new_balance)}", False),
    ], color=discord.Color.green())

@commands.command()
async def remove(ctx, *, args: str = None):
    if not await ensure_db_ready(ctx):
        return

    if not is_superowner_id(ctx.author.id):
        await send_owner_only(ctx)
        return
    bulk_confirmed, args = strip_bulk_confirm(args)
    target, amount = parse_target_amount_args(args, allow_all=True)
    if not target:
        await send_economy_command_input_ui(ctx, "remove", "Enter the target and amount. Either order works.")
        return
    if len(getattr(ctx.message, "mentions", []) or []) > 1:
        try:
            targets = await resolve_admin_targets(ctx, target)
        except commands.BadArgument:
            targets = None
        if targets and targets["kind"] == "members":
            blocked = [user_id for user_id in targets["user_ids"] if not can_economy_act_on(ctx.author.id, user_id, ctx.guild)]
            if blocked:
                await ctx.send(f"{Q_DENIED} You can't edit one or more of those users' 𝚀𝚞𝚎wo balances.")
                return
            if not bulk_confirmed and await require_bulk_confirmation(ctx, "remove", len(targets["user_ids"])):
                return
            raw_amount = amount
            total_removed = 0
            changed = 0
            before_balances = {}
            after_balances = {}
            progress_msg = await send_bulk_progress(ctx, "remove quesos", len(targets["user_ids"]))
            try:
                def run_multi_remove():
                    local_total = 0
                    local_changed = 0
                    local_before = {}
                    local_after = {}
                    for user_id in targets["user_ids"]:
                        data = get_user(user_id)
                        old_balance = int(data["balance"])
                        local_before[user_id] = old_balance
                        if str(raw_amount).lower() == "all":
                            remove_amount = old_balance
                        else:
                            remove_amount = parse_whole_number(raw_amount)
                            if remove_amount is None:
                                raise ValueError
                        if remove_amount <= 0:
                            local_after[user_id] = old_balance
                            continue
                        remove_amount = min(remove_amount, old_balance)
                        new_balance = max(0, old_balance - remove_amount)
                        update_user(user_id, balance=new_balance)
                        log_transaction(user_id, "owner_remove", -remove_amount, f"By {ctx.author.id}")
                        local_after[user_id] = new_balance
                        local_total += remove_amount
                        local_changed += 1
                    receipt = create_receipt(ctx.guild.id if ctx.guild else 0, ctx.channel.id, ctx.author.id, targets["user_ids"], "remove_multi", local_total, f"changed={local_changed}; raw={raw_amount}")
                    return local_total, local_changed, local_before, local_after, receipt
                total_removed, changed, before_balances, after_balances, receipt_id = await asyncio.to_thread(run_multi_remove)
            except ValueError:
                await send_or_edit_bulk_error(ctx, progress_msg, "Use `.remove @user @user 10k` or `.remove @user @user all`.")
                return
            except Exception:
                await send_or_edit_bulk_error(ctx, progress_msg, "Database unavailable. Try again shortly.")
                return
            if changed <= 0:
                if progress_msg:
                    await progress_msg.edit(content=f"{Q_DENIED} No balances were changed.")
                else:
                    await send_nonpositive_amount_error(ctx, raw_amount)
                return
            await send_bulk_before_after_result(
                ctx,
                progress_msg,
                f"{Q_SUCCESS} Removed from **{changed:,}** users.\n"
                f"Total Removed: **{format_balance(total_removed)}**",
                targets["user_ids"],
                before_balances,
                after_balances,
                format_balance,
                "Balance",
                receipt_id=receipt_id,
            )
            await send_economy_log(ctx, "𝚀𝚞𝚎wo Multi Remove", [
                ("Targets", targets["log_label"], False),
                ("Recipients", f"{changed:,}", True),
                ("Total Removed", format_balance(total_removed), True),
            ], color=discord.Color.red())
            return
    try:
        member = await commands.MemberConverter().convert(ctx, target)
    except commands.BadArgument:
        await ctx.send(f"{Q_DENIED} Mention a user or paste their ID. Example: `.remove @user 10k`.")
        return
    if not can_economy_act_on(ctx.author.id, member.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You can't edit that user's 𝚀𝚞𝚎wo balance.")
        return

    try:
        raw_amount = amount
        def run_single_remove():
            target_data = get_user(member.id)
            old = target_data['balance']
            if str(raw_amount).lower() == "all":
                remove_amount = old
            else:
                remove_amount = parse_whole_number(raw_amount)
                if remove_amount is None:
                    raise ValueError
            if remove_amount <= 0:
                return old, old, remove_amount, None
            remove_amount = min(remove_amount, old)
            new = max(0, old - remove_amount)
            update_user(member.id, balance=new)
            log_transaction(member.id, "owner_remove", -remove_amount, f"By {ctx.author.id}")
            receipt = create_receipt(ctx.guild.id if ctx.guild else 0, ctx.channel.id, ctx.author.id, [member.id], "remove", remove_amount, f"{old}->{new}")
            return old, new, remove_amount, receipt
        old_balance, new_balance, amount, receipt_id = await asyncio.to_thread(run_single_remove)
        if amount <= 0:
            await send_nonpositive_amount_error(ctx, raw_amount)
            return
    except ValueError:
        await ctx.send(f"{Q_DENIED} Use `.remove @user all`, `.remove all @user`, or a number like `10k`.")
        return
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    await reply_to_command(
        ctx,
        f"{Q_SUCCESS} Removed **{format_balance(amount)}** from **{user_mention(member.id)}**\n"
        f"Balance: **{format_balance(old_balance)}** → **{format_balance(new_balance)}**"
        f"{receipt_line(receipt_id)}",
        allowed_mentions=discord.AllowedMentions.none()
    )
    await send_economy_log(ctx, "𝚀𝚞𝚎wo Remove", [
        ("Target", f"{user_mention(member.id)} ({member.id})", False),
        ("Amount", format_balance(amount), True),
        ("Balance", f"{format_balance(old_balance)} → {format_balance(new_balance)}", False),
    ], color=discord.Color.red())

@commands.command()
async def addtick(ctx, *, args: str = None):
    """𝚀𝚞𝚎 owner only. Adds free lottery tickets to a user, role, or everyone."""
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} `.addtick` only works in servers.")
        return
    if not await ensure_db_ready(ctx):
        return
    if not is_superowner_id(ctx.author.id):
        await send_owner_only(ctx)
        return
    bulk_confirmed, args = strip_bulk_confirm(args)
    target, amount = parse_target_amount_args(args)
    if not target:
        await send_economy_command_input_ui(ctx, "addtick", "Enter the target and ticket amount. Users, roles, and @everyone work.")
        return
    amount = parse_whole_number(amount)
    if amount is None:
        await ctx.send(f"{Q_DENIED} Use `.addtick @user <tickets>`, `.addtick @role <tickets>`, or `.addtick @everyone <tickets>`.")
        return
    if amount <= 0:
        await ctx.send(f"{Q_DENIED} Ticket amount must be positive.")
        return

    config = await asyncio.to_thread(get_lottery_config, ctx.guild.id)
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
    if not bulk_confirmed and await require_bulk_confirmation(ctx, "ticket add", len(targets["user_ids"])):
        return

    progress_msg = await send_bulk_progress(ctx, "add lottery tickets", len(targets["user_ids"]))
    try:
        def run_ticket_add():
            before = get_lottery_ticket_counts(ctx.guild.id, targets["user_ids"])
            count = bulk_adjust_lottery_tickets(ctx.guild.id, targets["user_ids"], amount, "add", ctx.author.id)
            after = get_lottery_ticket_counts(ctx.guild.id, targets["user_ids"])
            updated_config = get_lottery_config(ctx.guild.id)
            receipt = create_receipt(ctx.guild.id, ctx.channel.id, ctx.author.id, targets["user_ids"], "addtick", amount, f"count={count}; total_added={amount * count}")
            return before, count, after, updated_config, receipt
        before_tickets, count, after_tickets, updated, receipt_id = await asyncio.to_thread(run_ticket_add)
        schedule_lottery_refresh(ctx.guild, updated)
        if count == 1 and targets["member"] is not None:
            await assign_lottery_role(ctx.guild, targets["member"].id, updated.get("role_id") if updated else None)
    except Exception:
        await send_or_edit_bulk_error(ctx, progress_msg, "Database unavailable. Try again shortly.")
        return
    total_added = amount * count
    await send_bulk_before_after_result(
        ctx,
        progress_msg,
        f"{Q_SUCCESS} Added **{amount:,}** free {Q_TICKET} tickets to **{count:,}** target(s).\n"
        f"Total Entries Added: **{total_added:,}**",
        targets["user_ids"],
        before_tickets,
        after_tickets,
        lambda value: f"{int(value):,}",
        "Entries",
        receipt_id=receipt_id,
    )
    await send_economy_log(ctx, "Lottery Tickets Added", [
        ("Target", targets["log_label"], False),
        ("Recipients", f"{count:,}", True),
        ("Tickets Each", f"{amount:,}", True),
        ("Total Tickets", f"{total_added:,}", True),
    ], color=discord.Color.green())

@commands.command(aliases=["remtick", "deltick"])
async def removetick(ctx, *, args: str = None):
    """𝚀𝚞𝚎 owner only. Removes lottery tickets from a user, role, or everyone."""
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} `.removetick` only works in servers.")
        return
    if not await ensure_db_ready(ctx):
        return
    if not is_superowner_id(ctx.author.id):
        await send_owner_only(ctx)
        return
    bulk_confirmed, args = strip_bulk_confirm(args)
    target, amount = parse_target_amount_args(args)
    if not target:
        await send_economy_command_input_ui(ctx, "removetick", "Enter the target and ticket amount to remove. Users, roles, and @everyone work.")
        return
    amount = parse_whole_number(amount)
    if amount is None:
        await ctx.send(f"{Q_DENIED} Use `.removetick @user <tickets>`, `.removetick @role <tickets>`, or `.removetick @everyone <tickets>`.")
        return
    if amount <= 0:
        await ctx.send(f"{Q_DENIED} Ticket amount must be positive.")
        return

    config = await asyncio.to_thread(get_lottery_config, ctx.guild.id)
    if config is None:
        await ctx.send("Lottery is not set up yet.")
        return

    try:
        targets = await resolve_admin_targets(ctx, target)
    except commands.BadArgument:
        await ctx.send(f"{Q_DENIED} Use `.removetick @user <tickets>`, `.removetick @role <tickets>`, or `.removetick @everyone <tickets>`.")
        return
    if not targets["user_ids"]:
        await ctx.send(f"{Q_DENIED} No users matched that target.")
        return
    if not bulk_confirmed and await require_bulk_confirmation(ctx, "ticket remove", len(targets["user_ids"])):
        return

    progress_msg = await send_bulk_progress(ctx, "remove lottery tickets", len(targets["user_ids"]))
    try:
        def run_ticket_remove():
            before = get_lottery_ticket_counts(ctx.guild.id, targets["user_ids"])
            count = bulk_adjust_lottery_tickets(ctx.guild.id, targets["user_ids"], amount, "remove", ctx.author.id)
            after = get_lottery_ticket_counts(ctx.guild.id, targets["user_ids"])
            updated_config = get_lottery_config(ctx.guild.id)
            removed = sum(max(0, before.get(user_id, 0) - after.get(user_id, 0)) for user_id in targets["user_ids"])
            receipt = create_receipt(ctx.guild.id, ctx.channel.id, ctx.author.id, targets["user_ids"], "removetick", removed, f"limit_each={amount}; count={count}")
            return before, count, after, updated_config, removed, receipt
        before_tickets, count, after_tickets, updated, total_removed, receipt_id = await asyncio.to_thread(run_ticket_remove)
        schedule_lottery_refresh(ctx.guild, updated)
    except Exception:
        await send_or_edit_bulk_error(ctx, progress_msg, "Database unavailable. Try again shortly.")
        return
    await send_bulk_before_after_result(
        ctx,
        progress_msg,
        f"{Q_SUCCESS} Removed up to **{amount:,}** {Q_TICKET_MINUS} tickets from **{count:,}** target(s).\n"
        f"Entries Removed: **{total_removed:,}**",
        targets["user_ids"],
        before_tickets,
        after_tickets,
        lambda value: f"{int(value):,}",
        "Entries",
        receipt_id=receipt_id,
    )
    await send_economy_log(ctx, "Lottery Tickets Removed", [
        ("Target", targets["log_label"], False),
        ("Recipients", f"{count:,}", True),
        ("Tickets Each", f"{amount:,}", True),
        ("Tickets Removed", f"{total_removed:,}", True),
    ], color=discord.Color.red())

@commands.command()
async def settick(ctx, *, args: str = None):
    """𝚀𝚞𝚎 owner only. Sets lottery tickets for a user, role, or everyone."""
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} `.settick` only works in servers.")
        return
    if not await ensure_db_ready(ctx):
        return
    if not is_superowner_id(ctx.author.id):
        await send_owner_only(ctx)
        return
    bulk_confirmed, args = strip_bulk_confirm(args)
    target, amount = parse_target_amount_args(args)
    if not target:
        await send_economy_command_input_ui(ctx, "settick", "Enter the target and exact ticket amount. Users, roles, and @everyone work.")
        return
    amount = parse_whole_number(amount)
    if amount is None:
        await ctx.send(f"{Q_DENIED} Use `.settick @user <tickets>`, `.settick @role <tickets>`, or `.settick @everyone <tickets>`.")
        return
    if amount < 0:
        await ctx.send(f"{Q_DENIED} Ticket amount cannot be negative.")
        return

    config = await asyncio.to_thread(get_lottery_config, ctx.guild.id)
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
    if not bulk_confirmed and await require_bulk_confirmation(ctx, "ticket set", len(targets["user_ids"])):
        return

    progress_msg = await send_bulk_progress(ctx, "set lottery tickets", len(targets["user_ids"]))
    try:
        def run_ticket_set():
            before = get_lottery_ticket_counts(ctx.guild.id, targets["user_ids"])
            count = bulk_adjust_lottery_tickets(ctx.guild.id, targets["user_ids"], amount, "set", ctx.author.id)
            after = get_lottery_ticket_counts(ctx.guild.id, targets["user_ids"])
            updated_config = get_lottery_config(ctx.guild.id)
            receipt = create_receipt(ctx.guild.id, ctx.channel.id, ctx.author.id, targets["user_ids"], "settick", amount, f"count={count}")
            return before, count, after, updated_config, receipt
        before_tickets, count, after_tickets, updated, receipt_id = await asyncio.to_thread(run_ticket_set)
        schedule_lottery_refresh(ctx.guild, updated)
        if count == 1 and amount > 0 and targets["member"] is not None:
            await assign_lottery_role(ctx.guild, targets["member"].id, updated.get("role_id") if updated else None)
    except Exception:
        await send_or_edit_bulk_error(ctx, progress_msg, "Database unavailable. Try again shortly.")
        return
    await send_bulk_before_after_result(
        ctx,
        progress_msg,
        f"{Q_SUCCESS} Set {Q_TICKET} tickets to **{amount:,}** for **{count:,}** target(s).",
        targets["user_ids"],
        before_tickets,
        after_tickets,
        lambda value: f"{int(value):,}",
        "Entries",
        receipt_id=receipt_id,
    )
    await send_economy_log(ctx, "Lottery Tickets Set", [
        ("Target", targets["log_label"], False),
        ("Recipients", f"{count:,}", True),
        ("Tickets Each", f"{amount:,}", True),
    ], color=discord.Color.gold())

@commands.command()
async def setquesos(ctx, *, args: str = None):
    """𝚀𝚞𝚎 owner only. Sets balances for a user, role, or everyone."""
    if ctx.guild is None:
        await ctx.send(f"{Q_DENIED} `.setquesos` only works in servers.")
        return
    if not await ensure_db_ready(ctx):
        return
    if not is_superowner_id(ctx.author.id):
        await send_owner_only(ctx)
        return
    bulk_confirmed, args = strip_bulk_confirm(args)
    target, amount = parse_target_amount_args(args)
    if not target:
        await send_economy_command_input_ui(ctx, "setquesos", "Enter the target and exact balance. Users, roles, and @everyone work.")
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
    if not bulk_confirmed and await require_bulk_confirmation(ctx, "balance set", len(targets["user_ids"])):
        return

    progress_msg = await send_bulk_progress(ctx, "set balances", len(targets["user_ids"]))
    try:
        def run_bulk_set():
            before = get_balances_for_users(targets["user_ids"])
            count = bulk_set_balances(targets["user_ids"], amount, ctx.author.id, targets["log_label"])
            after = get_balances_for_users(targets["user_ids"])
            receipt = create_receipt(ctx.guild.id, ctx.channel.id, ctx.author.id, targets["user_ids"], "setquesos", amount, f"count={count}; target={targets['log_label']}")
            return before, count, after, receipt
        before_balances, count, after_balances, receipt_id = await asyncio.to_thread(run_bulk_set)
    except Exception:
        await send_or_edit_bulk_error(ctx, progress_msg, "Database unavailable. Try again shortly.")
        return
    await send_bulk_before_after_result(
        ctx,
        progress_msg,
        f"{Q_SUCCESS} Set balance to **{format_balance(amount)}** for **{count:,}** target(s).",
        targets["user_ids"],
        before_balances,
        after_balances,
        format_balance,
        "Balance",
        receipt_id=receipt_id,
    )
    await send_economy_log(ctx, "𝚀𝚞𝚎wo Balance Set", [
        ("Target", targets["log_label"], False),
        ("Recipients", f"{count:,}", True),
        ("New Balance", format_balance(amount), True),
    ], color=discord.Color.gold())

# =====================
# SCRATCH CARD
# =====================
# Design: horizontal ticket with 5 hidden cells, animated one-by-one reveal
# Win: all 5 symbols match. QScratchMark pays x10, QSlotJackpot pays x12.

SCRATCH_TIERS = [
    (Q_SCRATCH_MARK, 10, 80),
    (Q_SLOT_JACKPOT, 12, 20),
]
SCRATCH_SYMBOLS = [Q_SCRATCH_MARK, Q_SLOT_STAR, Q_SLOT_DIAMOND, Q_SLOT_CROWN, Q_SLOT_JACKPOT]
SCRATCH_WIN_CHANCE = 0.08

@commands.command(aliases=["scratchcard"])
async def scratch(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "scratch")
    if cd > 0:
        await send_gambling_cooldown(ctx, cd)
        return

    user_id = ctx.author.id

    try:
        data = await asyncio.to_thread(get_user, user_id)
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

    if not await check_daily_loss_limit(ctx, data, amount):
        return

    if random.random() < chance_with_luck(SCRATCH_WIN_CHANCE, data, cap=0.25):
        win_symbol, _, _ = random.choices(
            SCRATCH_TIERS,
            weights=[weight for _, _, weight in SCRATCH_TIERS],
            k=1
        )[0]
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
        multiplier = next(
            (tier_multiplier for symbol, tier_multiplier, _ in SCRATCH_TIERS if symbol == best_symbol),
            10
        )
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
        data = await asyncio.to_thread(get_user, user_id)
        if multiplier > 0:
            new_streak = next_gambling_streak(data)
            mult = payout_multiplier(data, new_streak)
            winnings = int(amount * mult * multiplier)
            new_balance = data['balance'] + winnings - amount
            await asyncio.to_thread(update_user,
                user_id,
                balance=new_balance,
                gamble_streak=new_streak,
                total_won=data['total_won'] + winnings - amount
            )
            streak_msg = gambling_streak_text(data, new_streak)
            await msg.edit(
                content=(
                    f"{QOIN_CHEST} **SCRATCH CARD — WIN!**\n"
                    f"─────────────────\n"
                    f"{'  '.join(cell_states)}\n"
                    f"─────────────────\n"
                    f">>> {Q_SUCCESS} **{match_count}/5 {best_symbol} matched!**\n"
                    f"Multiplier: ×{multiplier}  |  Streak bonus: ×{mult:.2f}\n"
                    f"Prize: **{format_balance(winnings)}**{streak_msg}\n"
                    f"New Balance: **{format_balance(new_balance)}**"
                ),
                view=double_or_nothing_view(user_id, "scratch", {"winnings": winnings})
            )
        else:
            new_balance = max(0, data['balance'] - amount)
            await asyncio.to_thread(update_user,
                user_id,
                balance=new_balance,
                gamble_streak=0,
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


# =====================
# TOWER
# =====================
TOWER_MULTIPLIERS = [1.15, 1.45, 2.00, 3.00, 4.50, 7.00]
TOWER_TRAPS_BY_FLOOR = [2, 2, 2, 2, 2, 2]

@commands.command(aliases=["towers", "qtower"])
async def tower(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "tower")
    if cd > 0:
        await send_gambling_cooldown(ctx, cd)
        return

    user_id = ctx.author.id
    try:
        data = await asyncio.to_thread(get_user, user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    raw_amount = amount
    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data["balance"])
    if parsed is None:
        await ctx.send(f"{Q_DENIED} Use `.tower all` or `.tower <amount>` (max {MAX_BET:,} {CURRENCY_EMOJI})")
        return
    amount = parsed
    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return
    if amount > data["balance"] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return

    if not await check_daily_loss_limit(ctx, data, amount):
        return

    floor = 0
    game_over = False
    bad_doors = [
        set(random.sample(range(3), trap_count))
        for trap_count in TOWER_TRAPS_BY_FLOOR
    ]

    def render(extra=None):
        current_mult = TOWER_MULTIPLIERS[floor - 1] if floor > 0 else 0
        next_mult = TOWER_MULTIPLIERS[floor] if floor < len(TOWER_MULTIPLIERS) else current_mult
        lines = [
            f"{Q_TOWER} **Q TOWERS**",
            "─────────────────",
            f"Bet: **{format_balance(amount)}**",
            f"Floor: **{floor}/{len(TOWER_MULTIPLIERS)}**",
            f"Traps This Floor: **{TOWER_TRAPS_BY_FLOOR[floor] if floor < len(TOWER_TRAPS_BY_FLOOR) else TOWER_TRAPS_BY_FLOOR[-1]}/3**",
            f"Cash Out: **×{current_mult:.2f}**" if floor > 0 else "Cash Out: **locked**",
            f"Next Safe Pick: **×{next_mult:.2f}**",
        ]
        if extra:
            lines.append(extra)
        return "\n".join(lines)

    class TowerDoor(discord.ui.Button):
        def __init__(self, index):
            super().__init__(label=f"Door {index + 1}", style=discord.ButtonStyle.primary)
            self.index = index

        async def callback(self, interaction):
            nonlocal floor, game_over
            if interaction.user.id != user_id:
                await interaction.response.send_message("Use your own tower.", ephemeral=True)
                return
            if game_over:
                await interaction.response.defer()
                return
            if self.index in bad_doors[floor]:
                game_over = True
                await interaction.response.defer()
                try:
                    latest = await asyncio.to_thread(get_user, user_id)
                    new_balance = max(0, latest["balance"] - amount)
                    await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=0, total_lost=latest["total_lost"] + amount)
                except Exception:
                    await interaction.edit_original_response(content=render(f"{Q_DENIED} Database unavailable."), view=None)
                    return
                view.clear_items()
                await interaction.edit_original_response(
                    content=render(
                        f">>> {Q_TOWER_TRAP} **Door {self.index + 1} was trapped.**\n"
                        f"Lost: **{format_balance(amount)}**\n"
                        f"New Balance: **{format_balance(new_balance)}**"
                    ),
                    view=view
                )
                return
            floor += 1
            if floor >= len(TOWER_MULTIPLIERS):
                await cash_out(interaction, "Top floor cleared!")
                return
            await interaction.response.edit_message(content=render(f"{Q_SUCCESS} Door {self.index + 1} was safe."), view=view)

    async def cash_out(interaction, label):
        nonlocal game_over
        game_over = True
        base_multiplier = TOWER_MULTIPLIERS[floor - 1]
        if not interaction.response.is_done():
            await interaction.response.defer()
        try:
            latest = await asyncio.to_thread(get_user, user_id)
            new_streak = next_gambling_streak(latest)
            streak_mult = payout_multiplier(latest, new_streak)
            winnings = int(amount * base_multiplier * streak_mult)
            new_balance = latest["balance"] + winnings - amount
            await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=new_streak, total_won=latest["total_won"] + winnings - amount)
        except Exception:
            await interaction.edit_original_response(content=render(f"{Q_DENIED} Database unavailable."), view=None)
            return
        view.clear_items()
        await interaction.edit_original_response(
            content=render(
                f">>> {Q_SUCCESS} **{label}**\n"
                f"Multiplier: **×{base_multiplier * streak_mult:.3f}** (base ×{base_multiplier:.2f}, streak ×{streak_mult:.3f})"
                f"{gambling_streak_text(latest, new_streak)}\n"
                f"Prize: **{format_balance(winnings)}**\n"
                f"New Balance: **{format_balance(new_balance)}**"
            ),
            view=double_or_nothing_view(user_id, "tower", {"winnings": winnings})
        )

    class TowerCashOut(discord.ui.Button):
        def __init__(self):
            super().__init__(label="Cash Out", style=discord.ButtonStyle.success)

        async def callback(self, interaction):
            if interaction.user.id != user_id:
                await interaction.response.send_message("Use your own tower.", ephemeral=True)
                return
            if floor <= 0:
                await interaction.response.send_message("Pick at least one safe door before cashing out.", ephemeral=True)
                return
            await cash_out(interaction, "Cashed out.")

    class TowerView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=60)
            for i in range(3):
                self.add_item(TowerDoor(i))
            self.add_item(TowerCashOut())

        async def on_timeout(self):
            nonlocal game_over
            if game_over:
                return
            game_over = True
            self.clear_items()
            try:
                latest = await asyncio.to_thread(get_user, user_id)
                new_balance = max(0, latest["balance"] - amount)
                await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=0, total_lost=latest["total_lost"] + amount)
                await self.message.edit(content=render(f"{Q_TIMER} Timed out. Lost **{format_balance(amount)}**\nNew Balance: **{format_balance(new_balance)}**"), view=self)
            except Exception:
                pass

    view = TowerView()
    view.message = await ctx.send(render(), view=view)


# =====================
# VAULT
# =====================
VAULT_GUESSES = 7
VAULT_MULTIPLIER = 4

@commands.command(aliases=["qvault"])
async def vault(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "vault")
    if cd > 0:
        await send_gambling_cooldown(ctx, cd)
        return

    user_id = ctx.author.id
    try:
        data = await asyncio.to_thread(get_user, user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    raw_amount = amount
    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data["balance"])
    if parsed is None:
        await ctx.send(f"{Q_DENIED} Use `.vault all` or `.vault <amount>` (max {MAX_BET:,} {CURRENCY_EMOJI})")
        return
    amount = parsed
    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return
    if amount > data["balance"] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return

    if not await check_daily_loss_limit(ctx, data, amount):
        return

    code = "".join(random.sample("0123456789", 3))
    guesses = []
    prompt = await ctx.send(
        f"{Q_VAULT} **Q VAULT**\n"
        f"Guess the 3-digit code. Digits do not repeat.\n"
        f"Type guesses in this channel. You have **{VAULT_GUESSES}** tries.\n"
        f"Bet: **{format_balance(amount)}** | Prize: **×{VAULT_MULTIPLIER}**"
    )

    def check(message):
        return message.author.id == user_id and message.channel.id == ctx.channel.id

    while len(guesses) < VAULT_GUESSES:
        try:
            message = await bot.wait_for("message", timeout=75, check=check)
        except asyncio.TimeoutError:
            break
        guess = message.content.strip()
        if not re.fullmatch(r"\d{3}", guess) or len(set(guess)) != 3:
            await ctx.send(f"{Q_DENIED} Guess **3 different digits**, like `407`.")
            continue
        guesses.append(guess)
        exact = sum(1 for a, b in zip(guess, code) if a == b)
        close = sum(1 for digit in guess if digit in code) - exact
        if guess == code:
            try:
                latest = await asyncio.to_thread(get_user, user_id)
                new_streak = next_gambling_streak(latest)
                streak_mult = payout_multiplier(latest, new_streak)
                winnings = int(amount * VAULT_MULTIPLIER * streak_mult)
                new_balance = latest["balance"] + winnings - amount
                await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=new_streak, total_won=latest["total_won"] + winnings - amount)
            except Exception:
                await send_error(ctx, "Database unavailable. Try again shortly.")
                return
            await prompt.edit(
                content=(
                    f"{Q_VAULT} **Q VAULT — OPENED!**\n"
                    f"Code: **{code}**\n"
                    f"Guesses: **{len(guesses)}/{VAULT_GUESSES}**\n"
                    f"Multiplier: **×{VAULT_MULTIPLIER * streak_mult:.3f}** (base ×{VAULT_MULTIPLIER}, streak ×{streak_mult:.3f})"
                    f"{gambling_streak_text(latest, new_streak)}\n"
                    f"Prize: **{format_balance(winnings)}**\n"
                    f"New Balance: **{format_balance(new_balance)}**"
                ),
                view=double_or_nothing_view(user_id, "vault", {"winnings": winnings})
            )
            return
        await ctx.send(f"{Q_VAULT_DIAL} `{guess}` → **{exact}** exact, **{close}** close. Tries left: **{VAULT_GUESSES - len(guesses)}**")

    try:
        latest = await asyncio.to_thread(get_user, user_id)
        new_balance = max(0, latest["balance"] - amount)
        await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=0, total_lost=latest["total_lost"] + amount)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    await prompt.edit(
        content=(
            f"{Q_VAULT} **Q VAULT — LOCKED**\n"
            f"Code was **{code}**.\n"
            f"Lost: **{format_balance(amount)}**\n"
            f"New Balance: **{format_balance(new_balance)}**"
        )
    )


# =====================
# MEMORY
# =====================
MEMORY_SYMBOLS = [Q_SLOT_STAR, Q_SLOT_DIAMOND, Q_SLOT_CROWN, Q_SLOT_JACKPOT, Q_SCRATCH_MARK, Q_XP, Q_TICKET, Q_FORTUNE_VIAL]
MEMORY_MULTIPLIER = 3
MEMORY_MAX_MISTAKES = 6
MEMORY_PICK_SECONDS = 7
MEMORY_REVEAL_SECONDS = 0.75

@commands.command(name="memory", aliases=["mem", "qmemory"])
async def memory_game(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "memory")
    if cd > 0:
        await send_gambling_cooldown(ctx, cd)
        return

    user_id = ctx.author.id
    try:
        data = await asyncio.to_thread(get_user, user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    raw_amount = amount
    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data["balance"])
    if parsed is None:
        await ctx.send(f"{Q_DENIED} Use `.memory all` or `.memory <amount>` (max {MAX_BET:,} {CURRENCY_EMOJI})")
        return
    amount = parsed
    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return
    if amount > data["balance"] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return

    if not await check_daily_loss_limit(ctx, data, amount):
        return

    symbols = MEMORY_SYMBOLS * 2
    random.shuffle(symbols)
    revealed = [False] * 16
    matched = [False] * 16
    selected = []
    mistakes = 0
    busy = False
    game_over = False
    pick_timer_task = None

    def render(extra=None):
        rows = []
        for r in range(4):
            cells = []
            for c in range(4):
                idx = r * 4 + c
                cells.append(symbols[idx] if revealed[idx] or matched[idx] else Q_MEMORY_TILE)
            rows.append(" ".join(cells))
        text = (
            f"{Q_MEMORY} **Q MEMORY**\n"
            f"Bet: **{format_balance(amount)}** | Prize: **×{MEMORY_MULTIPLIER}** | Mistakes: **{mistakes}/{MEMORY_MAX_MISTAKES}**\n"
            + "\n".join(rows)
        )
        return f"{text}\n{extra}" if extra else text

    def shuffle_unsolved_tiles():
        unsolved = [index for index, is_matched in enumerate(matched) if not is_matched]
        values = [symbols[index] for index in unsolved]
        random.shuffle(values)
        for index, value in zip(unsolved, values):
            symbols[index] = value

    def cancel_pick_timer():
        nonlocal pick_timer_task
        if pick_timer_task and not pick_timer_task.done():
            pick_timer_task.cancel()
        pick_timer_task = None

    async def pick_timer_loop():
        nonlocal busy
        try:
            for remaining in range(MEMORY_PICK_SECONDS, 0, -1):
                if game_over or busy or len(selected) != 1:
                    return
                await view.message.edit(
                    content=render(f"{Q_TIMER_TICK} Pick another tile in **{remaining}s** or unsolved tiles shuffle."),
                    view=view
                )
                await asyncio.sleep(1)
            if game_over or busy or len(selected) != 1:
                return
            busy = True
            selected.clear()
            for index in range(len(revealed)):
                if not matched[index]:
                    revealed[index] = False
            shuffle_unsolved_tiles()
            busy = False
            await view.message.edit(
                content=render(f"{Q_TIMER} Too slow. Unsolved tiles shuffled."),
                view=view
            )
        except asyncio.CancelledError:
            return
        except Exception:
            return

    def start_pick_timer():
        nonlocal pick_timer_task
        cancel_pick_timer()
        pick_timer_task = asyncio.create_task(pick_timer_loop())

    class MemoryTile(discord.ui.Button):
        def __init__(self, index):
            super().__init__(label=str(index + 1), style=discord.ButtonStyle.secondary, row=index // 4)
            self.index = index

        async def callback(self, interaction):
            nonlocal mistakes, busy, game_over
            if interaction.user.id != user_id:
                await interaction.response.send_message("Use your own memory board.", ephemeral=True)
                return
            if busy or game_over or matched[self.index] or revealed[self.index]:
                await interaction.response.defer()
                return
            revealed[self.index] = True
            selected.append(self.index)
            await interaction.response.edit_message(content=render(), view=view)
            if len(selected) < 2:
                start_pick_timer()
                return
            cancel_pick_timer()
            busy = True
            first, second = selected
            selected.clear()
            if symbols[first] == symbols[second]:
                matched[first] = True
                matched[second] = True
                busy = False
                if all(matched):
                    game_over = True
                    cancel_pick_timer()
                    try:
                        latest = await asyncio.to_thread(get_user, user_id)
                        new_streak = next_gambling_streak(latest)
                        streak_mult = payout_multiplier(latest, new_streak)
                        winnings = int(amount * MEMORY_MULTIPLIER * streak_mult)
                        new_balance = latest["balance"] + winnings - amount
                        def settle_memory_win():
                            update_user(user_id, balance=new_balance, gamble_streak=new_streak, total_won=latest["total_won"] + winnings - amount)
                            stats = record_game_result(user_id, "memory", True, winnings - amount, winnings)
                            return maybe_award_game_achievements(user_id, "memory", stats)
                        achievements = await asyncio.to_thread(settle_memory_win)
                    except Exception:
                        await interaction.message.edit(content=render(f"{Q_DENIED} Database unavailable."), view=None)
                        return
                    view.clear_items()
                    await interaction.message.edit(
                        content=render(
                            f">>> {Q_SUCCESS} **All pairs matched!**\n"
                            f"Multiplier: **×{MEMORY_MULTIPLIER * streak_mult:.3f}** (base ×{MEMORY_MULTIPLIER}, streak ×{streak_mult:.3f})"
                            f"{gambling_streak_text(latest, new_streak)}\n"
                            f"Prize: **{format_balance(winnings)}**\n"
                            f"New Balance: **{format_balance(new_balance)}**"
                            f"{achievement_reward_text(achievements)}"
                        ),
                        view=double_or_nothing_view(user_id, "memory", {"winnings": winnings})
                    )
                else:
                    await interaction.message.edit(content=render(f"{Q_SUCCESS} Pair matched."), view=view)
                return

            mistakes += 1
            await asyncio.sleep(MEMORY_REVEAL_SECONDS)
            revealed[first] = False
            revealed[second] = False
            busy = False
            if mistakes >= MEMORY_MAX_MISTAKES:
                game_over = True
                cancel_pick_timer()
                try:
                    latest = await asyncio.to_thread(get_user, user_id)
                    new_balance = max(0, latest["balance"] - amount)
                    await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=0, total_lost=latest["total_lost"] + amount)
                    await asyncio.to_thread(record_game_result, user_id, "memory", False, -amount, 0)
                except Exception:
                    return
                view.clear_items()
                await interaction.message.edit(content=render(f">>> {Q_DENIED} **Too many mistakes.**\nLost: **{format_balance(amount)}**\nNew Balance: **{format_balance(new_balance)}**"), view=view)
                return
            await interaction.message.edit(content=render(f"{Q_DENIED} Not a match."), view=view)

    class MemoryView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=90)
            for i in range(16):
                self.add_item(MemoryTile(i))

        async def on_timeout(self):
            nonlocal game_over
            if game_over:
                return
            game_over = True
            cancel_pick_timer()
            self.clear_items()
            try:
                latest = await asyncio.to_thread(get_user, user_id)
                new_balance = max(0, latest["balance"] - amount)
                await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=0, total_lost=latest["total_lost"] + amount)
                await asyncio.to_thread(record_game_result, user_id, "memory", False, -amount, 0)
                await self.message.edit(content=render(f">>> {Q_TIMER} **Timed out.**\nLost: **{format_balance(amount)}**\nNew Balance: **{format_balance(new_balance)}**"), view=self)
            except Exception:
                pass

    view = MemoryView()
    view.message = await ctx.send(render(), view=view)


# =====================
# CARD LADDER
# =====================
CARD_LADDER_MULTIPLIERS = [1.25, 1.50, 1.75, 2.00, 3.00, 5.00]
CARD_SUITS = [Q_CARD_SPADE, Q_CARD_HEART, Q_CARD_DIAMOND, Q_CARD_CLUB]
CARD_RANK_LABELS = {
    2: "2", 3: "3", 4: "4", 5: "5", 6: "6", 7: "7", 8: "8", 9: "9", 10: "10",
    11: "J", 12: "Q", 13: "K", 14: "A",
}

def random_ladder_card():
    return random.randint(2, 14), random.choice(CARD_SUITS)

def format_ladder_card(card):
    rank, suit = card
    return f"{suit} **{CARD_RANK_LABELS[rank]}**"

@commands.command(name="cardladder", aliases=["ladder", "cards", "cladder"])
async def card_ladder(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "cardladder")
    if cd > 0:
        await send_gambling_cooldown(ctx, cd)
        return

    user_id = ctx.author.id
    try:
        data = await asyncio.to_thread(get_user, user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    raw_amount = amount
    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data["balance"])
    if parsed is None:
        await ctx.send(f"{Q_DENIED} Use `.cardladder all` or `.cardladder <amount>` (max {MAX_BET:,} {CURRENCY_EMOJI})")
        return
    amount = parsed
    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return
    if amount > data["balance"] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return

    if not await check_daily_loss_limit(ctx, data, amount):
        return

    current_card = random_ladder_card()
    rung = 0
    game_over = False

    def render(extra=None):
        current_mult = CARD_LADDER_MULTIPLIERS[rung - 1] if rung > 0 else 0
        next_mult = CARD_LADDER_MULTIPLIERS[rung] if rung < len(CARD_LADDER_MULTIPLIERS) else current_mult
        lines = [
            f"{Q_CARD_LADDER} **CARD LADDER**",
            "─────────────────",
            f"Bet: **{format_balance(amount)}**",
            f"Current Card: {format_ladder_card(current_card)}",
            f"Rung: **{rung}/{len(CARD_LADDER_MULTIPLIERS)}**",
            f"Cash Out: **×{current_mult:.2f}**" if rung > 0 else "Cash Out: **locked**",
            f"Next Correct Pick: **×{next_mult:.2f}**",
        ]
        if extra:
            lines.append(extra)
        return "\n".join(lines)

    async def finish_loss(interaction, next_card, label):
        nonlocal game_over, current_card
        game_over = True
        current_card = next_card
        try:
            latest = await asyncio.to_thread(get_user, user_id)
            new_balance = max(0, latest["balance"] - amount)
            await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=0, total_lost=latest["total_lost"] + amount)
            await asyncio.to_thread(record_game_result, user_id, "cardladder", False, -amount, 0)
        except Exception:
            await interaction.message.edit(content=render(f"{Q_DENIED} Database unavailable."), view=None)
            return
        result_block = gamble_result_block(
            "cardladder",
            amount,
            {"winnings": 0, "balance": new_balance},
            outcome=f"Wrong call ({label})",
        )
        view.clear_items()
        await interaction.message.edit(
            content=render(
                f">>> {Q_DENIED} **Wrong call.** You picked **{label}**.\n"
                f"{result_block}"
            ),
            view=view
        )

    async def cash_out(interaction, label):
        nonlocal game_over
        if rung <= 0:
            await interaction.response.send_message("Win at least one card before cashing out.", ephemeral=True)
            return
        game_over = True
        base_multiplier = CARD_LADDER_MULTIPLIERS[rung - 1]
        if not interaction.response.is_done():
            await interaction.response.defer()
        try:
            latest = await asyncio.to_thread(get_user, user_id)
            new_streak = next_gambling_streak(latest)
            streak_mult = payout_multiplier(latest, new_streak)
            winnings = int(amount * base_multiplier * streak_mult)
            new_balance = latest["balance"] + winnings - amount
            await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=new_streak, total_won=latest["total_won"] + winnings - amount)
            await asyncio.to_thread(record_game_result, user_id, "cardladder", True, winnings - amount, winnings)
        except Exception:
            await interaction.edit_original_response(content=render(f"{Q_DENIED} Database unavailable."), view=None)
            return
        result_block = gamble_result_block(
            "cardladder",
            amount,
            {"winnings": winnings, "balance": new_balance, "streak": new_streak, "streak_mult": streak_mult},
            base_multiplier,
            outcome=label,
        )
        view.clear_items()
        await interaction.edit_original_response(
            content=render(
                f">>> {Q_SUCCESS} **{label}**\n"
                f"{result_block}"
            ),
            view=double_or_nothing_view(user_id, "cardladder", {"winnings": winnings})
        )

    class LadderCall(discord.ui.Button):
        def __init__(self, label, higher):
            super().__init__(label=label, style=discord.ButtonStyle.primary)
            self.higher = higher

        async def callback(self, interaction):
            nonlocal current_card, rung, game_over
            if interaction.user.id != user_id:
                await interaction.response.send_message("Use your own card ladder.", ephemeral=True)
                return
            if game_over:
                await interaction.response.defer()
                return
            await interaction.response.defer()
            current_rank = current_card[0]
            winning_ranks = [
                rank for rank in range(2, 15)
                if (rank > current_rank if self.higher else rank < current_rank)
            ]
            if winning_ranks and random.random() < active_luck_bonus(data):
                next_card = (random.choice(winning_ranks), random.choice(CARD_SUITS))
            else:
                next_card = random_ladder_card()
            next_rank = next_card[0]
            correct = next_rank > current_rank if self.higher else next_rank < current_rank
            label = "Higher" if self.higher else "Lower"
            if not correct:
                await finish_loss(interaction, next_card, label)
                return
            current_card = next_card
            rung += 1
            if rung >= len(CARD_LADDER_MULTIPLIERS):
                await cash_out(interaction, "Ladder cleared!")
                return
            await interaction.message.edit(content=render(f"{Q_SUCCESS} **{label}** was right."), view=view)

    class LadderCashOut(discord.ui.Button):
        def __init__(self):
            super().__init__(label="Cash Out", style=discord.ButtonStyle.success)

        async def callback(self, interaction):
            if interaction.user.id != user_id:
                await interaction.response.send_message("Use your own card ladder.", ephemeral=True)
                return
            await cash_out(interaction, "Cashed out.")

    class LadderView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=75)
            self.add_item(LadderCall("Higher", True))
            self.add_item(LadderCall("Lower", False))
            self.add_item(LadderCashOut())

        async def on_timeout(self):
            nonlocal game_over
            if game_over:
                return
            game_over = True
            self.clear_items()
            try:
                latest = await asyncio.to_thread(get_user, user_id)
                new_balance = max(0, latest["balance"] - amount)
                await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=0, total_lost=latest["total_lost"] + amount)
                await asyncio.to_thread(record_game_result, user_id, "cardladder", False, -amount, 0)
                await self.message.edit(content=render(f"{Q_TIMER} Timed out. Lost **{format_balance(amount)}**\nNew Balance: **{format_balance(new_balance)}**"), view=self)
            except Exception:
                pass

    view = LadderView()
    view.message = await ctx.send(render(), view=view)


# =====================
# LOCKPICK
# =====================
LOCKPICK_PINS = 5
LOCKPICK_HEIGHTS = 9
LOCKPICK_TRIES = 3
LOCKPICK_MULTIPLIER = 4

@commands.command(name="lockpick", aliases=["lp", "picklock"])
async def lockpick(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "lockpick")
    if cd > 0:
        await send_gambling_cooldown(ctx, cd)
        return

    user_id = ctx.author.id
    try:
        data = await asyncio.to_thread(get_user, user_id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    raw_amount = amount
    parsed = parse_amount(amount, ctx.author.id, ctx.guild, data["balance"])
    if parsed is None:
        await ctx.send(f"{Q_DENIED} Use `.lockpick all` or `.lockpick <amount>` (max {MAX_BET:,} {CURRENCY_EMOJI})")
        return
    amount = parsed
    if amount <= 0:
        await send_nonpositive_amount_error(ctx, raw_amount)
        return
    if amount > data["balance"] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return

    if not await check_daily_loss_limit(ctx, data, amount):
        return

    target = [random.randint(1, LOCKPICK_HEIGHTS) for _ in range(LOCKPICK_PINS)]
    pins = [max(1, LOCKPICK_HEIGHTS // 2)] * LOCKPICK_PINS
    tries = 0
    game_over = False
    last_hint = "Adjust the pins, then test the lock."

    def pin_bar(value):
        return "▰" * value + "▱" * (LOCKPICK_HEIGHTS - value)

    def render(extra=None):
        pin_lines = [f"Pin {index + 1}: `{pin_bar(value)}` **{value}/{LOCKPICK_HEIGHTS}**" for index, value in enumerate(pins)]
        lines = [
            f"{Q_LOCKPICK} **LOCKPICK**",
            "─────────────────",
            f"Bet: **{format_balance(amount)}** | Prize: **×{LOCKPICK_MULTIPLIER:g}**",
            f"Tests: **{tries}/{LOCKPICK_TRIES}**",
            *pin_lines,
            f"> {last_hint}",
        ]
        if extra:
            lines.append(extra)
        return "\n".join(lines)

    async def finish_loss(interaction, reason):
        nonlocal game_over
        game_over = True
        try:
            latest = await asyncio.to_thread(get_user, user_id)
            new_balance = max(0, latest["balance"] - amount)
            await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=0, total_lost=latest["total_lost"] + amount)
            await asyncio.to_thread(record_game_result, user_id, "lockpick", False, -amount, 0)
        except Exception:
            await interaction.message.edit(content=render(f"{Q_DENIED} Database unavailable."), view=None)
            return
        result_block = gamble_result_block(
            "lockpick",
            amount,
            {"winnings": 0, "balance": new_balance},
            outcome=reason,
            details=f"Code: **{'-'.join(map(str, target))}**",
        )
        view.clear_items()
        await interaction.message.edit(
            content=render(
                f">>> {Q_DENIED} **{reason}**\n"
                f"{result_block}"
            ),
            view=view
        )

    async def finish_win(interaction):
        nonlocal game_over
        game_over = True
        try:
            latest = await asyncio.to_thread(get_user, user_id)
            new_streak = next_gambling_streak(latest)
            streak_mult = payout_multiplier(latest, new_streak)
            winnings = int(amount * LOCKPICK_MULTIPLIER * streak_mult)
            new_balance = latest["balance"] + winnings - amount
            await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=new_streak, total_won=latest["total_won"] + winnings - amount)
            await asyncio.to_thread(record_game_result, user_id, "lockpick", True, winnings - amount, winnings)
        except Exception:
            await interaction.message.edit(content=render(f"{Q_DENIED} Database unavailable."), view=None)
            return
        result_block = gamble_result_block(
            "lockpick",
            amount,
            {"winnings": winnings, "balance": new_balance, "streak": new_streak, "streak_mult": streak_mult},
            LOCKPICK_MULTIPLIER,
            outcome="Lock opened",
        )
        view.clear_items()
        await interaction.message.edit(
            content=render(
                f">>> {Q_SUCCESS} **Lock opened!**\n"
                f"{result_block}"
            ),
            view=double_or_nothing_view(user_id, "lockpick", {"winnings": winnings})
        )

    class PinButton(discord.ui.Button):
        def __init__(self, index):
            super().__init__(label=f"Pin {index + 1}", style=discord.ButtonStyle.secondary)
            self.index = index

        async def callback(self, interaction):
            nonlocal last_hint
            if interaction.user.id != user_id:
                await interaction.response.send_message("Use your own lockpick.", ephemeral=True)
                return
            if game_over:
                await interaction.response.defer()
                return
            pins[self.index] = 1 if pins[self.index] >= LOCKPICK_HEIGHTS else pins[self.index] + 1
            last_hint = f"Pin {self.index + 1} raised."
            await interaction.response.edit_message(content=render(), view=view)

    class TestLock(discord.ui.Button):
        def __init__(self):
            super().__init__(label="Test", style=discord.ButtonStyle.success)

        async def callback(self, interaction):
            nonlocal tries, last_hint
            if interaction.user.id != user_id:
                await interaction.response.send_message("Use your own lockpick.", ephemeral=True)
                return
            if game_over:
                await interaction.response.defer()
                return
            await interaction.response.defer()
            tries += 1
            if pins == target:
                await finish_win(interaction)
                return
            hints = []
            for index, (current, goal) in enumerate(zip(pins, target), 1):
                if current == goal:
                    hints.append(f"P{index} set")
                elif current < goal:
                    hints.append(f"P{index} low")
                else:
                    hints.append(f"P{index} high")
            last_hint = " | ".join(hints)
            if tries >= LOCKPICK_TRIES:
                await finish_loss(interaction, "The lock jammed.")
                return
            await interaction.message.edit(content=render(), view=view)

    class ResetPins(discord.ui.Button):
        def __init__(self):
            super().__init__(label="Reset", style=discord.ButtonStyle.danger)

        async def callback(self, interaction):
            nonlocal pins, last_hint
            if interaction.user.id != user_id:
                await interaction.response.send_message("Use your own lockpick.", ephemeral=True)
                return
            if game_over:
                await interaction.response.defer()
                return
            pins = [1] * LOCKPICK_PINS
            last_hint = "Pins reset to 1."
            await interaction.response.edit_message(content=render(), view=view)

    class LockpickView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=120)
            for index in range(LOCKPICK_PINS):
                self.add_item(PinButton(index))
            self.add_item(TestLock())
            self.add_item(ResetPins())

        async def on_timeout(self):
            nonlocal game_over
            if game_over:
                return
            game_over = True
            self.clear_items()
            try:
                latest = await asyncio.to_thread(get_user, user_id)
                new_balance = max(0, latest["balance"] - amount)
                await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=0, total_lost=latest["total_lost"] + amount)
                await asyncio.to_thread(record_game_result, user_id, "lockpick", False, -amount, 0)
                await self.message.edit(content=render(f">>> {Q_TIMER} **Timed out.**\nLost: **{format_balance(amount)}**\nNew Balance: **{format_balance(new_balance)}**"), view=self)
            except Exception:
                pass

    view = LockpickView()
    view.message = await ctx.send(render(), view=view)


# =====================
# NEW QUEWO GAMES
# =====================
async def prepare_gamble(ctx, amount_text, command_name):
    if not await ensure_db_ready(ctx):
        return None, None
    try:
        data = await asyncio.to_thread(get_user, ctx.author.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return None, None
    cd = check_cooldown(ctx.author.id, command_name, data=data)
    if cd > 0:
        await send_gambling_cooldown(ctx, cd)
        return None, None
    parsed = parse_amount(amount_text, ctx.author.id, ctx.guild, data["balance"])
    if parsed is None:
        await ctx.send(f"{Q_DENIED} Use `.{command_name} all` or `.{command_name} <amount>` (max {MAX_BET:,} {CURRENCY_EMOJI})")
        return None, None
    if parsed <= 0:
        await send_nonpositive_amount_error(ctx, amount_text)
        return None, None
    if parsed > data["balance"] and not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} You only have {format_balance(data['balance'])}")
        return None, None
    if not await check_daily_loss_limit(ctx, data, parsed):
        return None, None
    return data, parsed

async def send_new_game_result(ctx, game_key, title, amount, result, details="", base_multiplier=None):
    content = (
        f"{title}\n"
        f"─────────────────\n"
        f"{details}\n"
        f"{gamble_result_block(game_key, amount, result, base_multiplier)}"
    )
    view = double_or_nothing_view(ctx.author.id, game_key, result)
    await ctx.send(fit_discord_content(content), view=view, allowed_mentions=discord.AllowedMentions.none())
    try:
        await send_economy_log(ctx, f"Game Result: {game_display_name(game_key)}", [
            ("Player", f"{user_mention(ctx.author.id)} ({ctx.author.id})", False),
            ("Risk", risk_label(game_key), True),
            ("Bet", format_balance(amount), True),
            ("Result", "Win" if result.get("winnings", 0) > 0 else ("Loss" if result.get("net", 0) < 0 else "Neutral"), True),
            ("Payout", format_balance(result.get("winnings", 0)), True),
            ("Balance", format_balance(result.get("balance", 0)), True),
        ], color=discord.Color.green() if result.get("winnings", 0) > 0 else discord.Color.red())
    except Exception as e:
        print(f"Game result log failed: {type(e).__name__} - {e}")
    await send_achievement_notifications(ctx, result.get("achievements", []))

HEIST_ROUTES = {
    "silent": {"label": "Silent", "chance": 0.45, "mult": 2.0, "risk": "Medium"},
    "balanced": {"label": "Balanced", "chance": 0.30, "mult": 3.25, "risk": "High"},
    "loud": {"label": "Loud", "chance": 0.16, "mult": 6.0, "risk": "Extreme"},
}
HEIST_TOOLS = {
    "scanner": {"label": "Scanner", "chance": 0.08, "mult": -0.15, "text": "spots patrol routes"},
    "scrambler": {"label": "Scrambler", "chance": 0.04, "mult": 0.00, "text": "jams alarms"},
    "decoy": {"label": "Decoy", "chance": -0.03, "mult": 0.35, "text": "draws guards away"},
}

@commands.command(name="heist", aliases=["robbery", "qh"])
async def heist(ctx, amount: str):
    data, amount = await prepare_gamble(ctx, amount, "heist")
    if data is None:
        return
    route_choice = None
    tool_choice = None
    heist_stage = 0
    stage_choices = []
    heist_chance_bonus = 0
    heist_mult_bonus = 0
    prompt_message = None
    heist_stages = [
        {
            "name": "Entry",
            "options": [
                ("Pick Lock", 0.05, -0.05),
                ("Cut Power", 0.00, 0.10),
                ("Break Window", -0.06, 0.25),
            ],
        },
        {
            "name": "Vault",
            "options": [
                ("Slow Drill", 0.05, 0.00),
                ("Thermal Cut", -0.02, 0.18),
                ("Blast Door", -0.08, 0.40),
            ],
        },
        {
            "name": "Escape",
            "options": [
                ("Back Alley", 0.05, -0.05),
                ("Garage", 0.00, 0.10),
                ("Main Road", -0.05, 0.25),
            ],
        },
    ]

    def heist_prompt():
        if route_choice is None:
            return (
                f"{Q_HEIST} **HEIST SETUP**\n"
                "Choose the route.\n"
                + "\n".join(f"**{route['label']}**: {route['risk']} risk, pays ×{route['mult']:g}" for route in HEIST_ROUTES.values())
            )
        route = HEIST_ROUTES[route_choice]
        return (
            f"{Q_HEIST} **HEIST SETUP**\n"
            f"Route: **{route['label']}** ×{route['mult']:g}\n"
            "Choose your tool.\n"
            + "\n".join(
                f"**{tool['label']}**: {tool['text']}"
                for tool in HEIST_TOOLS.values()
            )
        )

    async def run_heist(interaction):
        route = HEIST_ROUTES[route_choice]
        tool = HEIST_TOOLS[tool_choice]
        await interaction.response.edit_message(content=f"{Q_HEIST} **HEIST STARTED**\nRoute: **{route['label']}** | Tool: **{tool['label']}**", view=view)
        await show_heist_stage()

    async def show_heist_stage():
        stage = heist_stages[heist_stage]
        view.clear_items()
        for label, chance_delta, mult_delta in stage["options"]:
            view.add_item(HeistStageButton(label, chance_delta, mult_delta))
        await prompt_message.edit(
            content=(
                f"{Q_HEIST} **HEIST - {stage['name']}** `{heist_stage + 1}/3`\n"
                "Choose how to handle this step.\n"
                + "\n".join(
                    f"**{label}**: chance {chance_delta:+.0%}, payout {mult_delta:+.2f}"
                    for label, chance_delta, mult_delta in stage["options"]
                )
            ),
            view=view
        )

    async def finish_heist(interaction):
        route = HEIST_ROUTES[route_choice]
        tool = HEIST_TOOLS[tool_choice]
        await interaction.response.edit_message(content=f"{Q_HEIST} **HEIST COMPLETE**\nEscaping...", view=None)
        latest = await asyncio.to_thread(get_user, ctx.author.id)
        chance = min(0.90, max(0.05, route["chance"] + tool["chance"] + heist_chance_bonus + active_luck_bonus(latest)))
        base_mult = max(1.1, route["mult"] + tool["mult"] + heist_mult_bonus)
        won = random.random() < chance
        result = await asyncio.to_thread(settle_gambling_result, ctx.author.id, "heist", amount, base_mult, won, data=latest)
        details = (
            f"{Q_HEIST} Route: **{route['label']}** | Tool: **{tool['label']}**\n"
            f"Choices: **{' / '.join(stage_choices)}**\n"
            f"Success chance: **{int(chance * 100)}%** | Target payout: **×{base_mult:g}**"
        )
        await prompt_message.delete()
        await send_new_game_result(ctx, "heist", f"{Q_HEIST} **HEIST**", amount, result, details, base_mult if won else None)

    class HeistButton(discord.ui.Button):
        def __init__(self, route_key, route):
            super().__init__(label=f"{route['label']} ×{route['mult']:g}", style=discord.ButtonStyle.primary)
            self.route_key = route_key
            self.route = route

        async def callback(self, interaction):
            nonlocal route_choice
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own heist.", ephemeral=True)
                return
            route_choice = self.route_key
            view.clear_items()
            for tool_key, tool in HEIST_TOOLS.items():
                view.add_item(HeistToolButton(tool_key, tool))
            await interaction.response.edit_message(content=heist_prompt(), view=view)

    class HeistToolButton(discord.ui.Button):
        def __init__(self, tool_key, tool):
            super().__init__(label=tool["label"], style=discord.ButtonStyle.secondary)
            self.tool_key = tool_key

        async def callback(self, interaction):
            nonlocal tool_choice
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own heist.", ephemeral=True)
                return
            tool_choice = self.tool_key
            await run_heist(interaction)

    class HeistStageButton(discord.ui.Button):
        def __init__(self, label, chance_delta, mult_delta):
            super().__init__(label=label, style=discord.ButtonStyle.primary)
            self.choice_label = label
            self.chance_delta = chance_delta
            self.mult_delta = mult_delta

        async def callback(self, interaction):
            nonlocal heist_stage, heist_chance_bonus, heist_mult_bonus
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own heist.", ephemeral=True)
                return
            stage_choices.append(self.choice_label)
            heist_chance_bonus += self.chance_delta
            heist_mult_bonus += self.mult_delta
            heist_stage += 1
            if heist_stage >= len(heist_stages):
                await finish_heist(interaction)
                return
            await interaction.response.defer()
            await show_heist_stage()

    class HeistView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=45)
            for route_key, route in HEIST_ROUTES.items():
                self.add_item(HeistButton(route_key, route))

    view = HeistView()
    prompt_message = await ctx.send(heist_prompt(), view=view)

@commands.command(name="diceduel", aliases=["dice", "dd"])
async def dice_duel(ctx, amount: str):
    data, amount = await prepare_gamble(ctx, amount, "diceduel")
    if data is None:
        return

    tactics = {
        "steady": {"label": "Steady", "mult": 1.75, "bonus": 1, "text": "+1 to your roll, lower payout"},
        "normal": {"label": "Normal", "mult": 2.0, "bonus": 0, "text": "classic dice duel"},
        "push": {"label": "Push", "mult": 2.75, "bonus": -1, "text": "-1 to your roll, higher payout"},
    }
    tactic = None
    player = []
    dealer = []

    def dice_content(extra=""):
        tactic_label = tactic["label"] if tactic else "not chosen"
        player_text = " + ".join(str(v) for v in player) if player else "-"
        dealer_text = " + ".join(str(v) for v in dealer) if dealer else "-"
        return (
            f"{Q_DICE_DUEL} **DICE DUEL**\n"
            f"Tactic: **{tactic_label}**\n"
            f"You: **{player_text}**\n"
            f"Dealer: **{dealer_text}**\n"
            f"{extra}"
        ).strip()

    class DiceButton(discord.ui.Button):
        def __init__(self, tactic_key, tactic):
            super().__init__(label=f"{tactic['label']} ×{tactic['mult']:g}", style=discord.ButtonStyle.primary)
            self.tactic_key = tactic_key
            self.tactic = tactic

        async def callback(self, interaction):
            nonlocal tactic
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own dice duel.", ephemeral=True)
                return
            tactic = self.tactic
            view.clear_items()
            view.add_item(RollPlayerDie())
            await interaction.response.edit_message(content=dice_content("Press **Roll Die** twice."), view=view)

    class RollPlayerDie(discord.ui.Button):
        def __init__(self):
            super().__init__(label="Roll Die", style=discord.ButtonStyle.success)

        async def callback(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own dice duel.", ephemeral=True)
                return
            roll = random.randint(1, 6)
            player.append(roll)
            if len(player) < 2:
                await interaction.response.edit_message(content=dice_content(f"You rolled **{roll}**. Roll the second die."), view=view)
                return
            view.clear_items()
            view.add_item(DealerRollButton())
            await interaction.response.edit_message(
                content=dice_content(f"You rolled **{roll}**. Your modifier is **{tactic['bonus']:+d}**. Now make the dealer roll."),
                view=view
            )

    class DealerRollButton(discord.ui.Button):
        def __init__(self):
            super().__init__(label="Dealer Roll", style=discord.ButtonStyle.danger)

        async def callback(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own dice duel.", ephemeral=True)
                return
            for item in view.children:
                item.disabled = True
            dealer.extend([random.randint(1, 6), random.randint(1, 6)])
            player_total = sum(player) + tactic["bonus"]
            dealer_total = sum(dealer)
            await interaction.response.edit_message(content=dice_content("Dealer rolled. Settling result..."), view=view)
            latest = await asyncio.to_thread(get_user, ctx.author.id)
            if player_total <= dealer_total and random.random() < active_luck_bonus(latest):
                high_index = 0 if dealer[0] >= dealer[1] else 1
                dealer[high_index] = max(1, dealer[high_index] - 2)
                dealer_total = sum(dealer)
            if player_total == dealer_total:
                result = await asyncio.to_thread(settle_gambling_result, ctx.author.id, "diceduel", amount, neutral=True, data=latest)
                details = (
                    f"{Q_DICE_DUEL} Tactic: **{tactic['label']}**\n"
                    f"You rolled **{player[0]} + {player[1]} {tactic['bonus']:+d} = {player_total}**\n"
                    f"Dealer rolled **{dealer[0]} + {dealer[1]} = {dealer_total}**\n"
                    "Push. Nothing won or lost."
                )
                await message.edit(content=f"{Q_DICE_DUEL} **DICE DUEL**\n─────────────────\n{details}\nNew Balance: **{format_balance(result['balance'])}**", view=view)
                return
            won = player_total > dealer_total
            result = await asyncio.to_thread(settle_gambling_result, ctx.author.id, "diceduel", amount, tactic["mult"], won, data=latest)
            details = (
                f"{Q_DICE_DUEL} Tactic: **{tactic['label']}**\n"
                f"You rolled **{player[0]} + {player[1]} {tactic['bonus']:+d} = {player_total}**\n"
                f"Dealer rolled **{dealer[0]} + {dealer[1]} = {dealer_total}**"
            )
            await message.delete()
            await send_new_game_result(ctx, "diceduel", f"{Q_DICE_DUEL} **DICE DUEL**", amount, result, details, tactic["mult"] if won else None)

    class DiceDuelView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=45)
            for tactic_key, tactic in tactics.items():
                self.add_item(DiceButton(tactic_key, tactic))

    view = DiceDuelView()
    message = await ctx.send(
        f"{Q_DICE_DUEL} **DICE DUEL**\nChoose how you roll.\n"
        + "\n".join(f"**{t['label']}**: {t['text']}" for t in tactics.values()),
        view=view
    )

CASE_TABLE = [
    ("Empty Case", 0, 42),
    ("Small Stack", 1.25, 30),
    ("Blue Cache", 2, 18),
    ("Royal Case", 5, 8),
    ("Mythic Case", 15, 2),
]

@commands.command(name="cases", aliases=["case", "qcase", "open"])
async def cases(ctx, amount: str):
    data, amount = await prepare_gamble(ctx, amount, "cases")
    if data is None:
        return
    case_slots = []
    for _ in range(3):
        index = random.choices(range(len(CASE_TABLE)), weights=[row[2] for row in CASE_TABLE])[0]
        case_slots.append(CASE_TABLE[index])
    picked_case = None
    key_mods = {
        "safe": {"label": "Safe Key", "shift": -1, "text": "safer but can downgrade"},
        "clean": {"label": "Clean Key", "shift": 0, "text": "normal open"},
        "royal": {"label": "Royal Key", "shift": 1, "text": "riskier, can upgrade"},
    }

    class CaseButton(discord.ui.Button):
        def __init__(self, index):
            super().__init__(label=f"Case {index + 1}", style=discord.ButtonStyle.primary)
            self.index = index

        async def callback(self, interaction):
            nonlocal picked_case
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own case.", ephemeral=True)
                return
            picked_case = self.index
            view.clear_items()
            for key, key_data in key_mods.items():
                view.add_item(KeyButton(key, key_data))
            await interaction.response.edit_message(
                content=(
                    f"{Q_CASES} **Q CASES**\n"
                    f"Case **{self.index + 1}** selected. Now choose the key.\n"
                    + "\n".join(f"**{key_data['label']}**: {key_data['text']}" for key_data in key_mods.values())
                ),
                view=view
            )

    class KeyButton(discord.ui.Button):
        def __init__(self, key, key_data):
            super().__init__(label=key_data["label"], style=discord.ButtonStyle.secondary)
            self.key_data = key_data

        async def callback(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own case.", ephemeral=True)
                return
            for item in view.children:
                item.disabled = True
            await interaction.response.edit_message(
                content=f"{Q_CASES} **Q CASES**\nCase **{picked_case + 1}** | Key: **{self.key_data['label']}**\nUnlocking...",
                view=view
            )
            reveal = [Q_CASES, Q_CASES, Q_CASES]
            for i in range(3):
                await asyncio.sleep(0.7)
                reveal[i] = QOIN_CHEST if i == picked_case else Q_LOCK
                await message.edit(content=f"{Q_CASES} **Q CASES**\n{'  '.join(reveal)}\n_Revealing locks..._", view=None)
            name, multiplier, _ = case_slots[picked_case]
            latest = await asyncio.to_thread(get_user, ctx.author.id)
            if multiplier > 0 and self.key_data["shift"]:
                table_index = next((i for i, row in enumerate(CASE_TABLE) if row[0] == name), 0)
                shifted_index = max(0, min(len(CASE_TABLE) - 1, table_index + self.key_data["shift"]))
                name, multiplier, _ = CASE_TABLE[shifted_index]
            if random.random() < active_luck_bonus(latest):
                table_index = next((i for i, row in enumerate(CASE_TABLE) if row[0] == name), 0)
                boosted_index = min(len(CASE_TABLE) - 1, table_index + 1)
                name, multiplier, _ = CASE_TABLE[boosted_index]
            result = await asyncio.to_thread(settle_gambling_result, ctx.author.id, "cases", amount, multiplier, multiplier > 0, data=latest)
            details = f"{Q_CASES} Picked: **Case {picked_case + 1}** | Key: **{self.key_data['label']}**\nOpened: **{name}** | Result: **×{multiplier:g}**"
            await message.delete()
            await send_new_game_result(ctx, "cases", f"{Q_CASES} **Q CASES**", amount, result, details, multiplier if multiplier > 0 else None)

    class CaseView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=45)
            for i in range(3):
                self.add_item(CaseButton(i))

    view = CaseView()
    message = await ctx.send(
        f"{Q_CASES} **Q CASES**\nThree locked cases are on the table. Pick one to open.\n"
        f"{Q_CASES}  {Q_CASES}  {Q_CASES}",
        view=view
    )

PLINKO_SLOTS = [
    (Q_WHEEL_BLANK, 0, 25),
    (Q_WHEEL_RED, 0, 20),
    (Q_WHEEL_BLUE, 1, 20),
    (Q_WHEEL_GREEN, 1.5, 15),
    (Q_WHEEL_ORANGE, 2, 12),
    (Q_WHEEL_GOLD, 5, 6),
    (Q_WHEEL_PINK, 10, 2),
]

def plinko_board(ball_row=None, ball_col=None):
    rows = []
    for row in range(6):
        cells = []
        for col in range(5):
            cells.append(QOIN_BAG if row == ball_row and col == ball_col else Q_MS_HIDDEN)
        rows.append(" ".join(cells))
    slots = " ".join(f"{emoji}×{mult:g}" for emoji, mult, _ in PLINKO_SLOTS)
    return "\n".join(rows) + "\n" + slots

def plinko_landing_from_drop(drop_col):
    col = drop_col
    path = [(0, col)]
    for row in range(1, 6):
        move = random.choice([-1, 0, 1])
        col = max(0, min(4, col + move))
        path.append((row, col))
    index = max(0, min(len(PLINKO_SLOTS) - 1, col + random.choice([0, 1, 2])))
    return path, index

@commands.command(name="plinko", aliases=["plink", "drop"])
async def plinko(ctx, amount: str):
    data, amount = await prepare_gamble(ctx, amount, "plinko")
    if data is None:
        return
    ball_row = 0
    ball_col = None

    class DropButton(discord.ui.Button):
        def __init__(self, index):
            super().__init__(label=f"Drop {index + 1}", style=discord.ButtonStyle.primary)
            self.index = index

        async def callback(self, interaction):
            nonlocal ball_col
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own plinko board.", ephemeral=True)
                return
            ball_col = self.index
            view.clear_items()
            view.add_item(NudgeButton("left", "Nudge Left", -1))
            view.add_item(NudgeButton("drop", "Let Drop", 0))
            view.add_item(NudgeButton("right", "Nudge Right", 1))
            await interaction.response.edit_message(
                content=f"{Q_PLINKO} **PLINKO**\nDrop lane **{self.index + 1}** selected.\nChoose a nudge each row.\n{plinko_board(ball_row, ball_col)}",
                view=view
            )

    class NudgeButton(discord.ui.Button):
        def __init__(self, key, label, direction):
            style = discord.ButtonStyle.secondary if direction == 0 else discord.ButtonStyle.primary
            super().__init__(label=label, style=style)
            self.direction = direction

        async def callback(self, interaction):
            nonlocal ball_row, ball_col
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own plinko board.", ephemeral=True)
                return
            if ball_col is None:
                await interaction.response.defer()
                return
            drift = random.choice([-1, 0, 1])
            ball_col = max(0, min(4, ball_col + self.direction + drift))
            ball_row += 1
            if ball_row < 6:
                await interaction.response.edit_message(
                    content=(
                        f"{Q_PLINKO} **PLINKO**\n"
                        f"Your nudge: **{self.label}** | Peg bounce: **{drift:+d}**\n"
                        f"{plinko_board(ball_row, ball_col)}"
                    ),
                    view=view
                )
                return
            for item in view.children:
                item.disabled = True
            slot_index = max(0, min(len(PLINKO_SLOTS) - 1, ball_col + random.choice([0, 1, 2])))
            await interaction.response.edit_message(content=f"{Q_PLINKO} **PLINKO COMPLETE**\n{plinko_board(5, ball_col)}\nSettling result...", view=view)
            latest = await asyncio.to_thread(get_user, ctx.author.id)
            emoji, multiplier, _ = PLINKO_SLOTS[slot_index]
            if multiplier <= 1 and random.random() < active_luck_bonus(latest):
                winning_indexes = [index for index, (_, mult, _) in enumerate(PLINKO_SLOTS) if mult > 1]
                slot_index = random.choice(winning_indexes)
                emoji, multiplier, _ = PLINKO_SLOTS[slot_index]
            if multiplier == 1:
                result = await asyncio.to_thread(settle_gambling_result, ctx.author.id, "plinko", amount, neutral=True, data=latest)
                await message.edit(
                    content=(
                        f"{Q_PLINKO} **PLINKO**\n{plinko_board(5, ball_col)}\n"
                        f"Landed: **{emoji} ×1**\nRefund. New Balance: **{format_balance(result['balance'])}**"
                    ),
                    view=view
                )
                return
            won = multiplier > 1
            result = await asyncio.to_thread(settle_gambling_result, ctx.author.id, "plinko", amount, multiplier, won, data=latest)
            details = f"{Q_PLINKO} Final lane: **{ball_col + 1}**\nLanded: **{emoji} ×{multiplier:g}**"
            await message.delete()
            await send_new_game_result(ctx, "plinko", f"{Q_PLINKO} **PLINKO**", amount, result, details, multiplier if won else None)

    class PlinkoView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=45)
            for i in range(5):
                self.add_item(DropButton(i))

    view = PlinkoView()
    message = await ctx.send(
        f"{Q_PLINKO} **PLINKO BOARD**\nPick a drop lane.\n{plinko_board()}",
        view=view
    )

LUCKY_NUMBER_RANGES = (10, 20, 50, 100)
LUCKY_NUMBER_DIFFICULTIES = {
    10: ((2, 3), (3, 2), (5, 1)),
    20: ((3, 5), (4, 4), (6, 2)),
    50: ((4, 10), (6, 6), (8, 4)),
    100: ((3, 15), (8, 8), (20, 2)),
}
LUCKY_NUMBER_GUESS_SECONDS = 60
LUCKY_NUMBER_PUBLIC_WINNER_SHARE = 0.80

@commands.command(name="luckynumber", aliases=["ln", "lucky", "number"])
async def lucky_number(ctx, amount: str):
    data, amount = await prepare_gamble(ctx, amount, "luckynumber")
    if data is None:
        return
    mode = None

    async def ask_range(interaction):
        view.clear_items()
        for max_number in LUCKY_NUMBER_RANGES:
            view.add_item(RangeButton(max_number))
        await interaction.response.edit_message(
            content=(
                f"{Q_LUCKY_NUMBER} **LUCKY NUMBER**\n"
                f"Mode: **{'Public channel' if mode == 'public' else 'Solo'}**\n"
                "Choose your number range first."
            ),
            view=view
        )

    class ModeButton(discord.ui.Button):
        def __init__(self, mode_key, label, style):
            super().__init__(label=label, style=style)
            self.mode_key = mode_key

        async def callback(self, interaction):
            nonlocal mode
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Only the player who started Lucky Number can choose the mode.", ephemeral=True)
                return
            mode = self.mode_key
            await ask_range(interaction)

    class RangeButton(discord.ui.Button):
        def __init__(self, max_number):
            super().__init__(label=f"1-{max_number}", style=discord.ButtonStyle.primary)
            self.max_number = max_number

        async def callback(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Only the player who started Lucky Number can choose the range.", ephemeral=True)
                return
            view.clear_items()
            for multiplier, tries in LUCKY_NUMBER_DIFFICULTIES[self.max_number]:
                view.add_item(DifficultyButton(self.max_number, multiplier, tries))
            await interaction.response.edit_message(
                content=(
                    f"{Q_LUCKY_NUMBER} **LUCKY NUMBER**\n"
                    f"Mode: **{'Public channel' if mode == 'public' else 'Solo'}** | Range: **1-{self.max_number}**\n"
                    "Choose payout and tries."
                ),
                view=view
            )

    class DifficultyButton(discord.ui.Button):
        def __init__(self, max_number, multiplier, tries):
            super().__init__(label=f"×{multiplier} ({tries} tries)", style=discord.ButtonStyle.primary)
            self.max_number = max_number
            self.multiplier = multiplier
            self.tries = tries

        async def callback(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Only the player who started Lucky Number can choose the payout.", ephemeral=True)
                return
            for item in view.children:
                item.disabled = True
            drawn = random.randint(1, self.max_number)
            deadline_ts = int(time.time() + LUCKY_NUMBER_GUESS_SECONDS)
            await interaction.response.edit_message(
                content=(
                    f"{Q_LUCKY_NUMBER} **LUCKY NUMBER**\n"
                    f"Range: **1-{self.max_number}** | Prize: **×{self.multiplier}** | Tries: **{self.tries}**\n"
                    f"{'Anyone in this channel can guess. Each user gets their own tries. Correct guesser gets 80%; starter gets 20%.' if mode == 'public' else 'Type your guess.'}\n"
                    f"Ends <t:{deadline_ts}:R>."
                ),
                view=view
            )

            attempts_by_user = {}

            def check(message):
                if message.author.bot or message.channel.id != ctx.channel.id:
                    return False
                if mode != "public" and message.author.id != ctx.author.id:
                    return False
                if attempts_by_user.get(message.author.id, 0) >= self.tries:
                    return False
                try:
                    guess = int(message.content.strip())
                except ValueError:
                    return False
                return 1 <= guess <= self.max_number

            picked = None
            guesser = None
            deadline = time.time() + LUCKY_NUMBER_GUESS_SECONDS
            try:
                while mode == "public" or attempts_by_user.get(ctx.author.id, 0) < self.tries:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError
                    message = await bot.wait_for("message", timeout=remaining, check=check)
                    guess = int(message.content.strip())
                    picked = guess
                    guesser = message.author
                    attempts_by_user[message.author.id] = attempts_by_user.get(message.author.id, 0) + 1
                    if guess == drawn:
                        break
                    tries_left = self.tries - attempts_by_user[message.author.id]
                    if tries_left > 0:
                        await ctx.send(
                            f"{Q_DENIED} {user_mention(message.author.id)} guessed **{guess}**. Not it. **{tries_left}** tries left.",
                            allowed_mentions=discord.AllowedMentions.none()
                        )
            except asyncio.TimeoutError:
                pass

            won = picked == drawn
            if not won and picked is not None and guesser:
                guesser_data = await asyncio.to_thread(get_user, guesser.id)
                if random.random() < active_luck_bonus(guesser_data):
                    drawn = picked
                    won = True
            starter_id = ctx.author.id
            winner_id = guesser.id if won and guesser else starter_id
            details = (
                f"{Q_LUCKY_NUMBER} Mode: **{'Public' if mode == 'public' else 'Solo'}** | Range: **1-{self.max_number}**\n"
                f"Tries: **{attempts_by_user.get((guesser or ctx.author).id, 0)}/{self.tries}** for {user_mention((guesser or ctx.author).id)} | Prize: **×{self.multiplier}**\n"
                f"Last Guess: **{picked if picked is not None else 'none'}** | Drawn: **{drawn}**\n"
                f"Guesser: **{user_mention(guesser.id) if guesser else 'none'}**"
            )
            if won and mode == "public" and winner_id != starter_id:
                def settle_public_lucky_number():
                    winner_data = get_user(winner_id)
                    new_streak = next_gambling_streak(winner_data)
                    streak_mult = payout_multiplier(winner_data, new_streak)
                    total_prize = int(amount * self.multiplier * streak_mult)
                    winner_share = int(total_prize * LUCKY_NUMBER_PUBLIC_WINNER_SHARE)
                    starter_share = total_prize - winner_share

                    starter_data = get_user(starter_id)
                    starter_net = starter_share - amount
                    starter_balance = max(0, int(starter_data["balance"]) + starter_net)
                    starter_updates = {
                        "balance": starter_balance,
                        "gamble_streak": 0,
                    }
                    if starter_net >= 0:
                        starter_updates["total_won"] = int(starter_data["total_won"]) + starter_net
                    else:
                        starter_updates["total_lost"] = int(starter_data["total_lost"]) + abs(starter_net)
                    update_user(starter_id, **starter_updates)
                    record_game_result(starter_id, "luckynumber", False, starter_net, starter_share)

                    winner_updated = update_user(
                        winner_id,
                        balance=int(winner_data["balance"]) + winner_share,
                        gamble_streak=new_streak,
                        total_won=int(winner_data["total_won"]) + winner_share
                    )
                    stats = record_game_result(winner_id, "luckynumber", True, winner_share, winner_share)
                    achievements = maybe_award_game_achievements(winner_id, "luckynumber", stats)
                    return new_streak, streak_mult, total_prize, winner_share, starter_share, starter_balance, winner_updated, achievements

                new_streak, streak_mult, total_prize, winner_share, starter_share, starter_balance, winner_updated, achievements = await asyncio.to_thread(settle_public_lucky_number)
                result = {
                    "balance": int(winner_updated["balance"]),
                    "winnings": winner_share,
                    "net": winner_share,
                    "streak": new_streak,
                    "streak_mult": streak_mult,
                    "achievements": achievements,
                }
                lines = [
                    f"{Q_LUCKY_NUMBER} **LUCKY NUMBER**",
                    "─────────────────",
                    details,
                    f"Risk: **{risk_label('luckynumber')}**",
                    f"Starter: **{user_mention(starter_id)}**",
                    f"Winner: **{user_mention(winner_id)}**",
                    f"Total Prize: **{format_balance(total_prize)}**",
                    f"Winner Share (80%): **{format_balance(winner_share)}**",
                    f"Starter Share (20%): **{format_balance(starter_share)}**",
                    f"Winner Balance: **{format_balance(result['balance'])}**",
                    f"Starter Balance: **{format_balance(starter_balance)}**",
                    f"Streak: **{new_streak}** win(s)",
                    "Double or Nothing replays the same game with the prize at risk.",
                    achievement_reward_text(achievements),
                ]
                await ctx.send(
                    "\n".join(line for line in lines if line),
                    view=double_or_nothing_view(winner_id, "luckynumber", result),
                    allowed_mentions=discord.AllowedMentions.none()
                )
                return

            latest = await asyncio.to_thread(get_user, starter_id)
            result = await asyncio.to_thread(settle_gambling_result, starter_id, "luckynumber", amount, self.multiplier, won, data=latest)
            await send_new_game_result(ctx, "luckynumber", f"{Q_LUCKY_NUMBER} **LUCKY NUMBER**", amount, result, details, self.multiplier if won else None)

    class LuckyNumberView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=45)
            self.add_item(ModeButton("solo", "Solo", discord.ButtonStyle.primary))
            self.add_item(ModeButton("public", "Public Channel", discord.ButtonStyle.success))

    view = LuckyNumberView()
    await ctx.send(
        f"{Q_LUCKY_NUMBER} **LUCKY NUMBER**\nWho can guess the number?",
        view=view
    )

JACKPOT_SYMBOLS = [
    (Q_SLOT_STAR, "Star", 2, 0.42),
    (Q_SLOT_DIAMOND, "Diamond", 5, 0.16),
    (Q_SLOT_CROWN, "Crown", 10, 0.07),
    (Q_SLOT_JACKPOT, "Jackpot", 50, 0.014),
]

def jackpot_total_hit_chance(symbol, data=None):
    _, _, multiplier, base_chance = symbol
    luck = active_luck_bonus(data) / max(multiplier, 2)
    return min(0.48, max(0.005, base_chance + luck))

def jackpot_spin_hit_chance(symbol, data=None, max_spins=3):
    total_chance = jackpot_total_hit_chance(symbol, data)
    return 1 - ((1 - total_chance) ** (1 / max_spins))

@commands.command(name="jackpotspin", aliases=["jackpot", "jspin", "jps"])
async def jackpot_spin(ctx, amount: str):
    data, amount = await prepare_gamble(ctx, amount, "jackpotspin")
    if data is None:
        return
    target = None
    pointer_index = random.randrange(len(JACKPOT_SYMBOLS))
    spins_used = 0
    max_spins = 3

    def wheel_line():
        parts = []
        for index, (emoji, _, _, _) in enumerate(JACKPOT_SYMBOLS):
            marker = Q_TARGET if index == pointer_index else Q_WHEEL_BLANK
            parts.append(f"{marker}{emoji}")
        return "  ".join(parts)

    def target_text():
        if target is None:
            return "No target selected."
        emoji, label, multiplier, _ = target
        total_chance = jackpot_total_hit_chance(target, data)
        return f"Target: **{emoji} {label} ×{multiplier:g}** | Chance: **{total_chance * 100:.1f}%**"

    async def settle_jackpot(interaction):
        nonlocal spins_used
        for item in view.children:
            item.disabled = True
        landed = JACKPOT_SYMBOLS[pointer_index]
        landed_emoji, landed_label, _, _ = landed
        target_emoji, target_label, target_multiplier, _ = target
        won = landed_emoji == target_emoji
        await interaction.response.edit_message(
            content=(
                f"{Q_JACKPOT_SPIN} **JACKPOT SPIN**\n"
                f"{wheel_line()}\n"
                f"Landed: **{landed_emoji} {landed_label}**\n"
                "Settling result..."
            ),
            view=view
        )
        latest = await asyncio.to_thread(get_user, ctx.author.id)
        result = await asyncio.to_thread(settle_gambling_result, ctx.author.id, "jackpotspin", amount, target_multiplier, won, data=latest)
        details = (
            f"{Q_JACKPOT_SPIN} {target_text()}\n"
            f"Landed: **{landed_emoji} {landed_label}** | Spins used: **{spins_used}/{max_spins}**"
        )
        await message.delete()
        await send_new_game_result(ctx, "jackpotspin", f"{Q_JACKPOT_SPIN} **JACKPOT SPIN**", amount, result, details, target_multiplier if won else None)

    class TargetButton(discord.ui.Button):
        def __init__(self, symbol):
            emoji, label, multiplier, _ = symbol
            super().__init__(label=f"{label} ×{multiplier:g}", style=discord.ButtonStyle.primary)
            self.symbol = symbol

        async def callback(self, interaction):
            nonlocal target
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own jackpot spin.", ephemeral=True)
                return
            target = self.symbol
            view.clear_items()
            view.add_item(SpinWheelButton())
            await interaction.response.edit_message(
                content=(
                    f"{Q_JACKPOT_SPIN} **JACKPOT SPIN**\n"
                    f"{target_text()}\n"
                    f"{wheel_line()}\n"
                    f"Press **Spin Wheel**. You get **{max_spins}** tries to land on your target."
                ),
                view=view
            )

    class SpinWheelButton(discord.ui.Button):
        def __init__(self):
            super().__init__(label="Spin Wheel", style=discord.ButtonStyle.danger)

        async def callback(self, interaction):
            nonlocal pointer_index, spins_used
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own jackpot spin.", ephemeral=True)
                return
            spins_used += 1
            latest = await asyncio.to_thread(get_user, ctx.author.id)
            target_emoji = target[0]
            hit_chance = jackpot_spin_hit_chance(target, latest, max_spins)
            if random.random() < hit_chance:
                pointer_index = next(index for index, symbol in enumerate(JACKPOT_SYMBOLS) if symbol[0] == target_emoji)
            else:
                miss_indexes = [index for index, symbol in enumerate(JACKPOT_SYMBOLS) if symbol[0] != target_emoji]
                pointer_index = random.choice(miss_indexes)
            landed_emoji = JACKPOT_SYMBOLS[pointer_index][0]
            if landed_emoji == target_emoji or spins_used >= max_spins:
                await settle_jackpot(interaction)
                return
            await interaction.response.edit_message(
                content=(
                    f"{Q_JACKPOT_SPIN} **JACKPOT SPIN**\n"
                    f"{target_text()}\n"
                    f"{wheel_line()}\n"
                    f"Miss. Tries left: **{max_spins - spins_used}**"
                ),
                view=view
            )

    class JackpotView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=45)
            for symbol in JACKPOT_SYMBOLS:
                self.add_item(TargetButton(symbol))

    view = JackpotView()
    message = await ctx.send(
        f"{Q_JACKPOT_SPIN} **JACKPOT SPIN**\nChoose your target symbol. Harder targets pay more.",
        view=view
    )

DUNGEON_MAX_DEPTH = 6
DUNGEON_MAX_HP = 5
DUNGEON_CLEAR_BONUS = 30_000
DUNGEON_RELIC_VALUE = 25_000
DUNGEON_REWARD_CAP = 175_000
DUNGEON_ROOMS = ("chest", "monster", "trap", "shrine", "locked")

def dungeon_hp_text(hp):
    hp = max(0, int(hp))
    return f"{Q_DUNGEON_HEART} " * hp + f"`{hp}/{DUNGEON_MAX_HP}`"

def dungeon_room_title(room):
    return {
        "chest": f"{QOIN_CHEST} Treasure Room",
        "monster": f"{Q_DUNGEON_MONSTER} Crystal Guard",
        "trap": f"{Q_TOWER_TRAP} Trap Hall",
        "shrine": f"{Q_DUNGEON_RELIC} Relic Shrine",
        "locked": f"{Q_DUNGEON_KEY} Locked Gate",
        "boss": f"{Q_DUNGEON} Final Door",
    }.get(room, "Dungeon Room")

@commands.command(name="dungeon", aliases=["dng", "qdungeon"])
async def dungeon(ctx):
    """Free solo dungeon run with choices, HP, keys, relics, and a clear reward."""
    if not await ensure_db_ready(ctx):
        return
    try:
        data = await asyncio.to_thread(get_user, ctx.author.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return

    class DungeonChoiceButton(discord.ui.Button):
        def __init__(self, key, label, style=discord.ButtonStyle.primary):
            super().__init__(label=label, style=style)
            self.key = key

        async def callback(self, interaction):
            try:
                await self.view.choose(interaction, self.key)
            except Exception as e:
                print(f"Dungeon interaction failed: {type(e).__name__} - {e}")
                message = f"{Q_DENIED} Dungeon action failed. Try starting a new run."
                if interaction.response.is_done():
                    await interaction.followup.send(message, ephemeral=True)
                else:
                    await interaction.response.send_message(message, ephemeral=True)

    class DungeonView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=180)
            self.user_id = ctx.author.id
            self.data = data
            self.depth = 1
            self.hp = DUNGEON_MAX_HP
            self.keys = 0
            self.relics = 0
            self.loot = 0
            self.rests = 1
            self.finished = False
            self.lock_active = False
            self.lock_target = []
            self.lock_pins = []
            self.lock_tries = 0
            self.lock_hint = ""
            self.duel_active = False
            self.duel_hits = 0
            self.duel_misses = 0
            self.duel_stance = None
            self.trap_active = False
            self.trap_sequence = []
            self.trap_progress = 0
            self.trap_mistakes = 0
            self.trap_hint = ""
            self.boss_active = False
            self.boss_sequence = []
            self.boss_progress = 0
            self.boss_mistakes = 0
            self.boss_hint = ""
            self.room = random.choice(DUNGEON_ROOMS)
            self.message = None
            self.refresh_buttons()

        async def interaction_check(self, interaction):
            if interaction.user.id != self.user_id:
                await interaction.response.send_message("Use your own dungeon run.", ephemeral=True)
                return False
            return True

        def room_text(self):
            text = {
                "chest": "A sealed chest hums with queso light.",
                "monster": "A crystal guard blocks the path.",
                "trap": "The floor is wired with glowing pressure plates.",
                "shrine": "A quiet shrine offers a risky blessing.",
                "locked": "A heavy gate blocks the next hallway.",
                "boss": "The final door wakes up. Crack its rune pattern or spend a key.",
            }
            return text.get(self.room, "The dungeon shifts around you.")

        def status_text(self):
            return (
                f"Room: **{self.depth}/{DUNGEON_MAX_DEPTH}** | HP: {dungeon_hp_text(self.hp)}\n"
                f"Loot: **{format_balance(self.loot)}** | {Q_DUNGEON_KEY} Keys: **{self.keys}** | {Q_DUNGEON_RELIC} Relics: **{self.relics}**"
            )

        def render(self, result_text=None):
            lines = [
                f"{Q_DUNGEON} **Q DUNGEON**",
                "─────────────────",
                self.status_text(),
                "",
                f"**{dungeon_room_title(self.room)}**",
                self.room_text(),
            ]
            if result_text:
                lines.extend(["", result_text])
            return "\n".join(lines)

        def render_lockpick(self, result_text=None):
            pin_lines = [
                f"Pin {index + 1}: `{self.lock_bar(value)}` **{value}/5**"
                for index, value in enumerate(self.lock_pins)
            ]
            lines = [
                f"{Q_DUNGEON} **Q DUNGEON**",
                "─────────────────",
                self.status_text(),
                "",
                f"**{Q_DUNGEON_KEY} Gate Lockpick**",
                "Raise the pins, then test the lock. Hints tell you if each pin is low, high, or set.",
                f"Tests: **{self.lock_tries}/3**",
                *pin_lines,
                f"> {self.lock_hint}",
            ]
            if result_text:
                lines.extend(["", result_text])
            return "\n".join(lines)

        def lock_bar(self, value):
            return "▰" * value + "▱" * (5 - value)

        def render_duel(self, result_text=None):
            clue = {
                "high": "The guard lifts its blade high.",
                "low": "The guard crouches low.",
                "charge": "The guard starts a straight charge.",
            }.get(self.duel_stance, "Read the guard, then answer.")
            lines = [
                f"{Q_DUNGEON} **Q DUNGEON**",
                "─────────────────",
                self.status_text(),
                "",
                f"**{Q_DUNGEON_MONSTER} Guard Duel**",
                "Win 2 reads before taking 2 bad hits.",
                f"Hits: **{self.duel_hits}/2** | Bad Reads: **{self.duel_misses}/2**",
                f"> {clue}",
                "High blade = Dodge | Low crouch = Strike | Charge = Guard",
            ]
            if result_text:
                lines.extend(["", result_text])
            return "\n".join(lines)

        def render_trap_disarm(self, result_text=None):
            sequence = " -> ".join(label for _, label in self.trap_sequence)
            lines = [
                f"{Q_DUNGEON} **Q DUNGEON**",
                "─────────────────",
                self.status_text(),
                "",
                f"**{Q_TOWER_TRAP} Wire Sequence**",
                "Cut the wires in the shown order. One wrong cut resets the sequence. Two wrong cuts triggers the trap.",
                f"Sequence: **{sequence}**",
                f"Progress: **{self.trap_progress}/{len(self.trap_sequence)}** | Mistakes: **{self.trap_mistakes}/2**",
                f"> {self.trap_hint}",
            ]
            if result_text:
                lines.extend(["", result_text])
            return "\n".join(lines)

        def refresh_buttons(self):
            self.clear_items()
            if self.room == "chest":
                choices = [
                    ("open", "Open Chest", discord.ButtonStyle.success),
                    ("inspect", "Inspect", discord.ButtonStyle.primary),
                    ("leave", "Take Scraps", discord.ButtonStyle.secondary),
                ]
            elif self.room == "monster":
                choices = [
                    ("fight", "Fight", discord.ButtonStyle.danger),
                    ("sneak", "Sneak", discord.ButtonStyle.primary),
                    ("key", "Use Key", discord.ButtonStyle.success),
                ]
            elif self.room == "trap":
                choices = [
                    ("disarm", "Disarm", discord.ButtonStyle.primary),
                    ("dash", "Dash", discord.ButtonStyle.danger),
                    ("careful", "Careful Step", discord.ButtonStyle.secondary),
                ]
            elif self.room == "shrine":
                choices = [
                    ("pray", "Pray", discord.ButtonStyle.primary),
                    ("relic", "Take Relic", discord.ButtonStyle.success),
                    ("rest", "Rest", discord.ButtonStyle.secondary),
                ]
            elif self.room == "locked":
                choices = [
                    ("pick", "Pick Lock", discord.ButtonStyle.primary),
                    ("force", "Force Door", discord.ButtonStyle.danger),
                    ("key", "Use Key", discord.ButtonStyle.success),
                ]
            else:
                choices = [
                    ("strike", "Rune Clash", discord.ButtonStyle.danger),
                    ("guard", "Brace", discord.ButtonStyle.primary),
                    ("key", "Use Key", discord.ButtonStyle.success),
                ]
            for key, label, style in choices:
                self.add_item(DungeonChoiceButton(key, label, style))

        def refresh_lock_buttons(self):
            self.clear_items()
            for index in range(3):
                self.add_item(DungeonChoiceButton(f"lock_pin_{index}", f"Pin {index + 1}", discord.ButtonStyle.secondary))
            self.add_item(DungeonChoiceButton("lock_test", "Test", discord.ButtonStyle.success))
            self.add_item(DungeonChoiceButton("lock_reset", "Reset", discord.ButtonStyle.danger))

        def refresh_duel_buttons(self):
            self.clear_items()
            self.add_item(DungeonChoiceButton("duel_strike", "Strike", discord.ButtonStyle.danger))
            self.add_item(DungeonChoiceButton("duel_guard", "Guard", discord.ButtonStyle.primary))
            self.add_item(DungeonChoiceButton("duel_dodge", "Dodge", discord.ButtonStyle.success))

        def refresh_trap_buttons(self):
            self.clear_items()
            self.add_item(DungeonChoiceButton("trap_wire_blue", "Blue Wire", discord.ButtonStyle.primary))
            self.add_item(DungeonChoiceButton("trap_wire_cyan", "Cyan Wire", discord.ButtonStyle.success))
            self.add_item(DungeonChoiceButton("trap_wire_gold", "Gold Wire", discord.ButtonStyle.secondary))
            self.add_item(DungeonChoiceButton("trap_restart", "Restart", discord.ButtonStyle.danger))

        def render_boss_clash(self, result_text=None):
            sequence = " -> ".join(label for _, label in self.boss_sequence)
            lines = [
                f"{Q_DUNGEON} **Q DUNGEON**",
                "─────────────────",
                self.status_text(),
                "",
                f"**{Q_DUNGEON} Rune Clash**",
                "Press the runes in order. Two wrong presses seals the door.",
                f"Sequence: **{sequence}**",
                f"Progress: **{self.boss_progress}/{len(self.boss_sequence)}** | Mistakes: **{self.boss_mistakes}/2**",
                f"> {self.boss_hint}",
            ]
            if result_text:
                lines.extend(["", result_text])
            return "\n".join(lines)

        def refresh_boss_buttons(self):
            self.clear_items()
            self.add_item(DungeonChoiceButton("boss_heart", "Heart Rune", discord.ButtonStyle.danger))
            self.add_item(DungeonChoiceButton("boss_key", "Key Rune", discord.ButtonStyle.primary))
            self.add_item(DungeonChoiceButton("boss_relic", "Relic Rune", discord.ButtonStyle.success))
            self.add_item(DungeonChoiceButton("boss_restart", "Restart", discord.ButtonStyle.secondary))

        def roll(self, chance):
            bonus = min(0.10, active_luck_bonus(self.data) / 2)
            return random.random() < min(0.95, chance + bonus)

        def change_hp(self, amount):
            self.hp = max(0, min(DUNGEON_MAX_HP, self.hp + int(amount)))

        def apply_choice(self, choice):
            if self.room == "chest":
                if choice == "open":
                    gain = random.randint(18_000, 38_000)
                    self.loot += gain
                    text = f"{Q_SUCCESS} Chest opened: +**{format_balance(gain)}**."
                    if random.random() < 0.25:
                        self.change_hp(-1)
                        text += " A hidden spike hit you for **1 HP**."
                    if random.random() < 0.15:
                        self.keys += 1
                        text += f" Found {Q_DUNGEON_KEY} **1 key**."
                    return text, True, False
                if choice == "inspect":
                    gain = random.randint(10_000, 18_000)
                    self.loot += gain
                    if random.random() < 0.35:
                        self.keys += 1
                        return f"{Q_DUNGEON_KEY} You inspected safely: +**{format_balance(gain)}** and found **1 key**.", True, False
                    return f"{Q_SUCCESS} You inspected safely: +**{format_balance(gain)}**.", True, False
                gain = 5_000
                self.loot += gain
                return f"{Q_SUCCESS} You took loose scraps: +**{format_balance(gain)}**.", True, False

            if self.room == "monster":
                if choice == "fight":
                    if self.roll(0.64):
                        gain = random.randint(22_000, 38_000)
                        self.loot += gain
                        if random.random() < 0.12:
                            self.relics += 1
                            return f"{Q_SUCCESS} Guard defeated: +**{format_balance(gain)}** and **1 relic**.", True, False
                        return f"{Q_SUCCESS} Guard defeated: +**{format_balance(gain)}**.", True, False
                    self.change_hp(-2)
                    return f"{Q_DENIED} The guard punished the attack. Lost **2 HP**.", True, False
                if choice == "sneak":
                    if self.roll(0.72):
                        gain = random.randint(8_000, 18_000)
                        self.loot += gain
                        return f"{Q_SUCCESS} You slipped past and lifted +**{format_balance(gain)}**.", True, False
                    self.change_hp(-1)
                    return f"{Q_DENIED} You were spotted. Lost **1 HP**.", True, False
                if self.keys > 0:
                    self.keys -= 1
                    gain = 25_000
                    self.loot += gain
                    return f"{Q_DUNGEON_KEY} Key flash stunned the guard: +**{format_balance(gain)}**.", True, False
                self.change_hp(-1)
                return f"{Q_DENIED} No key. The guard clipped you for **1 HP**.", True, False

            if self.room == "trap":
                if choice == "disarm":
                    if self.roll(0.60):
                        gain = 15_000
                        self.loot += gain
                        if random.random() < 0.20:
                            self.keys += 1
                            return f"{Q_SUCCESS} Trap disarmed: +**{format_balance(gain)}** and **1 key**.", True, False
                        return f"{Q_SUCCESS} Trap disarmed: +**{format_balance(gain)}**.", True, False
                    self.change_hp(-1)
                    return f"{Q_DENIED} Bad wire. Lost **1 HP**.", True, False
                if choice == "dash":
                    if self.roll(0.45):
                        gain = 28_000
                        self.loot += gain
                        return f"{Q_SUCCESS} Clean dash: +**{format_balance(gain)}**.", True, False
                    self.change_hp(-2)
                    return f"{Q_DENIED} The trap caught you. Lost **2 HP**.", True, False
                gain = 7_000
                self.loot += gain
                if random.random() < 0.10:
                    self.change_hp(-1)
                    return f"{Q_WARNING} Careful path: +**{format_balance(gain)}**, but lost **1 HP**.", True, False
                return f"{Q_SUCCESS} Careful path: +**{format_balance(gain)}**.", True, False

            if self.room == "shrine":
                if choice == "pray":
                    outcome = random.choice(("heal", "relic", "loot", "hurt"))
                    if outcome == "heal":
                        self.change_hp(1)
                        return f"{Q_SUCCESS} The shrine healed **1 HP**.", True, False
                    if outcome == "relic":
                        self.relics += 1
                        return f"{Q_DUNGEON_RELIC} The shrine gave **1 relic**.", True, False
                    if outcome == "loot":
                        gain = 18_000
                        self.loot += gain
                        return f"{Q_SUCCESS} The shrine gave +**{format_balance(gain)}**.", True, False
                    self.change_hp(-1)
                    return f"{Q_DENIED} The shrine rejected you. Lost **1 HP**.", True, False
                if choice == "relic":
                    self.relics += 1
                    text = f"{Q_DUNGEON_RELIC} Relic taken."
                    if random.random() < 0.35:
                        self.change_hp(-1)
                        text += " The shrine burned **1 HP**."
                    return text, True, False
                if self.rests > 0:
                    self.rests -= 1
                    self.change_hp(2)
                    return f"{Q_SUCCESS} You rested and recovered **2 HP**. Rests left: **{self.rests}**.", True, False
                gain = 5_000
                self.loot += gain
                return f"{Q_WARNING} No rests left. You took +**{format_balance(gain)}** incense coins instead.", True, False

            if self.room == "locked":
                if choice == "pick":
                    if self.roll(0.55):
                        gain = 20_000
                        self.loot += gain
                        return f"{Q_SUCCESS} Lock picked: +**{format_balance(gain)}**.", True, False
                    self.change_hp(-1)
                    return f"{Q_DENIED} Lock snapped back. Lost **1 HP**.", True, False
                if choice == "force":
                    if self.roll(0.40):
                        gain = 35_000
                        self.loot += gain
                        return f"{Q_SUCCESS} Gate forced open: +**{format_balance(gain)}**.", True, False
                    self.change_hp(-2)
                    return f"{Q_DENIED} The gate slammed shut. Lost **2 HP**.", True, False
                if self.keys > 0:
                    self.keys -= 1
                    gain = 30_000
                    self.loot += gain
                    return f"{Q_DUNGEON_KEY} Key used: +**{format_balance(gain)}**.", True, False
                self.change_hp(-1)
                return f"{Q_DENIED} No key. The lock bit you for **1 HP**.", True, False

            if choice == "strike":
                return f"{Q_DUNGEON} The runes wake up.", False, False
            if choice == "guard":
                if self.roll(0.75):
                    gain = 25_000
                    self.loot += gain
                    return f"{Q_SUCCESS} You outlasted the final door: +**{format_balance(gain)}**.", True, True
                self.change_hp(-1)
                return f"{Q_DENIED} Guard broke. Lost **1 HP**.", True, False
            if self.keys > 0:
                self.keys -= 1
                gain = 40_000
                self.loot += gain
                return f"{Q_DUNGEON_KEY} Master key opened the final door: +**{format_balance(gain)}**.", True, True
            self.change_hp(-1)
            return f"{Q_DENIED} No key for the final door. Lost **1 HP**.", True, False

        async def start_lockpick(self, interaction):
            self.lock_active = True
            self.lock_target = [random.randint(1, 5) for _ in range(3)]
            self.lock_pins = [3, 3, 3]
            self.lock_tries = 0
            self.lock_hint = "Adjust the pins, then test the gate."
            self.refresh_lock_buttons()
            await interaction.response.edit_message(content=self.render_lockpick(), view=self)

        async def handle_lockpick(self, interaction, choice):
            if choice.startswith("lock_pin_"):
                index = int(choice.rsplit("_", 1)[1])
                self.lock_pins[index] = 1 if self.lock_pins[index] >= 5 else self.lock_pins[index] + 1
                self.lock_hint = f"Pin {index + 1} raised."
                await interaction.response.edit_message(content=self.render_lockpick(), view=self)
                return
            if choice == "lock_reset":
                self.lock_pins = [1, 1, 1]
                self.lock_hint = "Pins reset to 1."
                await interaction.response.edit_message(content=self.render_lockpick(), view=self)
                return
            if choice != "lock_test":
                await interaction.response.defer()
                return
            self.lock_tries += 1
            if self.lock_pins == self.lock_target:
                self.lock_active = False
                gain = 24_000
                self.loot += gain
                await self.advance_room(
                    interaction,
                    f"{Q_SUCCESS} Gate picked cleanly: +**{format_balance(gain)}**."
                )
                return
            hints = []
            for index, (current, goal) in enumerate(zip(self.lock_pins, self.lock_target), 1):
                if current == goal:
                    hints.append(f"P{index} set")
                elif current < goal:
                    hints.append(f"P{index} low")
                else:
                    hints.append(f"P{index} high")
            self.lock_hint = " | ".join(hints)
            if self.lock_tries >= 3:
                self.lock_active = False
                self.change_hp(-1)
                await self.advance_room(
                    interaction,
                    f"{Q_DENIED} The lock snapped shut. Code was **{'-'.join(map(str, self.lock_target))}**. Lost **1 HP**."
                )
                return
            await interaction.response.edit_message(content=self.render_lockpick(), view=self)

        def next_duel_stance(self):
            self.duel_stance = random.choice(("high", "low", "charge"))

        async def start_duel(self, interaction):
            self.duel_active = True
            self.duel_hits = 0
            self.duel_misses = 0
            self.next_duel_stance()
            self.refresh_duel_buttons()
            await interaction.response.edit_message(content=self.render_duel(), view=self)

        async def handle_duel(self, interaction, choice):
            answers = {
                "high": "duel_dodge",
                "low": "duel_strike",
                "charge": "duel_guard",
            }
            if choice not in {"duel_strike", "duel_guard", "duel_dodge"}:
                await interaction.response.defer()
                return
            if answers.get(self.duel_stance) == choice:
                self.duel_hits += 1
                if self.duel_hits >= 2:
                    self.duel_active = False
                    gain = random.randint(24_000, 40_000)
                    self.loot += gain
                    bonus_text = ""
                    if random.random() < 0.15:
                        self.relics += 1
                        bonus_text = f" and {Q_DUNGEON_RELIC} **1 relic**"
                    await self.advance_room(
                        interaction,
                        f"{Q_SUCCESS} You read the guard perfectly: +**{format_balance(gain)}**{bonus_text}."
                    )
                    return
                self.next_duel_stance()
                await interaction.response.edit_message(
                    content=self.render_duel(f"{Q_SUCCESS} Clean read. One more."),
                    view=self,
                )
                return
            self.duel_misses += 1
            self.change_hp(-1)
            if self.hp <= 0:
                await self.finish(interaction, False, f"{Q_DENIED} Bad read. Lost **1 HP**.\n{Q_DENIED} You were knocked out. No loot escaped.")
                return
            if self.duel_misses >= 2:
                self.duel_active = False
                await self.advance_room(
                    interaction,
                    f"{Q_DENIED} The guard forced you back. Lost **1 HP**."
                )
                return
            self.next_duel_stance()
            await interaction.response.edit_message(
                content=self.render_duel(f"{Q_DENIED} Bad read. Lost **1 HP**."),
                view=self,
            )

        async def start_trap_disarm(self, interaction):
            self.trap_active = True
            wires = [
                ("trap_wire_blue", "Blue"),
                ("trap_wire_cyan", "Cyan"),
                ("trap_wire_gold", "Gold"),
            ]
            self.trap_sequence = [random.choice(wires) for _ in range(3)]
            self.trap_progress = 0
            self.trap_mistakes = 0
            self.trap_hint = "Start with the first wire in the sequence."
            self.refresh_trap_buttons()
            await interaction.response.edit_message(content=self.render_trap_disarm(), view=self)

        async def handle_trap_disarm(self, interaction, choice):
            if choice == "trap_restart":
                self.trap_progress = 0
                self.trap_hint = "Sequence restarted."
                await interaction.response.edit_message(content=self.render_trap_disarm(), view=self)
                return
            if not choice.startswith("trap_wire_"):
                await interaction.response.defer()
                return
            expected_key, expected_label = self.trap_sequence[self.trap_progress]
            pressed_label = {
                "trap_wire_blue": "Blue",
                "trap_wire_cyan": "Cyan",
                "trap_wire_gold": "Gold",
            }.get(choice, "Unknown")
            if choice == expected_key:
                self.trap_progress += 1
                if self.trap_progress < len(self.trap_sequence):
                    next_label = self.trap_sequence[self.trap_progress][1]
                    self.trap_hint = f"{Q_SUCCESS} {pressed_label} cut. Next: **{next_label}**."
                    await interaction.response.edit_message(content=self.render_trap_disarm(), view=self)
                    return
                self.trap_active = False
                gain = 20_000
                self.loot += gain
                if random.random() < 0.25:
                    self.keys += 1
                    await self.advance_room(
                        interaction,
                        f"{Q_SUCCESS} Trap disarmed: +**{format_balance(gain)}** and {Q_DUNGEON_KEY} **1 key**."
                    )
                    return
                await self.advance_room(
                    interaction,
                    f"{Q_SUCCESS} Trap disarmed: +**{format_balance(gain)}**."
                )
                return
            self.trap_mistakes += 1
            self.trap_progress = 0
            if self.trap_mistakes >= 2:
                self.trap_active = False
                self.change_hp(-1)
                sequence = " -> ".join(label for _, label in self.trap_sequence)
                await self.advance_room(
                    interaction,
                    f"{Q_DENIED} Wrong wire. Sequence was **{sequence}**. Lost **1 HP**."
                )
                return
            self.trap_hint = f"{Q_DENIED} Wrong wire. You cut **{pressed_label}**, needed **{expected_label}**. Start over."
            await interaction.response.edit_message(content=self.render_trap_disarm(), view=self)

        async def start_boss_clash(self, interaction):
            self.boss_active = True
            runes = [
                ("boss_heart", "Heart"),
                ("boss_key", "Key"),
                ("boss_relic", "Relic"),
            ]
            self.boss_sequence = [random.choice(runes) for _ in range(4)]
            self.boss_progress = 0
            self.boss_mistakes = 0
            self.boss_hint = "Start with the first rune."
            self.refresh_boss_buttons()
            await interaction.response.edit_message(content=self.render_boss_clash(), view=self)

        async def handle_boss_clash(self, interaction, choice):
            if choice == "boss_restart":
                self.boss_progress = 0
                self.boss_hint = "Rune sequence restarted."
                await interaction.response.edit_message(content=self.render_boss_clash(), view=self)
                return
            if not choice.startswith("boss_"):
                await interaction.response.defer()
                return
            expected_key, expected_label = self.boss_sequence[self.boss_progress]
            pressed_label = {
                "boss_heart": "Heart",
                "boss_key": "Key",
                "boss_relic": "Relic",
            }.get(choice, "Unknown")
            if choice == expected_key:
                self.boss_progress += 1
                if self.boss_progress < len(self.boss_sequence):
                    self.boss_hint = f"{Q_SUCCESS} {pressed_label} rune lit. Next: **{self.boss_sequence[self.boss_progress][1]}**."
                    await interaction.response.edit_message(content=self.render_boss_clash(), view=self)
                    return
                self.boss_active = False
                gain = 60_000
                self.loot += gain
                await self.finish(interaction, True, f"{Q_SUCCESS} Rune clash solved: +**{format_balance(gain)}**.")
                return
            self.boss_mistakes += 1
            self.boss_progress = 0
            if self.boss_mistakes >= 2:
                self.boss_active = False
                self.change_hp(-2)
                sequence = " -> ".join(label for _, label in self.boss_sequence)
                if self.hp <= 0:
                    await self.finish(interaction, False, f"{Q_DENIED} Wrong rune. Sequence was **{sequence}**. You were knocked out.")
                    return
                await self.finish(interaction, False, f"{Q_DENIED} Wrong rune. Sequence was **{sequence}**. The final door stayed sealed.")
                return
            self.boss_hint = f"{Q_DENIED} Wrong rune. You pressed **{pressed_label}**, needed **{expected_label}**. Start over."
            await interaction.response.edit_message(content=self.render_boss_clash(), view=self)

        async def advance_room(self, interaction, result_text):
            if self.hp <= 0:
                await self.finish(interaction, False, f"{result_text}\n{Q_DENIED} You were knocked out. No loot escaped.")
                return
            self.depth += 1
            self.room = "boss" if self.depth >= DUNGEON_MAX_DEPTH else random.choice(DUNGEON_ROOMS)
            self.refresh_buttons()
            await interaction.response.edit_message(content=self.render(result_text), view=self)

        async def choose(self, interaction, choice):
            if self.finished:
                await interaction.response.send_message("This dungeon run is already finished.", ephemeral=True)
                return
            if self.lock_active:
                await self.handle_lockpick(interaction, choice)
                return
            if self.duel_active:
                await self.handle_duel(interaction, choice)
                return
            if self.trap_active:
                await self.handle_trap_disarm(interaction, choice)
                return
            if self.boss_active:
                await self.handle_boss_clash(interaction, choice)
                return
            if self.room == "locked" and choice == "pick":
                await self.start_lockpick(interaction)
                return
            if self.room == "monster" and choice == "fight":
                await self.start_duel(interaction)
                return
            if self.room == "trap" and choice == "disarm":
                await self.start_trap_disarm(interaction)
                return
            if self.room == "boss" and choice == "strike":
                await self.start_boss_clash(interaction)
                return
            result_text, advance, cleared = self.apply_choice(choice)
            if self.hp <= 0:
                await self.finish(interaction, False, f"{result_text}\n{Q_DENIED} You were knocked out. No loot escaped.")
                return
            if self.room == "boss":
                if cleared:
                    await self.finish(interaction, True, result_text)
                else:
                    await self.finish(interaction, False, f"{result_text}\n{Q_DENIED} The final room stayed sealed. No loot escaped.")
                return
            if advance:
                await self.advance_room(interaction, result_text)
                return

        async def finish(self, interaction, cleared, result_text):
            self.finished = True
            self.clear_items()
            if not interaction.response.is_done():
                await interaction.response.defer()
            reward = 0
            balance_line = ""
            achievements = []
            if cleared:
                reward = min(DUNGEON_REWARD_CAP, self.loot + self.relics * DUNGEON_RELIC_VALUE + DUNGEON_CLEAR_BONUS)
                def settle_dungeon_clear():
                    old_balance, new_balance = add_user_balance(self.user_id, reward, earned_delta=reward)
                    log_transaction(self.user_id, "dungeon_clear", reward, "Dungeon clear reward")
                    stats = record_game_result(self.user_id, "dungeon", True, reward, reward)
                    achievements = maybe_award_game_achievements(self.user_id, "dungeon", stats)
                    return old_balance, new_balance, achievements
                old_balance, new_balance, achievements = await asyncio.to_thread(settle_dungeon_clear)
                balance_line = f"\nReward: **{format_balance(reward)}**\nBalance: **{format_balance(old_balance)}** -> **{format_balance(new_balance)}**"
            else:
                await asyncio.to_thread(record_game_result, self.user_id, "dungeon", False, 0, 0)
            final_text = (
                f"{Q_DUNGEON} **Q DUNGEON**\n"
                "─────────────────\n"
                f"{self.status_text()}\n\n"
                f"{result_text}{balance_line}"
                f"{achievement_reward_text(achievements)}"
            )
            await interaction.message.edit(content=final_text, view=None)

        async def on_timeout(self):
            if self.finished:
                return
            self.finished = True
            self.clear_items()
            try:
                await asyncio.to_thread(record_game_result, self.user_id, "dungeon", False, 0, 0)
                await self.message.edit(
                    content=(
                        f"{Q_DUNGEON} **Q DUNGEON**\n"
                        "─────────────────\n"
                        f"{self.status_text()}\n\n"
                        f"{Q_TIMEOUT} Dungeon run expired. No loot escaped."
                    ),
                    view=None,
                )
            except Exception:
                pass

    view = DungeonView()
    view.message = await ctx.reply(
        view.render(),
        view=view,
        mention_author=False,
        allowed_mentions=discord.AllowedMentions.none(),
    )

@commands.command(name="gamestats", aliases=["gstats", "gamestat", "playstats"])
async def gamestats(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return
    user = member or ctx.author
    try:
        rows, data = await asyncio.gather(
            asyncio.to_thread(get_game_stats, user.id),
            asyncio.to_thread(get_user, user.id),
        )
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    embed = discord.Embed(
        title=f"{Q_GAME_STATS} Game Stats",
        description=user_mention(user.id),
        color=discord.Color.green()
    )
    if not rows:
        embed.add_field(name="Games", value="No tracked game stats yet.", inline=False)
    else:
        total_played = sum(int(row["played"] or 0) for row in rows)
        total_wins = sum(int(row["wins"] or 0) for row in rows)
        total_profit = sum(int(row["profit"] or 0) for row in rows)
        embed.add_field(name="Total", value=f"Played: **{total_played:,}**\nWins: **{total_wins:,}**\nProfit: **{format_balance(total_profit)}**", inline=False)
        lines = []
        for row in rows[:12]:
            played = int(row["played"] or 0)
            wins = int(row["wins"] or 0)
            losses = int(row["losses"] or 0)
            win_rate = (wins / played * 100) if played else 0
            lines.append(
                f"**{game_display_name(row['game_key'])}** - {wins:,}W/{losses:,}L, {win_rate:.1f}% win, {format_balance(int(row['profit'] or 0))}"
            )
        add_split_embed_field(embed, "By Game", lines, inline=False)
    achievements = achievement_ids(data)
    game_badges = [GAME_ACHIEVEMENTS[achievement_id]["name"] for achievement_id in achievements if achievement_id in GAME_ACHIEVEMENTS]
    add_split_embed_field(embed, "Badges", [", ".join(game_badges[:20])] if game_badges else ["No game badges yet."], inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="achievements", aliases=["achievement", "badges", "achs"])
async def achievements(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return
    user = member or ctx.author
    try:
        data, rows = await asyncio.gather(
            asyncio.to_thread(get_user, user.id),
            asyncio.to_thread(get_game_stats, user.id),
        )
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    owned = set(achievement_ids(data))
    per_game = {row["game_key"]: row for row in rows}
    totals = {
        "played": sum(int(row["played"] or 0) for row in rows),
        "wins": sum(int(row["wins"] or 0) for row in rows),
        "losses": sum(int(row["losses"] or 0) for row in rows),
    }
    closest = []
    embed = discord.Embed(
        title=f"{Q_ACHIEVEMENT_UNLOCKED} Achievements",
        description=user_mention(user.id),
        color=discord.Color.gold(),
    )
    grouped_lines = defaultdict(list)
    for achievement_id, achievement in GAME_ACHIEVEMENTS.items():
        if achievement["game"] is None:
            progress = totals.get(achievement["field"], 0)
        else:
            row = per_game.get(achievement["game"], {})
            progress = int(row.get(achievement["field"], 0) or 0)
        target = int(achievement["target"])
        status = Q_ACHIEVEMENT_UNLOCKED if achievement_id in owned else Q_ACHIEVEMENT_LOCKED
        if achievement_id not in owned:
            closest.append((progress / target if target else 0, achievement["name"], progress, target, achievement["reward"]))
        tier = achievement.get("tier") or "Standard"
        grouped_lines[tier].append(
            f"{status} **{achievement['name']}** - {min(progress, target):,}/{target:,} "
            f"({format_balance(achievement['reward'])})"
        )
    tier_order = ["Bronze", "Standard", "Gold", "Royal"]
    for tier in tier_order:
        lines = grouped_lines.get(tier) or []
        if lines:
            add_split_embed_field(embed, f"{tier} Badges", lines, inline=False)
    if closest:
        closest.sort(reverse=True)
        close_lines = [
            f"**{name}** - {progress:,}/{target:,} ({format_balance(reward)})"
            for _, name, progress, target, reward in closest[:3]
        ]
        embed.add_field(name="Closest", value="\n".join(close_lines), inline=False)

    class AchievementView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)

        async def interaction_check(self, interaction):
            if interaction.user.id == ctx.author.id:
                return True
            await interaction.response.send_message("Open your own achievements with `.achievements`.", ephemeral=True)
            return False

        @discord.ui.button(label="Guide", style=discord.ButtonStyle.secondary)
        async def guide_button(self, interaction, button):
            await interaction.response.send_message(
                f"{Q_BADGE} Achievements are long-term badges. Win games, build play counts, and rewards pay automatically when a game result unlocks one. Use `.setbadge` to show up to 3 earned badges on your profile.",
                ephemeral=True,
            )

        @discord.ui.button(label="Profile Badges", style=discord.ButtonStyle.primary)
        async def badge_button(self, interaction, button):
            await interaction.response.send_message("Use `.setbadge` to choose which earned badges show on your profile.", ephemeral=True)

    await ctx.send(embed=embed, view=AchievementView(), allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="setbadge", aliases=["badge", "equipbadge", "profilebadge"])
async def setbadge(ctx, *, badge_ids: str = None):
    if not await ensure_db_ready(ctx):
        return
    try:
        data = await asyncio.to_thread(get_user, ctx.author.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    owned = achievement_ids(data)
    if not badge_ids:
        if not owned:
            await ctx.reply(f"{Q_DENIED} You have no earned badges yet.", mention_author=False)
            return
        lines = [
            f"`{badge_id}` - {achievement_display(badge_id)}"
            for badge_id in owned
            if badge_id in GAME_ACHIEVEMENTS
        ]
        await ctx.reply(
            f"{Q_ACHIEVEMENT_UNLOCKED} Use `.setbadge <badge id> [badge id] [badge id]` to show up to 3 on your profile, or `.setbadge clear`.\n"
            + "\n".join(lines[:20]),
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return
    raw = [part.strip() for part in re.split(r"[,\s]+", badge_ids) if part.strip()]
    if raw and raw[0].casefold() in {"clear", "none", "reset"}:
        await asyncio.to_thread(update_user, ctx.author.id, equipped_badges=[])
        await ctx.reply(f"{Q_SUCCESS} Profile badges cleared.", mention_author=False)
        return
    selected = []
    for badge_id in raw:
        if badge_id not in owned or badge_id not in GAME_ACHIEVEMENTS:
            await ctx.reply(f"{Q_DENIED} You have not earned `{badge_id}`.", mention_author=False)
            return
        if badge_id not in selected:
            selected.append(badge_id)
    selected = selected[:3]
    await asyncio.to_thread(update_user, ctx.author.id, equipped_badges=selected)
    label = " | ".join(achievement_display(badge_id) for badge_id in selected) if selected else "None"
    await ctx.reply(f"{Q_SUCCESS} Profile badges set: **{label}**", mention_author=False, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="streaks", aliases=["streak", "claimstreaks"])
async def streaks(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return
    user = member or ctx.author
    try:
        data = await asyncio.to_thread(get_user, user.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    embed = discord.Embed(
        title=f"{Q_STREAK_FIRE} Streaks",
        description=user_mention(user.id),
        color=discord.Color.gold(),
    )
    embed.add_field(name="Daily", value=f"{plural_unit(data['daily_streak'], 'day')}\nNext: {claim_cooldown_text(data.get('last_daily'), 86400)}", inline=True)
    embed.add_field(name="Weekly", value=f"{plural_unit(data['weekly_streak'], 'week')}\nNext: {claim_cooldown_text(data.get('last_weekly'), 604800)}", inline=True)
    embed.add_field(name="Monthly", value=f"{plural_unit(data['monthly_streak'], 'month')}\nNext: {claim_cooldown_text(data.get('last_monthly'), 2592000)}", inline=True)
    gamble_streak = int(data.get("gamble_streak", 0) or 0)
    embed.add_field(
        name="Gambling",
        value=f"{gamble_streak} win(s)\nPayout: ×{payout_multiplier(data, gamble_streak):.3f}" if gamble_streak else "No active gambling streak.",
        inline=False,
    )
    await ctx.reply(embed=embed, mention_author=False, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="dailychallenge", aliases=["challenge", "dc", "qchallenge"])
async def dailychallenge(ctx, action: str = None):
    if not await ensure_db_ready(ctx):
        return
    try:
        if action and action.casefold() in {"claim", "collect", "reward"}:
            result = await asyncio.to_thread(claim_daily_challenge, ctx.author.id)
            ok, reason, challenge, progress, balance = result[:5]
            if ok:
                challenge_streak = result[5] if len(result) > 5 else 1
                streak_bonus = result[6] if len(result) > 6 else 0
                bonus_line = f"\nStreak: **{challenge_streak} day(s)**"
                if streak_bonus:
                    bonus_line += f" | Bonus: **{format_balance(streak_bonus)}**"
                await ctx.reply(
                    f"{Q_SUCCESS} Claimed **{challenge['name']}** for **{format_balance(challenge['reward'])}**.\n"
                    f"{bonus_line}\n"
                    f"New Balance: **{format_balance(balance)}**",
                    mention_author=False,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                return
            if reason == "claimed":
                await ctx.reply(f"{Q_DENIED} You already claimed today's challenge.", mention_author=False)
                return
            await ctx.reply(
                f"{Q_TIMER_TICK} Not finished yet: **{progress:,}/{challenge['target']:,}**.",
                mention_author=False,
            )
            return
        challenge, progress, claimed = await asyncio.to_thread(get_daily_challenge_status, ctx.author.id)
        challenge_streak = await asyncio.to_thread(get_daily_challenge_streak, ctx.author.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    target = int(challenge["target"])
    reset = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    status = "Claimed" if claimed else ("Ready to claim" if progress >= target else "In progress")
    embed = discord.Embed(
        title=f"{Q_QUEST} Daily Challenge",
        description=(
            f"**{challenge['name']}**\n"
            f"Progress: **{min(progress, target):,}/{target:,}**\n"
            f"Reward: **{format_balance(challenge['reward'])}**\n"
            f"Challenge Streak: **{challenge_streak} day(s)**\n"
            f"Status: **{status}**\n"
            f"Resets {discord_relative_time(reset)}"
        ),
        color=discord.Color.green(),
    )
    embed.set_footer(text=f"Use {getattr(ctx, 'prefix', '.')}dailychallenge claim when it is ready.")
    await ctx.reply(embed=embed, mention_author=False, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="gamebalance", aliases=["balancegames", "risks", "gamerisks"])
async def gamebalance(ctx):
    lines = []
    risk_counts = Counter()
    for game_key in GAME_DISPLAY_NAMES:
        risk_counts[risk_label(game_key)] += 1
        lines.append(f"**{game_display_name(game_key)}** - Risk: **{risk_label(game_key)}**")
    embed = discord.Embed(
        title=f"{Q_WARNING} Game Balance",
        description="Risk labels are the quick balance audit for current 𝚀𝚞𝚎wo games. Higher risk means harder wins or larger loss swings.",
        color=discord.Color.orange(),
    )
    summary = [
        f"**{label}**: {count}"
        for label, count in sorted(risk_counts.items(), key=lambda item: item[0])
    ]
    embed.add_field(name="Risk Mix", value=", ".join(summary) or "No games found.", inline=False)
    embed.add_field(
        name="Balance Checks",
        value=(
            f"- Max standard bet: **{format_balance(MAX_BET)}**\n"
            "- Wins use the universal gambling streak bonus.\n"
            "- Use `.gameaudit` after real play to catch games that are too generous or too harsh.\n"
            "- Use `.balanceaudit` for the wider economy flow."
        ),
        inline=False,
    )
    add_split_embed_field(embed, "Games", lines, inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="gameaudit", aliases=["gaudit", "auditgames"])
async def gameaudit(ctx, days: int = 7):
    if not await ensure_db_ready(ctx):
        return
    if not has_economy_owner_power(ctx.author.id, ctx.guild):
        return await ctx.send(f"{Q_DENIED} Server owner or admin only.")
    days = max(1, min(int(days or 7), 30))
    try:
        rows = await asyncio.to_thread(get_game_audit_rows, days)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    embed = discord.Embed(
        title=f"{Q_GAME_AUDIT} Game Audit",
        description=f"Last **{days} day(s)**. Positive house profit means users lost more than they won.",
        color=discord.Color.orange(),
    )
    if not rows:
        embed.add_field(name="Games", value="No tracked games in this window.", inline=False)
    else:
        lines = []
        warnings = []
        for row in rows:
            plays = int(row["plays"] or 0)
            wins = int(row["wins"] or 0)
            losses = int(row["losses"] or 0)
            user_profit = int(row["profit"] or 0)
            house_profit = -user_profit
            win_rate = (wins / max(1, wins + losses)) * 100
            flag = ""
            if plays >= 20 and win_rate >= 60 and user_profit > 0:
                flag = " too generous"
                warnings.append(f"**{game_display_name(row['game_key'])}** may be too generous ({win_rate:.1f}% win, users +{format_balance(user_profit)}).")
            elif plays >= 20 and win_rate <= 12 and house_profit > 0:
                flag = " too harsh"
                warnings.append(f"**{game_display_name(row['game_key'])}** may be too harsh ({win_rate:.1f}% win).")
            lines.append(
                f"{risk_emoji(row['game_key'])} **{game_display_name(row['game_key'])}** - "
                f"{plays:,} plays, {win_rate:.1f}% win, house {format_balance(house_profit)}{flag}"
            )
        add_split_embed_field(embed, "Games", lines[:20], inline=False)
        if warnings:
            add_split_embed_field(embed, "Signals", warnings[:8], inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="balanceaudit", aliases=["balaudit", "economybalance", "balancing"])
async def balanceaudit(ctx, days: int = 14):
    if not await ensure_db_ready(ctx):
        return
    if not has_economy_owner_power(ctx.author.id, ctx.guild):
        return await ctx.send(f"{Q_DENIED} Server owner or admin only.")
    days = max(1, min(int(days or 14), 30))
    try:
        rows = await asyncio.to_thread(get_game_audit_rows, days)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    embed = discord.Embed(
        title=f"{Q_BALANCE} Balance Audit",
        description=f"Last **{days} day(s)**. This looks for games that may need odds or payout tuning.",
        color=discord.Color.orange(),
    )
    if not rows:
        embed.add_field(name="Signals", value="No tracked games in this window.", inline=False)
    else:
        lines = []
        actions = []
        total_house = 0
        total_plays = 0
        for row in rows:
            game_key = row["game_key"]
            plays = int(row["plays"] or 0)
            wins = int(row["wins"] or 0)
            losses = int(row["losses"] or 0)
            user_profit = int(row["profit"] or 0)
            house_profit = -user_profit
            total_house += house_profit
            total_plays += plays
            win_rate = (wins / max(1, wins + losses)) * 100
            label = risk_label(game_key)
            if plays < 10:
                verdict = "needs more data"
            elif win_rate >= 60 and user_profit > 0:
                verdict = "watch/nerf"
                actions.append(f"**{game_display_name(game_key)}**: high win rate and users are ahead.")
            elif win_rate <= 10 and house_profit > 0:
                verdict = "watch/buff"
                actions.append(f"**{game_display_name(game_key)}**: very low win rate.")
            elif abs(house_profit) > 2_000_000 and plays >= 20:
                verdict = "big swing"
                actions.append(f"**{game_display_name(game_key)}**: large money movement, review recent changes.")
            else:
                verdict = "stable"
            lines.append(
                f"{risk_emoji(game_key)} **{game_display_name(game_key)}** - "
                f"{plays:,} plays, {win_rate:.1f}% win, risk **{label}**, house {format_balance(house_profit)} | {verdict}"
            )
        embed.add_field(name="Summary", value=f"Tracked Plays: **{total_plays:,}**\nHouse Net: **{format_balance(total_house)}**", inline=False)
        add_split_embed_field(embed, "Games", lines[:20], inline=False)
        add_split_embed_field(embed, "Action Notes", actions[:8] or ["No obvious balance problems from recent tracked data."], inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

def economy_event_embed(guild, row):
    embed = discord.Embed(
        title=f"{Q_EVENT} Server Event",
        color=discord.Color.blue(),
    )
    if not row:
        embed.description = "No active 𝚀𝚞𝚎wo event in this server."
        embed.add_field(name="Start One", value="`.event start jackpot 1h`\nTypes: jackpot, shopdiscount, doublexp, taxfree", inline=False)
        return embed
    ends_at = row["ends_at"]
    if ends_at and ends_at.tzinfo is None:
        ends_at = ends_at.replace(tzinfo=timezone.utc)
    event_key = row["event_key"]
    embed.description = f"**{event_key.title()}**\n{EVENT_TYPES.get(event_key, 'Server event')}"
    embed.add_field(name="Pot / Burned", value=format_balance(int(row.get("pot") or 0)), inline=True)
    embed.add_field(name="Ends", value=discord_relative_time(ends_at) if ends_at else "Unknown", inline=True)
    embed.add_field(name="Started By", value=user_mention(row["started_by"]), inline=True)
    embed.set_footer(text="Donate with .event donate 50k. Stop with .event stop.")
    return embed

@commands.command(name="event", aliases=["qevent", "events"])
async def event(ctx, action: str = None, event_key: str = None, duration: str = None):
    if not await ensure_db_ready(ctx):
        return
    if ctx.guild is None:
        return await ctx.send(f"{Q_DENIED} Events only work in servers.")
    action_key = str(action or "status").casefold()
    if action_key in {"status", "show", "info", "current"}:
        row = await asyncio.to_thread(get_economy_event, ctx.guild.id)
        return await ctx.send(embed=economy_event_embed(ctx.guild, row), allowed_mentions=discord.AllowedMentions.none())
    if action_key in {"start", "set", "begin"}:
        if not has_economy_owner_power(ctx.author.id, ctx.guild):
            return await ctx.send(f"{Q_DENIED} Server owner or admin only.")
        seconds = parse_event_duration(duration or "1h")
        if not event_key or event_key.casefold() not in EVENT_TYPES or not seconds:
            return await ctx.send(
                f"{Q_DENIED} Use `.event start <type> <duration>`.\n"
                "Types: `jackpot`, `shopdiscount`, `doublexp`, `taxfree`. Example: `.event start jackpot 2h`"
            )
        row = await asyncio.to_thread(start_economy_event, ctx.guild.id, event_key, ctx.author.id, ctx.channel.id, seconds)
        return await ctx.send(embed=economy_event_embed(ctx.guild, row), allowed_mentions=discord.AllowedMentions.none())
    if action_key in {"stop", "end", "cancel"}:
        if not has_economy_owner_power(ctx.author.id, ctx.guild):
            return await ctx.send(f"{Q_DENIED} Server owner or admin only.")
        row = await asyncio.to_thread(stop_economy_event, ctx.guild.id)
        if not row:
            return await ctx.send(f"{Q_DENIED} No active event to stop.")
        return await ctx.send(f"{Q_SUCCESS} Stopped **{row['event_key']}**. Final pot: **{format_balance(int(row.get('pot') or 0))}**.")
    if action_key in {"donate", "burn", "fund"}:
        amount = parse_amount(event_key, ctx.author.id, ctx.guild, None) if event_key else None
        if amount is None or amount <= 0:
            return await ctx.send(f"{Q_DENIED} Use `.event donate <amount>`.")
        try:
            old_balance, new_balance, row = await asyncio.to_thread(donate_to_economy_event, ctx.guild.id, ctx.author.id, amount)
        except ValueError as e:
            reason = str(e)
            if reason == "no_event":
                return await ctx.send(f"{Q_DENIED} No active event in this server.")
            if reason == "insufficient_balance":
                return await ctx.send(f"{Q_DENIED} You do not have enough {CURRENCY_EMOJI}.")
            return await ctx.send(f"{Q_DENIED} Donation amount has to be positive.")
        await ctx.send(
            f"{Q_ACCEPT} Donated **{format_balance(amount)}** to **{row['event_key']}**.\n"
            f"Event Pot: **{format_balance(int(row.get('pot') or 0))}**\n"
            f"Balance: **{format_balance(old_balance)}** -> **{format_balance(new_balance)}**",
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return
    await ctx.send("Use `.event`, `.event start jackpot 1h`, `.event donate 50k`, or `.event stop`.")

@commands.command(name="limits", aliases=["limit", "safety", "safetylimits", "gamblelimit", "betlimit"])
async def limits(ctx, target_or_action: str = None, value: str = None):
    if not await ensure_db_ready(ctx):
        return
    action_key = str(target_or_action or "").casefold()
    if action_key in {"set", "cap", "max"}:
        limit = parse_whole_number(value)
        if limit is None or limit <= 0:
            return await ctx.reply(f"{Q_DENIED} Use `.limits set <amount>`, like `.limits set 50k`.", mention_author=False)
        limit = min(limit, MAX_BET)
        data = await asyncio.to_thread(set_user_safety_settings, ctx.author.id, personal_bet_limit=limit)
        return await ctx.reply(
            f"{Q_LIMITS} Your personal gambling cap is now **{format_balance(int(data.get('personal_bet_limit') or limit))}**.",
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    if action_key in {"clear", "reset", "default"}:
        data = await asyncio.to_thread(set_user_safety_settings, ctx.author.id, clear_limit=True, clear_pause=True)
        return await ctx.reply(f"{Q_SUCCESS} Cleared your personal gambling cap and pause.", mention_author=False)
    if action_key in {"pause", "break", "stop"}:
        seconds = parse_event_duration(value or "1h")
        if not seconds:
            return await ctx.reply(f"{Q_DENIED} Use `.limits pause <time>`, like `.limits pause 2h`.", mention_author=False)
        pause_until = datetime.now(timezone.utc) + timedelta(seconds=seconds)
        data = await asyncio.to_thread(set_user_safety_settings, ctx.author.id, gambling_pause_until=pause_until)
        saved_until = data.get("gambling_pause_until") or pause_until
        if saved_until.tzinfo is None:
            saved_until = saved_until.replace(tzinfo=timezone.utc)
        return await ctx.reply(
            f"{Q_TIMER_TICK} Gambling paused until {discord_relative_time(saved_until)}.",
            mention_author=False,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    if action_key in {"resume", "unpause"}:
        await asyncio.to_thread(set_user_safety_settings, ctx.author.id, clear_pause=True)
        return await ctx.reply(f"{Q_SUCCESS} Gambling pause removed.", mention_author=False)

    user = ctx.author
    if target_or_action:
        try:
            user = await commands.MemberConverter().convert(ctx, target_or_action)
        except Exception:
            return await ctx.reply(f"{Q_DENIED} Use `.limits`, `.limits @user`, `.limits set 50k`, `.limits pause 2h`, or `.limits clear`.", mention_author=False)
    try:
        data, lottery_spend = await asyncio.gather(
            asyncio.to_thread(get_user, user.id),
            asyncio.to_thread(get_lottery_user_spend, ctx.guild.id if ctx.guild else None, user.id),
        )
        loss = await asyncio.to_thread(daily_loss_status, user.id, int(data["balance"]), 0)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    remaining = max(0, int(loss["hard_limit"]) - int(loss["lost_today"]))
    cooldown_multiplier = cooldown_multiplier_for_user(user.id, data)
    effective_cooldown = max(1, int(COOLDOWN_SECS * cooldown_multiplier))
    luck_until = data.get("luck_boost_until")
    luck_text = "Inactive"
    if luck_until:
        if luck_until.tzinfo is None:
            luck_until = luck_until.replace(tzinfo=timezone.utc)
        if luck_until > datetime.now(timezone.utc):
            luck_text = f"Active until {discord_relative_time(luck_until)}"
    embed = discord.Embed(
        title=f"{Q_LIMITS} Safety Limits",
        description=user_mention(user.id),
        color=discord.Color.orange(),
    )
    embed.add_field(
        name="Daily Gambling Loss",
        value=(
            f"Lost Today: **{format_balance(loss['lost_today'])}**\n"
            f"Warning At: **{format_balance(loss['warning_limit'])}**\n"
            f"Hard Limit: **{format_balance(loss['hard_limit'])}**\n"
            f"Remaining Risk: **{format_balance(remaining)}**"
        ),
        inline=False,
    )
    if lottery_spend:
        round_spent = int(lottery_spend["spent"] or 0)
        spend_base = int(data["balance"]) + round_spent
        max_round_spend = int(spend_base * LOTTERY_MAX_BALANCE_SPEND_RATIO)
        embed.add_field(
            name="Lottery Round",
            value=(
                f"Tickets: **{int(lottery_spend['tickets'] or 0):,}**\n"
                f"Spent: **{format_balance(round_spent)}**\n"
                f"Limit: **{format_balance(max_round_spend)}**"
            ),
            inline=False,
        )
        remaining_lottery = max(0, max_round_spend - round_spent)
        embed.add_field(name="Lottery Remaining", value=format_balance(remaining_lottery), inline=True)
    else:
        embed.add_field(name="Lottery Round", value="No current lottery tickets in this server.", inline=False)
    embed.add_field(name="Bet Cap", value=format_balance(MAX_BET), inline=True)
    personal_limit = data.get("personal_bet_limit")
    pause_until = data.get("gambling_pause_until")
    if pause_until and pause_until.tzinfo is None:
        pause_until = pause_until.replace(tzinfo=timezone.utc)
    if pause_until and pause_until <= datetime.now(timezone.utc):
        pause_until = None
    embed.add_field(
        name="Personal Cap",
        value=format_balance(int(personal_limit)) if personal_limit else "Not set",
        inline=True,
    )
    embed.add_field(
        name="Personal Pause",
        value=discord_relative_time(pause_until) if pause_until else "Not active",
        inline=True,
    )
    embed.add_field(name="Gambling Cooldown", value=f"{plural_unit(effective_cooldown, 'second')} effective\nBase: {plural_unit(COOLDOWN_SECS, 'second')}", inline=True)
    embed.add_field(name="Lottery Spend Cap", value=f"{int(LOTTERY_MAX_BALANCE_SPEND_RATIO * 100)}% of lottery-adjusted balance", inline=True)
    embed.add_field(name="Daily Loss Rules", value=f"Warns at {int(DAILY_LOSS_WARNING_RATIO * 100)}%; blocks before {int(DAILY_LOSS_HARD_RATIO * 100)}%.", inline=False)
    embed.add_field(name="Luck Boost", value=luck_text, inline=True)
    await ctx.reply(embed=embed, mention_author=False, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="gamehistory", aliases=["history", "ghistory", "recentgames"])
async def gamehistory(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return
    user = member or ctx.author
    try:
        rows = await asyncio.to_thread(get_game_history, user.id, 12)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    embed = discord.Embed(
        title=f"{Q_HISTORY} Game History",
        description=user_mention(user.id),
        color=discord.Color.blurple(),
    )
    if not rows:
        embed.add_field(name="Recent", value="No tracked game history yet.", inline=False)
    else:
        lines = []
        replay_lines = []
        for index, row in enumerate(rows, start=1):
            ts = row["created_at"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            result = "Win" if row["won"] is True else ("Loss" if row["won"] is False else "Push")
            net = int(row["net_amount"] or 0)
            sign = "+" if net > 0 else ""
            lines.append(
                f"`#{index}` **{game_display_name(row['game_key'])}** - {result} `{sign}{net:,}` {CURRENCY_EMOJI} <t:{int(ts.timestamp())}:R>"
            )
            replay_lines.append(
                f"`#{index}` {game_display_name(row['game_key'])}: {result}, net `{sign}{net:,}`, payout `{int(row['payout'] or 0):,}`"
            )
        add_split_embed_field(embed, "Recent", lines, inline=False)

    class GameHistoryView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)

        async def interaction_check(self, interaction):
            if interaction.user.id == ctx.author.id:
                return True
            await interaction.response.send_message("Open your own game history with `.gamehistory`.", ephemeral=True)
            return False

        @discord.ui.button(label="Replay", style=discord.ButtonStyle.secondary)
        async def replay_button(self, interaction, button):
            if not rows:
                return await interaction.response.send_message("No game results to replay yet.", ephemeral=True)
            replay_embed = discord.Embed(
                title=f"{Q_HISTORY} Result Replay",
                description="Compact replay of the stored results. Full move replay is shown only for games that save turn-by-turn state.",
                color=discord.Color.blurple(),
            )
            add_split_embed_field(replay_embed, "Timeline", replay_lines, inline=False)
            await interaction.response.send_message(embed=replay_embed, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

    await ctx.reply(embed=embed, view=GameHistoryView(), mention_author=False, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="economyaudit", aliases=["audit", "qaudit", "econaudit"])
async def economyaudit(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return
    if not has_economy_owner_power(ctx.author.id, ctx.guild):
        await ctx.send(f"{Q_DENIED} Server owner or admin only.")
        return
    if member is not None:
        try:
            data = await asyncio.to_thread(get_user, member.id)
            rows = await asyncio.to_thread(get_game_stats, member.id)
            history_rows = await asyncio.to_thread(get_game_history, member.id, 5)
            loss = await asyncio.to_thread(daily_loss_status, member.id, int(data["balance"]), 0)
            tx_rows = await asyncio.to_thread(get_recent_transactions, member.id, 8)
        except Exception:
            await send_error(ctx, "Database unavailable. Try again shortly.")
            return
        net = int(data["total_won"] or 0) - int(data["total_lost"] or 0)
        embed = discord.Embed(
            title=f"{Q_AUDIT} User Economy Audit",
            description=user_mention(member.id),
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Balance", value=format_balance(data["balance"]), inline=True)
        embed.add_field(name="Level", value=f"{int(data['level'] or 1):,}", inline=True)
        embed.add_field(name="XP", value=f"{int(data['xp'] or 0):,}", inline=True)
        embed.add_field(name="Total Earned", value=format_balance(data["total_earned"]), inline=True)
        embed.add_field(name="Total Won", value=format_balance(data["total_won"]), inline=True)
        embed.add_field(name="Total Lost", value=format_balance(data["total_lost"]), inline=True)
        embed.add_field(name="Net Gambling", value=format_balance(net), inline=True)
        embed.add_field(name="Lost Today", value=f"{format_balance(loss['lost_today'])}\nRemaining: {format_balance(loss['remaining'])}", inline=True)
        embed.add_field(name="Gambling Streak", value=f"{int(data.get('gamble_streak', 0) or 0):,}", inline=True)
        game_lines = []
        for row in rows[:8]:
            played = int(row["played"] or 0)
            wins = int(row["wins"] or 0)
            win_rate = wins / played * 100 if played else 0
            game_lines.append(
                f"**{game_display_name(row['game_key'])}** - {played:,} played, {win_rate:.1f}% win, {format_balance(int(row['profit'] or 0))}"
            )
        add_split_embed_field(embed, "Game Signals", game_lines or ["No game stats yet."], inline=False)
        history_lines = []
        for row in history_rows:
            ts = row["created_at"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            result = "Win" if row["won"] is True else ("Loss" if row["won"] is False else "Push")
            net_amount = int(row["net_amount"] or 0)
            sign = "+" if net_amount > 0 else ""
            history_lines.append(f"**{game_display_name(row['game_key'])}** - {result} `{sign}{net_amount:,}` <t:{int(ts.timestamp())}:R>")
        add_split_embed_field(embed, "Recent Games", history_lines or ["No tracked games yet."], inline=False)
        tx_lines = []
        for row in tx_rows:
            ts = row["created_at"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            amount = int(row["amount"] or 0)
            sign = "+" if amount >= 0 else ""
            tx_lines.append(f"`{row['kind']}` `{sign}{amount:,}` <t:{int(ts.timestamp())}:R>")
        add_split_embed_field(embed, "Recent Transactions", tx_lines or ["No transactions yet."], inline=False)
        warnings = []
        if loss["lost_today"] >= loss["warning_limit"]:
            warnings.append(f"{Q_WARNING} User is near or over the daily gambling warning line.")
        if net < 0 and abs(net) > max(1, int(data["total_earned"] or 0)) * 0.5:
            warnings.append(f"{Q_WARNING} User has heavy net gambling losses versus total earned.")
        add_split_embed_field(embed, "Warnings", warnings or [f"{Q_SUCCESS} No obvious user audit warnings."], inline=False)
        await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        return
    try:
        stats = await asyncio.to_thread(get_economy_audit)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    net_gambling = stats["total_won"] - stats["total_lost"]
    embed = discord.Embed(
        title=f"{Q_AUDIT} Economy Audit",
        description="Money flow, risk, and balancing signals.",
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Money Supply", value=format_balance(stats["total_balance"]), inline=True)
    embed.add_field(name="Total Earned", value=format_balance(stats["total_earned"]), inline=True)
    embed.add_field(name="Net Gambling", value=format_balance(net_gambling), inline=True)
    embed.add_field(name="Lost Today", value=format_balance(stats["lost_today"]), inline=True)
    embed.add_field(name="24h Transactions", value=f"{stats['transactions_24h']:,}", inline=True)
    embed.add_field(name="24h Net Tx Amount", value=format_balance(stats["transaction_amount_24h"]), inline=True)
    tx = stats["transaction_totals"]
    tracked_sink = abs(tx.get("shop_purchase", 0)) + tx.get("transfer_tax", 0) + tx.get("lottery_house_cut", 0)
    embed.add_field(name="Tracked Taxes / Payments", value=format_balance(tracked_sink), inline=True)
    embed.add_field(name="Lottery Pots", value=format_balance(stats["lottery_pots"]), inline=True)
    embed.add_field(name="Lottery Tickets", value=f"{stats['lottery_tickets']:,}", inline=True)
    source_lines = [
        f"Total earned: **{format_balance(stats['total_earned'])}**",
        f"Gambling won: **{format_balance(stats['total_won'])}**",
        f"Shop payments to {QUE_OWNER_DISPLAY}: **{format_balance(tx.get('shop_payment', 0))}**",
    ]
    sink_lines = [
        f"Gambling lost: **{format_balance(stats['total_lost'])}**",
        f"Shop purchases: **{format_balance(abs(tx.get('shop_purchase', 0)))}**",
        f"Lottery cuts: **{format_balance(tx.get('lottery_house_cut', 0))}**",
        f"Transfer taxes: **{format_balance(tx.get('transfer_tax', 0))}**",
    ]
    add_split_embed_field(embed, "Money Sources", source_lines, inline=False)
    add_split_embed_field(embed, "Money Sinks / Tax Flow", sink_lines, inline=False)
    supply = max(1, int(stats["total_balance"] or 0))
    sink_ratio = tracked_sink / supply
    flow_ratio = abs(stats["transaction_amount_24h"]) / supply
    if sink_ratio >= 0.12 and flow_ratio <= 0.20:
        health = f"{Q_SUCCESS} Stable"
    elif flow_ratio > 0.35 or stats["lost_today"] > supply * 0.12:
        health = f"{Q_WARNING} Hot"
    else:
        health = f"{Q_THINKING} Watch"
    tx_sources = {
        "Shop Payments": tx.get("shop_payment", 0),
        "Lottery House Cut": tx.get("lottery_house_cut", 0),
        "Transfer Tax": tx.get("transfer_tax", 0),
    }
    biggest_flow = max(tx_sources.items(), key=lambda item: abs(item[1])) if tx_sources else ("None", 0)
    embed.add_field(name="Health", value=f"{health}\n24h flow: **{flow_ratio * 100:.1f}%** of supply", inline=True)
    embed.add_field(name="Biggest Tracked Flow", value=f"{biggest_flow[0]}\n{format_balance(biggest_flow[1])}", inline=True)
    game_lines = []
    for row in stats["games"]:
        played = int(row["played"] or 0)
        wins = int(row["wins"] or 0)
        win_rate = wins / played * 100 if played else 0
        profit = int(row["profit"] or 0)
        game_lines.append(
            f"**{game_display_name(row['game_key'])}** - {played:,} played, {win_rate:.1f}% win, profit {format_balance(profit)}"
        )
    add_split_embed_field(embed, "Game Signals", game_lines or ["No game stats yet."], inline=False)
    warnings = []
    if stats["lost_today"] > supply * 0.08:
        warnings.append(f"{Q_WARNING} Daily gambling losses are high versus current supply.")
    if abs(stats["transaction_amount_24h"]) > supply * 0.15:
        warnings.append(f"{Q_WARNING} 24h transaction flow is high versus current supply.")
    for row in stats["games"]:
        played = int(row["played"] or 0)
        profit = int(row["profit"] or 0)
        if played >= 50 and profit > 0:
            warnings.append(f"{Q_WARNING} {game_display_name(row['game_key'])} is net-positive for players; review odds.")
        if len(warnings) >= 4:
            break
    add_split_embed_field(embed, "Warnings", warnings or [f"{Q_SUCCESS} No obvious audit warnings."], inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="guide", aliases=["start", "begin", "gettingstarted"])
async def guide(ctx):
    prefix = getattr(ctx, "prefix", ".")
    embed = discord.Embed(
        title=f"{Q_BOOK} Pro𝚀𝚞𝚎 Guide",
        description="Quick path for new users.",
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="Earn",
        value=f"`{prefix}daily`, `{prefix}weekly`, `{prefix}monthly`, chat XP, Flag Quiz, Dungeon, quests, and achievements.",
        inline=False,
    )
    embed.add_field(
        name="Play",
        value=f"`{prefix}games` for filters. Start safe with `{prefix}dungeon`, `{prefix}flagquiz`, or small bets.",
        inline=False,
    )
    embed.add_field(
        name="Spend",
        value=f"`{prefix}shop` for boosts, `{prefix}inventory` for owned items, `{prefix}setbadge` for profile badges.",
        inline=False,
    )
    embed.add_field(
        name="Stay Safe",
        value=f"`{prefix}limits` shows daily gambling risk, lottery spend, max bet, and cooldowns.",
        inline=False,
    )
    embed.set_footer(text=f"More: {prefix}econhelp, {prefix}help, {prefix}explain <command>")
    await ctx.reply(embed=embed, mention_author=False, allowed_mentions=discord.AllowedMentions.none())

class OnboardingView(discord.ui.View):
    def __init__(self, author_id):
        super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
        self.author_id = int(author_id)

    async def interaction_check(self, interaction):
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Open your own onboarding with `.onboard`.", ephemeral=True)
        return False

    @discord.ui.button(label="Claim Daily", emoji=Q_DAILY_SPICE, style=discord.ButtonStyle.success)
    async def claim_daily_button(self, interaction, button):
        command = bot.get_command("daily") if bot else None
        if not command:
            return await interaction.response.send_message("Daily command is unavailable.", ephemeral=True)
        ctx = ReplayContext(interaction)
        await interaction.response.defer(ephemeral=True)
        await command.callback(ctx)

    @discord.ui.button(label="Games", emoji=Q_GAME_WIN, style=discord.ButtonStyle.primary)
    async def games_button(self, interaction, button):
        await interaction.response.send_message("Use `.games` for the game menu, then pick How To Play for rules.", ephemeral=True)

    @discord.ui.button(label="Shop", emoji=Q_SHOP, style=discord.ButtonStyle.secondary)
    async def shop_button(self, interaction, button):
        await interaction.response.send_message("Use `.shop` to buy boosts and cosmetics.", ephemeral=True)

    @discord.ui.button(label="Profile", emoji=Q_XP, style=discord.ButtonStyle.secondary)
    async def profile_button(self, interaction, button):
        data = await asyncio.to_thread(get_user, interaction.user.id)
        await interaction.response.send_message(embed=build_profile_embed(interaction.user, data), ephemeral=True)

@commands.command(name="onboard", aliases=["onboarding", "newuser"])
async def onboard(ctx):
    embed = discord.Embed(
        title=f"{Q_BOOK} Pro𝚀𝚞𝚎 Start",
        description="Start here if you're new. These buttons point you to the safest first actions.",
        color=discord.Color.blurple(),
    )
    embed.add_field(name="1. Earn", value="Claim daily rewards and chat to level up.", inline=False)
    embed.add_field(name="2. Play", value="Try free games first, then small bets.", inline=False)
    embed.add_field(name="3. Build", value="Buy shop boosts, equip badges, and customize your profile.", inline=False)
    await ctx.reply(embed=embed, view=OnboardingView(ctx.author.id), mention_author=False, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="season", aliases=["seasons", "seasonlb"])
async def season(ctx, season_key: str = None):
    if not await ensure_db_ready(ctx):
        return
    season_key = season_key or current_season_key()
    try:
        rows = await asyncio.to_thread(get_season_leaderboard, season_key, 10)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    current_key = current_season_key()
    embed = discord.Embed(
        title=f"{Q_BADGE} 𝚀𝚞𝚎wo Season {season_key}",
        description=(
            "Ranked by monthly gambling/game profit, then wins.\n"
            f"Current season ends <t:{season_end_timestamp()}:R>."
            if season_key == current_key else "Archived season standings."
        ),
        color=discord.Color.gold(),
    )
    reward_lines = [f"#{i + 1}: **{format_balance(reward)}**" for i, reward in enumerate(SEASON_REWARDS)]
    embed.add_field(name="Rewards", value="\n".join(reward_lines), inline=False)
    if not rows:
        embed.add_field(name="Leaderboard", value="No season scores yet.", inline=False)
    else:
        lines = []
        for index, row in enumerate(rows, 1):
            lines.append(
                f"**#{index}** {user_mention(row['user_id'])} - Profit **{format_balance(int(row['profit'] or 0))}**, "
                f"Wins **{int(row['wins'] or 0):,}**, Played **{int(row['played'] or 0):,}**"
            )
        add_split_embed_field(embed, "Leaderboard", lines, inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="seasonpass", aliases=["monthlychallenges", "pass", "spass"])
async def seasonpass(ctx):
    if not await ensure_db_ready(ctx):
        return
    try:
        data = await asyncio.to_thread(get_user, ctx.author.id)
        rows = await asyncio.to_thread(get_game_stats, ctx.author.id)
    except Exception:
        return await send_error(ctx, "Database unavailable. Try again shortly.")
    played = sum(int(row["played"] or 0) for row in rows)
    wins = sum(int(row["wins"] or 0) for row in rows)
    profit = sum(int(row["profit"] or 0) for row in rows)
    unique_games = len([row for row in rows if int(row["played"] or 0) > 0])
    goals = [
        ("Warmup", played, 25, 500_000),
        ("Winner", wins, 10, 1_000_000),
        ("Explorer", unique_games, 8, 1_500_000),
        ("Profit Push", max(0, profit), 2_000_000, 2_500_000),
        ("Level Grind", int(data.get("level") or 1), 15, 2_000_000),
    ]
    embed = discord.Embed(
        title=f"{Q_SEASON_PASS} Season Pass",
        description=f"Monthly goals for **{current_season_key()}**. Rewards are claimed automatically through related systems where supported.",
        color=discord.Color.gold(),
    )
    lines = []
    for name, value, target, reward in goals:
        status = Q_SUCCESS if value >= target else Q_ACHIEVEMENT_LOCKED
        lines.append(f"{status} **{name}** - {min(value, target):,}/{target:,} | Reward **{format_balance(reward)}**")
    embed.add_field(name="Challenges", value="\n".join(lines), inline=False)
    embed.add_field(name="Tip", value="Use `.season` for ranking rewards and `.achievements` for long-term badges.", inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="endseason", aliases=["rewardseason"])
async def endseason(ctx, season_key: str = None):
    if not await ensure_db_ready(ctx):
        return
    if not is_superowner_id(ctx.author.id):
        return await send_owner_only(ctx)
    season_key = season_key or current_season_key()
    try:
        rewarded = await asyncio.to_thread(reward_previous_season, season_key)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    if not rewarded:
        return await ctx.send(f"{Q_DENIED} No unrewarded top players found for `{season_key}`.")
    lines = [f"#{rank} {user_mention(user_id)} - **{format_balance(reward)}**" for rank, user_id, reward in rewarded]
    await ctx.send(f"{Q_BADGE} Season `{season_key}` rewards paid:\n" + "\n".join(lines), allowed_mentions=discord.AllowedMentions.none())

def get_abuse_audit_rows():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, COUNT(*) AS plays, COALESCE(SUM(net_amount), 0) AS profit
        FROM economy_game_history
        WHERE created_at > NOW() - INTERVAL '1 hour'
        GROUP BY user_id
        HAVING COUNT(*) >= 15 OR COALESCE(SUM(net_amount), 0) >= 1000000
        ORDER BY profit DESC, plays DESC
        LIMIT 10
        """
    )
    games = cur.fetchall()
    cur.execute(
        """
        SELECT user_id, COUNT(*) AS tx_count, COALESCE(SUM(ABS(amount)), 0) AS volume
        FROM economy_transactions
        WHERE created_at > NOW() - INTERVAL '24 hours'
          AND kind IN ('give_sent', 'give_received', 'transfer_tax_paid', 'transfer_tax', 'owner_add', 'owner_set', 'season_reward')
        GROUP BY user_id
        HAVING COUNT(*) >= 8 OR COALESCE(SUM(ABS(amount)), 0) >= 5000000
        ORDER BY volume DESC
        LIMIT 10
        """
    )
    transactions = cur.fetchall()
    cur.execute(
        """
        SELECT user_id, amount
        FROM economy_daily_losses
        WHERE loss_date = CURRENT_DATE AND amount >= 1000000
        ORDER BY amount DESC
        LIMIT 10
        """
    )
    losses = cur.fetchall()
    cur.execute(
        """
        SELECT user_id, guild_id, tickets, spent
        FROM lottery_tickets
        WHERE tickets >= 20 OR spent >= 2000000
        ORDER BY spent DESC, tickets DESC
        LIMIT 10
        """
    )
    lottery_watch = cur.fetchall()
    cur.close()
    conn.close()
    return games, transactions, losses, lottery_watch

@commands.command(name="abuseaudit", aliases=["antiexploit", "riskwatch"])
async def abuseaudit(ctx):
    if not await ensure_db_ready(ctx):
        return
    if not has_economy_owner_power(ctx.author.id, ctx.guild):
        return await ctx.send(f"{Q_DENIED} Server owner or admin only.")
    try:
        games, transactions, losses, lottery_watch = await asyncio.to_thread(get_abuse_audit_rows)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    embed = discord.Embed(
        title=f"{Q_AUDIT} Anti-Abuse Watch",
        description="Signals only. Review context before acting.",
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    game_lines = [f"{user_mention(r['user_id'])} - **{int(r['plays']):,}** plays, profit **{format_balance(int(r['profit'] or 0))}**" for r in games]
    tx_lines = [f"{user_mention(r['user_id'])} - **{int(r['tx_count']):,}** tx, volume **{format_balance(int(r['volume'] or 0))}**" for r in transactions]
    loss_lines = [f"{user_mention(r['user_id'])} - lost today **{format_balance(int(r['amount'] or 0))}**" for r in losses]
    lottery_lines = [f"{user_mention(r['user_id'])} - **{int(r['tickets']):,}** tickets, spent **{format_balance(int(r['spent'] or 0))}** in `{r['guild_id']}`" for r in lottery_watch]
    add_split_embed_field(embed, "Fast Game Profit / Volume", game_lines or ["No high-risk game signals."], inline=False)
    add_split_embed_field(embed, "Transaction Volume", tx_lines or ["No high-risk transaction signals."], inline=False)
    add_split_embed_field(embed, "Daily Loss Watch", loss_lines or ["No major daily loss signals."], inline=False)
    add_split_embed_field(embed, "Lottery Spend Watch", lottery_lines or ["No major lottery spend signals."], inline=False)
    embed.add_field(
        name="What To Check",
        value="Look for repeated rapid actions, role/@everyone changes, sudden transfers, and users pushing lottery spend near their safety cap.",
        inline=False,
    )
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

@commands.command(name="riskprofile", aliases=["risk", "userrisk", "riskcheck"])
async def riskprofile(ctx, member: discord.Member = None):
    if not await ensure_db_ready(ctx):
        return
    user = member or ctx.author
    try:
        data = await asyncio.to_thread(get_user, user.id)
        rows = await asyncio.to_thread(get_game_stats, user.id)
        tx_rows = await asyncio.to_thread(get_recent_transactions, user.id, 8)
        lottery_spend = await asyncio.to_thread(get_lottery_user_spend, ctx.guild.id if ctx.guild else None, user.id)
    except Exception:
        await send_error(ctx, "Database unavailable. Try again shortly.")
        return
    played = sum(int(row["played"] or 0) for row in rows)
    wins = sum(int(row["wins"] or 0) for row in rows)
    profit = sum(int(row["profit"] or 0) for row in rows)
    losses = await asyncio.to_thread(daily_loss_status, user.id, int(data.get("balance") or 0), 0)
    win_rate = (wins / played * 100) if played else 0
    loss_ratio = float(losses.get("ratio", 0) or 0)
    if loss_ratio >= DAILY_LOSS_WARNING_RATIO or profit < -1_000_000:
        risk_word = "High"
        color = discord.Color.red()
    elif played >= 25 or abs(profit) >= 500_000:
        risk_word = "Medium"
        color = discord.Color.orange()
    else:
        risk_word = "Low"
        color = discord.Color.green()
    risk_icon = {"Low": Q_RISK_LOW, "Medium": Q_RISK_MEDIUM, "High": Q_RISK_HIGH}.get(risk_word, Q_RISK_MEDIUM)
    embed = discord.Embed(
        title=f"{risk_icon} User Risk Profile",
        description=user_mention(user.id),
        color=color,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Risk", value=f"**{risk_word}**", inline=True)
    embed.add_field(name="Balance", value=format_balance(int(data.get("balance") or 0)), inline=True)
    embed.add_field(name="Daily Loss", value=f"{format_balance(int(losses.get('used') or 0))} / {format_balance(int(losses.get('limit') or 0))}", inline=True)
    embed.add_field(name="Games", value=f"Played: **{played:,}**\nWins: **{wins:,}** ({win_rate:.1f}%)\nProfit: **{format_balance(profit)}**", inline=False)
    if lottery_spend:
        embed.add_field(
            name="Current Lottery Spend",
            value=f"Spent: **{format_balance(int(lottery_spend['spent'] or 0))}**\nTickets: **{int(lottery_spend['tickets'] or 0):,}**",
            inline=True,
        )
    recent_volume = sum(abs(int(row["amount"] or 0)) for row in tx_rows)
    embed.add_field(name="Recent Transaction Volume", value=format_balance(recent_volume), inline=True)
    notes = []
    if loss_ratio >= DAILY_LOSS_WARNING_RATIO:
        notes.append("Daily loss warning threshold reached.")
    if profit < -1_000_000:
        notes.append("Large recent tracked game losses.")
    if played == 0:
        notes.append("No tracked game history yet.")
    embed.add_field(name="Notes", value="\n".join(notes) if notes else "No major warning signs.", inline=False)
    await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())


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
        await send_gambling_cooldown(ctx, cd)
        return

    user_id = ctx.author.id

    try:
        data = await asyncio.to_thread(get_user, user_id)
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

    if not await check_daily_loss_limit(ctx, data, amount):
        return

    class SizeView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=30)
            self.grid = None

        async def interaction_check(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own Mine Hunt prompt.", ephemeral=True)
                return False
            return True

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
    if active_luck_bonus(data) and bomb_count > 1:
        bomb_count -= 1

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
    multiplier = 2.0

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
            header += f"\n> Current multiplier: ×{multiplier:.2f} (×{2 + (revealed_count * 0.15):.2f} if won now)"
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
                await interaction.response.defer()
                return

            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own Mine Hunt game.", ephemeral=True)
                return

            await interaction.response.defer()
            revealed[self.row][self.col] = True
            cell = board[self.row][self.col]

            if cell == 'bomb':
                game_over = True
                game_won = False
                # Reveal all
                for r in range(rows):
                    for c in range(cols):
                        revealed[r][c] = True
                try:
                    latest = await asyncio.to_thread(get_user, user_id)
                    new_balance = max(0, latest['balance'] - amount)
                    await asyncio.to_thread(update_user,
                        user_id,
                        balance=new_balance,
                        gamble_streak=0,
                        total_lost=latest['total_lost'] + amount
                    )
                    await asyncio.to_thread(record_game_result, user_id, "ms", False, -amount, 0)
                except Exception:
                    self.view.clear_items()
                    await interaction.edit_original_response(
                        content=render_board() + f"\n{Q_DENIED} Database unavailable. Try again shortly.",
                        view=self.view
                    )
                    return
                new_content = (
                    render_board() +
                    f"\nLost: **{format_balance(amount)}**\n"
                    f"New Balance: **{format_balance(new_balance)}**"
                )
                self.view.clear_items()
                await interaction.edit_original_response(content=new_content, view=self.view)
                return

            revealed_count += 1
            multiplier = 2 + (revealed_count * 0.15)

            # Check win
            if revealed_count == safe_cells:
                game_over = True
                game_won = True
                try:
                    latest = await asyncio.to_thread(get_user, user_id)
                    new_streak = next_gambling_streak(latest)
                    streak_mult = payout_multiplier(latest, new_streak)
                    winnings = int(amount * multiplier * streak_mult)
                    new_balance = latest['balance'] + winnings - amount
                    await asyncio.to_thread(update_user,
                        user_id,
                        balance=new_balance,
                        gamble_streak=new_streak,
                        total_won=latest['total_won'] + winnings - amount
                    )
                    def finish_ms_achievements():
                        stats = record_game_result(user_id, "ms", True, winnings - amount, winnings)
                        return maybe_award_game_achievements(user_id, "ms", stats)
                    achievements = await asyncio.to_thread(finish_ms_achievements)
                except Exception:
                    self.view.clear_items()
                    await interaction.edit_original_response(
                        content=render_board() + f"\n{Q_DENIED} Database unavailable. Try again shortly.",
                        view=self.view
                    )
                    return
                new_content = (
                    render_board() +
                    f"\nMultiplier: ×{multiplier * streak_mult:.3f} (base ×{multiplier:.2f}, streak ×{streak_mult:.3f})" +
                    gambling_streak_text(latest, new_streak) +
                    f"\nPrize: **{format_balance(winnings)}**\n"
                    f"New Balance: **{format_balance(new_balance)}**"
                    f"{achievement_reward_text(achievements)}"
                )
                self.view.clear_items()
                await interaction.edit_original_response(content=new_content, view=double_or_nothing_view(user_id, "ms", {"winnings": winnings}))
                return

            # Update board
            new_content = render_board()
            await interaction.edit_original_response(content=new_content, view=self.view)

    class MSView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=60)
            for r in range(rows):
                for c in range(cols):
                    self.add_item(MSCell(r, c))

        async def interaction_check(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own Mine Hunt game.", ephemeral=True)
                return False
            return True

        async def on_timeout(self):
            nonlocal game_over, game_won, revealed_count, multiplier
            if not game_over:
                game_over = True
                game_won = False
                self.clear_items()
                try:
                    latest = await asyncio.to_thread(get_user, user_id)
                    new_balance = max(0, latest['balance'] - amount)
                    await asyncio.to_thread(update_user, user_id, balance=new_balance, gamble_streak=0, total_lost=latest['total_lost'] + amount)
                    await asyncio.to_thread(record_game_result, user_id, "ms", False, -amount, 0)
                    content = (
                        render_board() +
                        f"\n> {Q_TIMER} Timed out! Lost **{format_balance(amount)}**\n"
                        f"New Balance: **{format_balance(new_balance)}**"
                    )
                    await self.message.edit(content=content, view=self)
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
WHEEL_WEIGHTS = [35, 25, 25, 5, 3, 2, 4, 1]  # sum = 100

def slot_symbol_help_text():
    labels = ["first", "second", "third", "fourth"]
    return ", ".join(
        f"{labels[index]} {emoji} pays ×{multiplier}"
        for index, (emoji, multiplier) in enumerate(SLOT_SYMBOL_PAYOUTS)
    )

def scratch_symbol_help_text():
    return " ".join(SCRATCH_SYMBOLS)

def scratch_tier_help_text():
    return ", ".join(f"{symbol} 5/5 pays ×{multiplier}" for symbol, multiplier, _ in SCRATCH_TIERS)

def wheel_segment_help_text():
    return ", ".join(f"{emoji} {label}" for label, _, emoji in WHEEL_SEGMENTS)

@commands.command(aliases=["spin"])
async def wheel(ctx, amount: str):
    if not await ensure_db_ready(ctx):
        return

    cd = check_cooldown(ctx.author.id, "wheel")
    if cd > 0:
        await send_gambling_cooldown(ctx, cd)
        return

    user_id = ctx.author.id

    try:
        data = await asyncio.to_thread(get_user, user_id)
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

    if not await check_daily_loss_limit(ctx, data, amount):
        return

    # Pre-select outcome
    if random.random() < active_luck_bonus(data):
        winning_segments = [
            i for i, segment in enumerate(WHEEL_SEGMENTS)
            if segment[0] not in {"×0.5", "×1", "BLANK"}
        ]
        segment_idx = random.choice(winning_segments)
    else:
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
        return header + "\n" + wheel_art

    msg = await ctx.send(render_wheel(spinning=True))

    final_offset = (segment_idx - 2) % len(WHEEL_SEGMENTS)

    for step in range(12):
        await asyncio.sleep(0.15 if step < 8 else 0.25)
        await msg.edit(content=render_wheel(spinning=True, offset=step % len(WHEEL_SEGMENTS)))

    try:
        if label == 'BLANK':
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
            new_streak = next_gambling_streak(data) if mult_val > 1 else 0
            mult = payout_multiplier(data, new_streak) if new_streak else 1
            winnings = int(amount * mult * mult_val)
            if winnings > amount or (mult_val > 1 and winnings > 0):
                # Win
                new_balance = data['balance'] + winnings - amount
                await asyncio.to_thread(update_user,
                    user_id,
                    balance=new_balance,
                    gamble_streak=new_streak,
                    total_won=data['total_won'] + winnings - amount
                )
                streak_msg = gambling_streak_text(data, new_streak)
                await msg.edit(
                    content=(
                        render_wheel(spinning=False, offset=final_offset, landed_idx=segment_idx) +
                        "\n" +
                    f">>> **{emoji} {label}!**\n"
                    f"Multiplier: ×{mult * mult_val:.2f} (base ×{mult_val}, streak ×{mult:.2f})\n"
                    f"Prize: **{format_balance(winnings)}**{streak_msg}\n"
                    f"New Balance: **{format_balance(new_balance)}**"
                    ),
                    view=double_or_nothing_view(user_id, "wheel", {"winnings": winnings})
                )
            else:
                loss_amount = amount - winnings
                new_balance = max(0, data['balance'] - loss_amount)
                await asyncio.to_thread(update_user,
                    user_id,
                    balance=new_balance,
                    gamble_streak=0,
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


class ReplayContext:
    def __init__(self, interaction):
        self.author = interaction.user
        self.guild = interaction.guild
        self.channel = interaction.channel
        self.bot = bot
        self.command = None
        self.prefix = "."
        self.message = getattr(interaction, "message", None)

    async def send(self, *args, **kwargs):
        return await self.channel.send(*args, **kwargs)

    async def reply(self, *args, **kwargs):
        return await self.send(*args, **kwargs)


async def replay_double_or_nothing_game(interaction, game_key, stake):
    command_map = {
        "cf": gamble,
        "roulette": roulette,
        "slots": slots,
        "blackjack": blackjack,
        "scratch": scratch,
        "tower": tower,
        "vault": vault,
        "memory": memory_game,
        "cardladder": card_ladder,
        "lockpick": lockpick,
        "heist": heist,
        "diceduel": dice_duel,
        "cases": cases,
        "plinko": plinko,
        "luckynumber": lucky_number,
        "jackpotspin": jackpot_spin,
        "ms": minesweeper,
        "wheel": wheel,
    }
    command = command_map.get(game_key)
    if command is None:
        await interaction.followup.send(f"{Q_DENIED} Double or Nothing is not available for this game yet.")
        return
    _cooldowns.pop((interaction.user.id, "quewo"), None)
    replay_ctx = ReplayContext(interaction)
    await command.callback(replay_ctx, str(int(stake)))


# =====================
# EXPLAIN
# =====================
EXPLANATIONS = {
    "admin": f"Admin power means {QUE_OWNER_DISPLAY}, actual server owner, or Discord Administrator. Server owner outranks admins, and {QUE_OWNER_DISPLAY} is highest.",
    "settings": "Admin-power command. Opens a server setup dashboard for prefix, logs, birthdays, activity reports, lottery status, and disabled commands.",
    "setup": "Alias for `.settings`. Opens the server setup dashboard.",
    "config": "Alias for `.settings`. Opens the server setup dashboard.",
    "games": "Shows the bot's games, short rules, bet support, and how to start each one.",
    "gamelist": "Alias for `.games`. Shows available games.",
    "flagquiz": "Starts a flag quiz with solo/public mode and 10, 20, 50, or all flags.",
    "flags": "Alias for `.flagquiz`. Starts a flag quiz.",
    "fq": "Alias for `.flagquiz`. Starts a flag quiz.",
    "flagstats": "Shows tracked Flag Quiz attempts, correct flags, and rewards.",
    "flagscore": "Alias for `.flagstats`. Shows Flag Quiz stats.",
    "fqstats": "Alias for `.flagstats`. Shows Flag Quiz stats.",
    "bal": "Shows balance, streaks, and total earned/won/lost. Use `.bal` or `.bal @user`.",
    "balance": "Alias for `.bal`. Shows balance, streaks, and total earned/won/lost.",
    "cash": "Alias for `.bal`. Shows balance, streaks, and total earned/won/lost.",
    "bank": "Shows your protected bank, moves cash into/out of it, or claims small daily bank interest. Banked money cannot be robbed.",
    "safe": "Alias for `.bank`. Shows or manages protected money.",
    "vaultcash": "Alias for `.bank`. Shows or manages protected money.",
    "deposit": "Alias for `.bank deposit`. Moves cash into protected bank storage.",
    "withdraw": "Alias for `.bank withdraw`. Moves money from bank back to cash.",
    "tutorial": "Shows or toggles tutorial mode. New users get starter tips until they turn it off.",
    "tutorialmode": "Alias for `.tutorial`. Shows or toggles tutorial mode.",
    "tips": "Alias for `.tutorial`. Shows or toggles tutorial mode.",
    "recommendgame": "Suggests a useful game based on your cash, risk, and recent game stats.",
    "recgame": "Alias for `.recommendgame`. Suggests a game.",
    "whatgame": "Alias for `.recommendgame`. Suggests a game.",
    "suggestgame": "Alias for `.recommendgame`. Suggests a game.",
    "rob": "Attempts to rob cash from another user if this server has robbing enabled. Banked money is protected.",
    "stealqs": "Alias for `.rob`. Attempts to rob cash.",
    "mug": "Alias for `.rob`. Attempts to rob cash.",
    "robsettings": "Admin command. Enables or disables robbing for this server.",
    "robbing": "Alias for `.robsettings`. Shows or changes robbing status.",
    "setrob": "Alias for `.robsettings`. Shows or changes robbing status.",
    "robconfig": "Alias for `.robsettings`. Shows or changes robbing status.",
    "profile": "Shows level, XP, balance, stats, and owned items. Use `.profile` or `.profile @user`.",
    "level": "Alias for `.profile`. Shows level, XP, balance, stats, and owned items.",
    "lvl": "Alias for `.profile`. Shows level, XP, balance, stats, and owned items.",
    "inventory": "Opens a paged inventory UI for owned items, active effects, passive bonuses, and item categories.",
    "settheme": "Equips a profile theme from owned decorative shop items.",
    "theme": "Alias for `.settheme`. Equips a profile theme.",
    "profiletheme": "Alias for `.settheme`. Equips a profile theme.",
    "inv": "Alias for `.inventory`. Opens the paged 𝚀𝚞𝚎wo inventory UI.",
    "items": "Alias for `.inventory`. Opens the paged 𝚀𝚞𝚎wo inventory UI.",
    "streaks": "Shows daily, weekly, monthly, and gambling streaks with next claim times.",
    "streak": "Alias for `.streaks`. Shows streaks.",
    "claimstreaks": "Alias for `.streaks`. Shows streaks.",
    "guide": "Shows a quick getting-started guide for 𝚀𝚞𝚎wo.",
    "onboard": "Interactive first-user onboarding with buttons for daily, games, shop, and profile.",
    "onboarding": "Alias for `.onboard`. Opens the new-user onboarding UI.",
    "newuser": "Alias for `.onboard`. Opens the new-user onboarding UI.",
    "start": "Alias for `.guide`. Shows the getting-started guide.",
    "begin": "Alias for `.guide`. Shows the getting-started guide.",
    "gettingstarted": "Alias for `.guide`. Shows the getting-started guide.",
    "quests": "Opens your quests UI with main, daily, weekly, and monthly quests plus claim/refresh buttons.",
    "dailychallenge": "Shows today's 𝚀𝚞𝚎wo challenge and lets you claim its reward when finished.",
    "challenge": "Alias for `.dailychallenge`. Shows today's 𝚀𝚞𝚎wo challenge.",
    "dc": "Alias for `.dailychallenge`. Shows today's 𝚀𝚞𝚎wo challenge.",
    "qchallenge": "Alias for `.dailychallenge`. Shows today's 𝚀𝚞𝚎wo challenge.",
    "shop": "Opens the categorized 𝚀𝚞𝚎wo shop UI. Select an item, press Buy, then enter quantity. The shop refreshes after purchases and while the UI is open.",
    "limits": "Shows daily gambling loss limits, remaining risk, lottery spend, max bet, cooldown, and personal safety settings.",
    "limit": "Alias for `.limits`. Shows safety limits.",
    "safety": "Alias for `.limits`. Shows safety limits.",
    "safetylimits": "Alias for `.limits`. Shows safety limits.",
    "gamblelimit": "Alias for `.limits`. Shows or sets personal gambling safety limits.",
    "betlimit": "Alias for `.limits`. Shows or sets personal gambling safety limits.",
    "cooldowns": "Shows daily, weekly, monthly, and gambling cooldowns. Use `.cooldowns` or `.cd`.",
    "transactions": "Shows recent 𝚀𝚞𝚎wo transactions. Use `.transactions` or `.transactions @user`.",
    "lottery": "Shows and refreshes the lottery ticket panel. First server run sets channel and draw period.",
    "editlottery": "Server owner/admin command. Edits lottery price, duration, house cut, or channel, then refreshes the panel.",
    "stoplottery": "Server owner/admin command. Stops this server's lottery and clears its tickets/config.",
    "lotterystats": "Shows lottery prize, tickets, players, next draw, paginated ticket holders, and panel link.",
    "buytick": "Legacy text command for buying lottery tickets. The lottery panel buttons are preferred. Lottery spending is capped at 60% of your lottery-adjusted balance per round.",
    "daily": "Claim a daily reward. Higher daily streak means a small bonus.",
    "weekly": "Claim a weekly reward. Higher weekly streak means a bigger bonus.",
    "monthly": "Claim a monthly reward. Higher monthly streak means a bigger bonus.",
    "cf": "Bet on heads or tails. Use `.cf <amount> h` or `.cf <amount> tails`.",
    "flip": "Bet on heads or tails. Use `.cf <amount> h` or `.cf <amount> tails`.",
    "roulette": f"Bet on {Q_ROULETTE_RED} red, {Q_ROULETTE_BLACK} black, or {Q_ROULETTE_GREEN} green. Matching color wins.",
    "slots": f"Slot machine. All 3 reels must match to win: {slot_symbol_help_text()}.",
    "blackjack": "Blackjack with Hit and Stand buttons. Beat the dealer without busting.",
    "scratch": f"Scratch card. All 5 symbols must match to win: {scratch_tier_help_text()}.",
    "tower": f"{Q_TOWER} Pick safe doors, climb floors, and cash out before hitting a trap.",
    "towers": "Alias for `.tower`. Pick safe doors, climb floors, and cash out before hitting a trap.",
    "vault": f"{Q_VAULT} Guess a 3-digit code with exact/close hints before your tries run out.",
    "memory": f"{Q_MEMORY} Match all hidden pairs before too many mistakes or timeout.",
    "mem": "Alias for `.memory`. Match all hidden pairs before too many mistakes or timeout.",
    "cardladder": f"{Q_CARD_LADDER} Higher/lower card ladder. Guess the next card direction and cash out before missing.",
    "ladder": "Alias for `.cardladder`. Guess higher/lower and cash out before missing.",
    "cards": "Alias for `.cardladder`. Guess higher/lower and cash out before missing.",
    "cladder": "Alias for `.cardladder`. Guess higher/lower and cash out before missing.",
    "lockpick": f"{Q_LOCKPICK} Adjust lock pins, use high/low hints, and open the lock before your tests run out.",
    "lp": "Alias for `.lockpick`. Adjust lock pins and open the lock before your tests run out.",
    "picklock": "Alias for `.lockpick`. Adjust lock pins and open the lock before your tests run out.",
    "heist": f"{Q_HEIST} Choose a route, tool, entry plan, vault plan, and escape plan.",
    "robbery": "Alias for `.heist`. Choose a route and tool, then try to escape.",
    "qh": "Alias for `.heist`. Choose a route and tool, then try to escape.",
    "diceduel": f"{Q_DICE_DUEL} Pick a tactic, roll both dice yourself, then make the dealer roll.",
    "dice": "Alias for `.diceduel`. Pick a tactic and roll against the dealer.",
    "dd": "Alias for `.diceduel`. Pick a tactic and roll against the dealer.",
    "cases": f"{Q_CASES} Pick a locked case, then pick the key that changes risk/reward.",
    "case": "Alias for `.cases`. Pick one of three locked cases.",
    "qcase": "Alias for `.cases`. Pick one of three locked cases.",
    "plinko": f"{Q_PLINKO} Choose a drop lane, then nudge the ball every row.",
    "plink": "Alias for `.plinko`. Choose a drop lane and watch the board.",
    "drop": "Alias for `.plinko`. Choose a drop lane and watch the board.",
    "luckynumber": f"{Q_LUCKY_NUMBER} Choose solo/public mode, a number range, then payout/tries. Public winners split 80% to guesser and 20% to starter.",
    "ln": "Alias for `.luckynumber`. Choose mode, range, and payout/tries.",
    "lucky": "Alias for `.luckynumber`. Choose mode, range, and payout/tries.",
    "number": "Alias for `.luckynumber`. Choose mode, range, and payout/tries.",
    "jackpotspin": f"{Q_JACKPOT_SPIN} Pick a target symbol, then spin up to 3 times to land on it.",
    "jackpot": "Alias for `.jackpotspin`. Pick a target and spin the wheel.",
    "jspin": "Alias for `.jackpotspin`. Pick a target and spin the wheel.",
    "jps": "Alias for `.jackpotspin`. Pick a target and spin the wheel.",
    "dungeon": f"{Q_DUNGEON} Free solo dungeon run. Choose through 6 rooms, manage HP/keys/relics, and escape with loot.",
    "dng": "Alias for `.dungeon`. Free solo dungeon run.",
    "qdungeon": "Alias for `.dungeon`. Free solo dungeon run.",
    "gamestats": "Shows tracked game wins, losses, profit, and game badges.",
    "gstats": "Alias for `.gamestats`. Shows tracked game stats.",
    "gamestat": "Alias for `.gamestats`. Shows tracked game stats.",
    "playstats": "Alias for `.gamestats`. Shows tracked game stats.",
    "achievements": "Shows hard 𝚀𝚞𝚎wo game badges, progress, and rewards.",
    "achievement": "Alias for `.achievements`. Shows 𝚀𝚞𝚎wo badges.",
    "badges": "Alias for `.achievements`. Shows 𝚀𝚞𝚎wo badges.",
    "achs": "Alias for `.achievements`. Shows 𝚀𝚞𝚎wo badges.",
    "setbadge": "Equips up to 3 earned badges on your profile.",
    "badge": "Alias for `.setbadge`. Equips profile badges.",
    "equipbadge": "Alias for `.setbadge`. Equips profile badges.",
    "profilebadge": "Alias for `.setbadge`. Equips profile badges.",
    "gamebalance": "Shows the current game risk labels in one audit page.",
    "balancegames": "Alias for `.gamebalance`. Shows game risk labels.",
    "risks": "Alias for `.gamebalance`. Shows game risk labels.",
    "gamerisks": "Alias for `.gamebalance`. Shows game risk labels.",
    "gameaudit": "Admin-power command. Audits recent game win rates, house profit, and balance warnings.",
    "gaudit": "Alias for `.gameaudit`. Audits game balance.",
    "auditgames": "Alias for `.gameaudit`. Audits game balance.",
    "balanceaudit": "Admin-power command. Shows recent balance signals, risk labels, house net, and tune/watch notes.",
    "balaudit": "Alias for `.balanceaudit`. Shows balance tuning signals.",
    "economybalance": "Alias for `.balanceaudit`. Shows balance tuning signals.",
    "balancing": "Alias for `.balanceaudit`. Shows balance tuning signals.",
    "balancedashboard": f"{QUE_OWNER_DISPLAY} command. Shows global money supply, game balance, lottery, tax/payment flow, and anti-abuse watch signals.",
    "ecodashboard": "Alias for `.balancedashboard`. Shows the global economy balance dashboard.",
    "moneydashboard": "Alias for `.balancedashboard`. Shows the global economy balance dashboard.",
    "sinkdashboard": "Alias for `.balancedashboard`. Shows tracked economy sinks and flow.",
    "riskprofile": "Shows a user's 𝚀𝚞𝚎wo risk profile: daily loss usage, game volume, profit, lottery spend, and recent transaction volume.",
    "risk": "Alias for `.riskprofile`. Shows a user's risk profile.",
    "userrisk": "Alias for `.riskprofile`. Shows a user's risk profile.",
    "riskcheck": "Alias for `.riskprofile`. Shows a user's risk profile.",
    "gamehistory": "Shows recent tracked game results.",
    "history": "Alias for `.gamehistory`. Shows recent game results.",
    "ghistory": "Alias for `.gamehistory`. Shows recent game results.",
    "recentgames": "Alias for `.gamehistory`. Shows recent game results.",
    "ms": f"Pick a grid size, reveal safe tiles {Q_XP}, avoid bombs {Q_MINE}.",
    "minesweeper": f"Pick a grid size, reveal safe tiles {Q_XP}, avoid bombs {Q_MINE}.",
    "minesweepeer": f"Pick a grid size, reveal safe tiles {Q_XP}, avoid bombs {Q_MINE}.",
    "wheel": f"Spin the wheel for a multiplier or blank result: {wheel_segment_help_text()}.",
    "give": "Transfers quesos to another user. Target and amount can be in either order.",
    "lb": "Shows a paginated local/global leaderboard with ranking types for quesos, level, earnings, wins, losses, net gambling, and messages.",
    "leaderboard": "Shows a paginated local/global leaderboard with ranking types for quesos, level, earnings, wins, losses, net gambling, and messages.",
    "qstats": "Admin-power command. Shows global 𝚀𝚞𝚎wo economy health, or use `.qstats @user` for that user's audit.",
    "economyhealth": "Alias for `.qstats`. Shows global 𝚀𝚞𝚎wo economy health.",
    "ecohealth": "Alias for `.qstats`. Shows global 𝚀𝚞𝚎wo economy health.",
    "moneyhealth": "Alias for `.qstats`. Shows global 𝚀𝚞𝚎wo economy health.",
    "supply": "Alias for `.qstats`. Shows global 𝚀𝚞𝚎wo economy health.",
    "economystats": "Alias for `.qstats`. Shows global 𝚀𝚞𝚎wo economy health.",
    "qstatus": "Alias for `.qstats`. Shows global 𝚀𝚞𝚎wo economy health.",
    "economyaudit": "Admin-power command. Shows global economy audit signals, or use `.economyaudit @user` for one user's audit.",
    "audit": "Alias for `.economyaudit`. Shows economy audit signals.",
    "qaudit": "Alias for `.economyaudit`. Shows economy audit signals.",
    "econaudit": "Alias for `.economyaudit`. Shows economy audit signals.",
    "abuseaudit": "Admin-power anti-abuse watch for fast game profit, transaction volume, and daily loss signals.",
    "antiexploit": "Alias for `.abuseaudit`. Shows anti-abuse signals.",
    "riskwatch": "Alias for `.abuseaudit`. Shows anti-abuse signals.",
    "season": "Shows the current monthly 𝚀𝚞𝚎wo season leaderboard and rewards.",
    "seasons": "Alias for `.season`. Shows season standings.",
    "seasonlb": "Alias for `.season`. Shows season standings.",
    "seasonpass": "Shows monthly challenge-style goals for the current season.",
    "monthlychallenges": "Alias for `.seasonpass`. Shows monthly goals.",
    "pass": "Alias for `.seasonpass`. Shows monthly goals.",
    "spass": "Alias for `.seasonpass`. Shows monthly goals.",
    "endseason": "Admin-power command that pays season rewards for a season key.",
    "rewardseason": "Alias for `.endseason`. Pays season rewards.",
    "event": "Server owner/admin command for server 𝚀𝚞𝚎wo events. Use `.event`, `.event start jackpot 1h`, `.event donate 50k`, or `.event stop`.",
    "qevent": "Alias for `.event`. Shows or manages server 𝚀𝚞𝚎wo events.",
    "events": "Alias for `.event`. Shows or manages server 𝚀𝚞𝚎wo events.",
    "add": f"{QUE_OWNER_DISPLAY} command. Adds quesos. Target and amount can be in either order.",
    "remove": f"{QUE_OWNER_DISPLAY} command. Removes quesos. Target and amount can be in either order.",
    "addtick": f"{QUE_OWNER_DISPLAY} command. Adds free lottery tickets. Target and tickets can be in either order.",
    "removetick": f"{QUE_OWNER_DISPLAY} command. Removes lottery tickets. Target and tickets can be in either order.",
    "remtick": "Alias for `.removetick`. Removes lottery tickets.",
    "deltick": "Alias for `.removetick`. Removes lottery tickets.",
    "settick": f"{QUE_OWNER_DISPLAY} command. Sets lottery tickets. Target and tickets can be in either order.",
    "setquesos": f"{QUE_OWNER_DISPLAY} command. Sets balances. Target and amount can be in either order.",
    "disable": f"Admin-power command. Disables one bot command. {QUE_OWNER_DISPLAY} can still bypass disabled commands.",
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
    "lock": "Admin-power command. Permission-locks this channel for @everyone.",
    "unlock": "Admin-power command. Undoes `.lock` and restores the channel send-message override.",
    "lockdown": "Admin-power command. Bot-level channel lockdown: non-admin messages are deleted.",
    "reopen": "Admin-power command. Reopens a bot-level `.lockdown` channel.",
    "rlockdown": "Admin-power command. Disables reactions in this channel.",
    "runlock": "Admin-power command. Enables reactions in this channel.",
    "purge": "Admin-power command. Deletes messages. Count and member can be in either order.",
    "send": "Admin-power command. Sends a message in the current channel or a chosen channel.",
    "reply": "Admin-power command. Replies to a message by ID or link.",
    "fwd": "Admin-power command. Forwards recent messages or specific message links. Use `.fwd 5`, `.fwd 5 @user`, `.fwd #channel 5`, or `.fwd #channel <message link>`.",
    "forward": "Alias for `.fwd`. Forwards recent messages or message links.",
    "fw": "Alias for `.fwd`. Forwards recent messages or message links.",
    "quote": "Admin-power command. Quotes one message by link or ID.",
    "archive": "Admin-power command. Creates a transcript file from recent messages.",
    "transcript": "Alias for `.archive`. Creates a transcript file.",
    "auditcommands": f"{QUE_OWNER_DISPLAY} command. Audits command help, examples, aliases, and input UI coverage.",
    "cmdaudit": "Alias for `.auditcommands`. Audits command registry coverage.",
    "commandaudit": "Alias for `.auditcommands`. Audits command registry coverage.",
    "permaudit": f"{QUE_OWNER_DISPLAY} command. Audits sensitive command exposure and permission notes.",
    "permsaudit": "Alias for `.permaudit`. Audits sensitive command exposure.",
    "permissionaudit": "Alias for `.permaudit`. Audits sensitive command exposure.",
    "sensitiveaudit": "Alias for `.permaudit`. Audits sensitive command exposure.",
    "receipt": "Shows one sensitive action receipt. Use `.receipt latest` for the newest receipt.",
    "receipts": f"{QUE_OWNER_DISPLAY} command. Lists recent sensitive action receipts or receipts involving one user.",
    "receiptlist": "Alias for `.receipts`. Lists recent receipts.",
    "txreceipts": "Alias for `.receipts`. Lists recent receipts.",
    "aihistory": f"{QUE_OWNER_DISPLAY} command. Shows recent AI-triggered bot actions. Use `.aihistory @user` to filter by actor.",
    "aiactions": "Alias for `.aihistory`. Shows AI-triggered actions.",
    "actionhistory": "Alias for `.aihistory`. Shows AI-triggered actions.",
    "aiperms": f"{QUE_OWNER_DISPLAY} command. Shows what the AI is allowed to do and what sensitive actions are restricted.",
    "aipermissions": "Alias for `.aiperms`. Shows AI control permissions.",
    "addrole": "Admin-power command. Adds a role to a member. Member and role can be in either order.",
    "removerole": "Admin-power command. Removes a role from a member. Member and role can be in either order.",
    "reactcount": "Admin-power command. Counts reactions on a message.",
    "sleep": "Marks you as sleeping, tracks mentions, and clears when you send a message.",
    "fsleep": f"{QUE_OWNER_DISPLAY} command. Marks members as sleeping and tracks their mentions.",
    "wake": f"{QUE_OWNER_DISPLAY} command. Removes sleep mode from members.",
    "afk": "Marks you AFK, tracks mentions, and clears when you send a message.",
    "setbday": "Saves your birthday. Use `.setbday 25/12`, or run `.setbday` to open a setup UI.",
    "removebday": "Removes your birthday.",
    "setbdaychannel": "Sets the server's birthday announcement channel. Use `.setbdaychannel #channel` or `.setbdaychannel <channel id>`.",
    "bdaychannel": "Alias for `.setbdaychannel`. Sets the server's birthday announcement channel.",
    "birthdaychannel": "Alias for `.setbdaychannel`. Sets the server's birthday announcement channel.",
    "activity": "Shows this server's activity report status. Use `.activity setup` to set or change the report channel.",
    "activitystats": "Shows this server's activity report status, current top 5, report channel, and next report time.",
    "astats": "Alias for `.activitystats`.",
    "messages": "Shows a message-count tracker with time ranges, a top 10 leaderboard, and per-user counts.",
    "msgstats": "Alias for `.messages`.",
    "messagestats": "Alias for `.messages`.",
    "mstats": "Alias for `.messages`.",
    "editactivity": "Admin command. Opens a UI or edits the activity report channel/next report time.",
    "activityedit": "Alias for `.editactivity`.",
    "endactivity": "Admin command. Ends the current activity report now, posts the previous winners, and starts a fresh activity window.",
    "stopcurrentactivity": "Alias for `.endactivity`.",
    "currentactivitystop": "Alias for `.endactivity`.",
    "resetactivity": "Alias for `.endactivity`.",
    "activityreset": "Alias for `.endactivity`.",
    "stopactivity": "Admin command. Stops this server's daily activity reports and clears the current activity window.",
    "activitystop": "Alias for `.stopactivity`.",
    "away": "Shows a live board of AFK and sleeping users.",
    "listbans": "Admin-power command. Lists blacklisted users.",
    "calc": "Calculates a math expression. Use `.calc 2+2*5`, or run `.calc` to open a setup UI.",
    "define": "Looks up a word definition. Use `.define example`, or run `.define` to open a setup UI.",
    "timer": "Starts a timer. Use `.timer 10m`, `.timer 10m study`, `.timer study 10m`, or run `.timer` to open a setup UI.",
    "ctimer": "Cancels one of your active timers from a menu.",
    "alarm": "Sets an alarm. Use `.alarm 1h reminder`, `.alarm 25/12`, or `.alarm 25/12/2026 18:30 reminder`.",
    "poll": "Creates a reaction poll. Use `.poll Is this good?`, `.poll Best color? blue red`, `.poll Best color? | Blue | Red | 10m`, or run `.poll` to open a setup UI.",
    "epoll": "Ends one of your active polls. Admin-power users can end any active poll in the server.",
    "giveaway": "Admin-power command. Starts a timed giveaway. Use `.giveaway 10m prize`, `.giveaway prize 10m`, or run `.giveaway` to open a setup UI.",
    "picker": "Picks one option. Use spaces, commas, pipes, quoted multi-word options, or run `.picker` to open a setup UI.",
    "steal": "Admin-power command. Copies a custom emoji or sticker into this server if the bot has permission.",
    "ask": "Asks the AI a question.",
    "generate": "Generates text with AI.",
    "analyse": "Analyzes an image from a replied message.",
    "analyze": "Alias for `.analyse`. Analyzes an image from a replied message.",
    "summarize": "Summarizes recent channel messages, optionally filtered by user, channel, message count, or time range.",
    "summarise": "Alias for `.summarize`. Summarizes recent chat.",
    "summary": "Alias for `.summarize`. Summarizes recent chat.",
    "aisummary": "Alias for `.summarize`. Summarizes recent chat with AI.",
    "tldr": "Alias for `.summarize`. Summarizes recent chat.",
    "recap": "Alias for `.summarize`. Summarizes recent chat.",
    "aidetect": "Checks whether an essay or text has AI-like writing patterns. It is a likelihood check, not proof.",
    "aicheck": "Alias for `.aidetect`. Checks AI-like writing patterns.",
    "detectai": "Alias for `.aidetect`. Checks AI-like writing patterns.",
    "authenticity": "Alias for `.aidetect`. Checks writing authenticity signals.",
    "authcheck": "Alias for `.aidetect`. Checks writing authenticity signals.",
    "essaycheck": "Alias for `.aidetect`. Checks AI-like essay patterns.",
    "aimemory": f"Shows AI memory. Use `.aimemory`; {QUE_OWNER_DISPLAY} can inspect another user with `.aimemory @user`.",
    "aime": "Alias for `.aimemory`. Shows AI memory.",
    "memoryai": "Alias for `.aimemory`. Shows AI memory.",
    "whatyouknow": "Alias for `.aimemory`. Shows what the AI knows about a user.",
    "aiknow": "Shows what the AI currently knows about a bot command.",
    "aiknowledge": "Alias for `.aiknow`. Shows AI command knowledge.",
    "knowcmd": "Alias for `.aiknow`. Shows AI command knowledge.",
    "aicmd": "Alias for `.aiknow`. Shows AI command knowledge.",
    "aidoctor": "Admin-power command. Shows bot health/debug info for AI-assisted troubleshooting.",
    "botdoctor": "Alias for `.aidoctor`. Shows bot health/debug info.",
    "doctorai": "Alias for `.aidoctor`. Shows bot health/debug info.",
    "diagnosebot": "Alias for `.aidoctor`. Shows bot health/debug info.",
    "styleaudit": f"{QUE_OWNER_DISPLAY} command. Audits output style expectations by command category.",
    "uiaudit": "Alias for `.styleaudit`. Audits message and UI consistency.",
    "messageaudit": "Alias for `.styleaudit`. Audits message and UI consistency.",
    "commandcleanup": f"{QUE_OWNER_DISPLAY} command. Shows command cleanup gaps: categories, examples, generic explain text, and duplicate aliases.",
    "cleanupcommands": "Alias for `.commandcleanup`. Shows command cleanup gaps.",
    "cmdcleanup": "Alias for `.commandcleanup`. Shows command cleanup gaps.",
    "translate": "Translates text. Use `.translate hello to Italian`, `.translate it hello`, or reply to a message with `.translate to Spanish`.",
    "econhelp": "Shows 𝚀𝚞𝚎wo commands, aliases, and short explanations.",
    "economyhelp": "Alias for `.econhelp`. Shows 𝚀𝚞𝚎wo commands, aliases, and short explanations.",
    "quewohelp": "Alias for `.econhelp`. Shows 𝚀𝚞𝚎wo commands, aliases, and short explanations.",
    "ehelp": "Alias for `.econhelp`. Shows 𝚀𝚞𝚎wo commands, aliases, and short explanations.",
    "explain": "Shows detailed help for one command. Use `.explain <command>`.",
    "prefix": "Shows or changes this server's command prefix.",
    "preifx": "Typo alias for `.prefix`. Shows or changes this server's command prefix.",
    "ttt": "Starts Tic Tac Toe against another user. If the challenger sets a bet, the opponent must accept that bet too.",
    "c4": "Starts Connect 4 against another user. If the challenger sets a bet, the opponent must accept that bet too.",
    "chess": "Starts a chess game against another user with move confirmation, optional bets, live 10-minute player clocks, and a board that flips by turn.",
    "move": "Fallback chess command. Makes a chess move with notation like `.move e2e4` or `.move Nf3`.",
    "chessmove": "Alias for `.move`. Makes a chess move with notation like `.move e2e4` or `.move Nf3`.",
    "resign": "Resigns the active chess game in this channel.",
}

DETAILED_EXPLANATIONS = {
    "daily": f"Gives a reward once every 24 hours. Base reward is 10,000-15,000 {CURRENCY_EMOJI}. Your daily streak adds a small bonus after day 1.",
    "bank": "Protected savings for 𝚀𝚞𝚎wo. Use `.bank` to view cash/bank/total, `.bank deposit 100k` to protect cash, `.bank deposit all` to store all cash, `.bank withdraw 50k` to move money back to spendable cash, or `.bank interest` to claim small daily interest. Robbing can only touch cash, never banked money.",
    "tutorial": "Tutorial mode is on for new users. After a few starter prompts it shows an End Tutorial button. Use `.tutorial off` to stop starter tips or `.tutorial on` to bring them back.",
    "recommendgame": "Looks at your cash and recent game stats, then suggests a game that makes sense for your bankroll and risk. It prioritizes safe/free games when you are low on cash and cashout-control games when you have more room.",
    "rob": "Robbing is server-controlled with `.robsettings on/off`. If enabled, `.rob @user` can steal a small chunk of the target's cash balance, capped at 200k, but it can fail and pay the target a fine. Banked money cannot be robbed.",
    "profile": f"Shows level, current XP toward the next level, balance, net gambling result, and shop items. Chat XP can level you up and level rewards start at {format_balance(LEVEL_REWARD_BASE)}.",
    "level": f"Alias for `.profile`. Shows level, current XP toward the next level, balance, net gambling result, and shop items. Level rewards start at {format_balance(LEVEL_REWARD_BASE)}.",
    "lvl": f"Alias for `.profile`. Shows level, current XP toward the next level, balance, net gambling result, and shop items. Level rewards start at {format_balance(LEVEL_REWARD_BASE)}.",
    "inventory": "Opens a paged inventory UI. Overview shows balance, item count, active temporary effects, passive bonuses, and owned categories. Other pages show Active Effects, Owned Items, or a full category list with descriptions and owned counts.",
    "settheme": "Equips a profile theme from owned decorative shop items. Use `.settheme` to list themes, then `.settheme velvet`, `.settheme gold`, `.settheme royal`, or `.settheme highroller` if you own the needed item.",
    "streaks": "Shows claim streaks and live next-claim timestamps for daily, weekly, and monthly. Also shows the active universal gambling win streak and current payout multiplier.",
    "guide": "New-user guide for earning, playing, spending, and safety commands. Use it when someone joins and does not know where to start.",
    "onboard": "Interactive first-user onboarding. It shows a compact start guide with buttons for daily rewards, games, shop, and profile so new users do not need to memorize commands.",
    "inv": "Alias for `.inventory`. Opens the paged 𝚀𝚞𝚎wo inventory UI.",
    "items": "Alias for `.inventory`. Opens the paged 𝚀𝚞𝚎wo inventory UI.",
    "quests": "Main quests track long streak achievements: 30 daily claims, 8 weekly claims, and 5 monthly claims. Daily, weekly, and monthly random quests rotate by period and can be claimed from the `.quests` UI.",
    "dailychallenge": "One rotating daily challenge is active per day. Game wins update it automatically, and Flag Quiz point challenges count each correct flag. Use `.dailychallenge claim` when your progress reaches the target.",
    "shop": "Opens an interactive categorized 𝚀𝚞𝚎wo shop. Select an item, press Buy, then enter the quantity. The bot checks your balance, item limit, and total price before purchasing.",
    "cooldowns": "Shows daily, weekly, monthly, and active gambling command cooldowns in one place.",
    "transactions": "Shows recent money movement including shop purchases, quest rewards, level rewards, transfer tax, admin changes, and lottery activity.",
    "limits": f"Shows your daily gambling loss safety limit. The bot warns near {int(DAILY_LOSS_WARNING_RATIO * 100)}% and blocks bets before daily losses exceed {int(DAILY_LOSS_HARD_RATIO * 100)}% of your current daily gambling bankroll. It also shows lottery round spending, your personal gambling cap, and any active gambling pause. Use `.limits set 50k`, `.limits pause 2h`, `.limits resume`, or `.limits clear`.",
    "lottery": f"Server lottery. First run asks the server owner or an admin for a channel and draw period, locks the channel, and posts a persistent ticket panel with buy buttons. Existing active lottery data is preserved when the panel is refreshed. The prize is the full current pot. Tickets cost {format_balance(LOTTERY_TICKET_COST)} and {int(LOTTERY_HOUSE_CUT * 100)}% is burned as a money sink. Users can spend up to {int(LOTTERY_MAX_BALANCE_SPEND_RATIO * 100)}% of their lottery-adjusted balance per round.",
    "editlottery": "Server owner/admin command. Run `.editlottery` to open the edit UI, or use `.editlottery price 250000`, `.editlottery duration 12h`, `.editlottery cut 5`, or `.editlottery channel #lottery`. Duration resets the next draw timer. Channel posts a fresh lottery panel. Updates ping the lottery participant role.",
    "stoplottery": "Server owner/admin command. Use `.stoplottery` to remove the lottery setup for this server, clear the current pot/tickets, and delete the participant role if the bot can. It leaves channels and panel messages alone.",
    "lotterystats": "Shows the current lottery prize pot, total ticket count, number of players, participant role, next draw time, panel link, and paginated ticket holders with approximate odds.",
    "buytick": f"Legacy text command for buying tickets for the configured server lottery. The lottery panel buttons are preferred because they send private confirmations and update the panel automatically. Each ticket costs {format_balance(LOTTERY_TICKET_COST)}. The prize is the full current lottery pot; every ticket is one entry. Ticket spending is capped at {int(LOTTERY_MAX_BALANCE_SPEND_RATIO * 100)}% of your lottery-adjusted balance for the current round, so earning or spending quesos changes how many more tickets you can buy.",
    "weekly": f"Gives a reward once every 7 days. Base reward is 20,000-30,000 {CURRENCY_EMOJI}. Your weekly streak adds a bonus after week 1.",
    "monthly": f"Gives a reward once every 30 days. Base reward is 40,000-60,000 {CURRENCY_EMOJI}. Your monthly streak adds a bigger bonus after month 1.",
    "cf": "Pick heads or tails with `.cf <amount> h`, `.cf <amount> t`, `.flip <amount> heads`, or `.flip <amount> tails`. If you do not pick, the bot asks you. Winning pays ×2 before the universal gambling streak bonus, so betting 100 wins 200 total before bonuses. Losing removes the bet and resets the gambling streak.",
    "flip": "Pick heads or tails with `.cf <amount> h`, `.cf <amount> t`, `.flip <amount> heads`, or `.flip <amount> tails`. If you do not pick, the bot asks you. Winning pays ×2 before the universal gambling streak bonus, so betting 100 wins 200 total before bonuses. Losing removes the bet and resets the gambling streak.",
    "roulette": f"Pick {Q_ROULETTE_RED} red, {Q_ROULETTE_BLACK} black, {Q_ROULETTE_GREEN} green, or use the button menu if you leave the color blank. Matching your color pays ×3 before the universal gambling streak bonus. Losing removes the bet and resets the gambling streak.",
    "slots": f"The bot spins 3 reels with 4 custom symbols. All 3 reels must match to win: {slot_symbol_help_text()}. The base match chance is about {int(SLOTS_WIN_CHANCE * 100)}%. Non-perfect results lose the bet and reset the universal gambling streak.",
    "blackjack": "You get cards against the dealer and use Hit or Stand buttons. Try to get closer to 21 than the dealer without going over. The dealer hits below 16, usually hits on 16, and stands on 17+. A normal win pays +1x your bet as profit before the universal gambling streak bonus. Losing removes the bet and resets the streak. A push changes nothing.",
    "scratch": f"The ticket reveals 5 symbols one by one: {scratch_symbol_help_text()}. All 5 symbols must match to win. Payout tiers: {scratch_tier_help_text()}. The base win chance is intentionally low at about 8%. Losing resets the universal gambling streak.",
    "tower": f"Choose one of 3 doors per floor. Every floor has 2 trapped doors and 1 safe door. Safe floors raise the cash-out multiplier through {', '.join(f'×{m:.2f}' for m in TOWER_MULTIPLIERS)}. Cash out anytime after one safe door, or risk climbing higher. Wins use the universal gambling streak bonus; traps and timeouts reset it.",
    "towers": "Alias for `.tower`. Climb safe doors and cash out before hitting a trap.",
    "vault": f"Guess a secret 3-digit code with no repeated digits. Each guess tells you exact digits and close digits. Opening the vault within {VAULT_GUESSES} tries pays ×{VAULT_MULTIPLIER} before the universal gambling streak bonus. Running out of tries resets the streak.",
    "memory": f"Flip a 4x4 board and match 8 pairs. After opening one tile, you have {MEMORY_PICK_SECONDS} seconds to pick the second tile or all unsolved tiles shuffle. Wrong pairs show briefly for {MEMORY_REVEAL_SECONDS:g}s. Matching every pair before {MEMORY_MAX_MISTAKES} mistakes or timeout pays ×{MEMORY_MULTIPLIER} before the universal gambling streak bonus. Too many mistakes or timeout resets the streak.",
    "mem": "Alias for `.memory`. Match all hidden pairs before too many mistakes or timeout.",
    "cardladder": f"You start with one card, then choose Higher or Lower for the next card. Ties count as misses. Correct calls climb the ladder through {', '.join(f'×{m:.2f}' for m in CARD_LADDER_MULTIPLIERS)}. Cash out after any correct call, or clear the ladder for the top multiplier. Wins use the universal gambling streak bonus; wrong calls and timeouts reset it.",
    "ladder": "Alias for `.cardladder`. Guess higher/lower and cash out before missing.",
    "cards": "Alias for `.cardladder`. Guess higher/lower and cash out before missing.",
    "cladder": "Alias for `.cardladder`. Guess higher/lower and cash out before missing.",
    "lockpick": f"Set {LOCKPICK_PINS} lock pins from 1-{LOCKPICK_HEIGHTS}. Press each pin to raise it, Test to spend one try, and use the per-pin high/low/set hints to solve the lock. Opening it within {LOCKPICK_TRIES} tests pays ×{LOCKPICK_MULTIPLIER:g} before the universal gambling streak bonus. Running out of tests or timing out resets the streak.",
    "lp": "Alias for `.lockpick`. Adjust lock pins and open the lock before your tests run out.",
    "picklock": "Alias for `.lockpick`. Adjust lock pins and open the lock before your tests run out.",
    "heist": "Choose a route and a tool, then play through Entry, Vault, and Escape choices. Safer choices raise success chance and lower payout; louder choices lower success chance and raise payout. Wins can use Double or Nothing to replay the same game with the prize at risk.",
    "diceduel": "Choose Steady, Normal, or Push, then press Roll Die twice yourself and make the dealer roll. Steady adds +1 with lower payout, Push subtracts -1 with higher payout, and Normal is classic. Higher total wins; ties refund.",
    "cases": "Three locked cases appear. Pick a case, then pick Safe, Clean, or Royal key. Safe can downgrade, Royal can upgrade, and Clean opens normally. Better case tiers are rarer. Wins can use Double or Nothing to replay the same game with the prize at risk.",
    "plinko": "Choose one of five drop lanes, then nudge left, let drop, or nudge right every row. Peg bounce still adds luck, but your row choices affect the final lane and multiplier. ×1 refunds; higher than ×1 wins.",
    "luckynumber": "Choose Solo or Public Channel, then choose a range: 1-10, 1-20, 1-50, or 1-100. After the range, choose payout/tries. 1-10: 3 tries ×2, 2 tries ×3, 1 try ×5. 1-20: 5 tries ×3, 4 tries ×4, 2 tries ×6. 1-50: 10 tries ×4, 6 tries ×6, 4 tries ×8. 1-100: 15 tries ×3, 8 tries ×8, 2 tries ×20. All modes have 1 minute to finish. In public mode, each user gets their own tries. If another user guesses correctly, they get 80% of the prize and the starter gets 20%. If nobody gets it, only the starter loses.",
    "jackpotspin": f"Choose a target symbol, then press Spin Wheel up to 3 times. Normal total hit chances are {Q_SLOT_STAR} ×2 at 42%, {Q_SLOT_DIAMOND} ×5 at 16%, {Q_SLOT_CROWN} ×10 at 7%, and {Q_SLOT_JACKPOT} ×50 at 1.4%. Luck bonuses help smaller targets more than huge targets, so ×50 stays rare. Missing all 3 spins loses the bet.",
    "dungeon": f"Free solo adventure. Pick choices through 6 rooms, manage {Q_DUNGEON_HEART} HP, collect {Q_DUNGEON_KEY} keys and {Q_DUNGEON_RELIC} relics, then beat the final room to carry the loot out. Fight starts a guard-read mini-game, Disarm starts a wire-sequence mini-game, and Pick Lock starts a 3-pin lockpick mini-game. Clearing the run pays the loot you escaped with and records a Dungeon win. Getting knocked out pays nothing.",
    "dng": "Alias for `.dungeon`. Free solo adventure with choices, HP, keys, relics, and a clear reward.",
    "qdungeon": "Alias for `.dungeon`. Free solo adventure with choices, HP, keys, relics, and a clear reward.",
    "gamestats": "Shows tracked game stats for you or a mentioned user: total played, wins, profit, per-game records, and hard game badges. Example: `.gamestats` or `.gamestats @user`.",
    "achievements": "Shows hard game badge progress and rewards. Badges are intentionally long-term, like 100 wins in a game or 1,000 total games played. Earned badge rewards are paid automatically when the tracked game result completes.",
    "setbadge": "Use `.setbadge` with no arguments to list earned badge IDs. Use `.setbadge <badge_id> [badge_id] [badge_id]` to show up to 3 earned badges on `.profile`, or `.setbadge clear` to remove them.",
    "gamebalance": "Lists every 𝚀𝚞𝚎wo game with its current risk label. Use this as the quick balance audit before changing odds, multipliers, or max-risk behavior.",
    "riskprofile": "Shows user-specific safety signals rather than game-wide risk. It combines balance, current daily loss usage, tracked game play/win/profit totals, current lottery spend in this server, recent transaction volume, and plain warning notes.",
    "gameaudit": "Admin-power balancing dashboard. It reads recent tracked game history, then shows plays, win rate, house profit, and signals for games that look too generous or too harsh. Use `.gameaudit` for 7 days or `.gameaudit 30` for up to 30 days.",
    "balanceaudit": "Admin-power economy balancing dashboard. It reads recent tracked game history, compares play count, win rate, house net, and risk labels, then gives tune/watch notes for games that may need odds, payout, or risk-label updates. Use `.balanceaudit` for 14 days or `.balanceaudit 30` for up to 30 days.",
    "balancedashboard": f"{QUE_OWNER_DISPLAY}-only global economy dashboard. It combines money supply, tracked tax/payment flow, lottery size, recent game balance, and anti-abuse watch signals into one compact view. Use `.balancedashboard` or `.balancedashboard 30`.",
    "styleaudit": "Checks whether major command categories have clear style expectations: games, 𝚀𝚞𝚎wo, confirmations/admin tools, status messages, AI, and utility commands. Use it after big UI/message changes.",
    "commandcleanup": "Shows a focused command registry cleanup plan: commands missing help categories, commands with required input but no input UI/example, generic explain text, duplicate aliases, and near-duplicate names.",
    "gamehistory": "Shows your recent tracked game results from the shared game history table. Newer and tracked games appear here with result, net amount, and timestamp.",
    "season": "Monthly 𝚀𝚞𝚎wo season leaderboard. Game results add to the current month automatically. Ranking is by monthly game profit, then wins, then games played. Top rewards are 100m, 80m, and 50m.",
    "seasonpass": "Monthly challenge-style page that shows broad goals like games played, wins, unique games tried, profit, and level. It points users toward season and achievement progression without replacing `.season` rewards.",
    "endseason": "Admin-power season payout command. Use `.endseason 2026-05` to pay the top 3 for that month once. If no season is passed, it pays the current season.",
    "event": "Server event and sink system. `.event` shows the active event. `.event start jackpot 1h` starts a server event for 1 minute to 14 days. `.event donate 50k` burns the user's quesos into the event pot and records an event_sink transaction. `.event stop` ends the event and shows the final pot.",
    "abuseaudit": "Admin-power watch page for suspicious economy activity. It flags high one-hour game volume/profit, high 24-hour transaction volume, and large daily gambling losses. It is a signal page only; review context before acting.",
    "ms": f"Choose 3x3, 4x4, or 5x5, then reveal tiles. Hidden tiles show as {Q_MS_HIDDEN}, safe gems show as {Q_XP}, bombs show as {Q_MINE}, and your cursor shows as {Q_MS_CURSOR}. 3x3 has 1 bomb, 4x4 has 3 bombs, and 5x5 has 5 bombs. Reveal every safe tile to win. The final multiplier starts at ×2.00 and each safe reveal adds +0.15, then the universal gambling streak bonus applies. Hitting a bomb or timing out loses the bet and resets the streak.",
    "minesweeper": f"Choose 3x3, 4x4, or 5x5, then reveal tiles. Hidden tiles show as {Q_MS_HIDDEN}, safe gems show as {Q_XP}, bombs show as {Q_MINE}, and your cursor shows as {Q_MS_CURSOR}. 3x3 has 1 bomb, 4x4 has 3 bombs, and 5x5 has 5 bombs. Reveal every safe tile to win. The final multiplier starts at ×2.00 and each safe reveal adds +0.15, then the universal gambling streak bonus applies. Hitting a bomb or timing out loses the bet and resets the streak.",
    "minesweepeer": f"Choose 3x3, 4x4, or 5x5, then reveal tiles. Hidden tiles show as {Q_MS_HIDDEN}, safe gems show as {Q_XP}, bombs show as {Q_MINE}, and your cursor shows as {Q_MS_CURSOR}. 3x3 has 1 bomb, 4x4 has 3 bombs, and 5x5 has 5 bombs. Reveal every safe tile to win. The final multiplier starts at ×2.00 and each safe reveal adds +0.15, then the universal gambling streak bonus applies. Hitting a bomb or timing out loses the bet and resets the streak.",
    "wheel": f"The wheel lands on a segment: {wheel_segment_help_text()}. ×2, ×3, and ×5 are wins. ×1 gives your stake back, ×0.5 loses half the bet, and BLANK changes nothing. Wins use the universal gambling streak bonus; losses and partial losses reset it.",
    "give": f"Moves quesos from you to another user. Normal transfers burn {int(TRANSFER_TAX_RATE * 100)}% as tax. Use `.give @user 10k` or `.give 10k @user`. Admin-power users can use `all`. The message shows sent amount, tax, received amount, and balances.",
    "fwd": "Forwards messages as quoted copies with the original author mention, source channel, jump link, text, attachments, stickers, and basic embed info. Use `.fwd 5` for the last 5 messages, `.fwd 5 @user` to filter by sender, `.fwd #logs 5` to forward into another channel, or `.fwd #logs <message link>` for exact messages. Limit is 25 messages per run.",
    "forward": "Alias for `.fwd`. Forwards recent messages or exact message links.",
    "fw": "Alias for `.fwd`. Forwards recent messages or exact message links.",
    "quote": "Quotes one exact message by link or current-channel message ID. It uses the same forwarded-message format as `.fwd`, including author mention without pinging, source channel, jump link, content, and attachment links.",
    "archive": "Creates a plain-text transcript file from recent messages. Use `.archive 50`, `.archive 50 @user`, or `.archive #logs 50`. Limit is 100 messages per archive.",
    "auditcommands": f"{QUE_OWNER_DISPLAY}-only maintenance report. It scans the live command registry for missing help categories, missing explanation text, missing detailed text, missing examples, input commands without UI/example coverage, and duplicate aliases.",
    "permaudit": f"{QUE_OWNER_DISPLAY}-only permission audit. It checks sensitive command registration, public help visibility, AI/slash visibility notes, and the risk note for each command that can move money, alter tickets, send as the bot, or change AI controls.",
    "receipt": "Shows full details for one sensitive action receipt, including actor, targets, amount, time, and stored details. Use `.receipt latest` for the newest receipt or `.receipt QTX-...` for a specific one.",
    "receipts": f"{QUE_OWNER_DISPLAY}-only receipt list. Use `.receipts latest` for recent sensitive actions in the server or `.receipts @user` to see receipts involving a specific user. Use `.receipt latest` or `.receipt <id>` for full details.",
    "aihistory": f"{QUE_OWNER_DISPLAY}-only safety log for AI-triggered actions. It shows recent AI command runs and batch rewards since the current bot restart. Use `.aihistory @user` to filter by actor.",
    "aiperms": f"{QUE_OWNER_DISPLAY}-only AI permissions dashboard. It lists safe commands the AI can run directly, sensitive controls that stay restricted, ignored users, and current AI settings.",
    "lb": "Shows a paginated leaderboard. Use the buttons to switch between local server rankings and global all-server rankings. Use the ranking type menu to sort by quesos, level, earnings, total won, total lost, net gambling, or messages. The embed also shows your rank for the selected scope and type.",
    "leaderboard": "Alias for `.lb`. Shows local/global paginated rankings with selectable ranking types and your rank.",
    "qstats": "Admin-power command for checking the global 𝚀𝚞𝚎wo economy. It shows total money supply, total earned, gambling won/lost/net, active lotteries, lottery pots, ticket count, tracked taxes/payments, richest user, and tracked messages. If you pass a member, it redirects to that user's economy audit.",
    "economyhealth": "Alias for `.qstats`. Use it when you want the global money-supply health view instead of a specific user's audit.",
    "economyaudit": "Admin-power economy audit page. Without a member, it adds balancing signals on top of qstats: daily losses, recent transaction volume, tracked sink/payment totals, and per-game play/win/profit signals sorted by biggest impact. With a member, it shows that user's balance, level, net gambling, daily loss usage, game signals, recent games, recent transactions, and warnings.",
    "add": f"{QUE_OWNER_DISPLAY}-only command. Adds new quesos to a user. `.add @user <amount>` and `.add <amount> @user` both work. {QUE_OWNER_DISPLAY} can use @everyone or a role. It does not support `all`. Sensitive changes generate a receipt ID.",
    "remove": f"{QUE_OWNER_DISPLAY}-only command. Removes quesos from a user. `.remove @user <amount>` and `.remove <amount> @user` both work. Use `all` to remove their full balance. Sensitive changes generate a receipt ID.",
    "addtick": f"{QUE_OWNER_DISPLAY}-only lottery admin command. Adds free entries to the current lottery. `.addtick @user <tickets>` and `.addtick <tickets> @user` both work, including roles and @everyone.",
    "removetick": f"{QUE_OWNER_DISPLAY}-only lottery admin command. Removes entries from the current lottery. `.removetick @user <tickets>` and `.removetick <tickets> @user` both work, including roles and @everyone. Tickets never go below 0.",
    "settick": f"{QUE_OWNER_DISPLAY}-only lottery admin command. Sets current lottery entries to an exact number. `.settick @user <tickets>` and `.settick <tickets> @user` both work, including roles and @everyone.",
    "setquesos": f"{QUE_OWNER_DISPLAY}-only 𝚀𝚞𝚎wo admin command. Sets balances directly. `.setquesos @user <amount>` and `.setquesos <amount> @user` both work, including roles and @everyone.",
    "prefix": f"Changes the command prefix for this server. Use `.prefix !` or `.preifx !`. If {QUE_OWNER_DISPLAY} is in the server, only {QUE_OWNER_DISPLAY} can change it. If not, the server owner or admins can change it.",
    "preifx": "Typo alias for `.prefix`. Changes the command prefix for this server.",
    "setbdaychannel": "Sets the birthday announcement channel for this server. Use `.setbdaychannel #birthdays` or `.setbdaychannel <channel id>`. Users keep one birthday date globally, and the bot announces it in every server where they are still a member and a birthday channel is configured.",
    "activity": "Shows the daily activity report status for this server, including report channel, next report time, and current top 5. Use `.activity setup` to set or change the report channel. Every 24 hours, the bot posts the top 5 members by tracked messages since the last report, then resets that server's activity window.",
    "activitystats": "Shows the same activity status embed as `.activity`, including report channel, next report time, and the current top 5.",
    "messages": "Opens a tracker UI for server message counts. Pick a range from 1 day up to 1 year, then view either the top 10 leaderboard or a specific user's count.",
    "editactivity": "Admin command. Run `.editactivity` to open the edit UI, use `.editactivity channel #activity` to move reports, or use `.editactivity next 12h` to reset when the next 24-hour activity report posts.",
    "endactivity": "Admin command. Use `.endactivity` to finish the current report immediately. It clears the report channel, posts the previous activity winners, starts a fresh 24-hour activity window, and keeps activity reports enabled.",
    "stopactivity": "Admin command. Use `.stopactivity` to disable daily activity reports for this server and clear the current tracked activity window.",
    "timer": "Starts a live countdown and pings you when it ends. The time can come before or after the title: `.timer 10m study`, `.timer study 10m`, `.timer 1h 20m`, or `.timer 30s`. Use `.ctimer` to cancel one of your active timers.",
    "ctimer": f"Opens a menu of your active timers so you can cancel one. {QUE_OWNER_DISPLAY} can cancel any active timer.",
    "alarm": "Sets a one-off alarm and pings you when it is due. Relative times work, like `.alarm 1h feed cat`; dates work too, like `.alarm 25/12`, `.alarm 25/12 18:00`, or `.alarm 25/12/2026 18:30 travel`.",
    "poll": "Creates a reaction poll. For a yes/no poll, use `.poll Is this good?`. For custom choices, use `.poll Best color? blue red`, `.poll Best color? \"light blue\" red`, or `.poll Best color? | Blue | Red | 10m`. A final time like `10m`, `2h`, or `1d` makes the poll end automatically.",
    "epoll": "Opens a menu to end one of your active polls. Admin-power users can end any active poll in the server.",
    "picker": "Randomly chooses one option. Use `.picker apple banana orange`, comma-separated options, pipes, or quotes for multi-word options like `.picker \"ice cream\" pizza sushi`.",
    "giveaway": "Admin-power command. Starts a reaction giveaway. The time can come before or after the prize: `.giveaway 10m Nitro` or `.giveaway Nitro 10m`. Users react with the confetti emoji to enter.",
    "calc": "Safely evaluates math expressions. Supports normal operators plus functions like `sqrt`, `sin`, `cos`, `log`, and constants like `pi`. Use `.calc 2+2*5`, or run `.calc` to open the calculator UI.",
    "define": "Looks up English dictionary definitions. Use `.define example`, or run `.define` to enter the word through a UI.",
    "setbday": "Saves your birthday as day/month. Use `.setbday 25/12`, or run `.setbday` to enter it through a UI. Birthday announcements use the server's configured birthday channel.",
    "ask": "Asks Pro𝚀𝚞𝚎's AI a question. The AI answers from its model knowledge, bot context, reply context, and saved memory; live web search is not connected.",
    "summarize": "Summarizes recent messages from the current channel, a mentioned channel, a specific user, or a time/message window. Examples: `.summarize`, `.summarize 50 messages`, `.summarize @user today`, `.summarize #general last 2 hours`. Mention or reply to Pro𝚀𝚞𝚎 with a natural request like `summarize what @user said today` and it will run directly without asking for confirmation.",
    "aidetect": "Writing authenticity check for essays and long text. Use `.aidetect <text>` or reply to text with `.aidetect`. It returns Low/Medium/High AI-like likelihood, a score, signals, and next-step suggestions. It never treats the result as proof because AI detectors can falsely flag human writing and miss edited AI writing.",
    "aimemory": f"Shows the AI memory attached to a Discord user, including explicit remembered facts and bot-saved profile details. AI memory stays on so Pro𝚀𝚞𝚎 can keep context and bot help consistent. Normal users can use `.aimemory` for themselves. {QUE_OWNER_DISPLAY} can inspect another user with `.aimemory @user` or `.aimemory <user id>`.",
    "aiknow": "Debug/helper command that shows exactly what Pro𝚀𝚞𝚎's AI knows about a command from the live command registry, aliases, help text, permission note, and detailed explanation data.",
    "aidoctor": "Admin-power bot doctor panel. Shows task health, 𝚀𝚞𝚎wo DB status, slash sync status, active sessions, disabled commands, and slow command stats. Useful when replying to errors and asking the AI to diagnose them.",
    "translate": "Translates provided text or the message you reply to. Friendly forms work: `.translate hello to Italian`, `.translate to Spanish hello`, `.translate it hello`, or reply to a message with `.translate to Spanish`. If no target is given, it translates to English.",
    "settings": "Admin-power server setup dashboard. It summarizes prefix, logs, reaction logs, birthday channel, activity reports, lottery, and disabled command count. Buttons let admins refresh the dashboard, change the prefix, rerun log setup, or set birthdays/activity to the current channel.",
    "explain": "Shows detailed help for a command, including usage, aliases, short explanation, and longer details when available. Example: `.explain slots`.",
    "games": "Shows a central game menu with quick usage for Tic Tac Toe, Connect 4, chess, Tower, Vault, Memory, Minesweeper, and Picker. The select menu gives the start command for each game.",
    "flagquiz": "Starts a photo-based flag quiz. Choose Solo or Public Channel, then choose 10, 20, 50, or all 197 flags. Each flag gives 2 tries, each guess has 30 seconds, small typos are accepted, and correct answers pay 20,000 quesos each. Wrong first guesses can request a hint.",
    "flagstats": "Shows a user's Flag Quiz tracking: quizzes played, estimated correct flags from rewards earned, and total flag rewards.",
    "ttt": "Challenge a user to Tic Tac Toe. The opponent accepts the game first. If the challenger enables a bet and enters an amount, the opponent gets a second accept/decline prompt for that exact bet before the game starts.",
    "c4": "Challenge a user to Connect 4. The opponent accepts the game first. If the challenger enables a bet and enters an amount, the opponent gets a second accept/decline prompt for that exact bet before the game starts. The board shows column numbers below the grid.",
    "chess": "Challenge a user to chess. The opponent accepts first. If the challenger enables a bet and enters an amount, the opponent gets a second accept/decline prompt. The board uses dropdown UI controls: choose one of your pieces, choose a legal move, then confirm or cancel. Each player has a live 10-minute total clock, and the board flips to the current player's perspective. Movement legality, check, checkmate, stalemate, draw detection, and time-loss handling are enforced.",
    "move": "Fallback chess command for manual notation. Use UCI like `.move e2e4` or SAN like `.move Nf3`. The clickable chess UI is preferred.",
    "chessmove": "Fallback chess command for manual notation. Use UCI like `.chessmove e2e4` or SAN like `.chessmove Nf3`. The clickable chess UI is preferred.",
    "resign": "Ends the active chess game in this channel and awards the win to the other player.",
}

ECONHELP_COMMANDS = [
    ("Core", ["guide", "onboard", "tutorial", "bal", "bank", "profile", "inventory", "settheme", "quests", "dailychallenge", "streaks", "shop", "cooldowns", "transactions", "limits", "lb"]),
    ("Stats", ["gamestats", "achievements", "setbadge", "gamebalance", "gameaudit", "balanceaudit", "balancedashboard", "gamehistory", "season", "seasonpass", "qstats", "economyhealth", "economyaudit", "abuseaudit", "riskprofile", "recommendgame"]),
    ("Claims", ["daily", "weekly", "monthly"]),
    ("Lottery", ["lottery", "buytick", "lotterystats", "editlottery", "stoplottery"]),
    ("Gambling", ["cf", "roulette", "slots", "blackjack", "scratch", "tower", "vault", "memory", "cardladder", "lockpick", "heist", "diceduel", "cases", "plinko", "luckynumber", "jackpotspin", "dungeon", "ms", "wheel", "rob", "robsettings"]),
    ("Transfers", ["give"]),
    ("Help", ["econhelp", "explain"]),
]
ECONHELP_SUPEROWNER_COMMANDS = ["add", "remove", "addtick", "removetick", "settick", "setquesos"]
ECONHELP_SUPEROWNER_HIDDEN = {*ECONHELP_SUPEROWNER_COMMANDS, "remtick", "deltick"}

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
        text = (command.help or "").strip().splitlines()[0] if command and command.help else "No short explanation is written for this command yet."
    text = apply_prefix_to_help_text(text, prefix)
    risk = risk_text(command.name if command and command.name in GAME_RISK_LABELS else command_name)
    if risk:
        text = f"{risk}\n{text}"
    return f"`{prefix}{usage_name}`{alias_text}\n{text}"

def add_split_embed_field(embed, name, lines, inline=False, limit=1024):
    chunks = []
    current = ""
    for line in lines:
        candidate = line if not current else f"{current}\n\n{line}"
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            chunks.append(current)
        current = line[:limit - 1] + "…" if len(line) > limit else line
    if current:
        chunks.append(current)
    if not chunks:
        chunks = ["None."]
    for index, chunk in enumerate(chunks, 1):
        field_name = name if len(chunks) == 1 else f"{name} {index}/{len(chunks)}"
        embed.add_field(name=field_name, value=chunk, inline=inline)

@commands.command(name="econhelp", aliases=["economyhelp", "quewohelp", "ehelp"])
async def econhelp(ctx):
    """Shows 𝚀𝚞𝚎wo commands, aliases, and short explanations."""
    prefix = getattr(ctx, "prefix", ".")
    visible_econhelp_commands = list(ECONHELP_COMMANDS)
    if is_superowner_id(ctx.author.id):
        visible_econhelp_commands.append(("Superowner", ECONHELP_SUPEROWNER_COMMANDS))

    def build_embed(category_name="Core", page=0):
        selected = next(
            ((category, commands_) for category, commands_ in visible_econhelp_commands if category == category_name),
            visible_econhelp_commands[0],
        )
        category, commands_ = selected
        page = max(0, int(page or 0))
        per_page = 8
        page_count = max(1, math.ceil(len(commands_) / per_page))
        page = min(page, page_count - 1)
        category_list = " | ".join(name for name, _ in visible_econhelp_commands)
        embed = discord.Embed(
            title=f"{Q_BOOK} 𝚀𝚞𝚎wo Help - {category}",
            description=(
                f"Use `{prefix}explain <command>` for detailed help, or `{prefix}help` for the full bot command list.\n"
                f"Categories: {category_list}"
            ),
            color=discord.Color.gold()
        )
        start = page * per_page
        page_commands = commands_[start:start + per_page]
        lines = [command_help_line(command_name, prefix) for command_name in page_commands]
        embed.add_field(
            name=f"{category} {start + 1}-{min(start + per_page, len(commands_))}",
            value="\n\n".join(lines) if lines else "None.",
            inline=False,
        )
        embed.set_footer(text=f"Page {page + 1}/{page_count} • Examples: {prefix}explain lottery, {prefix}explain shop, {prefix}explain cf")
        return embed

    class EconHelpSelect(discord.ui.Select):
        def __init__(self):
            options = [
                discord.SelectOption(
                    label=category,
                    value=category,
                    description=f"{len(commands_)} 𝚀𝚞𝚎wo command(s)"
                )
                for category, commands_ in visible_econhelp_commands
            ]
            super().__init__(placeholder="Choose a 𝚀𝚞𝚎wo help category", min_values=1, max_values=1, options=options)

        async def callback(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own econhelp menu.", ephemeral=True)
                return
            self.view.category = self.values[0]
            self.view.page = 0
            await interaction.response.edit_message(embed=build_embed(self.view.category, self.view.page), view=self.view)

    class EconHelpPageButton(discord.ui.Button):
        def __init__(self, direction):
            self.direction = direction
            label = "Previous" if direction < 0 else "Next"
            super().__init__(label=label, style=discord.ButtonStyle.secondary)

        async def callback(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own econhelp menu.", ephemeral=True)
                return
            commands_ = next((items for category, items in visible_econhelp_commands if category == self.view.category), [])
            page_count = max(1, math.ceil(len(commands_) / 8))
            self.view.page = min(max(0, self.view.page + self.direction), page_count - 1)
            await interaction.response.edit_message(embed=build_embed(self.view.category, self.view.page), view=self.view)

    class EconHelpRefreshButton(discord.ui.Button):
        def __init__(self):
            try:
                emoji = discord.PartialEmoji.from_str(Q_TIMER)
            except Exception:
                emoji = Q_TIMER
            super().__init__(label="Refresh", emoji=emoji, style=discord.ButtonStyle.secondary)

        async def callback(self, interaction):
            if interaction.user.id != ctx.author.id:
                await interaction.response.send_message("Use your own econhelp menu.", ephemeral=True)
                return
            await interaction.response.edit_message(embed=build_embed(self.view.category, self.view.page), view=self.view)

    class EconHelpView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=LONG_HELP_VIEW_TIMEOUT)
            self.category = "Core"
            self.page = 0
            self.add_item(EconHelpSelect())
            self.add_item(EconHelpPageButton(-1))
            self.add_item(EconHelpPageButton(1))
            self.add_item(EconHelpRefreshButton())

        async def on_timeout(self):
            for item in self.children:
                item.disabled = True
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

    view = EconHelpView()
    view.message = await ctx.send(
        embed=build_embed("Core", 0),
        view=view,
        allowed_mentions=discord.AllowedMentions.none()
    )

@commands.command()
async def explain(ctx, command_name: str = None):
    """Shows detailed help for one command."""
    prefix = getattr(ctx, "prefix", ".")
    if not command_name:
        hidden = set(ECONHELP_SUPEROWNER_HIDDEN) if not is_superowner_id(ctx.author.id) else set()
        command_names = sorted({command.name for command in bot.commands if command.name not in hidden})
        names = ", ".join(command_names)
        await ctx.send(f"Use `{prefix}explain <command>`. Commands: {names}, admin")
        return

    key = command_name.casefold().removeprefix(prefix.casefold()).lstrip(".")
    if key in ECONHELP_SUPEROWNER_HIDDEN and not is_superowner_id(ctx.author.id):
        await ctx.send("I don't have a short explanation for that command.", delete_after=30)
        return
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
        text = (command.help or "").strip().splitlines()[0] if command.help else "No short explanation is written for this command yet."
    if not text and key != "admin":
        await ctx.send("I don't have a short explanation for that command.", delete_after=30)
        return
    detail = DETAILED_EXPLANATIONS.get(key)
    if command and not detail:
        detail = DETAILED_EXPLANATIONS.get(command.name)
    risk = risk_text(command.name if command and command.name in GAME_RISK_LABELS else key)
    if risk:
        text = f"{risk}\n{text}"
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
    print("Initializing 𝚀𝚞𝚎wo system...")
    await asyncio.to_thread(init_db)
    print(f"𝚀𝚞𝚎wo db_ready = {db_ready}")

    economy_commands = [
        bal, bank, tutorial, recommendgame, robsettings, rob, profile, inventory, settheme, quests, dailychallenge, streaks, guide, onboard, shop, cooldowns, transactions, limits, lottery, editlottery, stoplottery, lotterystats, buytick,
        daily, weekly, monthly, gamble, roulette, slots, blackjack,
        scratch, tower, vault, memory_game, card_ladder, lockpick, heist, dice_duel, cases, plinko, lucky_number, jackpot_spin, dungeon, minesweeper, wheel, give, lb, gamestats, achievements, setbadge, gamebalance, gameaudit, balanceaudit, balancedashboard, event, gamehistory, season, seasonpass, endseason, qstats, economyhealth, economyaudit, abuseaudit, riskprofile, add, remove, addtick, removetick, settick, setquesos, econhelp, explain
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
