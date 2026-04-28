"""
Shared PostgreSQL helpers for non-economy persistent data.
Migrated from JSON files to survive deployments.
"""
import os
import json
import time
import psycopg2
from psycopg2.extras import RealDictCursor

pg_ready = False

def pg_conn():
    """Get a PostgreSQL connection. Returns None if not ready."""
    if not pg_ready:
        return None
    try:
        return psycopg2.connect(os.getenv("DATABASE_URL"), connect_timeout=5)
    except Exception:
        return None

def pg_init():
    """Initialize DB tables with retry loop. Call from on_ready."""
    global pg_ready
    url = os.getenv("DATABASE_URL")
    if not url:
        print("⚠️ DATABASE_URL not set - bot config in JSON-only mode")
        return

    for attempt in range(1, 11):
        try:
            conn = psycopg2.connect(url, connect_timeout=5)
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_config (
                    key TEXT PRIMARY KEY,
                    value JSONB NOT NULL
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS birthdays (
                    user_id BIGINT PRIMARY KEY,
                    date TEXT NOT NULL
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS afk_users (
                    user_id BIGINT PRIMARY KEY,
                    reason TEXT,
                    since TIMESTAMP NOT NULL
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS sleeping_users (
                    user_id BIGINT PRIMARY KEY,
                    since TIMESTAMP NOT NULL
                )
            """)

            conn.commit()
            cur.close()
            conn.close()
            pg_ready = True
            print(f"✅ Bot config DB initialized (PostgreSQL) on attempt {attempt}")
            return
        except psycopg2.OperationalError as e:
            if attempt < 10:
                print(f"⏳ Bot config DB attempt {attempt}/10 failed (DB starting up), retrying in 5s...")
                time.sleep(5)
            else:
                print(f"❌ Bot config DB init failed after 10 attempts: {e}")
                pg_ready = False
        except Exception as e:
            print(f"❌ Bot config DB init failed: {e}")
            pg_ready = False
            return

# === OWNERS & MODS (stored as JSONB arrays in bot_config) ===

def load_bot_config(key, default=None):
    """Load a value from bot_config. Initializes DB if needed. Returns default if not found or DB unavailable."""
    _ensure_ready()
    if not pg_ready:
        return default
    try:
        conn = pg_conn()
        if conn is None:
            return default
        cur = conn.cursor()
        cur.execute("SELECT value FROM bot_config WHERE key = %s", (key,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return json.loads(row[0])
        return default
    except Exception:
        return default


def _ensure_ready():
    """Called on first use to initialize DB connection if not already tried."""
    global pg_ready
    if pg_ready:
        return
    # Try once without retry — on startup this may just be the DB still starting up
    # The retry loop in pg_init handles that case; here we just try once for early calls
    url = os.getenv("DATABASE_URL")
    if not url:
        pg_ready = False
        return
    try:
        conn = psycopg2.connect(url, connect_timeout=5)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_config (
                key TEXT PRIMARY KEY,
                value JSONB NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS birthdays (
                user_id BIGINT PRIMARY KEY,
                date TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS afk_users (
                user_id BIGINT PRIMARY KEY,
                reason TEXT,
                since TIMESTAMP NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sleeping_users (
                user_id BIGINT PRIMARY KEY,
                since TIMESTAMP NOT NULL
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        pg_ready = True
    except Exception:
        pg_ready = False

def save_bot_config(key, value):
    """Save a value to bot_config. Also writes to JSON backup."""
    if pg_ready:
        try:
            conn = pg_conn()
            if conn:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO bot_config (key, value) VALUES (%s, %s) "
                    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                    (key, json.dumps(value))
                )
                conn.commit()
                cur.close()
                conn.close()
        except Exception:
            pass  # Best-effort

# === BIRTHDAYS ===

def load_birthdays():
    """Load all birthdays from DB. Returns dict {user_id: date_str}."""
    if not pg_ready:
        return None
    try:
        conn = pg_conn()
        if conn is None:
            return None
        cur = conn.cursor()
        cur.execute("SELECT user_id, date FROM birthdays")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {uid: date for uid, date in rows}
    except Exception:
        return None

def save_birthday(user_id, date_str):
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO birthdays (user_id, date) VALUES (%s, %s) "
            "ON CONFLICT (user_id) DO UPDATE SET date = EXCLUDED.date",
            (user_id, date_str)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def remove_birthday(user_id):
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute("DELETE FROM birthdays WHERE user_id = %s", (user_id,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

# === AFK USERS ===

def load_afk_users():
    """Load all AFK users from DB. Returns dict {user_id: {"reason": str, "since": datetime}}."""
    if not pg_ready:
        return None
    try:
        conn = pg_conn()
        if conn is None:
            return None
        cur = conn.cursor()
        cur.execute("SELECT user_id, reason, since FROM afk_users")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        from datetime import datetime, timezone
        return {uid: {"reason": reason, "since": since.replace(tzinfo=timezone.utc) if since.tzinfo is None else since}
                for uid, reason, since in rows}
    except Exception:
        return None

def save_afk_user(user_id, reason, since):
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO afk_users (user_id, reason, since) VALUES (%s, %s, %s) "
            "ON CONFLICT (user_id) DO UPDATE SET reason = EXCLUDED.reason, since = EXCLUDED.since",
            (user_id, reason, since)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def remove_afk_user(user_id):
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute("DELETE FROM afk_users WHERE user_id = %s", (user_id,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

# === SLEEPING USERS ===

def load_sleeping_users():
    """Load all sleeping users from DB. Returns dict {user_id: datetime}."""
    if not pg_ready:
        return None
    try:
        conn = pg_conn()
        if conn is None:
            return None
        cur = conn.cursor()
        cur.execute("SELECT user_id, since FROM sleeping_users")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        from datetime import datetime, timezone
        return {uid: since.replace(tzinfo=timezone.utc) if since.tzinfo is None else since
                for uid, since in rows}
    except Exception:
        return None

def save_sleeping_user(user_id, since):
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO sleeping_users (user_id, since) VALUES (%s, %s) "
            "ON CONFLICT (user_id) DO UPDATE SET since = EXCLUDED.since",
            (user_id, since)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def remove_sleeping_user(user_id):
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute("DELETE FROM sleeping_users WHERE user_id = %s", (user_id,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass
