"""
Shared PostgreSQL helpers for non-economy persistent data.
"""
import os
import time
import psycopg2

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
        print("⚠️ DATABASE_URL not set - persistent bot data disabled")
        return

    for attempt in range(1, 11):
        try:
            conn = psycopg2.connect(url, connect_timeout=5)
            cur = conn.cursor()

            _create_tables(cur)
            _migrate_bot_config(cur)

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

def _create_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_owners (
            user_id BIGINT PRIMARY KEY
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_mods (
            user_id BIGINT PRIMARY KEY
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_log_config (
            guild_id BIGINT PRIMARY KEY,
            log_channel_id BIGINT NOT NULL,
            reaction_log_channel_id BIGINT NOT NULL
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

def _migrate_bot_config(cur):
    cur.execute("SELECT to_regclass('public.bot_config')")
    if cur.fetchone()[0] is None:
        return

    cur.execute("""
        INSERT INTO bot_owners (user_id)
        SELECT user_id_text::BIGINT
        FROM bot_config, jsonb_array_elements_text(value) AS user_id_text
        WHERE key = 'owners'
          AND jsonb_typeof(value) = 'array'
          AND user_id_text ~ '^[0-9]+$'
        ON CONFLICT DO NOTHING
    """)
    cur.execute("""
        INSERT INTO bot_mods (user_id)
        SELECT user_id_text::BIGINT
        FROM bot_config, jsonb_array_elements_text(value) AS user_id_text
        WHERE key = 'mods'
          AND jsonb_typeof(value) = 'array'
          AND user_id_text ~ '^[0-9]+$'
        ON CONFLICT DO NOTHING
    """)
    cur.execute("""
        INSERT INTO guild_log_config (guild_id, log_channel_id, reaction_log_channel_id)
        SELECT
            split_part(key, ':', 2)::BIGINT,
            (value->>'log_channel_id')::BIGINT,
            (value->>'reaction_log_channel_id')::BIGINT
        FROM bot_config
        WHERE key ~ '^guild_log_config:[0-9]+$'
          AND value ? 'log_channel_id'
          AND value ? 'reaction_log_channel_id'
          AND (value->>'log_channel_id') ~ '^[0-9]+$'
          AND (value->>'reaction_log_channel_id') ~ '^[0-9]+$'
        ON CONFLICT (guild_id) DO UPDATE SET
            log_channel_id = EXCLUDED.log_channel_id,
            reaction_log_channel_id = EXCLUDED.reaction_log_channel_id
    """)

    cur.execute("DROP TABLE bot_config")

def _fetch_id_set(table):
    _ensure_ready()
    if not pg_ready:
        return set()
    try:
        conn = pg_conn()
        if conn is None:
            return set()
        cur = conn.cursor()
        cur.execute(f"SELECT user_id FROM {table}")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {int(row[0]) for row in rows}
    except Exception:
        return set()

def _save_id_set(table, id_set):
    _ensure_ready()
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table}")
        for user_id in id_set:
            cur.execute(
                f"INSERT INTO {table} (user_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (int(user_id),)
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def load_owner_ids():
    return _fetch_id_set("bot_owners")

def save_owner_ids(id_set):
    _save_id_set("bot_owners", id_set)

def load_mod_ids():
    return _fetch_id_set("bot_mods")

def save_mod_ids(id_set):
    _save_id_set("bot_mods", id_set)


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
        _create_tables(cur)
        _migrate_bot_config(cur)
        conn.commit()
        cur.close()
        conn.close()
        pg_ready = True
    except Exception:
        pg_ready = False

def load_guild_log_config(guild_id):
    _ensure_ready()
    if not pg_ready:
        return None
    try:
        conn = pg_conn()
        if conn is None:
            return None
        cur = conn.cursor()
        cur.execute(
            "SELECT log_channel_id, reaction_log_channel_id FROM guild_log_config WHERE guild_id = %s",
            (guild_id,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        return {
            "log_channel_id": int(row[0]),
            "reaction_log_channel_id": int(row[1])
        }
    except Exception:
        return None

def save_guild_log_config(guild_id, log_channel_id, reaction_log_channel_id):
    _ensure_ready()
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO guild_log_config (guild_id, log_channel_id, reaction_log_channel_id) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (guild_id) DO UPDATE SET "
            "log_channel_id = EXCLUDED.log_channel_id, "
            "reaction_log_channel_id = EXCLUDED.reaction_log_channel_id",
            (int(guild_id), int(log_channel_id), int(reaction_log_channel_id))
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

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
