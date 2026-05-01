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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_blacklist (
            user_id BIGINT PRIMARY KEY
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS autoban_users (
            user_id BIGINT PRIMARY KEY
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS disabled_commands (
            command_name TEXT PRIMARY KEY
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS shutdown_channels (
            channel_id BIGINT PRIMARY KEY
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS reaction_shutdown_channels (
            channel_id BIGINT PRIMARY KEY
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS censored_phrases (
            phrase TEXT PRIMARY KEY
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS watched_users (
            user_id BIGINT PRIMARY KEY,
            owner_id BIGINT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS reaction_watched_users (
            user_id BIGINT PRIMARY KEY,
            owner_id BIGINT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_polls (
            message_id BIGINT PRIMARY KEY,
            channel_id BIGINT NOT NULL,
            guild_id BIGINT NOT NULL,
            author_id BIGINT NOT NULL,
            question TEXT NOT NULL,
            options TEXT[] NOT NULL,
            use_numbers BOOLEAN NOT NULL,
            end_time TIMESTAMP,
            ended BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_timers (
            message_id BIGINT PRIMARY KEY,
            channel_id BIGINT NOT NULL,
            guild_id BIGINT NOT NULL,
            owner_id BIGINT NOT NULL,
            title TEXT,
            time_str TEXT NOT NULL,
            end_time TIMESTAMP NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_bot_owners (
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_bot_mods (
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_bot_blacklist (
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_autoban_users (
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_disabled_commands (
            guild_id BIGINT NOT NULL,
            command_name TEXT NOT NULL,
            PRIMARY KEY (guild_id, command_name)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_shutdown_channels (
            guild_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            PRIMARY KEY (guild_id, channel_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_reaction_shutdown_channels (
            guild_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            PRIMARY KEY (guild_id, channel_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_censored_phrases (
            guild_id BIGINT NOT NULL,
            phrase TEXT NOT NULL,
            PRIMARY KEY (guild_id, phrase)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_watched_users (
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            owner_id BIGINT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_reaction_watched_users (
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            owner_id BIGINT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
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

def _fetch_id_set(table, column="user_id"):
    _ensure_ready()
    if not pg_ready:
        return set()
    try:
        conn = pg_conn()
        if conn is None:
            return set()
        cur = conn.cursor()
        cur.execute(f"SELECT {column} FROM {table}")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {int(row[0]) for row in rows}
    except Exception:
        return set()

def _save_id_set(table, id_set, column="user_id"):
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
                f"INSERT INTO {table} ({column}) VALUES (%s) ON CONFLICT DO NOTHING",
                (int(user_id),)
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def load_owner_ids():
    return _fetch_scoped_id_sets("guild_bot_owners")

def save_owner_ids(guild_id, id_set):
    _save_scoped_id_set("guild_bot_owners", guild_id, id_set)

def load_mod_ids():
    return _fetch_scoped_id_sets("guild_bot_mods")

def save_mod_ids(guild_id, id_set):
    _save_scoped_id_set("guild_bot_mods", guild_id, id_set)

def _fetch_text_set(table, column):
    _ensure_ready()
    if not pg_ready:
        return set()
    try:
        conn = pg_conn()
        if conn is None:
            return set()
        cur = conn.cursor()
        cur.execute(f"SELECT {column} FROM {table}")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {str(row[0]) for row in rows}
    except Exception:
        return set()

def _save_text_set(table, column, values):
    _ensure_ready()
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table}")
        for value in values:
            cur.execute(
                f"INSERT INTO {table} ({column}) VALUES (%s) ON CONFLICT DO NOTHING",
                (str(value),)
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def _fetch_id_map(table):
    _ensure_ready()
    if not pg_ready:
        return {}
    try:
        conn = pg_conn()
        if conn is None:
            return {}
        cur = conn.cursor()
        cur.execute(f"SELECT user_id, owner_id FROM {table}")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {int(user_id): int(owner_id) for user_id, owner_id in rows}
    except Exception:
        return {}

def _save_id_map(table, data):
    _ensure_ready()
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table}")
        for user_id, owner_id in data.items():
            cur.execute(
                f"INSERT INTO {table} (user_id, owner_id) VALUES (%s, %s) "
                "ON CONFLICT (user_id) DO UPDATE SET owner_id = EXCLUDED.owner_id",
                (int(user_id), int(owner_id))
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def _fetch_scoped_id_sets(table, column="user_id"):
    _ensure_ready()
    if not pg_ready:
        return {}
    try:
        conn = pg_conn()
        if conn is None:
            return {}
        cur = conn.cursor()
        cur.execute(f"SELECT guild_id, {column} FROM {table}")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        data = {}
        for guild_id, value in rows:
            data.setdefault(int(guild_id), set()).add(int(value))
        return data
    except Exception:
        return {}

def _save_scoped_id_set(table, guild_id, values, column="user_id"):
    _ensure_ready()
    if not pg_ready or guild_id is None:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table} WHERE guild_id = %s", (int(guild_id),))
        for value in values:
            cur.execute(
                f"INSERT INTO {table} (guild_id, {column}) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (int(guild_id), int(value))
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def _fetch_scoped_text_sets(table, column):
    _ensure_ready()
    if not pg_ready:
        return {}
    try:
        conn = pg_conn()
        if conn is None:
            return {}
        cur = conn.cursor()
        cur.execute(f"SELECT guild_id, {column} FROM {table}")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        data = {}
        for guild_id, value in rows:
            data.setdefault(int(guild_id), set()).add(str(value))
        return data
    except Exception:
        return {}

def _save_scoped_text_set(table, guild_id, values, column):
    _ensure_ready()
    if not pg_ready or guild_id is None:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table} WHERE guild_id = %s", (int(guild_id),))
        for value in values:
            cur.execute(
                f"INSERT INTO {table} (guild_id, {column}) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (int(guild_id), str(value))
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def _fetch_scoped_id_maps(table):
    _ensure_ready()
    if not pg_ready:
        return {}
    try:
        conn = pg_conn()
        if conn is None:
            return {}
        cur = conn.cursor()
        cur.execute(f"SELECT guild_id, user_id, owner_id FROM {table}")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        data = {}
        for guild_id, user_id, owner_id in rows:
            data.setdefault(int(guild_id), {})[int(user_id)] = int(owner_id)
        return data
    except Exception:
        return {}

def _save_scoped_id_map(table, guild_id, data):
    _ensure_ready()
    if not pg_ready or guild_id is None:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table} WHERE guild_id = %s", (int(guild_id),))
        for user_id, owner_id in data.items():
            cur.execute(
                f"INSERT INTO {table} (guild_id, user_id, owner_id) VALUES (%s, %s, %s) "
                "ON CONFLICT (guild_id, user_id) DO UPDATE SET owner_id = EXCLUDED.owner_id",
                (int(guild_id), int(user_id), int(owner_id))
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def load_blacklisted_users():
    return _fetch_scoped_id_sets("guild_bot_blacklist")

def save_blacklisted_users(guild_id, id_set):
    _save_scoped_id_set("guild_bot_blacklist", guild_id, id_set)

def load_autoban_ids():
    return _fetch_scoped_id_sets("guild_autoban_users")

def save_autoban_ids(guild_id, id_set):
    _save_scoped_id_set("guild_autoban_users", guild_id, id_set)

def load_disabled_commands():
    return _fetch_scoped_text_sets("guild_disabled_commands", "command_name")

def save_disabled_commands(guild_id, commands):
    _save_scoped_text_set("guild_disabled_commands", guild_id, commands, "command_name")

def load_shutdown_channels():
    return _fetch_scoped_id_sets("guild_shutdown_channels", "channel_id")

def save_shutdown_channels(guild_id, channel_ids):
    _save_scoped_id_set("guild_shutdown_channels", guild_id, channel_ids, "channel_id")

def load_reaction_shutdown_channels():
    return _fetch_scoped_id_sets("guild_reaction_shutdown_channels", "channel_id")

def save_reaction_shutdown_channels(guild_id, channel_ids):
    _save_scoped_id_set("guild_reaction_shutdown_channels", guild_id, channel_ids, "channel_id")

def load_censored_phrases():
    return {guild_id: list(values) for guild_id, values in _fetch_scoped_text_sets("guild_censored_phrases", "phrase").items()}

def save_censored_phrases(guild_id, phrases):
    _save_scoped_text_set("guild_censored_phrases", guild_id, phrases, "phrase")

def load_watchlist():
    return _fetch_scoped_id_maps("guild_watched_users")

def save_watchlist(guild_id, data):
    _save_scoped_id_map("guild_watched_users", guild_id, data)

def load_reaction_watchlist():
    return _fetch_scoped_id_maps("guild_reaction_watched_users")

def save_reaction_watchlist(guild_id, data):
    _save_scoped_id_map("guild_reaction_watched_users", guild_id, data)

def load_active_polls():
    _ensure_ready()
    if not pg_ready:
        return {}
    try:
        conn = pg_conn()
        if conn is None:
            return {}
        cur = conn.cursor()
        cur.execute(
            "SELECT message_id, channel_id, guild_id, author_id, question, options, use_numbers, end_time, ended "
            "FROM active_polls"
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {
            int(message_id): {
                "channel_id": int(channel_id),
                "guild_id": int(guild_id),
                "author_id": int(author_id),
                "question": question,
                "options": list(options),
                "use_numbers": bool(use_numbers),
                "end_time": end_time,
                "ended": bool(ended),
                "end_task": None,
            }
            for message_id, channel_id, guild_id, author_id, question, options, use_numbers, end_time, ended in rows
        }
    except Exception:
        return {}

def save_active_poll(message_id, data):
    _ensure_ready()
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO active_polls "
            "(message_id, channel_id, guild_id, author_id, question, options, use_numbers, end_time, ended) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (message_id) DO UPDATE SET "
            "channel_id = EXCLUDED.channel_id, guild_id = EXCLUDED.guild_id, "
            "author_id = EXCLUDED.author_id, question = EXCLUDED.question, "
            "options = EXCLUDED.options, use_numbers = EXCLUDED.use_numbers, "
            "end_time = EXCLUDED.end_time, ended = EXCLUDED.ended",
            (
                int(message_id),
                int(data["channel_id"]),
                int(data["guild_id"]),
                int(data["author_id"]),
                data["question"],
                list(data["options"]),
                bool(data["use_numbers"]),
                data.get("end_time"),
                bool(data.get("ended", False)),
            )
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def remove_active_poll(message_id):
    _ensure_ready()
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute("DELETE FROM active_polls WHERE message_id = %s", (int(message_id),))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def load_active_timers():
    _ensure_ready()
    if not pg_ready:
        return {}
    try:
        conn = pg_conn()
        if conn is None:
            return {}
        cur = conn.cursor()
        cur.execute(
            "SELECT message_id, channel_id, guild_id, owner_id, title, time_str, end_time FROM active_timers"
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {
            int(message_id): {
                "channel_id": int(channel_id),
                "guild_id": int(guild_id),
                "owner_id": int(owner_id),
                "title": title,
                "time_str": time_str,
                "end_time": end_time,
                "task": None,
                "message": None,
            }
            for message_id, channel_id, guild_id, owner_id, title, time_str, end_time in rows
        }
    except Exception:
        return {}

def save_active_timer(message_id, data):
    _ensure_ready()
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO active_timers "
            "(message_id, channel_id, guild_id, owner_id, title, time_str, end_time) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (message_id) DO UPDATE SET "
            "channel_id = EXCLUDED.channel_id, guild_id = EXCLUDED.guild_id, "
            "owner_id = EXCLUDED.owner_id, title = EXCLUDED.title, "
            "time_str = EXCLUDED.time_str, end_time = EXCLUDED.end_time",
            (
                int(message_id),
                int(data["channel_id"]),
                int(data["guild_id"]),
                int(data["owner_id"]),
                data.get("title"),
                data["time_str"],
                data["end_time"],
            )
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def remove_active_timer(message_id):
    _ensure_ready()
    if not pg_ready:
        return
    try:
        conn = pg_conn()
        if conn is None:
            return
        cur = conn.cursor()
        cur.execute("DELETE FROM active_timers WHERE message_id = %s", (int(message_id),))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass


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
        print(f"Guild log config unavailable: PostgreSQL not ready for guild {guild_id}.")
        return None
    try:
        conn = pg_conn()
        if conn is None:
            print(f"Guild log config unavailable: connection failed for guild {guild_id}.")
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
        print(f"Guild log config load failed for guild {guild_id}.")
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
