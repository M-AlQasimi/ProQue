"""
Shared PostgreSQL helpers for non-economy persistent data.
"""
import os
import time
import json
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
        CREATE TABLE IF NOT EXISTS guild_log_config (
            guild_id BIGINT PRIMARY KEY,
            log_channel_id BIGINT NOT NULL,
            reaction_log_channel_id BIGINT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_prefixes (
            guild_id BIGINT PRIMARY KEY,
            prefix TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_birthday_channels (
            guild_id BIGINT PRIMARY KEY,
            channel_id BIGINT NOT NULL,
            set_by_user_id BIGINT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_activity_config (
            guild_id BIGINT PRIMARY KEY,
            channel_id BIGINT NOT NULL,
            set_by_user_id BIGINT,
            next_report TIMESTAMP NOT NULL,
            current_message_id BIGINT
        )
    """)
    cur.execute("ALTER TABLE guild_activity_config ADD COLUMN IF NOT EXISTS current_message_id BIGINT")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_truth_or_dare_channels (
            guild_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            PRIMARY KEY (guild_id, channel_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_activity_counts (
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            messages BIGINT NOT NULL DEFAULT 0,
            reactions BIGINT NOT NULL DEFAULT 0,
            voice_events BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id)
        )
    """)
    cur.execute("ALTER TABLE guild_activity_counts ADD COLUMN IF NOT EXISTS reactions BIGINT NOT NULL DEFAULT 0")
    cur.execute("ALTER TABLE guild_activity_counts ADD COLUMN IF NOT EXISTS voice_events BIGINT NOT NULL DEFAULT 0")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS message_activity_events (
            message_id BIGINT PRIMARY KEY,
            guild_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_message_activity_events_guild_time ON message_activity_events (guild_id, created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_message_activity_events_user_time ON message_activity_events (guild_id, user_id, created_at DESC)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_message_events (
            guild_id BIGINT PRIMARY KEY,
            channel_id BIGINT NOT NULL,
            message_id BIGINT,
            title TEXT,
            started_by BIGINT NOT NULL,
            starts_at TIMESTAMP NOT NULL,
            ends_at TIMESTAMP NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_active_message_events_ends ON active_message_events (ends_at)")

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
        CREATE TABLE IF NOT EXISTS ai_channel_memory (
            id BIGSERIAL PRIMARY KEY,
            guild_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_channel_memory_lookup ON ai_channel_memory (guild_id, channel_id, created_at DESC)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_user_memory (
            id BIGSERIAL PRIMARY KEY,
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            fact TEXT NOT NULL,
            source TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_user_memory_lookup ON ai_user_memory (guild_id, user_id, updated_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_user_memory_user_lookup ON ai_user_memory (user_id, updated_at DESC)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_game_sessions (
            message_id BIGINT PRIMARY KEY,
            guild_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            game_key TEXT NOT NULL,
            players BIGINT[] NOT NULL DEFAULT '{}',
            state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)
    cur.execute("ALTER TABLE active_game_sessions ADD COLUMN IF NOT EXISTS state_json JSONB NOT NULL DEFAULT '{}'::jsonb")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_active_game_sessions_guild_channel ON active_game_sessions (guild_id, channel_id)")

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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS command_usage_stats (
            guild_id BIGINT NOT NULL,
            command_name TEXT NOT NULL,
            user_id BIGINT NOT NULL,
            uses BIGINT NOT NULL DEFAULT 0,
            last_used TIMESTAMP NOT NULL DEFAULT NOW(),
            PRIMARY KEY (guild_id, command_name, user_id)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_command_usage_stats_command ON command_usage_stats (command_name, uses DESC)")

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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bot_receipts_actor_time ON bot_receipts (actor_id, created_at DESC)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_control_settings (
            scope TEXT NOT NULL,
            scope_id BIGINT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            updated_by BIGINT,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
            PRIMARY KEY (scope, scope_id, key)
        )
    """)

def _migrate_bot_config(cur):
    cur.execute("SELECT to_regclass('public.bot_config')")
    if cur.fetchone()[0] is None:
        return

    cur.execute("""
        INSERT INTO guild_log_config (guild_id, log_channel_id, reaction_log_channel_id)
        SELECT
            split_part(bot_config.key, ':', 2)::BIGINT,
            (bot_config.value->>'log_channel_id')::BIGINT,
            (bot_config.value->>'reaction_log_channel_id')::BIGINT
        FROM bot_config
        WHERE bot_config.key ~ '^guild_log_config:[0-9]+$'
          AND bot_config.value ? 'log_channel_id'
          AND bot_config.value ? 'reaction_log_channel_id'
          AND (bot_config.value->>'log_channel_id') ~ '^[0-9]+$'
          AND (bot_config.value->>'reaction_log_channel_id') ~ '^[0-9]+$'
        ON CONFLICT (guild_id) DO UPDATE SET
            log_channel_id = EXCLUDED.log_channel_id,
            reaction_log_channel_id = EXCLUDED.reaction_log_channel_id
    """)

    cur.execute("DROP TABLE bot_config")

def record_command_usage(guild_id, command_name, user_id):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO command_usage_stats (guild_id, command_name, user_id, uses, last_used)
            VALUES (%s, %s, %s, 1, NOW())
            ON CONFLICT (guild_id, command_name, user_id) DO UPDATE SET
                uses = command_usage_stats.uses + 1,
                last_used = NOW()
            """,
            (int(guild_id or 0), str(command_name or "").casefold()[:80], int(user_id))
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def get_command_usage_stats(guild_id=None, limit=20):
    _ensure_ready()
    if not pg_ready:
        return []
    try:
        conn = pg_conn()
        if conn is None:
            return []
        cur = conn.cursor()
        if guild_id is None:
            cur.execute(
                """
                SELECT command_name, SUM(uses) AS uses, COUNT(DISTINCT user_id) AS users, MAX(last_used) AS last_used
                FROM command_usage_stats
                GROUP BY command_name
                ORDER BY uses DESC, command_name ASC
                LIMIT %s
                """,
                (int(limit),)
            )
        else:
            cur.execute(
                """
                SELECT command_name, SUM(uses) AS uses, COUNT(DISTINCT user_id) AS users, MAX(last_used) AS last_used
                FROM command_usage_stats
                WHERE guild_id = %s
                GROUP BY command_name
                ORDER BY uses DESC, command_name ASC
                LIMIT %s
                """,
                (int(guild_id), int(limit))
            )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception:
        return []

def save_bot_receipt(receipt_id, guild_id, channel_id, actor_id, target_ids, action, amount=None, details=None):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bot_receipts (receipt_id, guild_id, channel_id, actor_id, target_ids, action, amount, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (receipt_id) DO NOTHING
            """,
            (
                str(receipt_id),
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
        return True
    except Exception:
        return False

def get_bot_receipt(receipt_id):
    _ensure_ready()
    if not pg_ready:
        return None
    try:
        conn = pg_conn()
        if conn is None:
            return None
        cur = conn.cursor()
        cur.execute(
            "SELECT receipt_id, guild_id, channel_id, actor_id, target_ids, action, amount, details, created_at FROM bot_receipts WHERE receipt_id = %s",
            (str(receipt_id),)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row
    except Exception:
        return None

def set_ai_control_setting(scope, scope_id, key, value, updated_by=None):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO ai_control_settings (scope, scope_id, key, value, updated_by, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (scope, scope_id, key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_by = EXCLUDED.updated_by,
                updated_at = NOW()
            """,
            (str(scope), int(scope_id or 0), str(key), str(value), int(updated_by) if updated_by else None)
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def delete_ai_control_setting(scope, scope_id, key):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute("DELETE FROM ai_control_settings WHERE scope = %s AND scope_id = %s AND key = %s", (str(scope), int(scope_id or 0), str(key)))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def get_ai_control_settings(scope=None, scope_id=None):
    _ensure_ready()
    if not pg_ready:
        return []
    try:
        conn = pg_conn()
        if conn is None:
            return []
        cur = conn.cursor()
        if scope is None:
            cur.execute("SELECT scope, scope_id, key, value, updated_by, updated_at FROM ai_control_settings ORDER BY updated_at DESC")
        else:
            cur.execute(
                "SELECT scope, scope_id, key, value, updated_by, updated_at FROM ai_control_settings WHERE scope = %s AND scope_id = %s ORDER BY key ASC",
                (str(scope), int(scope_id or 0))
            )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception:
        return []

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

def load_truth_or_dare_channels():
    return _fetch_scoped_id_sets("guild_truth_or_dare_channels", "channel_id")

def save_truth_or_dare_channels(guild_id, channel_ids):
    _save_scoped_id_set("guild_truth_or_dare_channels", guild_id, channel_ids, "channel_id")

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

def load_guild_prefixes():
    _ensure_ready()
    if not pg_ready:
        return {}
    try:
        conn = pg_conn()
        if conn is None:
            return {}
        cur = conn.cursor()
        cur.execute("SELECT guild_id, prefix FROM guild_prefixes")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {int(guild_id): prefix for guild_id, prefix in rows}
    except Exception:
        return {}

def save_guild_prefix(guild_id, prefix):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO guild_prefixes (guild_id, prefix) VALUES (%s, %s) "
            "ON CONFLICT (guild_id) DO UPDATE SET prefix = EXCLUDED.prefix",
            (int(guild_id), str(prefix))
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def load_guild_birthday_channels():
    _ensure_ready()
    if not pg_ready:
        return {}
    try:
        conn = pg_conn()
        if conn is None:
            return {}
        cur = conn.cursor()
        cur.execute("SELECT guild_id, channel_id, set_by_user_id FROM guild_birthday_channels")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {
            int(guild_id): {
                "channel_id": int(channel_id),
                "set_by_user_id": int(set_by_user_id) if set_by_user_id is not None else None,
            }
            for guild_id, channel_id, set_by_user_id in rows
        }
    except Exception:
        return {}

def save_guild_birthday_channel(guild_id, channel_id, set_by_user_id=None):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO guild_birthday_channels (guild_id, channel_id, set_by_user_id) VALUES (%s, %s, %s) "
            "ON CONFLICT (guild_id) DO UPDATE SET channel_id = EXCLUDED.channel_id, set_by_user_id = EXCLUDED.set_by_user_id",
            (int(guild_id), int(channel_id), int(set_by_user_id) if set_by_user_id is not None else None)
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def delete_guild_birthday_channel(guild_id):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute("DELETE FROM guild_birthday_channels WHERE guild_id = %s", (int(guild_id),))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def load_guild_activity_channels():
    _ensure_ready()
    if not pg_ready:
        return {}
    try:
        conn = pg_conn()
        if conn is None:
            return {}
        cur = conn.cursor()
        cur.execute("SELECT guild_id, channel_id, set_by_user_id, next_report, current_message_id FROM guild_activity_config")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        from datetime import timezone
        return {
            int(guild_id): {
                "channel_id": int(channel_id),
                "set_by_user_id": int(set_by_user_id) if set_by_user_id is not None else None,
                "next_report": next_report.replace(tzinfo=timezone.utc) if next_report.tzinfo is None else next_report,
                "current_message_id": int(current_message_id) if current_message_id is not None else None,
            }
            for guild_id, channel_id, set_by_user_id, next_report, current_message_id in rows
        }
    except Exception:
        return {}

def save_guild_activity_channel(guild_id, channel_id, set_by_user_id=None, next_report=None, current_message_id=None):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        from datetime import datetime, timedelta, timezone
        if next_report is None:
            next_report = datetime.now(timezone.utc) + timedelta(hours=24)
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO guild_activity_config (guild_id, channel_id, set_by_user_id, next_report, current_message_id) VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (guild_id) DO UPDATE SET channel_id = EXCLUDED.channel_id, set_by_user_id = EXCLUDED.set_by_user_id, next_report = EXCLUDED.next_report, current_message_id = EXCLUDED.current_message_id",
            (
                int(guild_id),
                int(channel_id),
                int(set_by_user_id) if set_by_user_id is not None else None,
                next_report,
                int(current_message_id) if current_message_id is not None else None,
            )
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def update_guild_activity_next_report(guild_id, next_report):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            "UPDATE guild_activity_config SET next_report = %s WHERE guild_id = %s",
            (next_report, int(guild_id))
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def update_guild_activity_message_id(guild_id, message_id):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            "UPDATE guild_activity_config SET current_message_id = %s WHERE guild_id = %s",
            (int(message_id) if message_id is not None else None, int(guild_id))
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def delete_guild_activity_channel(guild_id):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute("DELETE FROM guild_activity_config WHERE guild_id = %s", (int(guild_id),))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def load_ai_channel_memory(guild_id, channel_id, limit=30):
    _ensure_ready()
    if not pg_ready:
        return []
    try:
        conn = pg_conn()
        if conn is None:
            return []
        cur = conn.cursor()
        cur.execute(
            """
            SELECT role, content, EXTRACT(EPOCH FROM created_at)
            FROM ai_channel_memory
            WHERE guild_id = %s AND channel_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (int(guild_id), int(channel_id), int(limit))
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            {"role": str(role), "content": str(content), "ts": float(ts or 0)}
            for role, content, ts in reversed(rows)
        ]
    except Exception:
        return []

def save_ai_channel_memory(guild_id, channel_id, role, content, max_rows=60):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO ai_channel_memory (guild_id, channel_id, role, content) VALUES (%s, %s, %s, %s)",
            (int(guild_id), int(channel_id), str(role), str(content)[:1000])
        )
        cur.execute(
            """
            DELETE FROM ai_channel_memory
            WHERE id IN (
                SELECT id FROM ai_channel_memory
                WHERE guild_id = %s AND channel_id = %s
                ORDER BY created_at DESC
                OFFSET %s
            )
            """,
            (int(guild_id), int(channel_id), int(max_rows))
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def load_ai_user_memory(guild_id, user_id, limit=20):
    _ensure_ready()
    if not pg_ready:
        return []
    try:
        conn = pg_conn()
        if conn is None:
            return []
        cur = conn.cursor()
        cur.execute(
            """
            SELECT fact, source, EXTRACT(EPOCH FROM updated_at)
            FROM ai_user_memory
            WHERE user_id = %s
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (int(user_id), int(limit))
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            {"fact": str(fact), "source": str(source or ""), "ts": float(ts or 0)}
            for fact, source, ts in rows
        ]
    except Exception:
        return []

def save_ai_user_memory(guild_id, user_id, fact, source=None, max_rows=30):
    _ensure_ready()
    fact = str(fact or "").strip()
    if not pg_ready or not fact:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO ai_user_memory (guild_id, user_id, fact, source) VALUES (%s, %s, %s, %s)",
            (0, int(user_id), fact[:500], str(source or "")[:120])
        )
        cur.execute(
            """
            DELETE FROM ai_user_memory
            WHERE id IN (
                SELECT id FROM ai_user_memory
                WHERE user_id = %s
                ORDER BY updated_at DESC
                OFFSET %s
            )
            """,
            (int(user_id), int(max_rows))
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def delete_ai_user_memory(guild_id, user_id):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM ai_user_memory WHERE user_id = %s",
            (int(user_id),)
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def save_active_game_session(guild_id, channel_id, message_id, game_key, players=None, state=None):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO active_game_sessions (message_id, guild_id, channel_id, game_key, players, state_json)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (message_id) DO UPDATE SET
                guild_id = EXCLUDED.guild_id,
                channel_id = EXCLUDED.channel_id,
                game_key = EXCLUDED.game_key,
                players = EXCLUDED.players,
                state_json = EXCLUDED.state_json
            """,
            (
                int(message_id),
                int(guild_id),
                int(channel_id),
                str(game_key),
                [int(player) for player in (players or [])],
                json.dumps(state or {}),
            )
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def delete_active_game_session(message_id):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute("DELETE FROM active_game_sessions WHERE message_id = %s", (int(message_id),))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def load_active_game_sessions():
    _ensure_ready()
    if not pg_ready:
        return []
    try:
        conn = pg_conn()
        if conn is None:
            return []
        cur = conn.cursor()
        cur.execute("SELECT message_id, guild_id, channel_id, game_key, players, state_json, created_at FROM active_game_sessions")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        sessions = []
        for message_id, guild_id, channel_id, game_key, players, state_json, created_at in rows:
            if isinstance(state_json, dict):
                state = state_json
            else:
                try:
                    state = json.loads(state_json or "{}")
                except Exception:
                    state = {}
            sessions.append({
                "message_id": int(message_id),
                "guild_id": int(guild_id),
                "channel_id": int(channel_id),
                "game_key": str(game_key),
                "players": [int(player) for player in (players or [])],
                "state": state,
                "created_at": created_at,
            })
        return sessions
    except Exception:
        return []

def add_guild_activity_counts(counts):
    _ensure_ready()
    if not pg_ready or not counts:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        for key, amount in counts.items():
            if amount <= 0:
                continue
            if len(key) == 2:
                guild_id, user_id = key
                column = "messages"
            else:
                guild_id, user_id, column = key
            if column != "messages":
                continue
            cur.execute(
                f"INSERT INTO guild_activity_counts (guild_id, user_id, {column}) VALUES (%s, %s, %s) "
                f"ON CONFLICT (guild_id, user_id) DO UPDATE SET {column} = guild_activity_counts.{column} + EXCLUDED.{column}",
                (int(guild_id), int(user_id), int(amount))
            )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def get_guild_activity_top(guild_id, limit=5):
    _ensure_ready()
    if not pg_ready:
        return []
    try:
        conn = pg_conn()
        if conn is None:
            return []
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, messages
            FROM guild_activity_counts
            WHERE guild_id = %s
            ORDER BY messages DESC, user_id ASC
            LIMIT %s
            """,
            (int(guild_id), int(limit))
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            {
                "user_id": int(user_id),
                "messages": int(messages),
                "activity_score": int(messages),
            }
            for user_id, messages in rows
        ]
    except Exception:
        return []

def clear_guild_activity_counts(guild_id):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute("DELETE FROM guild_activity_counts WHERE guild_id = %s", (int(guild_id),))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def add_message_activity_events(events):
    _ensure_ready()
    if not pg_ready or not events:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT INTO message_activity_events (message_id, guild_id, channel_id, user_id, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (message_id) DO NOTHING
            """,
            [
                (
                    int(event["message_id"]),
                    int(event["guild_id"]),
                    int(event["channel_id"]),
                    int(event["user_id"]),
                    event["created_at"],
                )
                for event in events
            ]
        )
        cur.execute("DELETE FROM message_activity_events WHERE created_at < NOW() - INTERVAL '370 days'")
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def get_message_activity_top(guild_id, since, limit=10):
    _ensure_ready()
    if not pg_ready:
        return []
    try:
        conn = pg_conn()
        if conn is None:
            return []
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, COUNT(*) AS messages
            FROM message_activity_events
            WHERE guild_id = %s AND created_at >= %s
            GROUP BY user_id
            ORDER BY messages DESC, user_id ASC
            LIMIT %s
            """,
            (int(guild_id), since, int(limit))
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [{"user_id": int(user_id), "messages": int(messages)} for user_id, messages in rows]
    except Exception:
        return []

def get_message_activity_count(guild_id, user_id, since):
    _ensure_ready()
    if not pg_ready:
        return 0
    try:
        conn = pg_conn()
        if conn is None:
            return 0
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*)
            FROM message_activity_events
            WHERE guild_id = %s AND user_id = %s AND created_at >= %s
            """,
            (int(guild_id), int(user_id), since)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        return int(row[0] or 0)
    except Exception:
        return 0

def get_message_activity_top_between(guild_id, starts_at, ends_at, limit=10):
    _ensure_ready()
    if not pg_ready:
        return []
    try:
        conn = pg_conn()
        if conn is None:
            return []
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, COUNT(*) AS messages
            FROM message_activity_events
            WHERE guild_id = %s AND created_at >= %s AND created_at <= %s
            GROUP BY user_id
            ORDER BY messages DESC, user_id ASC
            LIMIT %s
            """,
            (int(guild_id), starts_at, ends_at, int(limit))
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [{"user_id": int(user_id), "messages": int(messages)} for user_id, messages in rows]
    except Exception:
        return []

def save_message_event(guild_id, channel_id, message_id, title, started_by, starts_at, ends_at):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO active_message_events
                (guild_id, channel_id, message_id, title, started_by, starts_at, ends_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (guild_id) DO UPDATE SET
                channel_id = EXCLUDED.channel_id,
                message_id = EXCLUDED.message_id,
                title = EXCLUDED.title,
                started_by = EXCLUDED.started_by,
                starts_at = EXCLUDED.starts_at,
                ends_at = EXCLUDED.ends_at
            """,
            (
                int(guild_id),
                int(channel_id),
                int(message_id) if message_id else None,
                title,
                int(started_by),
                starts_at,
                ends_at,
            )
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False

def get_message_event(guild_id):
    _ensure_ready()
    if not pg_ready:
        return None
    try:
        conn = pg_conn()
        if conn is None:
            return None
        cur = conn.cursor()
        cur.execute(
            """
            SELECT guild_id, channel_id, message_id, title, started_by, starts_at, ends_at
            FROM active_message_events
            WHERE guild_id = %s
            """,
            (int(guild_id),)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        guild_id, channel_id, message_id, title, started_by, starts_at, ends_at = row
        return {
            "guild_id": int(guild_id),
            "channel_id": int(channel_id),
            "message_id": int(message_id) if message_id else None,
            "title": title,
            "started_by": int(started_by),
            "starts_at": starts_at,
            "ends_at": ends_at,
        }
    except Exception:
        return None

def load_message_events():
    _ensure_ready()
    if not pg_ready:
        return []
    try:
        conn = pg_conn()
        if conn is None:
            return []
        cur = conn.cursor()
        cur.execute("""
            SELECT guild_id, channel_id, message_id, title, started_by, starts_at, ends_at
            FROM active_message_events
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            {
                "guild_id": int(guild_id),
                "channel_id": int(channel_id),
                "message_id": int(message_id) if message_id else None,
                "title": title,
                "started_by": int(started_by),
                "starts_at": starts_at,
                "ends_at": ends_at,
            }
            for guild_id, channel_id, message_id, title, started_by, starts_at, ends_at in rows
        ]
    except Exception:
        return []

def delete_message_event(guild_id):
    _ensure_ready()
    if not pg_ready:
        return False
    try:
        conn = pg_conn()
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute("DELETE FROM active_message_events WHERE guild_id = %s", (int(guild_id),))
        conn.commit()
        deleted = cur.rowcount > 0
        cur.close()
        conn.close()
        return deleted
    except Exception:
        return False

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
