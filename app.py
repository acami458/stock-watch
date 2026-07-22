#!/usr/bin/env python3
"""
Stock Watch — single watchlist, REAL-TIME data via Alpaca (IEX feed, free).
Add box at top, amber 30s flash + sound on new alerts, tap for chart, email/push.
ENV: ALPACA_KEY, ALPACA_SECRET, DATABASE_URL, SECRET_KEY,
     SMTP_* (email, optional), VAPID_* (push, optional), APP_URL
RUN: export ALPACA_KEY=... ALPACA_SECRET=... ; python3 app.py  -> http://localhost:8765
"""

import base64
import hashlib
import hmac
import json
import os
import smtplib
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.message import EmailMessage
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from zoneinfo import ZoneInfo
except ImportError:
    raise SystemExit("Python 3.9+ required.")

# ----------------------------- CONFIG -----------------------------
HERE            = os.path.dirname(os.path.abspath(__file__))
ALPACA_KEY      = os.environ.get("ALPACA_KEY", "").strip()
ALPACA_SECRET   = os.environ.get("ALPACA_SECRET", "").strip()
ALPACA_FEED     = os.environ.get("ALPACA_FEED", "iex").strip()   # free tier = iex
DATA_URL        = "https://data.alpaca.markets/v2/stocks/snapshots"
DATABASE_URL    = os.environ.get("DATABASE_URL", "").strip()
SECRET_KEY      = os.environ.get("SECRET_KEY", "").strip() or base64.b64encode(os.urandom(24)).decode()
DB_PATH         = os.path.join(HERE, "stockwatch.db")
ICON_PATH       = os.path.join(HERE, "icon.png")
ET              = ZoneInfo("America/New_York")
PORT            = int(os.environ.get("PORT", "8765"))
REFRESH_SECONDS = 60
IDLE_SLEEP_SECONDS   = 300   # market closed: re-check every 5 min and touch NOTHING in the DB,
                             # so Neon's free-tier compute can auto-suspend overnight/weekends
SYMS_REFRESH_SECONDS = 600   # re-read the watchlist from the DB at most every 10 min while active
DAILY_SAVE_HOUR      = 15    # persist the daily snapshot once near the close...
DAILY_SAVE_MINUTE    = 55    # ...at 15:55 ET, instead of rewriting it every minute
RISE_PCT        = 0.005          # condition 1: price must rise >= 0.5% off the intraday low
VOL_SPIKE_MULT  = 1.5           # condition 4: last-3-min volume > 150% of the average
ABOVE_OPEN_STOP = 1.01          # condition 6: stop watching once price >= 1.01 x open
# Grandpa's numbered-alarm model (see the INTRADAY sketch): a *new* alarm fires
# each time the stock carves a fresh lower intraday low and then bounces
# RISE_PCT off it. Alarm #1 may be a "false alarm"; if the stock keeps making
# lower lows you get #2, #3 ... and the bounce off the deepest low is the real
# signal. NEW_LOW_MIN_DROP keeps trivial new lows from each firing an alarm:
# the new low must be at least this fraction below the low that fired the
# previous alarm.
NEW_LOW_MIN_DROP = float(os.environ.get("NEW_LOW_MIN_DROP", "0.003"))  # 0.3%
MAX_PER_USER    = 80
ALERT_SESSIONS  = {"Pre-market", "Open"}
APP_URL         = os.environ.get("APP_URL", "").strip()

# JT WatchList (06-15-2026). New accounts start empty; this list is loaded into a
# specific account on request via the seed_watchlist.py helper script.
DEFAULT_WATCHLIST = [
    "AAPL", "ADI", "ADMA", "AMZN", "BABA", "CBRL", "CL", "COPX",
    "CUBE", "CVX", "DE", "FUTU", "GE", "GEV", "GLD", "GOOG",
    "IEP", "INTU", "JNJ", "JPM", "KO", "LLY", "LMT", "MA",
    "MAIN", "META", "MSFT", "MU", "NVDA", "PFE", "RIO", "SLV",
    "TSLA", "VZ", "WMT", "ADSK", "AVGO", "SPCX",
]
HAVE_DATA       = bool(ALPACA_KEY and ALPACA_SECRET)

SMTP_HOST = os.environ.get("SMTP_HOST", "").strip()
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587") or 587)
SMTP_USER = os.environ.get("SMTP_USER", "").strip()
SMTP_PASS = os.environ.get("SMTP_PASS", "").strip()
SMTP_SSL  = os.environ.get("SMTP_SSL", "false").lower() in ("1", "true", "yes")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "").strip() or SMTP_USER
EMAIL_ON  = bool(SMTP_HOST and SMTP_USER and SMTP_PASS)

VAPID_PUBLIC_KEY  = os.environ.get("VAPID_PUBLIC_KEY", "").strip()
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY", "").strip()
VAPID_SUBJECT     = os.environ.get("VAPID_SUBJECT", "").strip() or ("mailto:" + (EMAIL_FROM or "admin@example.com"))
PUSH_ON = bool(VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY)

# Pushover (https://pushover.net) — keys come from env vars, not the source file.
#   export PUSHOVER_USER_KEY=...      export PUSHOVER_API_TOKEN=...
PUSHOVER_USER_KEY  = os.environ.get("PUSHOVER_USER_KEY", "").strip()
PUSHOVER_API_TOKEN = os.environ.get("PUSHOVER_API_TOKEN", "").strip()
PUSHOVER_URL       = "https://api.pushover.net/1/messages.json"
PUSHOVER_ON = bool(PUSHOVER_USER_KEY and PUSHOVER_API_TOKEN)

_FALLBACK_ICON = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")

# =========================== DATABASE ===========================
def _db():
    if DATABASE_URL:
        import psycopg
        return psycopg.connect(DATABASE_URL), "pg"
    import sqlite3
    return sqlite3.connect(DB_PATH), "sqlite"


def _ph(sql, kind):
    return sql if kind == "pg" else sql.replace("%s", "?")


def init_db():
    conn, kind = _db()
    try:
        cur = conn.cursor()
        if kind == "pg":
            cur.execute("""CREATE TABLE IF NOT EXISTS users(
                id SERIAL PRIMARY KEY, email TEXT UNIQUE NOT NULL,
                pw_hash TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW())""")
        else:
            cur.execute("""CREATE TABLE IF NOT EXISTS users(
                id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL,
                pw_hash TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS watchlist(
            user_id INTEGER NOT NULL, symbol TEXT NOT NULL, PRIMARY KEY(user_id, symbol))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS user_settings(
            user_id INTEGER PRIMARY KEY, alerts_on INTEGER DEFAULT 1)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS alerts_sent(
            user_id INTEGER NOT NULL, symbol TEXT NOT NULL, day TEXT NOT NULL,
            PRIMARY KEY(user_id, symbol, day))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS push_subs(
            endpoint TEXT PRIMARY KEY, user_id INTEGER NOT NULL, sub TEXT NOT NULL)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS alert_log(
            user_id INTEGER NOT NULL, symbol TEXT NOT NULL, day TEXT NOT NULL,
            ts TEXT, price REAL, from_low REAL, change REAL,
            PRIMARY KEY(user_id, symbol, day))""")
        # One row per numbered alarm (grandpa's model): the Nth new-low bounce
        # for a user/symbol on a given day. Powers the alert-history table.
        cur.execute("""CREATE TABLE IF NOT EXISTS alarm_events(
            user_id INTEGER NOT NULL, symbol TEXT NOT NULL, day TEXT NOT NULL,
            num INTEGER NOT NULL, ts TEXT, price REAL, from_low REAL, change REAL,
            PRIMARY KEY(user_id, symbol, day, num))""")
        # Highest alarm number we've already *notified* a user about, so each new
        # numbered alarm pushes exactly once (instead of once per whole day).
        cur.execute("""CREATE TABLE IF NOT EXISTS alarm_progress(
            user_id INTEGER NOT NULL, symbol TEXT NOT NULL, day TEXT NOT NULL,
            last_num INTEGER DEFAULT 0, PRIMARY KEY(user_id, symbol, day))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS daily_history(
            symbol TEXT NOT NULL, day TEXT NOT NULL, close REAL, low REAL,
            high REAL, prev_close REAL, PRIMARY KEY(symbol, day))""")
        conn.commit()
    finally:
        conn.close()


def create_user(email, pw_hash):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("SELECT id FROM users WHERE email=%s", kind), (email,))
        if cur.fetchone():
            return None
        if kind == "pg":
            cur.execute("INSERT INTO users(email, pw_hash) VALUES(%s,%s) RETURNING id", (email, pw_hash))
            uid = cur.fetchone()[0]
        else:
            cur.execute("INSERT INTO users(email, pw_hash) VALUES(?,?)", (email, pw_hash))
            uid = cur.lastrowid
        conn.commit()
        return uid
    finally:
        conn.close()


def get_user_by_email(email):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("SELECT id, pw_hash FROM users WHERE email=%s", kind), (email,))
        return cur.fetchone()
    finally:
        conn.close()


def get_email(uid):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("SELECT email FROM users WHERE id=%s", kind), (uid,))
        r = cur.fetchone()
        return r[0] if r else None
    finally:
        conn.close()


def get_watchlist(uid):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("SELECT symbol FROM watchlist WHERE user_id=%s ORDER BY symbol", kind), (uid,))
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def add_watch(uid, symbol):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("SELECT COUNT(*) FROM watchlist WHERE user_id=%s", kind), (uid,))
        if cur.fetchone()[0] >= MAX_PER_USER:
            return False
        try:
            cur.execute(_ph("INSERT INTO watchlist(user_id, symbol) VALUES(%s,%s)", kind), (uid, symbol))
            conn.commit()
        except Exception:
            conn.rollback()
        return True
    finally:
        conn.close()


def remove_watch(uid, symbol):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("DELETE FROM watchlist WHERE user_id=%s AND symbol=%s", kind), (uid, symbol))
        conn.commit()
    finally:
        conn.close()


def all_user_symbols():
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT symbol FROM watchlist")
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def get_alerts_on(uid):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("SELECT alerts_on FROM user_settings WHERE user_id=%s", kind), (uid,))
        r = cur.fetchone()
        return True if r is None else bool(r[0])
    finally:
        conn.close()


def set_alerts_on(uid, on):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        val = 1 if on else 0
        if kind == "pg":
            cur.execute("""INSERT INTO user_settings(user_id, alerts_on) VALUES(%s,%s)
                           ON CONFLICT (user_id) DO UPDATE SET alerts_on=EXCLUDED.alerts_on""", (uid, val))
        else:
            cur.execute("INSERT OR REPLACE INTO user_settings(user_id, alerts_on) VALUES(?,?)", (uid, val))
        conn.commit()
    finally:
        conn.close()


def alert_users():
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute("""SELECT u.id, u.email FROM users u
                       LEFT JOIN user_settings s ON u.id=s.user_id
                       WHERE COALESCE(s.alerts_on, 1)=1""")
        return cur.fetchall()
    finally:
        conn.close()


def already_alerted(uid, symbol, day):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("SELECT 1 FROM alerts_sent WHERE user_id=%s AND symbol=%s AND day=%s", kind),
                    (uid, symbol, day))
        return cur.fetchone() is not None
    finally:
        conn.close()


def mark_alerted(uid, symbol, day):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        try:
            cur.execute(_ph("INSERT INTO alerts_sent(user_id, symbol, day) VALUES(%s,%s,%s)", kind),
                        (uid, symbol, day))
            conn.commit()
        except Exception:
            conn.rollback()
    finally:
        conn.close()


def save_sub(uid, sub):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        ep = sub.get("endpoint")
        if not ep:
            return
        if kind == "pg":
            cur.execute("""INSERT INTO push_subs(endpoint, user_id, sub) VALUES(%s,%s,%s)
                           ON CONFLICT (endpoint) DO UPDATE SET user_id=EXCLUDED.user_id, sub=EXCLUDED.sub""",
                        (ep, uid, json.dumps(sub)))
        else:
            cur.execute("INSERT OR REPLACE INTO push_subs(endpoint, user_id, sub) VALUES(?,?,?)",
                        (ep, uid, json.dumps(sub)))
        conn.commit()
    finally:
        conn.close()


def delete_sub(endpoint):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("DELETE FROM push_subs WHERE endpoint=%s", kind), (endpoint,))
        conn.commit()
    finally:
        conn.close()


def subs_for_user(uid):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("SELECT endpoint, sub FROM push_subs WHERE user_id=%s", kind), (uid,))
        return [(r[0], json.loads(r[1])) for r in cur.fetchall()]
    finally:
        conn.close()


def all_user_ids():
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users")
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def log_alert(uid, symbol, day, ts, price, from_low, change):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        try:
            if kind == "pg":
                cur.execute("""INSERT INTO alert_log(user_id, symbol, day, ts, price, from_low, change)
                               VALUES(%s,%s,%s,%s,%s,%s,%s)
                               ON CONFLICT (user_id, symbol, day) DO NOTHING""",
                            (uid, symbol, day, ts, price, from_low, change))
            else:
                cur.execute("""INSERT OR IGNORE INTO alert_log(user_id, symbol, day, ts, price, from_low, change)
                               VALUES(?,?,?,?,?,?,?)""", (uid, symbol, day, ts, price, from_low, change))
            conn.commit()
        except Exception:
            conn.rollback()
    finally:
        conn.close()


def get_alert_log(uid, limit=200):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("""SELECT symbol, day, ts, price, from_low, change FROM alert_log
                           WHERE user_id=%s ORDER BY day DESC, ts DESC LIMIT %s""", kind), (uid, limit))
        return [{"symbol": r[0], "day": r[1], "ts": r[2], "price": r[3],
                 "from_low": r[4], "change": r[5]} for r in cur.fetchall()]
    finally:
        conn.close()


# --- numbered alarms (grandpa's model) ---
def log_alarm_event(uid, symbol, day, num, ts, price, from_low, change):
    """Record the Nth new-low bounce for a user/symbol/day (once, idempotent)."""
    conn, kind = _db()
    try:
        cur = conn.cursor()
        try:
            if kind == "pg":
                cur.execute("""INSERT INTO alarm_events(user_id, symbol, day, num, ts, price, from_low, change)
                               VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                               ON CONFLICT (user_id, symbol, day, num) DO NOTHING""",
                            (uid, symbol, day, num, ts, price, from_low, change))
            else:
                cur.execute("""INSERT OR IGNORE INTO alarm_events(user_id, symbol, day, num, ts, price, from_low, change)
                               VALUES(?,?,?,?,?,?,?,?)""", (uid, symbol, day, num, ts, price, from_low, change))
            conn.commit()
        except Exception:
            conn.rollback()
    finally:
        conn.close()


def max_logged_alarm(uid, symbol, day):
    """Highest alarm number already saved to history for this user/symbol/day."""
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("SELECT MAX(num) FROM alarm_events WHERE user_id=%s AND symbol=%s AND day=%s", kind),
                    (uid, symbol, day))
        r = cur.fetchone()
        return int(r[0]) if r and r[0] is not None else 0
    finally:
        conn.close()


def get_alarm_events(uid, limit=200):
    """Most recent numbered alarms for the history table."""
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("""SELECT symbol, day, num, ts, price, from_low, change FROM alarm_events
                           WHERE user_id=%s ORDER BY day DESC, ts DESC, num DESC LIMIT %s""", kind),
                    (uid, limit))
        return [{"symbol": r[0], "day": r[1], "num": r[2], "ts": r[3], "price": r[4],
                 "from_low": r[5], "change": r[6]} for r in cur.fetchall()]
    finally:
        conn.close()


def get_notified_alarm(uid, symbol, day):
    """Highest alarm number we've already pushed/emailed for this user/symbol/day."""
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("SELECT last_num FROM alarm_progress WHERE user_id=%s AND symbol=%s AND day=%s", kind),
                    (uid, symbol, day))
        r = cur.fetchone()
        return int(r[0]) if r and r[0] is not None else 0
    finally:
        conn.close()


def set_notified_alarm(uid, symbol, day, num):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        if kind == "pg":
            cur.execute("""INSERT INTO alarm_progress(user_id, symbol, day, last_num) VALUES(%s,%s,%s,%s)
                           ON CONFLICT (user_id, symbol, day) DO UPDATE SET last_num=EXCLUDED.last_num""",
                        (uid, symbol, day, num))
        else:
            cur.execute("INSERT OR REPLACE INTO alarm_progress(user_id, symbol, day, last_num) VALUES(?,?,?,?)",
                        (uid, symbol, day, num))
        conn.commit()
    finally:
        conn.close()


def upsert_daily(symbol, day, close, low, high, prev_close):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        if kind == "pg":
            cur.execute("""INSERT INTO daily_history(symbol, day, close, low, high, prev_close)
                           VALUES(%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (symbol, day) DO UPDATE SET
                             close=EXCLUDED.close, low=EXCLUDED.low,
                             high=EXCLUDED.high, prev_close=EXCLUDED.prev_close""",
                        (symbol, day, close, low, high, prev_close))
        else:
            cur.execute("""INSERT OR REPLACE INTO daily_history(symbol, day, close, low, high, prev_close)
                           VALUES(?,?,?,?,?,?)""", (symbol, day, close, low, high, prev_close))
        conn.commit()
    finally:
        conn.close()


def get_daily_history(symbol, limit=180):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("""SELECT day, close, low, high, prev_close FROM daily_history
                           WHERE symbol=%s ORDER BY day DESC LIMIT %s""", kind), (symbol, limit))
        rows = [{"d": r[0], "close": r[1], "low": r[2], "high": r[3], "prev_close": r[4]}
                for r in cur.fetchall()]
        rows.reverse()
        return rows
    finally:
        conn.close()


# =========================== AUTH ===========================
def hash_pw(pw, salt=None):
    salt = salt or os.urandom(16)
    h = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt, 200_000)
    return base64.b64encode(salt).decode() + ":" + base64.b64encode(h).decode()


def verify_pw(pw, stored):
    try:
        s, h = stored.split(":")
        salt, expected = base64.b64decode(s), base64.b64decode(h)
        test = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt, 200_000)
        return hmac.compare_digest(test, expected)
    except Exception:
        return False


def sign_session(uid):
    msg = str(uid).encode()
    sig = hmac.new(SECRET_KEY.encode(), msg, hashlib.sha256).hexdigest()
    return base64.urlsafe_b64encode(msg).decode() + "." + sig


def read_session(token):
    try:
        b64, sig = token.split(".")
        msg = base64.urlsafe_b64decode(b64)
        good = hmac.new(SECRET_KEY.encode(), msg, hashlib.sha256).hexdigest()
        if hmac.compare_digest(good, sig):
            return int(msg.decode())
    except Exception:
        pass
    return None


def clean_symbol(s):
    s = (s or "").strip().upper()
    if 1 <= len(s) <= 10 and all(c.isalnum() or c in ".-" for c in s):
        return s
    return None


# =========================== QUOTES (Alpaca) + HISTORY ===========================
_quotes = {}
_qlock = threading.Lock()
_hist = {}
_hist_lock = threading.Lock()
_hist_state = {"day": None}
HIST_MAX = 480

# Rolling per-symbol minute bars, used for the 3-minute average and the volume
# spike test. Each entry is {symbol: [[minute_str, close, volume], ...]} and is
# cleared at the start of each new trading day.
_bars = {}
_bars_lock = threading.Lock()
_bars_state = {"day": None}
BARS_MAX = 480

# Grandpa's numbered-alarm tracker. Per symbol, per day:
#   count      -> how many alarms have fired today (1st, 2nd, 3rd bounce...)
#   armed_low  -> the intraday low that fired the last alarm; a new alarm can
#                 only fire once the stock prints a low meaningfully BELOW this.
# Shared across all users (it's a property of the stock's price action).
_alarm = {}
_alarm_lock = threading.Lock()
_alarm_state = {"day": None}


def _update_alarm(sym, day_low, bounced):
    """Advance the numbered-alarm counter for one symbol on one tick.

    Fires (increments the count) when the price has bounced RISE_PCT off the
    intraday low (`bounced`) AND that low is a fresh new low at least
    NEW_LOW_MIN_DROP below the low that fired the previous alarm. Returns
    (current_count, fired_this_tick).
    """
    today = datetime.now(ET).strftime("%Y-%m-%d")
    with _alarm_lock:
        if _alarm_state["day"] != today:
            _alarm.clear()
            _alarm_state["day"] = today
        st = _alarm.setdefault(sym, {"count": 0, "armed_low": None})
        fired = False
        if bounced and day_low is not None:
            if st["armed_low"] is None or day_low <= st["armed_low"] * (1 - NEW_LOW_MIN_DROP):
                st["count"] += 1
                st["armed_low"] = day_low
                fired = True
        return st["count"], fired


def record_history():
    now = datetime.now(ET)
    today = now.strftime("%Y-%m-%d")
    hhmm = now.strftime("%H:%M")
    with _qlock:
        snapshot = {s: r.get("price") for s, r in _quotes.items()}
    with _hist_lock:
        if _hist_state["day"] != today:
            _hist.clear()
            _hist_state["day"] = today
        for s, p in snapshot.items():
            if p is None:
                continue
            lst = _hist.setdefault(s, [])
            if lst and lst[-1][0] == hhmm:
                lst[-1] = [hhmm, p]
            else:
                lst.append([hhmm, p])
            if len(lst) > HIST_MAX:
                del lst[:len(lst) - HIST_MAX]


def history_for(sym):
    with _hist_lock:
        return [{"t": t, "p": p} for t, p in _hist.get(sym, [])]


def market_open(dt):
    if dt.weekday() >= 5:
        return False
    m = dt.hour * 60 + dt.minute
    return 9 * 60 + 30 <= m < 16 * 60


def session_label(dt):
    if dt.weekday() >= 5:
        return "Closed (weekend)"
    m = dt.hour * 60 + dt.minute
    if 4 * 60 <= m < 9 * 60 + 30:
        return "Pre-market"
    if 9 * 60 + 30 <= m < 16 * 60:
        return "Open"
    if 16 * 60 <= m < 20 * 60:
        return "After-hours"
    return "Closed"


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch_snapshots(syms):
    """Alpaca multi-symbol snapshots -> {symbol: snapshot dict}."""
    out = {}
    headers = {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET,
               "User-Agent": "stock-watch"}
    for chunk in _chunks(sorted(syms), 90):
        url = DATA_URL + "?symbols=" + urllib.parse.quote(",".join(chunk)) + "&feed=" + urllib.parse.quote(ALPACA_FEED)
        req = urllib.request.Request(url, headers=headers)
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=15) as r:
                    data = json.loads(r.read().decode())
                snaps = data.get("snapshots", data) if isinstance(data, dict) else {}
                if isinstance(snaps, dict):
                    out.update(snaps)
                break
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt < 2:
                    time.sleep(1.5 * (attempt + 1)); continue
                break
            except Exception:
                if attempt < 2:
                    time.sleep(0.8 * (attempt + 1)); continue
                break
    return out


def _as_of(ts):
    if not ts:
        return ""
    try:
        dt = datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        return dt.astimezone(ET).strftime("%H:%M ET")
    except Exception:
        return ""


def _blank_row(t):
    return {"ticker": t, "price": None, "from_low": None, "prev_close": None,
            "change": None, "open": None, "high": None, "low": None, "vwap": None,
            "as_of": "", "near": False, "alert": False, "signal": None, "conditions": [],
            "alarm_num": 0, "new_alarm": False}


def build_row(t, snap):
    blank = _blank_row(t)
    try:
        if not snap:
            return blank
        lt = snap.get("latestTrade") or {}
        db = snap.get("dailyBar") or {}
        pdb = snap.get("prevDailyBar") or {}
        mb = snap.get("minuteBar") or {}
        c = lt.get("p") or db.get("c")
        low = db.get("l")
        o = db.get("o")
        hi = db.get("h")
        pc = pdb.get("c")
        vwap = db.get("vw")          # Alpaca daily VWAP
        if not c:
            return blank
        from_low = (c - low) / low * 100 if low else None
        change = (c - pc) / pc * 100 if pc else None
        row = {"ticker": t, "price": round(c, 2),
               "from_low": None if from_low is None else round(from_low, 2),
               "prev_close": None if not pc else round(pc, 2),
               "change": None if change is None else round(change, 2),
               "open": None if not o else round(o, 2),
               "high": None if not hi else round(hi, 2),
               "low": None if not low else round(low, 2),
               "vwap": None if not vwap else round(vwap, 2),
               "as_of": _as_of(lt.get("t")),
               # condition 1: price is >= 0.5% above the intraday low
               "near": bool(from_low is not None and from_low >= RISE_PCT * 100),
               "alert": False, "signal": None, "conditions": [],
               "alarm_num": 0, "new_alarm": False,
               # raw minute bar, used by evaluate_signal for the 3-min tests
               "_mb_t": mb.get("t"), "_mb_c": mb.get("c"), "_mb_v": mb.get("v")}
        evaluate_signal(row)
        return row
    except Exception:
        return blank


def _update_bars(sym, minute_ts, close, vol):
    """Append the latest minute bar (deduped by minute) to the rolling buffer."""
    if not minute_ts or close is None:
        return
    today = datetime.now(ET).strftime("%Y-%m-%d")
    minute = str(minute_ts)[:16]   # 'YYYY-MM-DDTHH:MM'
    with _bars_lock:
        if _bars_state["day"] != today:
            _bars.clear()
            _bars_state["day"] = today
        lst = _bars.setdefault(sym, [])
        v = 0.0 if vol is None else float(vol)
        if lst and lst[-1][0] == minute:
            lst[-1] = [minute, float(close), v]
        else:
            lst.append([minute, float(close), v])
        if len(lst) > BARS_MAX:
            del lst[:len(lst) - BARS_MAX]
        return list(lst)


def evaluate_signal(row):
    """Apply grandpa's multi-condition logic and attach a confidence score.

    Conditions (from the WatchList "Python" tab):
      1. price rose >= 0.5% off the intraday low
      2. price > 3-minute average
      3. price > VWAP
      4. last-3-min volume > 150% of the average minute volume
      5. price < open  (only hunting for a bounce while still below the open)
      6. stop once price >= 1.01 x open (the setup has played out)
    Confidence:  1+2 = Good, 1+2+3 = Very Good, 1+2+3+4 = Excellent.
    A push/email/Pushover alert fires only when 1 AND 2 AND 5 hold and the
    stop (6) has not triggered. Conditions 3 and 4 raise the confidence label.
    """
    sym = row["ticker"]
    price = row.get("price")
    o = row.get("open")
    bars = _update_bars(sym, row.pop("_mb_t", None), row.pop("_mb_c", None), row.pop("_mb_v", None))
    if price is None:
        return row

    # Condition 1 — already computed as row["near"].
    c1 = bool(row.get("near"))

    # Condition 2 — price above the average close of the last 3 minute bars.
    avg3 = None
    if bars and len(bars) >= 1:
        last3 = bars[-3:]
        avg3 = sum(b[1] for b in last3) / len(last3)
    c2 = bool(avg3 is not None and price > avg3)

    # Condition 3 — price above the daily VWAP.
    vwap = row.get("vwap")
    c3 = bool(vwap is not None and price > vwap)

    # Condition 4 — last-3-min volume exceeds 150% of the average minute volume.
    c4 = False
    if bars and len(bars) >= 4:
        vol3 = sum(b[2] for b in bars[-3:])
        baseline = sum(b[2] for b in bars) / len(bars)   # avg volume per minute
        c4 = bool(baseline > 0 and vol3 > VOL_SPIKE_MULT * baseline * 3)

    # Condition 5 — still trading below the open.
    c5 = bool(o is not None and price < o)
    # Condition 6 — stop watching once price has recovered to >= 1.01 x open.
    stopped = bool(o is not None and price >= ABOVE_OPEN_STOP * o)

    met = [i for i, ok in [(1, c1), (2, c2), (3, c3), (4, c4), (5, c5)] if ok]
    row["conditions"] = met
    row["vwap"] = None if vwap is None else round(vwap, 2)

    # Confidence ladder requires the 1+2 base.
    signal = None
    if c1 and c2:
        signal = "Good"
        if c3:
            signal = "Very Good"
            if c4:
                signal = "Excellent"
    row["signal"] = signal

    # Grandpa's numbered alarm: a fresh alarm each time a NEW lower intraday low
    # bounces >= RISE_PCT. Independent of the confidence ladder above, which is
    # kept intact as the quality label shown alongside the number.
    count, fired = _update_alarm(sym, row.get("low"), c1)
    row["alarm_num"] = count
    row["new_alarm"] = fired
    # Kept for reference/UI: the old full-confidence alert state.
    row["alert"] = bool(c1 and c2 and c5 and not stopped)
    return row


def refresh_symbols(syms):
    if not HAVE_DATA or not syms:
        return
    snaps = fetch_snapshots(list(syms))
    with _qlock:
        for s in syms:
            _quotes[s] = build_row(s, snaps.get(s))


def rows_for(syms):
    with _qlock:
        missing = [s for s in syms if s not in _quotes]
    if missing:
        refresh_symbols(missing)
    out = []
    with _qlock:
        for s in syms:
            out.append(_quotes.get(s) or _blank_row(s))
    return out


# ==================== ALERTS (email + web push + Pushover) ====================
def send_alert_email(to_email, row):
    if not EMAIL_ON:
        return False
    t = row["ticker"]
    sig = row.get("signal")
    num = row.get("alarm_num") or 1
    vwap_line = "" if row.get("vwap") is None else f"VWAP: ${row['vwap']:.2f}\n"
    body = (f"Alarm #{num} for {t}: it just bounced {row['from_low']:.2f}% off a new intraday low.\n\n"
            f"This is the #{num} new-low bounce today — the higher the number, the more the "
            f"stock has been driven down and re-tested.\n\n"
            f"Signal: {sig or 'n/a'} (conditions met: {row.get('conditions') or '—'})\n"
            f"Price: ${row['price']:.2f}\nDay low: ${row['low']:.2f}\n"
            f"{vwap_line}"
            f"Change vs prev close: {row['change']:+.2f}%\nAs of: {row['as_of']}\n")
    if APP_URL:
        body += f"\nOpen the dashboard: {APP_URL}\n"
    try:
        msg = EmailMessage()
        sig_tag = f" [{sig}]" if sig else ""
        msg["Subject"] = f"🔔 {t} Alarm #{num}{sig_tag} — +{row['from_low']:.2f}% off a new low"
        msg["From"] = EMAIL_FROM
        msg["To"] = to_email
        msg.set_content(body)
        ctx = ssl.create_default_context()
        if SMTP_SSL:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx, timeout=15) as s:
                s.login(SMTP_USER, SMTP_PASS); s.send_message(msg)
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
                s.starttls(context=ctx); s.login(SMTP_USER, SMTP_PASS); s.send_message(msg)
        return True
    except Exception as e:
        print(f"  (alert email failed: {e})", flush=True)
        return False


def send_push(sub, title, body):
    if not PUSH_ON:
        return None
    try:
        from pywebpush import webpush, WebPushException
    except Exception:
        return None
    try:
        webpush(sub, json.dumps({"title": title, "body": body, "url": APP_URL or "/"}),
                vapid_private_key=VAPID_PRIVATE_KEY, vapid_claims={"sub": VAPID_SUBJECT})
        return True
    except WebPushException as e:
        code = getattr(getattr(e, "response", None), "status_code", None)
        if code in (404, 410):
            return "gone"
        print(f"  (push failed: {str(e)[:90]})", flush=True)
        return False
    except Exception as e:
        print(f"  (push error: {str(e)[:90]})", flush=True)
        return False


def send_pushover(title, body):
    """Send a Pushover push notification to the configured user key."""
    if not PUSHOVER_ON:
        return None
    try:
        data = urllib.parse.urlencode({
            "token": PUSHOVER_API_TOKEN,
            "user": PUSHOVER_USER_KEY,
            "title": title,
            "message": body,
            "url": APP_URL or "",
        }).encode()
        req = urllib.request.Request(PUSHOVER_URL, data=data, headers={"User-Agent": "stock-watch"})
        with urllib.request.urlopen(req, timeout=15) as r:
            r.read()
        return True
    except Exception as e:
        print(f"  (pushover failed: {str(e)[:90]})", flush=True)
        return False


def notify_user(uid, email, row):
    sent = False
    sig = row.get("signal")
    num = row.get("alarm_num") or 1
    sig_tag = f" [{sig}]" if sig else ""
    if EMAIL_ON and send_alert_email(email, row):
        sent = True
    if PUSH_ON:
        title = f"🔔 {row['ticker']} Alarm #{num}{sig_tag} +{row['from_low']:.2f}% off new low"
        body = f"${row['price']:.2f} · {row['change']:+.2f}% on the day"
        for endpoint, sub in subs_for_user(uid):
            res = send_push(sub, title, body)
            if res == "gone":
                delete_sub(endpoint)
            elif res:
                sent = True
    if PUSHOVER_ON:
        title = f"🔔 {row['ticker']} Alarm #{num}{sig_tag} +{row['from_low']:.2f}% off new low"
        body = f"${row['price']:.2f} · {row['change']:+.2f}% on the day · signal: {sig or 'n/a'}"
        if send_pushover(title, body):
            sent = True
    return sent


def alert_check():
    if not (EMAIL_ON or PUSH_ON or PUSHOVER_ON):
        return
    now = datetime.now(ET)
    if session_label(now) not in ALERT_SESSIONS:
        return
    day = now.strftime("%Y-%m-%d")
    for uid, email in alert_users():
        for s in get_watchlist(uid):
            with _qlock:
                row = _quotes.get(s)
            # Grandpa's model: fire once per NEW numbered alarm (each new-low
            # bounce), not just once per day. Notify only when this symbol's
            # alarm count has climbed past what we've already sent this user.
            if row and row.get("price") is not None:
                num = row.get("alarm_num") or 0
                if num > 0 and num > get_notified_alarm(uid, s, day):
                    if notify_user(uid, email, row):
                        set_notified_alarm(uid, s, day, num)


def record_daily_all():
    """Persist a daily price snapshot per watched symbol (survives restarts).

    Uses ONE database connection and a single batched write, instead of opening
    a fresh connection per symbol.
    """
    day = datetime.now(ET).strftime("%Y-%m-%d")
    with _qlock:
        snap = {s: dict(r) for s, r in _quotes.items()}
    rows = [(s, day, r.get("price"), r.get("low"), r.get("high"), r.get("prev_close"))
            for s, r in snap.items() if r.get("price") is not None]
    if not rows:
        return
    conn, kind = _db()
    try:
        cur = conn.cursor()
        if kind == "pg":
            cur.executemany(
                """INSERT INTO daily_history(symbol, day, close, low, high, prev_close)
                   VALUES(%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (symbol, day) DO UPDATE SET
                     close=EXCLUDED.close, low=EXCLUDED.low,
                     high=EXCLUDED.high, prev_close=EXCLUDED.prev_close""", rows)
        else:
            cur.executemany(
                """INSERT OR REPLACE INTO daily_history(symbol, day, close, low, high, prev_close)
                   VALUES(?,?,?,?,?,?)""", rows)
        conn.commit()
    finally:
        conn.close()


def fetch_daily_bars(syms, days=365):
    """Fetch historical daily OHLC bars from Alpaca -> {symbol: [bar, ...]}.

    Uses the same keys/feed as the live snapshots. Handles symbol chunking and
    Alpaca's page_token pagination. Used to backfill daily_history so the
    Backtest tab has real data to work with immediately.
    """
    if not HAVE_DATA or not syms:
        return {}
    from datetime import timedelta
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    start_s, end_s = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    headers = {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET,
               "User-Agent": "stock-watch"}
    bars_url = "https://data.alpaca.markets/v2/stocks/bars"
    out = {}
    for chunk in _chunks(sorted(set(syms)), 90):
        page_token = None
        for _ in range(50):  # page cap, just in case
            params = {"symbols": ",".join(chunk), "timeframe": "1Day",
                      "start": start_s, "end": end_s, "feed": ALPACA_FEED,
                      "limit": "10000", "adjustment": "raw"}
            if page_token:
                params["page_token"] = page_token
            url = bars_url + "?" + urllib.parse.urlencode(params)
            req = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=20) as r:
                    data = json.loads(r.read().decode())
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    time.sleep(1.5); continue
                break
            except Exception:
                break
            for s, lst in (data.get("bars") or {}).items():
                out.setdefault(s, []).extend(lst)
            page_token = data.get("next_page_token")
            if not page_token:
                break
    return out


def backfill_daily_history(syms, days=365):
    """Populate daily_history from Alpaca daily bars. Returns rows written."""
    bars = fetch_daily_bars(syms, days)
    rows = []
    for s, lst in bars.items():
        lst = sorted(lst, key=lambda b: str(b.get("t", "")))
        prev_close = None
        for b in lst:
            day = str(b.get("t", ""))[:10]
            close = b.get("c")
            if not day or close is None:
                continue
            rows.append((s, day, close, b.get("l"), b.get("h"), prev_close))
            prev_close = close
    if not rows:
        return 0
    conn, kind = _db()
    try:
        cur = conn.cursor()
        if kind == "pg":
            cur.executemany(
                """INSERT INTO daily_history(symbol, day, close, low, high, prev_close)
                   VALUES(%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (symbol, day) DO UPDATE SET
                     close=EXCLUDED.close, low=EXCLUDED.low,
                     high=EXCLUDED.high, prev_close=EXCLUDED.prev_close""", rows)
        else:
            cur.executemany(
                """INSERT OR REPLACE INTO daily_history(symbol, day, close, low, high, prev_close)
                   VALUES(?,?,?,?,?,?)""", rows)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def record_alarms():
    """Save every numbered alarm to history, one row per (user, symbol, day, num).

    Runs regardless of whether email/push alerts are configured, so the History
    tab always shows the 1st / 2nd / 3rd ... new-low bounces for the day."""
    now = datetime.now(ET)
    if session_label(now) not in ALERT_SESSIONS:
        return
    day = now.strftime("%Y-%m-%d")
    ts = now.strftime("%H:%M ET")
    for uid in all_user_ids():
        for s in get_watchlist(uid):
            with _qlock:
                row = _quotes.get(s)
            if not (row and row.get("price") is not None):
                continue
            num = row.get("alarm_num") or 0
            if num <= 0:
                continue
            already = max_logged_alarm(uid, s, day)
            # Backfill any alarm numbers we haven't logged yet (usually just one).
            for n in range(already + 1, num + 1):
                log_alarm_event(uid, s, day, n, ts, row.get("price"),
                                row.get("from_low"), row.get("change"))


def refresher():
    """Background loop.

    Only contacts the database while the market is in an alerting session
    (pre-market / regular hours). Outside those hours it sleeps WITHOUT making
    any DB calls, so Neon's free-tier compute can auto-suspend overnight, on
    weekends and on holidays. That idle time is what keeps usage under quota.
    """
    cached_syms = []
    last_syms_fetch = 0.0
    last_daily_save = None   # date string of the last persisted daily snapshot
    while True:
        now = datetime.now(ET)
        # Active only during the sessions we actually alert in. Otherwise stay
        # idle and make NO database calls, letting the DB endpoint suspend.
        if session_label(now) not in ALERT_SESSIONS:
            time.sleep(IDLE_SLEEP_SECONDS)
            continue
        try:
            # Re-read the watchlist from the DB occasionally, not every cycle.
            mono = time.monotonic()
            if not cached_syms or (mono - last_syms_fetch) > SYMS_REFRESH_SECONDS:
                try:
                    cached_syms = sorted(set(all_user_symbols()))
                    last_syms_fetch = mono
                except Exception:
                    pass
            refresh_symbols(cached_syms)
            try:
                record_history()      # in-memory only, no DB
            except Exception:
                pass
            # Persist the daily snapshot once near the close, not every minute.
            day = now.strftime("%Y-%m-%d")
            if (now.hour == DAILY_SAVE_HOUR and now.minute >= DAILY_SAVE_MINUTE
                    and last_daily_save != day):
                try:
                    record_daily_all()
                    last_daily_save = day
                except Exception:
                    pass
            try:
                record_alarms()
            except Exception:
                pass
            try:
                alert_check()
            except Exception as e:
                print(f"  (alert_check error: {e})", flush=True)
        except Exception:
            pass
        time.sleep(REFRESH_SECONDS)


def meta():
    now = datetime.now(ET)
    return {"as_of": now.strftime("%a %b %d, %H:%M:%S ET"),
            "date": now.strftime("%Y-%m-%d"),
            "market_open": market_open(now),
            "session": session_label(now),
            "rule": f"You get a new numbered alarm each time a stock bounces {RISE_PCT*100:.1f}%+ off a "
                    f"fresh intraday low. Alarm #1 can be a false alarm — if the stock keeps making "
                    f"lower lows you'll see #2, #3… and the bounce off the deepest low is the real signal.",
            "have_key": HAVE_DATA,
            "email_on": EMAIL_ON, "push_on": PUSH_ON}


MANIFEST = json.dumps({
    "name": "Stock Watch", "short_name": "Stock Watch", "start_url": "/",
    "display": "standalone", "background_color": "#f6f7f9", "theme_color": "#1d4ed8",
    "icons": [{"src": "/icon.png", "sizes": "192x192", "type": "image/png"},
              {"src": "/icon.png", "sizes": "512x512", "type": "image/png"}],
})

SW_JS = """self.addEventListener('push', function(e){
  var d={}; try{d=e.data.json();}catch(_){d={title:'Stock Watch', body:(e.data?e.data.text():'')};}
  e.waitUntil(self.registration.showNotification(d.title||'Stock Watch',
    {body:d.body||'', icon:'/icon.png', badge:'/icon.png', data:(d.url||'/')}));
});
self.addEventListener('notificationclick', function(e){
  e.notification.close();
  e.waitUntil(clients.matchAll({type:'window'}).then(function(cl){
    for(var i=0;i<cl.length;i++){ if('focus' in cl[i]) return cl[i].focus(); }
    if(clients.openWindow) return clients.openWindow(e.notification.data||'/');
  }));
});"""


SIM_JS = r"""/* ============================================================
   Stock Watch — Backtest simulator ("buy the lowest")
   Served at /sim.js. Depends on Chart.js (already loaded by the page)
   and on the app's existing /api/quotes and /api/hist/daily endpoints.
   All logic is client-side; nothing here touches the server data layer.
   ============================================================ */
(function () {
  "use strict";

  // ---- strategy palette (matches the app's colors) ----
  var STRATS = [
    { key: "oracle", name: "Perfect low",  color: "#1d4ed8", hint: "needs hindsight" },
    { key: "dip",    name: "Buy the dip",  color: "#16a34a", hint: "realistic rule" },
    { key: "hold",   name: "Buy & hold",   color: "#d97706", hint: "buy day one" },
    { key: "random", name: "Random entry", color: "#64748b", hint: "no skill" }
  ];
  var ALARM_COLOR = "#7c3aed";
  // Daily analog of the app's numbered-alarm rule. Mirrors the server defaults:
  //   RISE_PCT (bounce off the low)      = 0.5%
  //   NEW_LOW_MIN_DROP (fresh lower low) = 0.3%
  var RISE_PCT_D = 0.005, NEW_LOW_DROP_D = 0.003;

  // ---- seedable RNG (mulberry32) + normal via Marsaglia polar ----
  function mulberry32(a) {
    return function () {
      a |= 0; a = (a + 0x6D2B79F5) | 0;
      var t = Math.imul(a ^ (a >>> 15), 1 | a);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }
  function makeNormal(rng) {
    var spare = null;
    return function () {
      if (spare !== null) { var s = spare; spare = null; return s; }
      var u, v, q;
      do { u = rng() * 2 - 1; v = rng() * 2 - 1; q = u * u + v * v; } while (q >= 1 || q === 0);
      var m = Math.sqrt(-2 * Math.log(q) / q);
      spare = v * m; return u * m;
    };
  }

  // ---- formatting ----
  function pct(x) { return (x * 100).toFixed(1) + "%"; }
  function pctS(x) { return (x >= 0 ? "+" : "") + (x * 100).toFixed(1) + "%"; }
  function cls(x) { return x >= 0 ? "up" : "dn"; }

  // ---------------------------------------------------------
  //  MONTE CARLO ENGINE
  // ---------------------------------------------------------
  function simulateMC(p) {
    var rng = mulberry32(p.seed >>> 0);
    var norm = makeNormal(rng);
    var dt = 1 / 252;
    var drift = (p.mu - 0.5 * p.sigma * p.sigma) * dt;
    var vol = p.sigma * Math.sqrt(dt);

    var returns = { oracle: [], dip: [], hold: [], random: [] };
    var dipTriggered = 0, dipBeatHold = 0, sample = null;

    for (var r = 0; r < p.runs; r++) {
      var n = p.days + 1;
      var path = new Float64Array(n);
      path[0] = 100;
      for (var t = 1; t < n; t++) path[t] = path[t - 1] * Math.exp(drift + vol * norm());

      var exit = path[n - 1], start = path[0];

      var minP = Infinity, minIdx = 0;
      for (t = 0; t < n - 1; t++) { if (path[t] < minP) { minP = path[t]; minIdx = t; } }

      var runMax = path[0], dipIdx = -1;
      for (t = 0; t < n - 1; t++) {
        if (path[t] > runMax) runMax = path[t];
        if (path[t] <= runMax * (1 - p.dip)) { dipIdx = t; break; }
      }
      var dipEntry = dipIdx >= 0 ? path[dipIdx] : start;
      if (dipIdx >= 0) dipTriggered++;

      var randIdx = Math.floor(rng() * (n - 1));
      var randEntry = path[randIdx];

      var rH = exit / start - 1, rD = exit / dipEntry - 1;
      returns.oracle.push(exit / minP - 1);
      returns.dip.push(rD);
      returns.hold.push(rH);
      returns.random.push(exit / randEntry - 1);
      if (rD > rH) dipBeatHold++;

      if (r === 0) sample = { path: path, minIdx: minIdx, dipIdx: dipIdx, randIdx: randIdx };
    }

    var stats = {};
    STRATS.forEach(function (s) {
      var arr = returns[s.key].slice().sort(function (a, b) { return a - b; });
      var m = arr.length;
      stats[s.key] = {
        mean: arr.reduce(function (x, y) { return x + y; }, 0) / m,
        median: arr[Math.floor(m / 2)],
        win: returns[s.key].filter(function (x) { return x > 0; }).length / m,
        p95: arr[Math.floor(m * 0.95)],
        p05: arr[Math.floor(m * 0.05)]
      };
    });

    return {
      mode: "mc", returns: returns, stats: stats, sample: sample,
      dipTriggerRate: dipTriggered / p.runs, dipBeatHoldRate: dipBeatHold / p.runs
    };
  }

  // ---------------------------------------------------------
  //  APP ALARM RULE — daily analog of the numbered-alarm model
  // ---------------------------------------------------------
  // A new numbered alarm fires each day the stock (a) bounces >= RISE off that
  // day's low [close is RISE above the low] AND (b) that low is a fresh new low
  // at least DROP below the low that armed the previous alarm. This mirrors the
  // server's _update_alarm(), applied to daily bars instead of intraday ticks.
  function computeAlarms(points, rise, drop) {
    // The first saved day sets the baseline reference low (no alarm — on daily
    // bars almost every day closes >RISE above its own low, so firing on day one
    // would be meaningless). After that, an alarm fires on a day that prints a
    // fresh lower low (>= DROP below the last armed low) AND closes >= RISE above
    // that day's low. Each fire arms the reference at the new, lower low — so the
    // numbers climb only as the stock is driven to genuinely deeper lows.
    var armed = null, count = 0, out = [];
    for (var i = 0; i < points.length; i++) {
      var low = points[i].low, close = points[i].close;
      if (low == null || close == null || low <= 0) continue;
      if (armed === null) { armed = low; continue; }             // baseline day
      var bounced = ((close - low) / low) >= rise;               // condition 1
      if (bounced && low <= armed * (1 - drop)) {                // fresh lower low
        count++; armed = low;
        out.push({ num: count, idx: i, close: close, low: low, fromLow: (close - low) / low });
      }
    }
    return out;
  }

  // ---------------------------------------------------------
  //  REAL-HISTORY ENGINE  (single actual price series)
  // ---------------------------------------------------------
  function backtestReal(points, dipPct, alarmN) {
    // points: [{d, low, close}], oldest -> newest
    var closes = points.map(function (p) { return p.close; });
    var n = closes.length;
    var exit = closes[n - 1], start = closes[0];

    var minP = Infinity, minIdx = 0;
    for (var i = 0; i < n - 1; i++) { if (closes[i] < minP) { minP = closes[i]; minIdx = i; } }

    var runMax = closes[0], dipIdx = -1;
    for (i = 0; i < n - 1; i++) {
      if (closes[i] > runMax) runMax = closes[i];
      if (closes[i] <= runMax * (1 - dipPct)) { dipIdx = i; break; }
    }
    var dipEntry = dipIdx >= 0 ? closes[dipIdx] : start;

    // "random" benchmark on one path = average over every possible entry day
    var sumRand = 0;
    for (i = 0; i < n - 1; i++) sumRand += exit / closes[i] - 1;
    var avgRandom = sumRand / (n - 1);

    // the app's own signal
    var alarms = computeAlarms(points, RISE_PCT_D, NEW_LOW_DROP_D);
    var alarmReturns = alarms.map(function (a) {
      return { num: a.num, idx: a.idx, d: points[a.idx].d, close: a.close, ret: exit / a.close - 1 };
    });
    var pick = alarms[(alarmN || 1) - 1] || null;

    var res = {
      oracle: { ret: exit / minP - 1, idx: minIdx, entry: minP },
      dip: { ret: exit / dipEntry - 1, idx: dipIdx, entry: dipEntry, triggered: dipIdx >= 0 },
      hold: { ret: exit / start - 1, idx: 0, entry: start },
      random: { ret: avgRandom, idx: -1, entry: null },
      alarm: pick
        ? { ret: exit / pick.close - 1, idx: pick.idx, entry: pick.close, num: alarmN, fired: alarms.length }
        : { ret: null, idx: -1, entry: null, num: alarmN, fired: alarms.length }
    };
    return { mode: "real", n: n, exit: exit, closes: closes, points: points,
      res: res, alarms: alarms, alarmReturns: alarmReturns };
  }

  // ---------------------------------------------------------
  //  RENDERING
  // ---------------------------------------------------------
  var pathChart = null, distChart = null, realChart = null;
  var lastMC = null;

  function tile(name, color, valHtml, meta) {
    return '<div class="sim-tile"><div class="sim-tname"><span class="sim-dot" style="background:' +
      color + '"></span>' + name + '</div><div class="sim-tval ' + (valHtml.indexOf("-") === 0 ? "dn" : "up") +
      '">' + valHtml + '</div><div class="sim-tmeta">' + meta + '</div></div>';
  }

  function renderMCTiles(res) {
    var html = STRATS.map(function (s) {
      var st = res.stats[s.key];
      return tile(s.name, s.color, pctS(st.mean), s.hint + " · wins " + pct(st.win) + " of runs");
    }).join("");
    document.getElementById("sim-tiles").innerHTML = html;
  }

  function renderMCTable(res) {
    var body = STRATS.map(function (s) {
      var st = res.stats[s.key];
      return '<div class="sim-trow"><span><span class="sim-dot" style="background:' + s.color + '"></span>' +
        s.name + '</span><span class="' + cls(st.mean) + '">' + pctS(st.mean) + '</span><span class="' +
        cls(st.median) + '">' + pctS(st.median) + '</span><span>' + pct(st.win) + '</span><span class="up">' +
        pctS(st.p95) + '</span><span class="dn">' + pctS(st.p05) + '</span></div>';
    }).join("");
    document.getElementById("sim-table").innerHTML =
      '<div class="sim-trow sim-thead"><span>Strategy</span><span>Avg</span><span>Median</span>' +
      '<span>Win rate</span><span>Best 5%</span><span>Worst 5%</span></div>' + body;
  }

  function renderMCPath(res) {
    var s = res.sample, path = s.path, pts = [];
    for (var i = 0; i < path.length; i++) pts.push({ x: i, y: path[i] });
    var marks = [
      { idx: s.minIdx, color: STRATS[0].color, label: "Perfect low" },
      { idx: s.dipIdx, color: STRATS[1].color, label: "Buy the dip" },
      { idx: 0, color: STRATS[2].color, label: "Buy & hold" },
      { idx: s.randIdx, color: STRATS[3].color, label: "Random" }
    ].filter(function (m) { return m.idx >= 0; });

    var ds = [{ type: "line", label: "Price", data: pts, borderColor: "#94a3b8",
      borderWidth: 1.6, pointRadius: 0, tension: 0.15, order: 2 }];
    marks.forEach(function (m) {
      ds.push({ type: "scatter", label: m.label,
        data: [{ x: m.idx, y: path[m.idx] }],
        backgroundColor: m.color, borderColor: "#fff", borderWidth: 2,
        pointRadius: 6, pointHoverRadius: 8, order: 1 });
    });

    if (pathChart) pathChart.destroy();
    pathChart = new Chart(document.getElementById("sim-path"), {
      data: { datasets: ds },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: true, labels: { boxWidth: 10, font: { size: 11 } } },
          tooltip: {
            callbacks: {
              label: function (c) {
                if (c.dataset.type === "scatter")
                  return c.dataset.label + ": day " + c.parsed.x + " · $" + c.parsed.y.toFixed(2);
                return "day " + c.parsed.x + " · $" + c.parsed.y.toFixed(2);
              }
            }
          }
        },
        scales: {
          x: { type: "linear", title: { display: true, text: "day", font: { size: 10 } },
            ticks: { font: { size: 10 }, maxTicksLimit: 8 } },
          y: { title: { display: true, text: "price ($)", font: { size: 10 } },
            ticks: { font: { size: 10 } } }
        }
      }
    });
  }

  function renderMCDist(res) {
    var all = [];
    STRATS.forEach(function (s) { all = all.concat(res.returns[s.key]); });
    all.sort(function (a, b) { return a - b; });
    var lo = all[Math.floor(all.length * 0.01)], hi = all[Math.floor(all.length * 0.99)];
    var nBins = 32, binW = (hi - lo) / nBins;

    var labels = [];
    for (var b = 0; b < nBins; b++) labels.push(((lo + (b + 0.5) * binW) * 100));

    var ds = STRATS.map(function (s) {
      var counts = new Array(nBins).fill(0);
      res.returns[s.key].forEach(function (v) {
        var bi = Math.floor((v - lo) / binW);
        if (bi < 0) bi = 0; if (bi >= nBins) bi = nBins - 1;
        counts[bi]++;
      });
      return { label: s.name, data: counts, borderColor: s.color, backgroundColor: "transparent",
        borderWidth: 2, pointRadius: 0, stepped: true, tension: 0 };
    });

    if (distChart) distChart.destroy();
    distChart = new Chart(document.getElementById("sim-dist"), {
      type: "line",
      data: { labels: labels, datasets: ds },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { display: true, labels: { boxWidth: 10, font: { size: 11 } } },
          tooltip: {
            callbacks: {
              title: function (items) { return "return ~ " + items[0].label.slice(0, 6) + "%"; },
              label: function (c) { return c.dataset.label + ": " + c.parsed.y + " runs"; }
            }
          }
        },
        scales: {
          x: { title: { display: true, text: "final return (%)", font: { size: 10 } },
            ticks: { font: { size: 10 }, maxTicksLimit: 9,
              callback: function (v) { return Math.round(this.getLabelForValue(v)) + "%"; } } },
          y: { title: { display: true, text: "runs", font: { size: 10 } }, ticks: { font: { size: 10 } } }
        }
      }
    });
  }

  function renderMCTakeaway(res) {
    var so = res.stats.oracle.mean, sd = res.stats.dip.mean, sh = res.stats.hold.mean;
    var timing = so - sh;
    var captured = timing !== 0 ? (sd - sh) / timing : 0;
    var dipVsHold = sd - sh;
    var edge = dipVsHold >= 0
      ? 'beat plain buy-&-hold by <span class="up">' + pctS(dipVsHold) + '</span> on average'
      : 'actually <span class="dn">trailed</span> buy-&-hold by ' + pct(Math.abs(dipVsHold)) + ' on average';
    document.getElementById("sim-takeaway").innerHTML =
      'Buying the <b>perfect low</b> returned <b>' + pctS(so) + '</b> vs <b>' + pctS(sh) +
      '</b> for just holding — so flawless timing was worth about <b>' + pctS(timing) +
      '</b> of extra return here. But that needs hindsight. The realistic <b>buy-the-dip</b> rule ' + edge +
      ', capturing roughly <b>' + (captured * 100).toFixed(0) + '%</b> of what perfect timing offered, and it beat holding in <b>' +
      pct(res.dipBeatHoldRate) + '</b> of runs. The dip trigger fired at all in <b>' + pct(res.dipTriggerRate) +
      '</b> of runs. Dip-buying tends to shine in <b>choppy, sideways</b> markets and to cost you in <b>strong steady uptrends</b>.';
  }

  function runMC() {
    var g = function (id) { return +document.getElementById(id).value; };
    var p = { runs: g("sim-runs"), days: g("sim-days"), mu: g("sim-mu") / 100,
      sigma: g("sim-sigma") / 100, dip: g("sim-dip") / 100, seed: g("sim-seed") };
    var note = document.getElementById("sim-note");
    note.textContent = "Running " + p.runs.toLocaleString() + " paths…";
    setTimeout(function () {
      var t0 = performance.now();
      lastMC = simulateMC(p);
      renderMCTiles(lastMC); renderMCPath(lastMC); renderMCDist(lastMC);
      renderMCTable(lastMC); renderMCTakeaway(lastMC);
      note.textContent = "Done — " + p.runs.toLocaleString() + " paths in " +
        Math.round(performance.now() - t0) + " ms.";
    }, 20);
  }

  // ---- real history ----
  function renderReal(bt, sym) {
    var r = bt.res, note = document.getElementById("sim-real-note");
    note.innerHTML = sym + " · " + bt.n + " saved trading days · exit at last close $" + bt.exit.toFixed(2) +
      " · " + bt.alarms.length + " app alarm" + (bt.alarms.length === 1 ? "" : "s") + " fired";

    // tiles (incl. the app's own alarm signal)
    var order = [["oracle", "Perfect low", STRATS[0].color], ["dip", "Buy the dip", STRATS[1].color],
      ["hold", "Buy & hold", STRATS[2].color], ["random", "Avg random day", STRATS[3].color]];
    var html = order.map(function (o) {
      var d = r[o[0]];
      var meta = o[0] === "dip" ? (d.triggered ? "bought a real dip" : "never dipped — held") :
        (o[0] === "oracle" ? "the true bottom" : (o[0] === "random" ? "every entry averaged" : "first saved day"));
      return tile(o[1], o[2], pctS(d.ret), meta);
    }).join("");
    var a = r.alarm;
    var alarmVal = (a.ret == null) ? "—" : pctS(a.ret);
    var alarmMeta = (a.ret == null)
      ? "alarm #" + a.num + " never fired (" + a.fired + " total)"
      : "bought your alarm #" + a.num + " signal";
    html += tile("App alarm #" + a.num, ALARM_COLOR, alarmVal, alarmMeta);
    document.getElementById("sim-real-tiles").innerHTML = html;

    // price chart with entry markers + every alarm firing
    var closes = bt.closes, pts = closes.map(function (c, i) { return { x: i, y: c }; });
    var ds = [{ type: "line", label: sym, data: pts, borderColor: "#94a3b8",
      borderWidth: 1.6, pointRadius: 0, tension: 0.1, order: 3 }];
    // faint markers for all alarm firings
    if (bt.alarms.length) {
      ds.push({ type: "scatter", label: "Alarms", order: 2,
        data: bt.alarms.map(function (al) { return { x: al.idx, y: closes[al.idx] }; }),
        backgroundColor: "rgba(124,58,237,0.28)", borderColor: ALARM_COLOR, borderWidth: 1,
        pointRadius: 4, pointHoverRadius: 6 });
    }
    var marks = [["oracle", "Perfect low", STRATS[0].color], ["dip", "Buy the dip", STRATS[1].color],
      ["hold", "Buy & hold", STRATS[2].color]];
    if (a.idx >= 0) marks.push(["alarm", "Bought alarm #" + a.num, ALARM_COLOR]);
    marks.forEach(function (o) {
      var d = r[o[0]];
      if (!d || d.idx < 0) return;
      ds.push({ type: "scatter", label: o[1], data: [{ x: d.idx, y: closes[d.idx] }],
        backgroundColor: o[2], borderColor: "#fff", borderWidth: 2, pointRadius: 6, pointHoverRadius: 8, order: 1 });
    });

    if (realChart) realChart.destroy();
    realChart = new Chart(document.getElementById("sim-real-chart"), {
      data: { datasets: ds },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { labels: { boxWidth: 10, font: { size: 11 } } },
          tooltip: { callbacks: { label: function (c) {
            var base = c.dataset.type === "scatter" ? c.dataset.label + ": " : "";
            return base + "day " + c.parsed.x + " · $" + c.parsed.y.toFixed(2); } } } },
        scales: { x: { type: "linear", title: { display: true, text: "trading day (oldest → newest)", font: { size: 10 } },
            ticks: { font: { size: 10 }, maxTicksLimit: 8 } },
          y: { title: { display: true, text: "close ($)", font: { size: 10 } }, ticks: { font: { size: 10 } } } }
      }
    });

    // per-alarm breakdown — does buying a DEEPER (higher-numbered) alarm pay off?
    var box = document.getElementById("sim-real-alarms");
    if (!bt.alarmReturns.length) {
      box.innerHTML = '<div class="muted" style="font-size:13px">No alarms fired on ' + sym +
        "'s saved history — it never bounced 0.5%+ off a fresh lower low. Deeper history or a choppier stock will surface some.</div>";
    } else {
      var head = '<div class="sim-trow sim-thead"><span>Alarm</span><span>Bought (date)</span>' +
        '<span>Entry $</span><span>Return to now</span></div>';
      var rows = bt.alarmReturns.map(function (ar) {
        var sel = (ar.num === a.num) ? ' style="background:#f5f3ff"' : "";
        return '<div class="sim-trow sim-arow"' + sel + '><span><span class="sim-dot" style="background:' +
          ALARM_COLOR + '"></span>#' + ar.num + '</span><span>' + (ar.d || "day " + ar.idx) + '</span><span>$' +
          ar.close.toFixed(2) + '</span><span class="' + cls(ar.ret) + '">' + pctS(ar.ret) + '</span></div>';
      }).join("");
      box.innerHTML = '<div class="sim-table sim-atable">' + head + rows + '</div>';
    }

    // takeaway
    var best = r.oracle.ret, held = r.hold.ret, dipR = r.dip.ret;
    var gap = best - held;
    var capt = gap !== 0 ? (dipR - held) / gap : 0;
    var txt = 'On ' + sym + "'s real saved history, nailing the exact bottom would have returned <b>" + pctS(best) +
      '</b> vs <b>' + pctS(held) + '</b> for buying the first day. ' +
      (r.dip.triggered
        ? 'The buy-the-dip rule ' + (dipR >= held ? 'added ' : 'gave up ') + pctS(Math.abs(dipR - held)) +
          ' vs holding, capturing about <b>' + (capt * 100).toFixed(0) + '%</b> of the perfect-timing gap. '
        : 'The stock never fell far enough to trigger the dip rule, so it fell back to buying the first day. ');
    // grade the app's own signal
    if (r.alarm.ret != null) {
      txt += 'Your app’s <b>alarm #' + a.num + '</b> signal would have returned <b>' + pctS(r.alarm.ret) +
        '</b> — ' + (r.alarm.ret >= held ? '<span class="up">' + pctS(r.alarm.ret - held) + ' better</span>'
          : '<span class="dn">' + pctS(r.alarm.ret - held) + '</span>') + ' than just holding. ';
      // deepest vs first, to test the "deeper low = truer signal" thesis
      if (bt.alarmReturns.length >= 2) {
        var first = bt.alarmReturns[0], deepest = bt.alarmReturns[bt.alarmReturns.length - 1];
        txt += 'Testing the “deeper low is the real signal” idea: alarm #' + first.num + ' returned ' +
          pctS(first.ret) + ' while the deepest (#' + deepest.num + ') returned ' + pctS(deepest.ret) + ' — ' +
          (deepest.ret > first.ret ? 'the deeper alarm did pay off here.' : 'the deeper alarm did not beat the first one here.');
        var bestA = bt.alarmReturns.slice().sort(function (x, y) { return y.ret - x.ret; })[0];
        txt += ' With full hindsight, buying <b>alarm #' + bestA.num + '</b> was the best of the ' +
          bt.alarmReturns.length + ' fired — it returned <b>' + pctS(bestA.ret) + '</b>.';
      }
    } else {
      txt += 'Your app’s alarm #' + a.num + ' never fired on this history (' + a.fired + ' alarm' +
        (a.fired === 1 ? '' : 's') + ' total), so there was nothing to buy on that signal.';
    }
    document.getElementById("sim-real-takeaway").innerHTML = txt;
  }

  function runReal() {
    var sel = document.getElementById("sim-real-sym");
    var sym = sel.value;
    var note = document.getElementById("sim-real-note");
    var dip = (+document.getElementById("sim-real-dip").value) / 100;
    var alarmN = +document.getElementById("sim-real-alarm").value;
    if (!sym) { note.textContent = "Add stocks to your watchlist (and let some daily history build up) to backtest real data."; return; }
    note.textContent = "Loading " + sym + " history…";
    fetch("/api/hist/daily?symbol=" + encodeURIComponent(sym), { cache: "no-store" })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        var pts = (d.points || []).filter(function (p) { return p.close != null; });
        if (pts.length < 5) {
          note.innerHTML = '<b>Not enough saved history for ' + sym + ' yet.</b> Click <b>⬇ Load full history</b> to pull ~1 year of real daily bars from Alpaca, or let it build up one close per day over time.';
          document.getElementById("sim-real-tiles").innerHTML = "";
          document.getElementById("sim-real-alarms").innerHTML = "";
          document.getElementById("sim-real-takeaway").innerHTML = "";
          if (realChart) { realChart.destroy(); realChart = null; }
          return;
        }
        renderReal(backtestReal(pts, dip, alarmN), sym);
      })
      .catch(function () { note.textContent = "Could not load history for " + sym + "."; });
  }

  // Pull real daily bars from Alpaca into saved history, then backtest.
  function runBackfill() {
    var sym = document.getElementById("sim-real-sym").value;
    var note = document.getElementById("sim-real-note");
    if (!sym) { note.textContent = "Pick a stock first (add one to your watchlist)."; return; }
    var btn = document.getElementById("sim-real-load");
    btn.disabled = true;
    note.textContent = "Pulling " + sym + " history from Alpaca…";
    fetch("/api/hist/backfill", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ symbol: sym }) })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        btn.disabled = false;
        if (d.error) { note.innerHTML = "<b>" + d.error + "</b>"; return; }
        note.textContent = "Loaded " + (d.saved || 0) + " days for " + sym + ". Backtesting…";
        runReal();
      })
      .catch(function () { btn.disabled = false; note.textContent = "Couldn’t reach Alpaca to load history."; });
  }

  function refreshRealSymbols() {
    var sel = document.getElementById("sim-real-sym");
    if (!sel) return;
    var syms = (window.LAST && window.LAST.mine ? window.LAST.mine : []).map(function (t) { return t.ticker; });
    var prev = sel.value;
    sel.innerHTML = syms.length
      ? syms.map(function (s) { return '<option value="' + s + '">' + s + '</option>'; }).join("")
      : '<option value="">(no stocks in watchlist)</option>';
    if (prev && syms.indexOf(prev) >= 0) sel.value = prev;
  }

  // ---------------------------------------------------------
  //  UI SCAFFOLD (built once into #sim-root)
  // ---------------------------------------------------------
  function styleTag() {
    var css =
      "#sim-root{max-width:960px}" +
      ".sim-seg{display:inline-flex;border:1px solid #d1d5db;border-radius:9px;overflow:hidden;margin:2px 0 12px}" +
      ".sim-seg button{border:none;border-radius:0;background:#fff;padding:7px 14px;font-weight:600;color:#374151}" +
      ".sim-seg button.on{background:#1d4ed8;color:#fff}" +
      ".sim-card{background:#fff;border:1px solid #e7e9ee;border-radius:12px;padding:14px 16px;margin-bottom:14px}" +
      ".sim-controls{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px 20px}" +
      ".sim-ctrl label{display:flex;justify-content:space-between;font-size:12px;color:#374151;margin-bottom:5px}" +
      ".sim-ctrl label b{color:#16181d}" +
      ".sim-ctrl input[type=range]{width:100%;accent-color:#1d4ed8}" +
      ".sim-tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px}" +
      ".sim-tile{border:1px solid #e7e9ee;border-radius:10px;padding:10px 12px}" +
      ".sim-tname{font-size:12px;color:#374151;display:flex;align-items:center;gap:6px}" +
      ".sim-dot{width:9px;height:9px;border-radius:3px;display:inline-block;flex:none}" +
      ".sim-tval{font-size:23px;font-weight:700;margin-top:5px}" +
      ".sim-tmeta{font-size:11px;color:#9ca3af;margin-top:1px}" +
      ".sim-chartbox{height:250px}" +
      ".sim-grid2{display:grid;grid-template-columns:1fr 1fr;gap:16px}" +
      "@media(max-width:760px){.sim-grid2{grid-template-columns:1fr}}" +
      ".sim-table{font-size:13px}" +
      ".sim-trow{display:grid;grid-template-columns:1.6fr 1fr 1fr 1fr 1fr 1fr;gap:6px;padding:7px 4px;border-bottom:1px solid #f1f1f1}" +
      ".sim-trow span:not(:first-child){text-align:right;font-variant-numeric:tabular-nums}" +
      ".sim-atable .sim-trow{grid-template-columns:1fr 1.4fr 1fr 1fr}" +
      ".sim-atable .sim-arow span:nth-child(2){text-align:right;color:#374151}" +
      ".sim-thead span{color:#9ca3af;font-weight:700;font-size:11px}" +
      ".sim-take{font-size:14px;line-height:1.6}" +
      ".sim-sub{font-size:12px;color:#374151;margin-bottom:6px}" +
      ".sim-h{font-size:13px;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:.04em;margin:0 0 10px}";
    var el = document.createElement("style");
    el.textContent = css;
    return el;
  }

  function slider(id, label, min, max, step, val, fmt) {
    return '<div class="sim-ctrl"><label>' + label + ' <b id="' + id + '-v">' + fmt(val) +
      '</b></label><input type="range" id="' + id + '" min="' + min + '" max="' + max +
      '" step="' + step + '" value="' + val + '"></div>';
  }

  function buildUI(root) {
    root.appendChild(styleTag());

    var mc =
      '<div id="sim-mc">' +
      '<div class="sim-card"><div class="sim-h">Market &amp; strategy settings</div>' +
      '<div class="sim-controls">' +
      slider("sim-runs", "Simulated runs", 200, 10000, 200, 2000, function (v) { return (+v).toLocaleString(); }) +
      slider("sim-days", "Days per run", 30, 756, 6, 252, function (v) { return v; }) +
      slider("sim-mu", "Annual drift", -20, 30, 1, 8, function (v) { return v + "%"; }) +
      slider("sim-sigma", "Annual volatility", 5, 80, 1, 30, function (v) { return v + "%"; }) +
      slider("sim-dip", "Dip trigger (buy on drop of)", 2, 40, 1, 10, function (v) { return v + "%"; }) +
      slider("sim-seed", "Random seed", 1, 999, 1, 42, function (v) { return v; }) +
      '</div><div style="margin-top:14px"><button class="primary" id="sim-run">Run simulation</button> ' +
      '<span class="muted" id="sim-note" style="font-size:12px">Ready.</span></div></div>' +
      '<div class="sim-card"><div class="sim-h">Average return per strategy · hold to end</div>' +
      '<div class="sim-tiles" id="sim-tiles"></div></div>' +
      '<div class="sim-card"><div class="sim-grid2">' +
      '<div><div class="sim-sub">One example path &amp; where each strategy buys</div><div class="sim-chartbox"><canvas id="sim-path"></canvas></div></div>' +
      '<div><div class="sim-sub">Distribution of final returns across all runs</div><div class="sim-chartbox"><canvas id="sim-dist"></canvas></div></div>' +
      '</div></div>' +
      '<div class="sim-card"><div class="sim-h">Full results</div><div class="sim-table" id="sim-table"></div></div>' +
      '<div class="sim-card"><div class="sim-h">The takeaway</div><div class="sim-take" id="sim-takeaway"></div></div>' +
      '</div>';

    var real =
      '<div id="sim-real" style="display:none">' +
      '<div class="sim-card"><div class="sim-h">Backtest on your saved daily history</div>' +
      '<div class="bar"><label style="font-size:13px">Stock:&nbsp;</label>' +
      '<select id="sim-real-sym" style="min-width:120px"></select>' +
      '<label style="font-size:13px;margin-left:10px">Dip trigger:</label>' +
      '<select id="sim-real-dip"><option value="5">5%</option><option value="10" selected>10%</option>' +
      '<option value="15">15%</option><option value="20">20%</option></select>' +
      '<label style="font-size:13px;margin-left:10px">Buy on alarm #:</label>' +
      '<select id="sim-real-alarm"><option value="1" selected>1</option><option value="2">2</option>' +
      '<option value="3">3</option><option value="4">4</option><option value="5">5</option></select>' +
      '<button id="sim-real-load" title="Pull ~1 year of real daily bars from Alpaca into your saved history">⬇ Load full history</button>' +
      '<button class="primary" id="sim-real-run">Backtest</button></div>' +
      '<div class="muted" id="sim-real-note" style="font-size:12px;margin-top:8px"></div></div>' +
      '<div class="sim-card"><div class="sim-h">Return per strategy</div><div class="sim-tiles" id="sim-real-tiles"></div></div>' +
      '<div class="sim-card"><div class="sim-sub">Saved daily closes &amp; where each strategy buys · faint dots = every app alarm firing</div>' +
      '<div class="sim-chartbox"><canvas id="sim-real-chart"></canvas></div></div>' +
      '<div class="sim-card"><div class="sim-h">App alarm signal · per-alarm returns</div>' +
      '<div class="sim-sub">Each numbered alarm your rule fired, and what buying it would have returned to the latest close. The highlighted row is the alarm # selected above.</div>' +
      '<div id="sim-real-alarms"></div></div>' +
      '<div class="sim-card"><div class="sim-h">The takeaway</div><div class="sim-take" id="sim-real-takeaway"></div></div>' +
      '</div>';

    var wrap = document.createElement("div");
    wrap.innerHTML =
      '<h2>🧪 SimuWatch — "buy the lowest"</h2>' +
      '<div class="rule">Catching a stock’s exact bottom needs hindsight. This asks the real question: how much does perfect timing pay, and how much can a rule you could actually follow capture? Try it on simulated markets, or on the daily history this app has saved.</div>' +
      '<div class="sim-seg"><button id="sim-tab-mc" class="on">Simulated</button><button id="sim-tab-real">Real history</button></div>' +
      mc + real;
    root.appendChild(wrap);

    document.getElementById("sim-run").addEventListener("click", runMC);
    document.getElementById("sim-real-run").addEventListener("click", runReal);
    document.getElementById("sim-real-load").addEventListener("click", runBackfill);
    ["sim-runs", "sim-days", "sim-mu", "sim-sigma", "sim-dip", "sim-seed"].forEach(function (id) {
      var inp = document.getElementById(id), lab = document.getElementById(id + "-v");
      var fmt = (id === "sim-runs") ? function (v) { return (+v).toLocaleString(); }
        : (id === "sim-mu" || id === "sim-sigma" || id === "sim-dip") ? function (v) { return v + "%"; }
        : function (v) { return v; };
      inp.addEventListener("input", function () { lab.textContent = fmt(inp.value); });
    });
    document.getElementById("sim-tab-mc").addEventListener("click", function () { showSub("mc"); });
    document.getElementById("sim-tab-real").addEventListener("click", function () { showSub("real"); });
  }

  function showSub(which) {
    document.getElementById("sim-mc").style.display = which === "mc" ? "block" : "none";
    document.getElementById("sim-real").style.display = which === "real" ? "block" : "none";
    document.getElementById("sim-tab-mc").classList.toggle("on", which === "mc");
    document.getElementById("sim-tab-real").classList.toggle("on", which === "real");
    if (which === "real") {
      refreshRealSymbols();
      var rn = document.getElementById("sim-real-note");
      if (rn && !document.getElementById("sim-real-tiles").children.length)
        rn.innerHTML = 'Pick a stock and click <b>Backtest</b> to replay the four strategies on the real daily closes this app has saved. History builds up one close per day, so the more days saved, the richer this gets.';
    }
  }

  var booted = false;
  window.initSim = function () {
    var root = document.getElementById("sim-root");
    if (!root) return;
    if (!booted) { buildUI(root); booted = true; runMC(); }
    refreshRealSymbols();
  };

  // expose engines for testing / reuse
  window.StockSim = { simulateMC: simulateMC, backtestReal: backtestReal };
})();
"""


def icon_bytes():
    try:
        with open(ICON_PATH, "rb") as f:
            return f.read()
    except Exception:
        return _FALLBACK_ICON


# =========================== WEB PAGE ===========================
PAGE = """<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Stock Watch</title>
<link rel="manifest" href="/manifest.webmanifest">
<link rel="apple-touch-icon" href="/icon.png">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-title" content="Stock Watch">
<meta name="theme-color" content="#1d4ed8">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root{color-scheme:light}*{box-sizing:border-box}
body{margin:0;padding:18px;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;background:#f6f7f9;color:#16181d}
h1{font-size:20px;margin:0 0 2px}h2{font-size:15px;margin:16px 0 8px}
.meta{color:#374151;font-size:12px}.rule{color:#374151;font-size:12px;margin:2px 0 10px}
.bar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:10px}
.addtop{background:#eef3fb;border:1px solid #cfe0f5;border-radius:10px;padding:10px;margin-bottom:12px}
button{font:inherit;font-size:13px;padding:7px 12px;border-radius:8px;border:1px solid #d1d5db;background:#fff;cursor:pointer}
button:hover{background:#f3f4f6}.primary{background:#1d4ed8;color:#fff;border-color:#1d4ed8}.primary:hover{background:#1e40af}
input{font:inherit;font-size:13px;padding:7px 10px;border:1px solid #d1d5db;border-radius:8px}
.open{background:#ecfdf5;color:#047857;border:1px solid #a7f3d0;padding:5px 11px;border-radius:999px;font-size:12px}
.closed{background:#fef2f2;color:#b91c1c;border:1px solid #fecaca;padding:5px 11px;border-radius:999px;font-size:12px}
.warn{background:#fffbeb;color:#92400e;border:1px solid #fde68a;padding:10px 12px;border-radius:8px;font-size:13px;margin-bottom:12px}
.card-auth{background:#fff;border:1px solid #e7e9ee;border-radius:12px;padding:16px;max-width:340px;margin-bottom:14px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(155px,1fr));gap:10px}
.card{background:#fff;border:1px solid #e7e9ee;border-left:4px solid #cbd5e1;border-radius:10px;padding:10px 12px;position:relative;cursor:pointer}
.card:hover{border-color:#c7ccd6}.card.near{border-left-color:#16a34a;background:#f0fdf4}
.tk{font-weight:700;font-size:15px}.pr{float:right;font-weight:700}
.sig{margin-left:8px;font-size:11px;font-weight:700;padding:1px 7px;border-radius:999px;vertical-align:middle}
.alrm{margin-left:6px;font-size:11px;font-weight:800;padding:1px 7px;border-radius:999px;vertical-align:middle;background:#fef3c7;color:#92400e;border:1px solid #fcd34d}
.alrm.deep{background:#fee2e2;color:#991b1b;border-color:#fca5a5}
.row{font-size:12px;color:#16181d;margin-top:3px;font-weight:600}
.card .muted{color:#1f2430;font-weight:700}
.up{color:#16a34a;font-weight:700}.dn{color:#dc2626;font-weight:700}
.muted{color:#9ca3af}.foot{color:#16181d;font-weight:600;font-size:12px;margin-top:18px}
.x{position:absolute;top:6px;right:8px;cursor:pointer;color:#9ca3af;font-size:14px;border:none;background:none;padding:2px 5px}
.x:hover{color:#dc2626}#msg,#authmsg{font-size:12px;color:#b91c1c}
.who{font-size:12px;color:#374151;display:flex;gap:10px;align-items:center;flex-wrap:wrap}
#overlay{position:fixed;inset:0;background:rgba(15,18,25,.45);display:none;align-items:center;justify-content:center;padding:16px;z-index:50}
#modal{background:#fff;border-radius:14px;max-width:440px;width:100%;padding:18px;position:relative}
.stat{display:flex;justify-content:space-between;font-size:13px;padding:5px 0;border-bottom:1px solid #f1f1f1}
@keyframes blinkamber{0%,100%{background:#fff7ed;border-color:#f59e0b}50%{background:#fde68a;border-color:#b45309}}
.card.alerting{animation:blinkamber 1s ease-in-out 30}
.empty{font-size:14px;color:#16181d;font-weight:600;margin-top:8px}
.tabs{display:flex;gap:4px;margin:10px 0 14px;border-bottom:1px solid #e7e9ee}
.tab{border:none;background:none;border-radius:0;border-bottom:2px solid transparent;padding:8px 14px;color:#374151;font-weight:600}
.tab:hover{background:#f3f4f6}
.tab.active{color:#1d4ed8;border-bottom-color:#1d4ed8}
.histtbl .stat span{flex:1}
.histtbl .stat span:nth-child(2),.histtbl .stat span:nth-child(3){text-align:center}
.histtbl .stat span:nth-child(4),.histtbl .stat span:nth-child(5){text-align:right}
</style></head><body>
<h1>📈 Stock Watch</h1>
<div class="meta" id="asof">loading…</div>
<div class="rule" id="rule"></div>
<div id="warn"></div>
<div id="authbox"></div>

<div class="tabs">
  <button id="tab-watch" class="tab active" onclick="showTab('watch')">⭐ Watchlist</button>
  <button id="tab-hist" class="tab" onclick="showTab('history')">🕘 History</button>
  <button id="tab-sim" class="tab" onclick="showTab('sim')">🧪 SimuWatch</button>
</div>

<div id="view-watch">
<div id="addbar" class="bar addtop" style="display:none">
  <input id="addsym" placeholder="Add a stock (e.g. NFLX)" maxlength="10" onkeydown="if(event.key==='Enter')addSym()" style="flex:1;min-width:160px">
  <button class="primary" onclick="addSym()">Add</button>
</div>
<div class="bar">
  <span id="status"></span>
  <button onclick="load()">Refresh</button>
  <button onclick="copyList('pre')">Copy pre-market</button>
  <label style="font-size:13px"><input type="checkbox" id="sndtog" checked> 🔊 Sound</label>
  <button id="pushbtn" style="display:none" onclick="enablePush()">🔔 Enable phone alerts</button>
  <span id="msg"></span>
</div>

<h2>⭐ My Watchlist</h2>
<div class="grid" id="mygrid"></div>
<div id="signedout" class="empty" style="display:none">Sign in above to build your watchlist.</div>

<div class="foot">Green cards have bounced up from today's low. Tap any stock for details & today's chart. Data: Alpaca (IEX) — real-time.</div>
</div>

<div id="view-history" style="display:none">
  <h2>🔔 Alert history &nbsp;<button onclick="loadHistory()" style="font-weight:600;font-size:12px;padding:4px 9px">Refresh</button></h2>
  <div id="histalerts" class="empty">Loading…</div>
  <h2 style="margin-top:18px">📊 Price history</h2>
  <div class="bar"><label style="font-size:13px">Stock:&nbsp;</label><select id="histsym" onchange="loadDaily()" style="min-width:120px"></select></div>
  <div style="height:240px;margin-top:6px"><canvas id="hist_chart"></canvas></div>
  <div class="muted" id="hist_note" style="font-size:12px;margin-top:6px"></div>
</div>

<div id="view-sim" style="display:none">
  <div id="sim-root"></div>
</div>

<div id="overlay" onclick="if(event.target===this)closeDetail()">
  <div id="modal">
    <button class="x" style="font-size:18px" onclick="closeDetail()">✕</button>
    <div style="font-size:20px;font-weight:700" id="d_tk"></div>
    <div style="font-size:22px;margin:4px 0 12px" id="d_price"></div>
    <div id="d_stats"></div>
    <div style="margin-top:14px;height:200px"><canvas id="d_chart"></canvas></div>
    <div class="muted" style="font-size:12px;margin-top:8px" id="d_note"></div>
  </div>
</div>
<script src="/sim.js"></script>
<script>
let LAST={mine:[]}, ME={logged_in:false}, _chart=null, prevAlarmNum={}, firstLoad=true, _curTab='watch', _histChart=null;
function pctSpan(v){if(v===null||v===undefined)return '<span class="muted">—</span>';var s=(v>=0?"+":"")+v.toFixed(2)+"%";return '<span class="'+(v>=0?'up':'dn')+'">'+s+'</span>';}
function money(v){return (v===null||v===undefined)?'<span class="muted">—</span>':'$'+v.toFixed(2);}
function beep(){try{var a=new (window.AudioContext||window.webkitAudioContext)();var o=a.createOscillator(),g=a.createGain();o.connect(g);g.connect(a.destination);o.type='sine';o.frequency.value=880;g.gain.setValueAtTime(0.0001,a.currentTime);g.gain.exponentialRampToValueAtTime(0.12,a.currentTime+0.02);g.gain.exponentialRampToValueAtTime(0.0001,a.currentTime+0.5);o.start();o.stop(a.currentTime+0.52);}catch(e){}}
function sigBadge(t){
 if(!t.signal)return '';
 var bg={'Good':'#fef3c7','Very Good':'#dbeafe','Excellent':'#dcfce7'}[t.signal]||'#eee';
 var fg={'Good':'#92400e','Very Good':'#1e40af','Excellent':'#166534'}[t.signal]||'#333';
 return '<span class="sig" style="background:'+bg+';color:'+fg+'">'+t.signal+'</span>';
}
function alarmBadge(t){
 var n=t.alarm_num||0; if(n<=0) return '';
 var deep=(n>=3)?' deep':'';
 return '<span class="alrm'+deep+'" title="'+n+' new-low bounce(s) today">🔔 #'+n+'</span>';
}
function card(t,blink){
 let cls='card';if(t.near)cls+=' near';if(blink)cls+=' alerting';
 const x='<button class="x" title="Remove" onclick="event.stopPropagation();delSym(\\''+t.ticker+'\\')">✕</button>';
 return '<div class="'+cls+'" onclick="openDetail(\\''+t.ticker+'\\')">'+x+'<span class="tk">'+t.ticker+'</span>'+sigBadge(t)+alarmBadge(t)+'<span class="pr">'+money(t.price)+'</span>'+
  '<div class="row">change: '+pctSpan(t.change)+'</div>'+
  '<div class="row">from day low: '+pctSpan(t.from_low)+'</div>'+
  '<div class="row muted">open: '+(t.open==null?'—':'$'+t.open.toFixed(2))+' · prev: '+(t.prev_close==null?'—':'$'+t.prev_close.toFixed(2))+'</div>'+
  '<div class="row muted">VWAP: '+(t.vwap==null?'—':'$'+t.vwap.toFixed(2))+'</div>'+
  '<div class="row muted">'+(t.as_of||'')+'</div></div>';
}
function findRow(tk){return (LAST.mine||[]).find(function(r){return r.ticker===tk;});}
async function openDetail(tk){
 const t=findRow(tk)||{ticker:tk};
 document.getElementById('overlay').style.display='flex';
 document.getElementById('d_tk').textContent=t.ticker;
 document.getElementById('d_price').innerHTML=money(t.price)+' &nbsp; '+pctSpan(t.change);
 const rows=[['Alarms today',(t.alarm_num&&t.alarm_num>0)?('#'+t.alarm_num+' (new-low bounces)'):'none yet'],
   ['Signal',t.signal||'—'],
   ['From day low',(t.from_low==null?'—':(t.from_low>=0?'+':'')+t.from_low.toFixed(2)+'%')],
   ['Open',t.open==null?'—':'$'+t.open.toFixed(2)],['Day high',t.high==null?'—':'$'+t.high.toFixed(2)],
   ['Day low',t.low==null?'—':'$'+t.low.toFixed(2)],['Prev close',t.prev_close==null?'—':'$'+t.prev_close.toFixed(2)],
   ['As of',t.as_of||'—']];
 document.getElementById('d_stats').innerHTML=rows.map(function(r){return '<div class="stat"><span class="muted">'+r[0]+'</span><span>'+r[1]+'</span></div>';}).join('');
 document.getElementById('d_note').textContent='Loading today’s chart…';
 try{const h=await (await fetch('/api/history?symbol='+encodeURIComponent(tk),{cache:'no-store'})).json();drawChart(h.points||[], (t.prev_close==null?null:t.prev_close));}
 catch(e){document.getElementById('d_note').textContent='Chart unavailable.';}
}
function closeDetail(){document.getElementById('overlay').style.display='none';if(_chart){_chart.destroy();_chart=null;}}
function drawChart(points,prevClose){
 const cv=document.getElementById('d_chart'),note=document.getElementById('d_note');
 if(_chart){_chart.destroy();_chart=null;}
 if(!points.length){cv.style.display='none';note.textContent='No intraday data yet today — check back during market hours.';return;}
 cv.style.display='block';note.textContent='Today’s movement · '+points.length+' points'+(prevClose!=null?' · dashed = prev close':'');
 const labels=points.map(function(p){return p.t;}),data=points.map(function(p){return p.p;});
 const up=data[data.length-1]>=data[0];
 const ds=[{data:data,borderColor:up?'#16a34a':'#dc2626',borderWidth:2,pointRadius:0,tension:0.25,fill:false}];
 if(prevClose!=null){ds.push({data:labels.map(function(){return prevClose;}),borderColor:'#9ca3af',borderWidth:1,borderDash:[5,4],pointRadius:0,fill:false});}
 _chart=new Chart(cv,{type:'line',data:{labels:labels,datasets:ds},
   options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{maxTicksLimit:6,font:{size:10}}},y:{ticks:{font:{size:10}}}}}});
}
async function whoami(){ME=await (await fetch('/api/me',{cache:'no-store'})).json();renderAuth();}
function renderAuth(){
 const b=document.getElementById('authbox');
 document.getElementById('addbar').style.display=ME.logged_in?'flex':'none';
 document.getElementById('pushbtn').style.display=(ME.logged_in&&ME.push_on)?'inline-block':'none';
 if(ME.logged_in){
   const al=ME.alerts_on?'checked':'';
   const note=(ME.email_on||ME.push_on)?'':' <span class="muted">(alerts not set up by site owner)</span>';
   b.innerHTML='<div class="who">Signed in as <b>'+ME.email+'</b> · <a href="#" onclick="logout();return false">Log out</a>'+
     ' · <label><input type="checkbox" id="altog" '+al+' onchange="toggleAlerts()"> Send me alerts</label>'+note+'</div>';
 }else{
   b.innerHTML='<div class="card-auth"><b>Sign in</b> to build your watchlist & get alerts'+
     '<div class="bar" style="margin-top:8px"><input id="em" placeholder="email" style="flex:1"></div>'+
     '<div class="bar"><input id="pw" type="password" placeholder="password" style="flex:1"></div>'+
     '<div class="bar"><button class="primary" onclick="auth(\\'login\\')">Log in</button>'+
     '<button onclick="auth(\\'signup\\')">Create account</button></div>'+
     '<div id="authmsg"></div></div>';
 }
}
async function auth(kind){
 const email=document.getElementById('em').value.trim(), pw=document.getElementById('pw').value;
 const r=await fetch('/api/'+kind,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email,password:pw})});
 const d=await r.json();
 if(d.ok){await whoami();load();}else{document.getElementById('authmsg').textContent=d.error||'Something went wrong';}
}
async function logout(){await fetch('/api/logout',{method:'POST'});ME={logged_in:false};renderAuth();load();}
async function toggleAlerts(){const on=document.getElementById('altog').checked;await fetch('/api/alerts',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({on})});ME.alerts_on=on;}
async function addSym(){
 const v=document.getElementById('addsym').value.trim();if(!v)return;
 const d=await (await fetch('/api/watch',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'add',symbol:v})})).json();
 if(d.error){document.getElementById('msg').textContent=d.error;}else{document.getElementById('addsym').value='';load();}
}
async function delSym(s){await fetch('/api/watch',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'remove',symbol:s})});load();}
function b64ToU8(b){b=(b||'').replace(/[^A-Za-z0-9_-]/g,'');const p='='.repeat((4-b.length%4)%4);const s=(b+p).replace(/-/g,'+').replace(/_/g,'/');const raw=atob(s);const a=new Uint8Array(raw.length);for(let i=0;i<raw.length;i++)a[i]=raw.charCodeAt(i);return a;}
async function enablePush(){
 const m=document.getElementById('msg');m.style.color='#b91c1c';
 if(!('serviceWorker' in navigator)||!('PushManager' in window)){m.textContent='This browser can’t do push. On iPhone, use Safari and Add to Home Screen first.';return;}
 try{
  const reg=await navigator.serviceWorker.register('/sw.js');
  const perm=await Notification.requestPermission();
  if(perm!=='granted'){m.textContent='Notifications were not allowed.';return;}
  const k=await (await fetch('/api/push/key')).json();
  if(!k.key){m.textContent='Push key missing on server.';return;}
  const sub=await reg.pushManager.subscribe({userVisibleOnly:true,applicationServerKey:b64ToU8(k.key)});
  await fetch('/api/push/subscribe',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({subscription:sub})});
  m.style.color='#047857';m.textContent='Phone alerts enabled on this device!';
 }catch(e){m.textContent='Could not enable alerts: '+(e.message||e);}
}
async function load(){
 try{
  const d=await (await fetch('/api/quotes',{cache:'no-store'})).json();LAST=d;
  document.getElementById('asof').textContent='As of '+d.meta.as_of;
  document.getElementById('rule').textContent=d.meta.rule||'';
  document.getElementById('warn').innerHTML=d.meta.have_key?'':'<div class="warn">No data keys set. Add ALPACA_KEY and ALPACA_SECRET.</div>';
  const ses=d.meta.session||'';const sc=(ses==='Open')?'open':'closed';
  document.getElementById('status').innerHTML='<span class="'+sc+'">● '+ses+'</span>';
  document.getElementById('signedout').style.display=ME.logged_in?'none':'block';
  const grid=document.getElementById('mygrid');
  if(ME.logged_in){
    // Sort: most alarms first, then closest to a fresh bounce off the low.
    const mine=(d.mine||[]).slice().sort((a,b)=>((b.alarm_num||0)-(a.alarm_num||0))||((b.from_low??-99)-(a.from_low??-99)));
    // A "new" alarm = this symbol's alarm number climbed since the last poll.
    const newOnes=new Set();
    const curNum={};
    mine.forEach(function(t){
      const n=t.alarm_num||0; curNum[t.ticker]=n;
      if(t.price!=null && n>(prevAlarmNum[t.ticker]||0)) newOnes.add(t.ticker);
    });
    grid.innerHTML=mine.length?mine.map(t=>card(t,newOnes.has(t.ticker))).join(''):'<div class="empty">No stocks yet — add one in the box at the top.</div>';
    const sndOn=document.getElementById('sndtog')&&document.getElementById('sndtog').checked;
    if(newOnes.size && sndOn && !firstLoad) beep();
    prevAlarmNum=curNum;
  }else{grid.innerHTML='';}
  firstLoad=false;
 }catch(e){document.getElementById('asof').textContent='could not load data';}
}
function copyList(kind){
 const date=(LAST.meta&&LAST.meta.date)||'';
 const priceHdr=(kind==='pre')?'Pre-Market Price':'Intraday Price';
 const h=["Date","Ticker","Prev Close",priceHdr];
 const rowsOf=(arr)=>arr.map(t=>[date,t.ticker,t.prev_close??"",t.price??""].join("\\t"));
 const all=rowsOf(LAST.mine||[]);
 const text=[h.join("\\t")].concat(all).join("\\n");
 navigator.clipboard.writeText(text).then(()=>{document.getElementById('msg').style.color='#047857';document.getElementById('msg').textContent='Copied '+(kind==='pre'?'pre-market':'intraday')+' list ('+all.length+' rows)!';setTimeout(()=>document.getElementById('msg').textContent='',2600);});
}
function showTab(t){
 _curTab=t;
 document.getElementById('view-watch').style.display=(t==='watch')?'block':'none';
 document.getElementById('view-history').style.display=(t==='history')?'block':'none';
 document.getElementById('view-sim').style.display=(t==='sim')?'block':'none';
 document.getElementById('tab-watch').classList.toggle('active',t==='watch');
 document.getElementById('tab-hist').classList.toggle('active',t==='history');
 document.getElementById('tab-sim').classList.toggle('active',t==='sim');
 if(t==='history')loadHistory();
 if(t==='sim'&&window.initSim)window.initSim();
}
async function loadHistory(){
 const box=document.getElementById('histalerts');
 if(!ME.logged_in){box.innerHTML='<div class="empty">Sign in to see your history.</div>';document.getElementById('histsym').innerHTML='';drawDaily([],'');return;}
 try{
  const d=await (await fetch('/api/hist/alerts',{cache:'no-store'})).json();
  const rows=d.rows||[];
  if(!rows.length){box.innerHTML='<div class="empty">No alarms yet. Each time a stock bounces 0.5%+ off a fresh intraday low during pre-market or market hours, a numbered alarm is saved here.</div>';}
  else{
   const head='<div class="stat" style="font-weight:700;color:#374151"><span>Date / time</span><span>Ticker</span><span>Alarm</span><span>Price</span><span>From low</span></div>';
   box.innerHTML='<div class="histtbl">'+head+rows.map(function(r){
     return '<div class="stat"><span class="muted">'+r.day+' '+(r.ts||'')+'</span><span class="tk">'+r.symbol+'</span><span>'+alarmBadge({alarm_num:r.num})+'</span><span>'+money(r.price)+'</span><span>'+pctSpan(r.from_low)+'</span></div>';
   }).join('')+'</div>';
  }
 }catch(e){box.innerHTML='<div class="empty">Could not load alert history.</div>';}
 const syms=(LAST.mine||[]).map(function(t){return t.ticker;});
 const sel=document.getElementById('histsym');const prev=sel.value;
 sel.innerHTML=syms.length?syms.map(function(s){return '<option value="'+s+'">'+s+'</option>';}).join(''):'<option value="">(no stocks)</option>';
 if(prev&&syms.indexOf(prev)>=0)sel.value=prev;
 loadDaily();
}
async function loadDaily(){
 const sym=document.getElementById('histsym').value;
 const note=document.getElementById('hist_note');
 if(!sym){drawDaily([],'');note.textContent='Add stocks to your watchlist to see their price history.';return;}
 try{
  const d=await (await fetch('/api/hist/daily?symbol='+encodeURIComponent(sym),{cache:'no-store'})).json();
  drawDaily(d.points||[],sym);
 }catch(e){note.textContent='Could not load price history.';}
}
function drawDaily(points,sym){
 const cv=document.getElementById('hist_chart'),note=document.getElementById('hist_note');
 if(_histChart){_histChart.destroy();_histChart=null;}
 if(!points.length){cv.style.display='none';if(sym)note.textContent='No saved history yet for '+sym+'. History builds up one point per day from now on.';return;}
 cv.style.display='block';note.textContent=sym+' · daily closing price · '+points.length+' day'+(points.length===1?'':'s');
 const labels=points.map(function(p){return p.d;}),data=points.map(function(p){return p.close;});
 const up=data[data.length-1]>=data[0];
 _histChart=new Chart(cv,{type:'line',data:{labels:labels,datasets:[{data:data,borderColor:up?'#16a34a':'#dc2626',borderWidth:2,pointRadius:2,tension:0.2,fill:false}]},
   options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{maxTicksLimit:8,font:{size:10}}},y:{ticks:{font:{size:10}}}}}});
}
if('serviceWorker' in navigator){navigator.serviceWorker.register('/sw.js').catch(function(){});}
whoami();load();setInterval(load,30000);
</script></body></html>"""


# =========================== HTTP ===========================
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype, extra=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or []):
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, extra=None):
        self._send(200, json.dumps(obj).encode("utf-8"), "application/json", extra)

    def _uid(self):
        c = SimpleCookie(self.headers.get("Cookie", ""))
        if "session" in c:
            return read_session(c["session"].value)
        return None

    def _body(self):
        n = int(self.headers.get("Content-Length", 0) or 0)
        if not n:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode())
        except Exception:
            return {}

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
        elif self.path.startswith("/manifest.webmanifest"):
            self._send(200, MANIFEST.encode("utf-8"), "application/manifest+json")
        elif self.path.startswith("/sw.js"):
            self._send(200, SW_JS.encode("utf-8"), "application/javascript")
        elif self.path.startswith("/sim.js"):
            self._send(200, SIM_JS.encode("utf-8"), "application/javascript")
        elif self.path.startswith("/icon.png") or self.path.startswith("/apple-touch-icon"):
            self._send(200, icon_bytes(), "image/png")
        elif self.path.startswith("/api/hist/alerts"):
            uid = self._uid()
            self._json({"rows": get_alarm_events(uid) if uid else []})
        elif self.path.startswith("/api/hist/daily"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            sym = clean_symbol((q.get("symbol") or [""])[0])
            self._json({"symbol": sym, "points": get_daily_history(sym) if sym else []})
        elif self.path.startswith("/api/history"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            sym = clean_symbol((q.get("symbol") or [""])[0])
            self._json({"symbol": sym, "points": history_for(sym) if sym else []})
        elif self.path.startswith("/api/push/key"):
            self._json({"key": VAPID_PUBLIC_KEY, "enabled": PUSH_ON})
        elif self.path.startswith("/api/me"):
            uid = self._uid()
            if uid:
                self._json({"logged_in": True, "email": get_email(uid),
                            "alerts_on": get_alerts_on(uid), "email_on": EMAIL_ON, "push_on": PUSH_ON})
            else:
                self._json({"logged_in": False, "email_on": EMAIL_ON, "push_on": PUSH_ON})
        elif self.path.startswith("/api/quotes"):
            uid = self._uid()
            out = {"meta": meta(), "mine": []}
            if uid:
                out["mine"] = rows_for(get_watchlist(uid))
            self._json(out)
        elif self.path.startswith("/healthz"):
            self._send(200, b"ok", "text/plain")
        else:
            self._send(404, b"not found", "text/plain")

    def do_POST(self):
        body = self._body()
        if self.path.startswith("/api/signup") or self.path.startswith("/api/login"):
            email = (body.get("email") or "").strip().lower()
            pw = body.get("password") or ""
            if "@" not in email or len(pw) < 6:
                return self._json({"ok": False, "error": "Enter a valid email and a password of 6+ characters."})
            if self.path.startswith("/api/signup"):
                uid = create_user(email, hash_pw(pw))
                if not uid:
                    return self._json({"ok": False, "error": "That email already has an account — try logging in."})
            else:
                row = get_user_by_email(email)
                if not row or not verify_pw(pw, row[1]):
                    return self._json({"ok": False, "error": "Wrong email or password."})
                uid = row[0]
            cookie = f"session={sign_session(uid)}; HttpOnly; Path=/; Max-Age=2592000; SameSite=Lax"
            return self._json({"ok": True}, extra=[("Set-Cookie", cookie)])
        elif self.path.startswith("/api/logout"):
            return self._json({"ok": True}, extra=[("Set-Cookie", "session=; Path=/; Max-Age=0")])
        elif self.path.startswith("/api/alerts"):
            uid = self._uid()
            if not uid:
                return self._json({"error": "Please log in first."})
            set_alerts_on(uid, bool(body.get("on")))
            return self._json({"ok": True})
        elif self.path.startswith("/api/push/subscribe"):
            uid = self._uid()
            if not uid:
                return self._json({"error": "Please log in first."})
            sub = body.get("subscription")
            if not isinstance(sub, dict) or not sub.get("endpoint"):
                return self._json({"error": "bad subscription"})
            save_sub(uid, sub)
            return self._json({"ok": True})
        elif self.path.startswith("/api/push/unsubscribe"):
            ep = body.get("endpoint")
            if ep:
                delete_sub(ep)
            return self._json({"ok": True})
        elif self.path.startswith("/api/watch"):
            uid = self._uid()
            if not uid:
                return self._json({"error": "Please log in first."})
            sym = clean_symbol(body.get("symbol"))
            if not sym:
                return self._json({"error": "That doesn't look like a valid symbol."})
            if body.get("action") == "add":
                if not add_watch(uid, sym):
                    return self._json({"error": f"Watchlist limit is {MAX_PER_USER}."})
            elif body.get("action") == "remove":
                remove_watch(uid, sym)
            return self._json({"ok": True})
        elif self.path.startswith("/api/hist/backfill"):
            uid = self._uid()
            if not uid:
                return self._json({"error": "Please log in first."})
            if not HAVE_DATA:
                return self._json({"error": "No Alpaca data keys set on the server."})
            sym = clean_symbol(body.get("symbol"))
            targets = [sym] if sym else get_watchlist(uid)
            if not targets:
                return self._json({"error": "No symbols to load — add one to your watchlist."})
            try:
                days = int(body.get("days") or 365)
            except Exception:
                days = 365
            try:
                n = backfill_daily_history(targets, days=max(30, min(days, 2000)))
                return self._json({"ok": True, "symbols": targets, "saved": n})
            except Exception as e:
                return self._json({"error": f"Backfill failed: {str(e)[:120]}"})
        self._send(404, b"not found", "text/plain")


def _ensure_db_ready():
    """Initialise the DB, retrying in the background until it succeeds.

    Lets the web server boot even when the database is temporarily unreachable
    (Neon suspended, over quota, or briefly down) instead of crashing on
    startup with exit status 1.
    """
    delay = 30
    while True:
        try:
            init_db()
            print("DB ready.", flush=True)
            return
        except Exception as e:
            print(f"  (DB not ready, retrying in {delay}s: {str(e)[:120]})", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 600)


def main():
    try:
        init_db()
    except Exception as e:
        print(f"  (initial DB init failed: {str(e)[:120]}; starting web server anyway, will retry)", flush=True)
        threading.Thread(target=_ensure_db_ready, daemon=True).start()
    if not HAVE_DATA:
        print("WARNING: ALPACA_KEY / ALPACA_SECRET not set.")
    print(f"Storage: {'Postgres' if DATABASE_URL else 'local SQLite ('+DB_PATH+')'}")
    print(f"Data: Alpaca feed={ALPACA_FEED} | Email: {'ON' if EMAIL_ON else 'OFF'} | "
          f"Push: {'ON' if PUSH_ON else 'OFF'} | Pushover: {'ON' if PUSHOVER_ON else 'OFF'}")
    print(f"Icon: {'icon.png found' if os.path.exists(ICON_PATH) else 'using fallback (add icon.png)'}")
    threading.Thread(target=refresher, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Stock Watch running on http://localhost:{PORT}  (Ctrl+C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
