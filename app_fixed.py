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
            "as_of": "", "near": False, "alert": False, "signal": None, "conditions": []}


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
    vwap_line = "" if row.get("vwap") is None else f"VWAP: ${row['vwap']:.2f}\n"
    body = (f"{t} just rose {row['from_low']:.2f}% above today's low.\n\n"
            f"Signal: {sig or 'n/a'} (conditions met: {row.get('conditions') or '—'})\n"
            f"Price: ${row['price']:.2f}\nDay low: ${row['low']:.2f}\n"
            f"{vwap_line}"
            f"Change vs prev close: {row['change']:+.2f}%\nAs of: {row['as_of']}\n")
    if APP_URL:
        body += f"\nOpen the dashboard: {APP_URL}\n"
    try:
        msg = EmailMessage()
        sig_tag = f" [{sig}]" if sig else ""
        msg["Subject"] = f"📈 {t} alert{sig_tag} — +{row['from_low']:.2f}% from the day's low"
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
    sig_tag = f" [{sig}]" if sig else ""
    if EMAIL_ON and send_alert_email(email, row):
        sent = True
    if PUSH_ON:
        title = f"📈 {row['ticker']}{sig_tag} +{row['from_low']:.2f}% from low"
        body = f"${row['price']:.2f} · {row['change']:+.2f}% on the day"
        for endpoint, sub in subs_for_user(uid):
            res = send_push(sub, title, body)
            if res == "gone":
                delete_sub(endpoint)
            elif res:
                sent = True
    if PUSHOVER_ON:
        title = f"📈 {row['ticker']}{sig_tag} +{row['from_low']:.2f}% from low"
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
            # Fire only on the full multi-condition signal (1 AND 2 AND 5, not stopped).
            if row and row.get("alert") and row.get("price") is not None:
                if not already_alerted(uid, s, day):
                    if notify_user(uid, email, row):
                        mark_alerted(uid, s, day)


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


def log_crossings():
    """Log every stock that crosses 0.5%+ from its day low, once per user/symbol/day.
    Recorded regardless of whether email/push alerts are configured."""
    now = datetime.now(ET)
    if session_label(now) not in ALERT_SESSIONS:
        return
    day = now.strftime("%Y-%m-%d")
    ts = now.strftime("%H:%M ET")
    for uid in all_user_ids():
        for s in get_watchlist(uid):
            with _qlock:
                row = _quotes.get(s)
            if row and row.get("near") and row.get("price") is not None:
                log_alert(uid, s, day, ts, row.get("price"), row.get("from_low"), row.get("change"))


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
                log_crossings()
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
            "rule": f"stocks turning green have climbed {RISE_PCT*100:.1f}%+ from their lowest price today. "
                    f"You'll get an alarm when one of your stocks meet logical code.",
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
.histtbl .stat span:nth-child(2){text-align:center}
.histtbl .stat span:nth-child(3),.histtbl .stat span:nth-child(4){text-align:right}
</style></head><body>
<h1>📈 Stock Watch</h1>
<div class="meta" id="asof">loading…</div>
<div class="rule" id="rule"></div>
<div id="warn"></div>
<div id="authbox"></div>

<div class="tabs">
  <button id="tab-watch" class="tab active" onclick="showTab('watch')">⭐ Watchlist</button>
  <button id="tab-hist" class="tab" onclick="showTab('history')">🕘 History</button>
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
<script>
let LAST={mine:[]}, ME={logged_in:false}, _chart=null, prevAlerts=new Set(), firstLoad=true, _curTab='watch', _histChart=null;
function pctSpan(v){if(v===null||v===undefined)return '<span class="muted">—</span>';var s=(v>=0?"+":"")+v.toFixed(2)+"%";return '<span class="'+(v>=0?'up':'dn')+'">'+s+'</span>';}
function money(v){return (v===null||v===undefined)?'<span class="muted">—</span>':'$'+v.toFixed(2);}
function beep(){try{var a=new (window.AudioContext||window.webkitAudioContext)();var o=a.createOscillator(),g=a.createGain();o.connect(g);g.connect(a.destination);o.type='sine';o.frequency.value=880;g.gain.setValueAtTime(0.0001,a.currentTime);g.gain.exponentialRampToValueAtTime(0.12,a.currentTime+0.02);g.gain.exponentialRampToValueAtTime(0.0001,a.currentTime+0.5);o.start();o.stop(a.currentTime+0.52);}catch(e){}}
function sigBadge(t){
 if(!t.signal)return '';
 var bg={'Good':'#fef3c7','Very Good':'#dbeafe','Excellent':'#dcfce7'}[t.signal]||'#eee';
 var fg={'Good':'#92400e','Very Good':'#1e40af','Excellent':'#166534'}[t.signal]||'#333';
 return '<span class="sig" style="background:'+bg+';color:'+fg+'">'+t.signal+'</span>';
}
function card(t,blink){
 let cls='card';if(t.near)cls+=' near';if(blink)cls+=' alerting';
 const x='<button class="x" title="Remove" onclick="event.stopPropagation();delSym(\\''+t.ticker+'\\')">✕</button>';
 return '<div class="'+cls+'" onclick="openDetail(\\''+t.ticker+'\\')">'+x+'<span class="tk">'+t.ticker+'</span>'+sigBadge(t)+'<span class="pr">'+money(t.price)+'</span>'+
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
 const rows=[['From day low',(t.from_low==null?'—':(t.from_low>=0?'+':'')+t.from_low.toFixed(2)+'%')],
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
    const mine=(d.mine||[]).slice().sort((a,b)=>((a.near?1:0)-(b.near?1:0))||((b.from_low??-99)-(a.from_low??-99)));
    const cur=new Set(mine.filter(t=>t.near && t.price!=null).map(t=>t.ticker));
    const newOnes=new Set([...cur].filter(x=>!prevAlerts.has(x)));
    grid.innerHTML=mine.length?mine.map(t=>card(t,newOnes.has(t.ticker))).join(''):'<div class="empty">No stocks yet — add one in the box at the top.</div>';
    const sndOn=document.getElementById('sndtog')&&document.getElementById('sndtog').checked;
    if(newOnes.size && sndOn && !firstLoad) beep();
    prevAlerts=cur;
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
 document.getElementById('tab-watch').classList.toggle('active',t==='watch');
 document.getElementById('tab-hist').classList.toggle('active',t==='history');
 if(t==='history')loadHistory();
}
async function loadHistory(){
 const box=document.getElementById('histalerts');
 if(!ME.logged_in){box.innerHTML='<div class="empty">Sign in to see your history.</div>';document.getElementById('histsym').innerHTML='';drawDaily([],'');return;}
 try{
  const d=await (await fetch('/api/hist/alerts',{cache:'no-store'})).json();
  const rows=d.rows||[];
  if(!rows.length){box.innerHTML='<div class="empty">No alerts yet. When a stock climbs 0.5%+ during pre-market or market hours, it’ll be saved here.</div>';}
  else{
   const head='<div class="stat" style="font-weight:700;color:#374151"><span>Date / time</span><span>Ticker</span><span>Price</span><span>From low</span></div>';
   box.innerHTML='<div class="histtbl">'+head+rows.map(function(r){
     return '<div class="stat"><span class="muted">'+r.day+' '+(r.ts||'')+'</span><span class="tk">'+r.symbol+'</span><span>'+money(r.price)+'</span><span>'+pctSpan(r.from_low)+'</span></div>';
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
        elif self.path.startswith("/icon.png") or self.path.startswith("/apple-touch-icon"):
            self._send(200, icon_bytes(), "image/png")
        elif self.path.startswith("/api/hist/alerts"):
            uid = self._uid()
            self._json({"rows": get_alert_log(uid) if uid else []})
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
