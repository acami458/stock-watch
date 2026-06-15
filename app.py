#!/usr/bin/env python3
"""
Stock Watch — full app (dashboard + accounts + shared list + email + push + detail view)
========================================================================================
Installable home-screen web app. Tap any stock to see full stats and a chart of
today's movement (built from the app's own minute-by-minute samples).

Files in this folder: app.py, requirements.txt, icon.png  (upload all three).

ENV VARS:
  FINNHUB_API_KEY, DATABASE_URL, SECRET_KEY,
  SMTP_HOST/PORT/USER/PASS, EMAIL_FROM, SMTP_SSL  (email, optional),
  VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY, VAPID_SUBJECT (push, optional), APP_URL

RUN LOCALLY:  export FINNHUB_API_KEY=your_key ; python3 app.py  -> http://localhost:8765
========================================================================================
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
from datetime import datetime
from email.message import EmailMessage
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from zoneinfo import ZoneInfo
except ImportError:
    raise SystemExit("Python 3.9+ required.")

# ----------------------------- CONFIG -----------------------------
SEED_TICKERS = [
    "AAPL", "ADI", "ADMA", "AMZN", "BABA", "CBRL", "CL", "COPX", "CUBE", "CVX",
    "DE", "FUTU", "GE", "GEV", "GLD", "GOOG", "IEP", "INTU", "JNJ", "JPM",
    "KO", "LLY", "LMT", "MA", "MAIN", "META", "MSFT", "MU", "NVDA", "PFE",
    "RIO", "SLV", "TSLA", "VZ", "WMT", "ADSK", "AVGO", "SPCX",
]
HERE            = os.path.dirname(os.path.abspath(__file__))
FINNHUB_KEY     = os.environ.get("FINNHUB_API_KEY", "").strip()
DATABASE_URL    = os.environ.get("DATABASE_URL", "").strip()
SECRET_KEY      = os.environ.get("SECRET_KEY", "").strip() or base64.b64encode(os.urandom(24)).decode()
DB_PATH         = os.path.join(HERE, "stockwatch.db")
ICON_PATH       = os.path.join(HERE, "icon.png")
ET              = ZoneInfo("America/New_York")
PORT            = int(os.environ.get("PORT", "8765"))
REFRESH_SECONDS = 60
MAX_WORKERS     = 4
RISE_PCT        = 0.005
MAX_PER_USER    = 60
MAX_SHARED      = 100
ALERT_SESSIONS  = {"Pre-market", "Open"}
APP_URL         = os.environ.get("APP_URL", "").strip()

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
        cur.execute("""CREATE TABLE IF NOT EXISTS shared_list(
            symbol TEXT PRIMARY KEY, sort_order INTEGER DEFAULT 0, added_by TEXT)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS push_subs(
            endpoint TEXT PRIMARY KEY, user_id INTEGER NOT NULL, sub TEXT NOT NULL)""")
        conn.commit()
        try:
            if kind == "pg":
                cur.execute("ALTER TABLE shared_list ADD COLUMN IF NOT EXISTS added_by TEXT")
            else:
                cur.execute("ALTER TABLE shared_list ADD COLUMN added_by TEXT")
            conn.commit()
        except Exception:
            conn.rollback()
        cur.execute("SELECT COUNT(*) FROM shared_list")
        if cur.fetchone()[0] == 0:
            for i, s in enumerate(SEED_TICKERS):
                cur.execute(_ph("INSERT INTO shared_list(symbol, sort_order) VALUES(%s,%s)", kind), (s, i))
            conn.commit()
    finally:
        conn.close()


def get_shared_list():
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT symbol FROM shared_list ORDER BY sort_order, symbol")
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def get_shared_added():
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT symbol, added_by FROM shared_list")
        return {r[0]: r[1] for r in cur.fetchall()}
    finally:
        conn.close()


def add_shared(symbol, added_by=None):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM shared_list")
        if cur.fetchone()[0] >= MAX_SHARED:
            return False
        cur.execute("SELECT COALESCE(MAX(sort_order),0)+1 FROM shared_list")
        nxt = cur.fetchone()[0]
        try:
            cur.execute(_ph("INSERT INTO shared_list(symbol, sort_order, added_by) VALUES(%s,%s,%s)", kind),
                        (symbol, nxt, added_by))
            conn.commit()
        except Exception:
            conn.rollback()
        return True
    finally:
        conn.close()


def remove_shared(symbol):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("DELETE FROM shared_list WHERE symbol=%s", kind), (symbol,))
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


# =========================== QUOTES + HISTORY ===========================
_quotes = {}
_qlock = threading.Lock()
_hist = {}                       # symbol -> [[ "HH:MM", price ], ...] for today
_hist_lock = threading.Lock()
_hist_state = {"day": None}
HIST_MAX = 480


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


def fh_quote(sym, retries=3):
    url = f"https://finnhub.io/api/v1/quote?symbol={urllib.parse.quote(sym)}&token={FINNHUB_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "stock-watch"})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=12) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1)); continue
            raise
        except Exception:
            if attempt < retries - 1:
                time.sleep(0.8 * (attempt + 1)); continue
            raise


def one_row(t):
    try:
        q = fh_quote(t)
        c, low, pc, ts = q.get("c"), q.get("l"), q.get("pc"), q.get("t")
        o, hi = q.get("o"), q.get("h")
        if not c:
            return {"ticker": t, "price": None, "from_low": None, "prev_close": None,
                    "change": None, "open": None, "high": None, "low": None,
                    "as_of": "", "near": False}
        from_low = (c - low) / low * 100 if low else None
        change = (c - pc) / pc * 100 if pc else None
        as_of = datetime.fromtimestamp(ts, ET).strftime("%H:%M ET") if ts else ""
        return {"ticker": t, "price": round(c, 2),
                "from_low": None if from_low is None else round(from_low, 2),
                "prev_close": None if not pc else round(pc, 2),
                "change": None if change is None else round(change, 2),
                "open": None if not o else round(o, 2),
                "high": None if not hi else round(hi, 2),
                "low": None if not low else round(low, 2),
                "as_of": as_of,
                "near": bool(from_low is not None and from_low >= RISE_PCT * 100)}
    except Exception:
        return {"ticker": t, "price": None, "from_low": None, "prev_close": None,
                "change": None, "open": None, "high": None, "low": None,
                "as_of": "err", "near": False}


def refresh_symbols(syms):
    if not FINNHUB_KEY or not syms:
        return
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        results = list(ex.map(lambda s: (s, one_row(s)), syms))
    with _qlock:
        for s, row in results:
            _quotes[s] = row


def rows_for(syms):
    with _qlock:
        missing = [s for s in syms if s not in _quotes]
    if missing:
        refresh_symbols(missing)
    out = []
    with _qlock:
        for s in syms:
            out.append(_quotes.get(s) or {"ticker": s, "price": None, "from_low": None,
                       "prev_close": None, "change": None, "open": None, "high": None,
                       "low": None, "as_of": "", "near": False})
    return out


# =========================== ALERTS (email + push) ===========================
def send_alert_email(to_email, row):
    if not EMAIL_ON:
        return False
    t = row["ticker"]
    body = (f"{t} just rose {row['from_low']:.2f}% above today's low.\n\n"
            f"Price: ${row['price']:.2f}\nDay low: ${row['low']:.2f}\n"
            f"Change vs prev close: {row['change']:+.2f}%\nAs of: {row['as_of']}\n")
    if APP_URL:
        body += f"\nOpen the dashboard: {APP_URL}\n"
    try:
        msg = EmailMessage()
        msg["Subject"] = f"📈 {t} alert — +{row['from_low']:.2f}% from the day's low"
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


def notify_user(uid, email, row):
    sent = False
    if EMAIL_ON and send_alert_email(email, row):
        sent = True
    if PUSH_ON:
        title = f"📈 {row['ticker']} +{row['from_low']:.2f}% from low"
        body = f"${row['price']:.2f} · {row['change']:+.2f}% on the day"
        for endpoint, sub in subs_for_user(uid):
            res = send_push(sub, title, body)
            if res == "gone":
                delete_sub(endpoint)
            elif res:
                sent = True
    return sent


def alert_check():
    if not (EMAIL_ON or PUSH_ON):
        return
    now = datetime.now(ET)
    if session_label(now) not in ALERT_SESSIONS:
        return
    day = now.strftime("%Y-%m-%d")
    for uid, email in alert_users():
        for s in get_watchlist(uid):
            with _qlock:
                row = _quotes.get(s)
            if row and row.get("near") and row.get("price") is not None:
                if not already_alerted(uid, s, day):
                    if notify_user(uid, email, row):
                        mark_alerted(uid, s, day)


def refresher():
    while True:
        try:
            syms = set(SEED_TICKERS)
            try:
                syms |= set(get_shared_list())
                syms |= set(all_user_symbols())
            except Exception:
                pass
            refresh_symbols(sorted(syms))
            try:
                record_history()
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
            "rule": f"highlighted when ≥ +{RISE_PCT*100:.1f}% above the day's low",
            "have_key": bool(FINNHUB_KEY),
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
h1{font-size:20px;margin:0 0 2px}h2{font-size:15px;margin:18px 0 8px}
.meta{color:#6b7280;font-size:12px}.rule{color:#374151;font-size:12px;margin:2px 0 10px}
.bar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:12px}
button{font:inherit;font-size:13px;padding:7px 12px;border-radius:8px;border:1px solid #d1d5db;background:#fff;cursor:pointer}
button:hover{background:#f3f4f6}.primary{background:#1d4ed8;color:#fff;border-color:#1d4ed8}.primary:hover{background:#1e40af}
input{font:inherit;font-size:13px;padding:7px 10px;border:1px solid #d1d5db;border-radius:8px}
.open{background:#ecfdf5;color:#047857;border:1px solid #a7f3d0;padding:5px 11px;border-radius:999px;font-size:12px}
.closed{background:#fef2f2;color:#b91c1c;border:1px solid #fecaca;padding:5px 11px;border-radius:999px;font-size:12px}
.warn{background:#fffbeb;color:#92400e;border:1px solid #fde68a;padding:10px 12px;border-radius:8px;font-size:13px;margin-bottom:12px}
.card-auth{background:#fff;border:1px solid #e7e9ee;border-radius:12px;padding:16px;max-width:340px;margin-bottom:14px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px}
.card{background:#fff;border:1px solid #e7e9ee;border-left:4px solid #cbd5e1;border-radius:10px;padding:10px 12px;position:relative;cursor:pointer}
.card:hover{border-color:#c7ccd6}.card.near{border-left-color:#16a34a;background:#f0fdf4}
.tk{font-weight:700;font-size:15px}.pr{float:right;font-weight:600}
.row{font-size:12px;color:#4b5563;margin-top:3px}.up{color:#16a34a;font-weight:600}.dn{color:#dc2626;font-weight:600}
.muted{color:#9ca3af}.foot{color:#9ca3af;font-size:11px;margin-top:18px}
.x{position:absolute;top:6px;right:8px;cursor:pointer;color:#9ca3af;font-size:14px;border:none;background:none;padding:2px 5px}
.x:hover{color:#dc2626}#msg,#authmsg{font-size:12px;color:#b91c1c}
.who{font-size:12px;color:#374151;display:flex;gap:10px;align-items:center;flex-wrap:wrap}
#overlay{position:fixed;inset:0;background:rgba(15,18,25,.45);display:none;align-items:center;justify-content:center;padding:16px;z-index:50}
#modal{background:#fff;border-radius:14px;max-width:440px;width:100%;padding:18px;position:relative}
.stat{display:flex;justify-content:space-between;font-size:13px;padding:5px 0;border-bottom:1px solid #f1f1f1}
</style></head><body>
<h1>📈 Stock Watch</h1>
<div class="meta" id="asof">loading…</div>
<div class="rule" id="rule"></div>
<div id="warn"></div>
<div id="authbox"></div>
<div class="bar">
  <span id="status"></span>
  <button onclick="load()">Refresh</button>
  <button onclick="copyExcel()">Copy for Excel</button>
  <button id="pushbtn" style="display:none" onclick="enablePush()">🔔 Enable phone alerts</button>
  <span id="msg"></span>
</div>
<div id="mywrap" style="display:none">
  <h2>⭐ My Watchlist</h2>
  <div class="bar">
    <input id="addsym" placeholder="Add to my list (e.g. NFLX)" maxlength="10" onkeydown="if(event.key==='Enter')addSym()">
    <button class="primary" onclick="addSym()">Add</button>
  </div>
  <div class="grid" id="mygrid"></div>
</div>
<h2>👥 Shared List</h2>
<div id="sharededit" class="bar" style="display:none">
  <input id="sharedsym" placeholder="Add to shared list" maxlength="10" onkeydown="if(event.key==='Enter')addShared()">
  <button onclick="addShared()">Add to shared</button>
  <span class="muted" style="font-size:12px">Everyone sees changes to the shared list.</span>
</div>
<div class="grid" id="grid"></div>
<div class="foot">Data: Finnhub · free tier delayed ~15 min · tap a stock for details & today's chart · alerts fire when one of YOUR stocks rises ≥0.5% above the day's low.</div>

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
let LAST={shared:[],mine:[]}, ME={logged_in:false}, _chart=null;
function pctSpan(v){if(v===null||v===undefined)return '<span class="muted">—</span>';var s=(v>=0?"+":"")+v.toFixed(2)+"%";return '<span class="'+(v>=0?'up':'dn')+'">'+s+'</span>';}
function money(v){return (v===null||v===undefined)?'<span class="muted">—</span>':'$'+v.toFixed(2);}
function card(t,removable,shared){
 const cls=t.near?'card near':'card';
 const fn=shared?'delShared':'delSym';
 let tip='Remove from my list';
 if(shared){tip=t.added_by?('Added by '+t.added_by+' — remove from shared'):'Original list — remove from shared';}
 const x=removable?'<button class="x" title="'+tip+'" onclick="event.stopPropagation();'+fn+'(\\''+t.ticker+'\\')">✕</button>':'';
 const by=(shared&&t.added_by)?'<div class="row muted">added by '+t.added_by+'</div>':'';
 return '<div class="'+cls+'" onclick="openDetail(\\''+t.ticker+'\\')">'+x+'<span class="tk">'+t.ticker+'</span><span class="pr">'+money(t.price)+'</span>'+
  '<div class="row">change: '+pctSpan(t.change)+'</div>'+
  '<div class="row">from day low: '+pctSpan(t.from_low)+'</div>'+
  '<div class="row muted">open: '+(t.open==null?'—':'$'+t.open.toFixed(2))+' · prev: '+(t.prev_close==null?'—':'$'+t.prev_close.toFixed(2))+'</div>'+
  '<div class="row muted">'+(t.as_of||'')+'</div>'+by+'</div>';
}
function findRow(tk){return (LAST.shared||[]).concat(LAST.mine||[]).find(function(r){return r.ticker===tk;});}
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
 document.getElementById('sharededit').style.display=ME.logged_in?'flex':'none';
 document.getElementById('pushbtn').style.display=(ME.logged_in&&ME.push_on)?'inline-block':'none';
 if(ME.logged_in){
   const al=ME.alerts_on?'checked':'';
   const note=(ME.email_on||ME.push_on)?'':' <span class="muted">(alerts not set up by site owner)</span>';
   b.innerHTML='<div class="who">Signed in as <b>'+ME.email+'</b> · <a href="#" onclick="logout();return false">Log out</a>'+
     ' · <label><input type="checkbox" id="altog" '+al+' onchange="toggleAlerts()"> Send me alerts</label>'+note+'</div>';
   document.getElementById('mywrap').style.display='block';
 }else{
   document.getElementById('mywrap').style.display='none';
   b.innerHTML='<div class="card-auth"><b>Sign in</b> to edit lists & get alerts'+
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
async function addShared(){
 const v=document.getElementById('sharedsym').value.trim();if(!v)return;
 const d=await (await fetch('/api/shared',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'add',symbol:v})})).json();
 if(d.error){document.getElementById('msg').textContent=d.error;}else{document.getElementById('sharedsym').value='';load();}
}
async function delShared(s){await fetch('/api/shared',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'remove',symbol:s})});load();}
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
  document.getElementById('rule').textContent='Note: '+d.meta.rule;
  document.getElementById('warn').innerHTML=d.meta.have_key?'':'<div class="warn">No data key set. Add FINNHUB_API_KEY.</div>';
  const ses=d.meta.session||'';const sc=(ses==='Open')?'open':'closed';
  document.getElementById('status').innerHTML='<span class="'+sc+'">● '+ses+'</span>';
  const sh=d.shared.slice().sort((a,b)=>(b.from_low??-99)-(a.from_low??-99));
  document.getElementById('grid').innerHTML=sh.map(t=>card(t,ME.logged_in,true)).join('');
  if(ME.logged_in){
    const mine=(d.mine||[]).slice().sort((a,b)=>(b.from_low??-99)-(a.from_low??-99));
    document.getElementById('mygrid').innerHTML=mine.length?mine.map(t=>card(t,true,false)).join(''):'<div class="muted" style="font-size:13px">No stocks yet — add one above.</div>';
  }
 }catch(e){document.getElementById('asof').textContent='could not load data';}
}
function copyExcel(){
 const ses=(LAST.meta&&LAST.meta.session)||'';const date=(LAST.meta&&LAST.meta.date)||'';
 const h=["List","Ticker","Price","Change %","% From Low","Open","Day Low","Prev Close","Session","Date","As Of"];
 const rowsOf=(arr,label)=>arr.map(t=>[label,t.ticker,t.price??"",t.change??"",t.from_low??"",t.open??"",t.low??"",t.prev_close??"",ses,date,t.as_of||""].join("\\t"));
 let all=[];
 if(ME.logged_in && LAST.mine && LAST.mine.length) all=all.concat(rowsOf(LAST.mine,"Mine"));
 all=all.concat(rowsOf(LAST.shared,"Shared"));
 const text=[h.join("\\t")].concat(all).join("\\n");
 navigator.clipboard.writeText(text).then(()=>{document.getElementById('msg').style.color='#047857';document.getElementById('msg').textContent='Copied '+all.length+' rows!';setTimeout(()=>document.getElementById('msg').textContent='',2500);});
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
            shared = rows_for(get_shared_list())
            added = get_shared_added()
            for r in shared:
                who = added.get(r["ticker"])
                r["added_by"] = who.split("@")[0] if who else None
            out = {"meta": meta(), "shared": shared, "mine": []}
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
        elif self.path.startswith("/api/shared"):
            uid = self._uid()
            if not uid:
                return self._json({"error": "Please log in first."})
            sym = clean_symbol(body.get("symbol"))
            if not sym:
                return self._json({"error": "That doesn't look like a valid symbol."})
            if body.get("action") == "add":
                if not add_shared(sym, get_email(uid)):
                    return self._json({"error": f"Shared list limit is {MAX_SHARED}."})
            elif body.get("action") == "remove":
                remove_shared(sym)
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


def main():
    init_db()
    if not FINNHUB_KEY:
        print("WARNING: FINNHUB_API_KEY not set.")
    print(f"Storage: {'Postgres' if DATABASE_URL else 'local SQLite ('+DB_PATH+')'}")
    print(f"Email alerts: {'ON' if EMAIL_ON else 'OFF'} | Push alerts: {'ON' if PUSH_ON else 'OFF'}")
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
