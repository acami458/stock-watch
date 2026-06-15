#!/usr/bin/env python3
"""
Stock Watch — Stage 2: Accounts + per-user watchlists
============================================================
A shared stock dashboard PLUS personal accounts: each person signs up,
logs in, and keeps their own editable watchlist (add/delete stocks) on
top of the shared list. Prices from Finnhub, refreshed once a minute and
served from a shared snapshot (rate-limit safe for many viewers).

STORAGE:
  - Production: set DATABASE_URL to your Neon Postgres connection string.
  - Local preview: if DATABASE_URL is unset, it uses a local SQLite file
    (stockwatch.db) so you can test sign-up/login on your Mac first.

REQUIRED ENV VARS (in production):
  FINNHUB_API_KEY  - your free Finnhub key
  DATABASE_URL     - your Neon Postgres URL (postgresql://...:...@.../db?sslmode=require)
  SECRET_KEY       - any long random string (signs the login cookie)

RUN LOCALLY:
  export FINNHUB_API_KEY=your_key
  python3 app.py        ->  http://localhost:8765
============================================================
"""

import base64
import hashlib
import hmac
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from zoneinfo import ZoneInfo
except ImportError:
    raise SystemExit("Python 3.9+ required.")

# ----------------------------- CONFIG -----------------------------
SHARED_TICKERS = [
    "AAPL", "ADI", "ADMA", "AMZN", "BABA", "CBRL", "CL", "COPX", "CUBE", "CVX",
    "DE", "FUTU", "GE", "GEV", "GLD", "GOOG", "IEP", "INTU", "JNJ", "JPM",
    "KO", "LLY", "LMT", "MA", "MAIN", "META", "MSFT", "MU", "NVDA", "PFE",
    "RIO", "SLV", "TSLA", "VZ", "WMT", "ADSK", "AVGO", "SPCX",
]
FINNHUB_KEY     = os.environ.get("FINNHUB_API_KEY", "").strip()
DATABASE_URL    = os.environ.get("DATABASE_URL", "").strip()
SECRET_KEY      = os.environ.get("SECRET_KEY", "").strip() or base64.b64encode(os.urandom(24)).decode()
DB_PATH         = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stockwatch.db")
ET              = ZoneInfo("America/New_York")
PORT            = int(os.environ.get("PORT", "8765"))
REFRESH_SECONDS = 60
MAX_WORKERS     = 4
RISE_PCT        = 0.005
MAX_PER_USER    = 60      # cap personal watchlist size
# ------------------------------------------------------------------

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
            cur.execute("""CREATE TABLE IF NOT EXISTS watchlist(
                user_id INTEGER NOT NULL, symbol TEXT NOT NULL,
                PRIMARY KEY(user_id, symbol))""")
        else:
            cur.execute("""CREATE TABLE IF NOT EXISTS users(
                id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL,
                pw_hash TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS watchlist(
                user_id INTEGER NOT NULL, symbol TEXT NOT NULL,
                PRIMARY KEY(user_id, symbol))""")
        conn.commit()
    finally:
        conn.close()


def create_user(email, pw_hash):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("SELECT id FROM users WHERE email=%s", kind), (email,))
        if cur.fetchone():
            return None  # already exists
        if kind == "pg":
            cur.execute("INSERT INTO users(email, pw_hash) VALUES(%s,%s) RETURNING id",
                        (email, pw_hash))
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
            cur.execute(_ph("INSERT INTO watchlist(user_id, symbol) VALUES(%s,%s)", kind),
                        (uid, symbol))
            conn.commit()
        except Exception:
            conn.rollback()  # already there
        return True
    finally:
        conn.close()


def remove_watch(uid, symbol):
    conn, kind = _db()
    try:
        cur = conn.cursor()
        cur.execute(_ph("DELETE FROM watchlist WHERE user_id=%s AND symbol=%s", kind),
                    (uid, symbol))
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


# =========================== QUOTES ===========================
_quotes = {}
_qlock = threading.Lock()
_tracked = set(SHARED_TICKERS)


def market_open(dt):
    if dt.weekday() >= 5:
        return False
    m = dt.hour * 60 + dt.minute
    return 9 * 60 + 30 <= m < 16 * 60


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
        if not c:
            return {"ticker": t, "price": None, "from_low": None,
                    "prev_close": None, "change": None, "as_of": "", "near": False}
        from_low = (c - low) / low * 100 if low else None
        change = (c - pc) / pc * 100 if pc else None
        as_of = datetime.fromtimestamp(ts, ET).strftime("%H:%M ET") if ts else ""
        return {"ticker": t, "price": round(c, 2),
                "from_low": None if from_low is None else round(from_low, 2),
                "prev_close": None if not pc else round(pc, 2),
                "change": None if change is None else round(change, 2),
                "as_of": as_of,
                "near": bool(from_low is not None and from_low >= RISE_PCT * 100)}
    except Exception:
        return {"ticker": t, "price": None, "from_low": None,
                "prev_close": None, "change": None, "as_of": "err", "near": False}


def refresh_symbols(syms):
    if not FINNHUB_KEY or not syms:
        return
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        results = list(ex.map(lambda s: (s, one_row(s)), syms))
    with _qlock:
        for s, row in results:
            _quotes[s] = row


def refresher():
    while True:
        try:
            syms = set(SHARED_TICKERS)
            try:
                syms |= set(all_user_symbols())
            except Exception:
                pass
            with _qlock:
                _tracked.update(syms)
            refresh_symbols(sorted(syms))
        except Exception:
            pass
        time.sleep(REFRESH_SECONDS)


def rows_for(syms):
    with _qlock:
        missing = [s for s in syms if s not in _quotes]
    if missing:
        refresh_symbols(missing)
    out = []
    with _qlock:
        _tracked.update(syms)
        for s in syms:
            out.append(_quotes.get(s) or {"ticker": s, "price": None, "from_low": None,
                       "prev_close": None, "change": None, "as_of": "", "near": False})
    return out


def meta():
    now = datetime.now(ET)
    return {"as_of": now.strftime("%a %b %d, %H:%M:%S ET"),
            "market_open": market_open(now),
            "rule": f"highlighted when ≥ +{RISE_PCT*100:.1f}% above the day's low",
            "have_key": bool(FINNHUB_KEY)}


# =========================== WEB PAGE ===========================
PAGE = """<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Stock Watch</title>
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
.card{background:#fff;border:1px solid #e7e9ee;border-left:4px solid #cbd5e1;border-radius:10px;padding:10px 12px;position:relative}
.card.near{border-left-color:#16a34a;background:#f0fdf4}
.tk{font-weight:700;font-size:15px}.pr{float:right;font-weight:600}
.row{font-size:12px;color:#4b5563;margin-top:3px}.up{color:#16a34a;font-weight:600}.dn{color:#dc2626;font-weight:600}
.muted{color:#9ca3af}.foot{color:#9ca3af;font-size:11px;margin-top:18px}
.x{position:absolute;top:6px;right:8px;cursor:pointer;color:#9ca3af;font-size:14px;border:none;background:none;padding:2px 5px}
.x:hover{color:#dc2626}#msg,#authmsg{font-size:12px;color:#b91c1c}
.who{font-size:12px;color:#374151}
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
  <span id="msg"></span>
</div>

<div id="mywrap" style="display:none">
  <h2>⭐ My Watchlist</h2>
  <div class="bar">
    <input id="addsym" placeholder="Add symbol (e.g. NFLX)" maxlength="10" onkeydown="if(event.key==='Enter')addSym()">
    <button class="primary" onclick="addSym()">Add</button>
  </div>
  <div class="grid" id="mygrid"></div>
</div>

<h2>👥 Shared List</h2>
<div class="grid" id="grid"></div>

<div class="foot">Data: Finnhub · free tier delayed ~15 min · updates ~once a minute · "Copy for Excel" copies the shared list.</div>
<script>
let LAST={shared:[],mine:[]}, ME={logged_in:false};
function pctSpan(v){if(v===null||v===undefined)return '<span class="muted">—</span>';var s=(v>=0?"+":"")+v.toFixed(2)+"%";return '<span class="'+(v>=0?'up':'dn')+'">'+s+'</span>';}
function money(v){return (v===null||v===undefined)?'<span class="muted">—</span>':'$'+v.toFixed(2);}
function card(t,withX){
 const cls=t.near?'card near':'card';
 const x=withX?'<button class="x" title="remove" onclick="delSym(\\''+t.ticker+'\\')">✕</button>':'';
 return '<div class="'+cls+'">'+x+'<span class="tk">'+t.ticker+'</span><span class="pr">'+money(t.price)+'</span>'+
  '<div class="row">change: '+pctSpan(t.change)+'</div>'+
  '<div class="row">from day low: '+pctSpan(t.from_low)+'</div>'+
  '<div class="row muted">prev close: '+(t.prev_close==null?'—':'$'+t.prev_close.toFixed(2))+' · '+(t.as_of||'')+'</div></div>';
}
async function whoami(){ME=await (await fetch('/api/me',{cache:'no-store'})).json();renderAuth();}
function renderAuth(){
 const b=document.getElementById('authbox');
 if(ME.logged_in){
   b.innerHTML='<div class="who">Signed in as <b>'+ME.email+'</b> · <a href="#" onclick="logout();return false">Log out</a></div>';
   document.getElementById('mywrap').style.display='block';
 }else{
   document.getElementById('mywrap').style.display='none';
   b.innerHTML='<div class="card-auth"><b>Sign in</b> to keep your own watchlist'+
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
async function addSym(){
 const v=document.getElementById('addsym').value.trim();if(!v)return;
 const r=await fetch('/api/watch',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'add',symbol:v})});
 const d=await r.json();if(d.error){document.getElementById('msg').textContent=d.error;}else{document.getElementById('addsym').value='';load();}
}
async function delSym(s){
 await fetch('/api/watch',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'remove',symbol:s})});load();
}
async function load(){
 try{
  const d=await (await fetch('/api/quotes',{cache:'no-store'})).json();LAST=d;
  document.getElementById('asof').textContent='As of '+d.meta.as_of;
  document.getElementById('rule').textContent='Note: '+d.meta.rule;
  document.getElementById('warn').innerHTML=d.meta.have_key?'':'<div class="warn">No data key set. Add FINNHUB_API_KEY.</div>';
  document.getElementById('status').innerHTML=d.meta.market_open?'<span class="open">● Market open</span>':'<span class="closed">● Market closed</span>';
  const sh=d.shared.slice().sort((a,b)=>(b.from_low??-99)-(a.from_low??-99));
  document.getElementById('grid').innerHTML=sh.map(t=>card(t,false)).join('');
  if(ME.logged_in){
    const mine=(d.mine||[]).slice().sort((a,b)=>(b.from_low??-99)-(a.from_low??-99));
    document.getElementById('mygrid').innerHTML=mine.length?mine.map(t=>card(t,true)).join(''):'<div class="muted" style="font-size:13px">No stocks yet — add one above.</div>';
  }
 }catch(e){document.getElementById('asof').textContent='could not load data';}
}
function copyExcel(){
 const h=["Ticker","Price","Change %","% From Low","Prev Close","As Of"];
 const lines=[h.join("\\t")].concat(LAST.shared.map(t=>[t.ticker,t.price??"",t.change??"",t.from_low??"",t.prev_close??"",t.as_of||""].join("\\t")));
 navigator.clipboard.writeText(lines.join("\\n")).then(()=>{document.getElementById('msg').style.color='#047857';document.getElementById('msg').textContent='Copied!';setTimeout(()=>document.getElementById('msg').textContent='',2500);});
}
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
        elif self.path.startswith("/api/me"):
            uid = self._uid()
            self._json({"logged_in": bool(uid), "email": get_email(uid) if uid else None})
        elif self.path.startswith("/api/quotes"):
            uid = self._uid()
            out = {"meta": meta(), "shared": rows_for(SHARED_TICKERS), "mine": []}
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
    print(f"Storage: {'Postgres (DATABASE_URL)' if DATABASE_URL else 'local SQLite ('+DB_PATH+')'}")
    threading.Thread(target=refresher, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Stock Watch running on http://localhost:{PORT}  (Ctrl+C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
