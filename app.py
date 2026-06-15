#!/usr/bin/env python3
"""
Stock Watch — Stage 1: Shared Dashboard (web app, Finnhub data)
============================================================
A small website showing live cards for a shared list of stocks:
current price, % from the day's low, change vs. previous close, plus a
"Copy for Excel" button.

Data source: Finnhub (https://finnhub.io). You need a FREE API key.
Prices on the free tier are delayed ~15 minutes — fine for watching;
upgrade to a paid real-time plan later if anyone trades off it.

----------------------------------------------------------------
GET A FREE KEY (2 min):
  1. Go to finnhub.io -> Sign up (free).
  2. Copy your API key from the dashboard.

RUN LOCALLY (preview on your Mac):
  Terminal:
     export FINNHUB_API_KEY=your_key_here
     python3 app.py
  then open http://localhost:8765

DEPLOY (Render): add an Environment Variable named FINNHUB_API_KEY
  with your key (see DEPLOY_GUIDE.md). Nothing else to change.
----------------------------------------------------------------
"""

import json
import os
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
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
FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
ET          = ZoneInfo("America/New_York")
PORT        = int(os.environ.get("PORT", "8765"))
CACHE_TTL   = 30      # seconds; shields the API from many viewers / rate limits
RISE_PCT    = 0.005   # reference highlight: 0.5% above the day's low
# ------------------------------------------------------------------

_cache = {"payload": None, "ts": 0.0}


def market_open(dt):
    if dt.weekday() >= 5:
        return False
    m = dt.hour * 60 + dt.minute
    return 9 * 60 + 30 <= m < 16 * 60


def fh_quote(sym):
    """Finnhub /quote -> {c:current, h:high, l:low, o:open, pc:prev close, t:epoch}."""
    url = f"https://finnhub.io/api/v1/quote?symbol={urllib.parse.quote(sym)}&token={FINNHUB_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "stock-watch"})
    with urllib.request.urlopen(req, timeout=12) as r:
        return json.loads(r.read().decode())


def one_row(t):
    try:
        q = fh_quote(t)
        c = q.get("c")
        low = q.get("l")
        pc = q.get("pc")
        ts = q.get("t")
        if not c:                     # 0 or None -> no data for this symbol
            return {"ticker": t, "price": None, "from_low": None,
                    "prev_close": None, "change": None, "as_of": "", "near": False}
        from_low = (c - low) / low * 100 if low else None
        change = (c - pc) / pc * 100 if pc else None
        as_of = datetime.fromtimestamp(ts, ET).strftime("%H:%M ET") if ts else ""
        return {
            "ticker": t,
            "price": round(c, 2),
            "from_low": None if from_low is None else round(from_low, 2),
            "prev_close": None if not pc else round(pc, 2),
            "change": None if change is None else round(change, 2),
            "as_of": as_of,
            "near": bool(from_low is not None and from_low >= RISE_PCT * 100),
        }
    except Exception:
        return {"ticker": t, "price": None, "from_low": None,
                "prev_close": None, "change": None, "as_of": "err", "near": False}


def fetch_quotes():
    if not FINNHUB_KEY:
        return {"as_of": "no API key", "market_open": False, "error": "missing_key",
                "rule": "", "rows": []}
    with ThreadPoolExecutor(max_workers=8) as ex:
        rows = list(ex.map(one_row, SHARED_TICKERS))
    now = datetime.now(ET)
    return {"as_of": now.strftime("%a %b %d, %H:%M:%S ET"),
            "market_open": market_open(now),
            "rule": f"highlighted when ≥ +{RISE_PCT*100:.1f}% above the day's low",
            "rows": rows}


def get_payload():
    if _cache["payload"] and (time.time() - _cache["ts"] < CACHE_TTL):
        return _cache["payload"]
    payload = fetch_quotes()
    _cache["payload"] = payload
    _cache["ts"] = time.time()
    return payload


PAGE = """<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Stock Watch</title>
<style>
:root{color-scheme:light}*{box-sizing:border-box}
body{margin:0;padding:18px;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;background:#f6f7f9;color:#16181d}
h1{font-size:20px;margin:0 0 2px}
.meta{color:#6b7280;font-size:12px}.rule{color:#374151;font-size:12px;margin:2px 0 12px}
.toolbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:12px}
button{font:inherit;font-size:13px;padding:7px 13px;border-radius:8px;border:1px solid #d1d5db;background:#fff;cursor:pointer}
button:hover{background:#f3f4f6}
.open{background:#ecfdf5;color:#047857;border:1px solid #a7f3d0;padding:5px 11px;border-radius:999px;font-size:12px}
.closed{background:#fef2f2;color:#b91c1c;border:1px solid #fecaca;padding:5px 11px;border-radius:999px;font-size:12px}
.warn{background:#fffbeb;color:#92400e;border:1px solid #fde68a;padding:10px 12px;border-radius:8px;font-size:13px;margin-bottom:12px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px}
.card{background:#fff;border:1px solid #e7e9ee;border-left:4px solid #cbd5e1;border-radius:10px;padding:10px 12px}
.card.near{border-left-color:#16a34a;background:#f0fdf4}
.tk{font-weight:700;font-size:15px}.pr{float:right;font-weight:600}
.row{font-size:12px;color:#4b5563;margin-top:3px}
.up{color:#16a34a;font-weight:600}.dn{color:#dc2626;font-weight:600}
.muted{color:#9ca3af}.foot{color:#9ca3af;font-size:11px;margin-top:16px}
#msg{font-size:12px;color:#047857}
</style></head><body>
<h1>📈 Stock Watch — shared list</h1>
<div class="meta" id="asof">loading…</div>
<div class="rule" id="rule"></div>
<div id="warn"></div>
<div class="toolbar">
  <span id="status"></span>
  <button onclick="refresh()">Refresh</button>
  <button onclick="copyExcel()">Copy for Excel</button>
  <span id="msg"></span>
</div>
<div class="grid" id="grid"></div>
<div class="foot">Data: Finnhub · free tier is delayed ~15 min · auto-refreshes every 30s · "Copy for Excel" copies a table you can paste into a spreadsheet.</div>
<script>
let LAST={rows:[]};
function pctSpan(v){if(v===null||v===undefined)return '<span class="muted">—</span>';var s=(v>=0?"+":"")+v.toFixed(2)+"%";return '<span class="'+(v>=0?'up':'dn')+'">'+s+'</span>';}
function money(v){return (v===null||v===undefined)?'<span class="muted">—</span>':'$'+v.toFixed(2);}
async function refresh(){
 try{
  const r=await fetch('/api/quotes',{cache:'no-store'});const d=await r.json();LAST=d;
  document.getElementById('asof').textContent='As of '+d.as_of;
  document.getElementById('rule').textContent=d.rule?('Note: '+d.rule):'';
  document.getElementById('warn').innerHTML = d.error==='missing_key'
    ? '<div class="warn">No data key set yet. Add an environment variable <b>FINNHUB_API_KEY</b> with your free Finnhub key, then reload.</div>' : '';
  document.getElementById('status').innerHTML=d.market_open?'<span class="open">● Market open</span>':'<span class="closed">● Market closed</span>';
  const rows=d.rows.slice().sort((a,b)=>(b.from_low??-99)-(a.from_low??-99));
  document.getElementById('grid').innerHTML=rows.map(card).join('');
 }catch(e){document.getElementById('asof').textContent='could not load data';}
}
function card(t){
 const cls=t.near?'card near':'card';
 return '<div class="'+cls+'"><span class="tk">'+t.ticker+'</span><span class="pr">'+money(t.price)+'</span>'+
  '<div class="row">change: '+pctSpan(t.change)+'</div>'+
  '<div class="row">from day low: '+pctSpan(t.from_low)+'</div>'+
  '<div class="row muted">prev close: '+(t.prev_close==null?'—':'$'+t.prev_close.toFixed(2))+' · '+(t.as_of||'')+'</div></div>';
}
function copyExcel(){
 const h=["Ticker","Price","Change %","% From Low","Prev Close","As Of"];
 const lines=[h.join("\\t")].concat(LAST.rows.map(t=>[t.ticker,t.price??"",t.change??"",t.from_low??"",t.prev_close??"",t.as_of||""].join("\\t")));
 navigator.clipboard.writeText(lines.join("\\n")).then(()=>{document.getElementById('msg').textContent='Copied! Paste into Excel.';
   setTimeout(()=>document.getElementById('msg').textContent='',3000);})
 .catch(()=>{document.getElementById('msg').textContent='Copy failed — click Refresh, then try again.';});
}
refresh();setInterval(refresh,30000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/quotes"):
            self._send(200, json.dumps(get_payload()).encode("utf-8"), "application/json")
        elif self.path in ("/", "/index.html"):
            self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
        elif self.path.startswith("/healthz"):
            self._send(200, b"ok", "text/plain")
        else:
            self._send(404, b"not found", "text/plain")


def main():
    if not FINNHUB_KEY:
        print("WARNING: FINNHUB_API_KEY is not set — the page will prompt you to add it.")
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Stock Watch running on http://localhost:{PORT}  (Ctrl+C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
