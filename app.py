#!/usr/bin/env python3
"""
Stock Watch — Stage 1: Shared Dashboard (web app)
============================================================
A small website that shows live cards for a shared list of stocks:
price, % from the day's intraday low, the 3-minute average, pre-market
price, and change vs. the previous close. Plus a "Copy for Excel" button.

This is the first stage of the shared cloud version. It runs the same
whether on your Mac (to preview) or on a web host (to share a link).

RUN LOCALLY (preview on your own computer):
    pip3 install --upgrade yfinance pandas
    python3 app.py
    then open  http://localhost:8765

DEPLOY (share a link): see DEPLOY_GUIDE.md — the host sets the PORT
environment variable automatically; nothing to change in this file.
============================================================
"""

import json
import os
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from zoneinfo import ZoneInfo
except ImportError:
    raise SystemExit("Python 3.9+ required.")

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    raise SystemExit("Missing dependencies. Run: pip3 install --upgrade yfinance pandas")

# ----------------------------- CONFIG -----------------------------
SHARED_TICKERS = [
    "AAPL", "ADI", "ADMA", "AMZN", "BABA", "CBRL", "CL", "COPX", "CUBE", "CVX",
    "DE", "FUTU", "GE", "GEV", "GLD", "GOOG", "IEP", "INTU", "JNJ", "JPM",
    "KO", "LLY", "LMT", "MA", "MAIN", "META", "MSFT", "MU", "NVDA", "PFE",
    "RIO", "SLV", "TSLA", "VZ", "WMT", "ADSK", "AVGO", "SPCX",
]
RISE_PCT     = 0.005          # reference: 0.5% from the intraday low
AVG_MINUTES  = 3              # trailing average window
ET           = ZoneInfo("America/New_York")
PORT         = int(os.environ.get("PORT", "8765"))   # host sets PORT; default 8765 locally
CACHE_TTL    = 30             # seconds; shields the data source from many viewers
# ------------------------------------------------------------------

_cache = {"payload": None, "ts": 0.0}
_lock = threading.Lock()


def market_open(dt):
    if dt.weekday() >= 5:
        return False
    m = dt.hour * 60 + dt.minute
    return 9 * 60 + 30 <= m < 16 * 60


def fetch_quotes():
    intraday = yf.download(" ".join(SHARED_TICKERS), period="1d", interval="1m",
                           prepost=True, group_by="ticker", progress=False,
                           auto_adjust=False, threads=True)
    daily = yf.download(" ".join(SHARED_TICKERS), period="7d", interval="1d",
                        prepost=False, group_by="ticker", progress=False,
                        auto_adjust=False, threads=True)

    def sub(frame, t):
        try:
            d = frame[t] if len(SHARED_TICKERS) > 1 else frame
            return d.dropna(subset=["Close"])
        except Exception:
            return None

    rows = []
    for t in SHARED_TICKERS:
        price = low = avg3 = prev = pct = chg = None
        as_of = ""
        idf = sub(intraday, t)
        if idf is not None and len(idf):
            price = float(idf["Close"].iloc[-1])
            low = float(idf["Low"].min())
            if len(idf) >= AVG_MINUTES:
                avg3 = float(idf["Close"].iloc[-AVG_MINUTES:].mean())
            ts = idf.index[-1]
            try:
                as_of = ts.tz_convert(ET).strftime("%H:%M ET")
            except Exception:
                as_of = str(ts)[-14:-3]
        ddf = sub(daily, t)
        if ddf is not None and len(ddf):
            prev = float(ddf["Close"].iloc[-1])
        if price is not None and low and low > 0:
            pct = (price - low) / low * 100
        if price is not None and prev:
            chg = (price - prev) / prev * 100
        rows.append({
            "ticker": t,
            "price": None if price is None else round(price, 2),
            "from_low": None if pct is None else round(pct, 2),
            "avg3": None if avg3 is None else round(avg3, 2),
            "prev_close": None if prev is None else round(prev, 2),
            "change": None if chg is None else round(chg, 2),
            "as_of": as_of,
            "near": bool(pct is not None and avg3 is not None
                         and pct >= RISE_PCT * 100 and price > avg3),
        })
    now = datetime.now(ET)
    return {"as_of": now.strftime("%a %b %d, %H:%M:%S ET"),
            "market_open": market_open(now),
            "rule": f"+{RISE_PCT*100:.1f}% from intraday low AND above {AVG_MINUTES}-min avg",
            "rows": rows}


def get_payload():
    with _lock:
        if _cache["payload"] and (time.time() - _cache["ts"] < CACHE_TTL):
            return _cache["payload"]
    try:
        payload = fetch_quotes()
    except Exception as e:
        payload = {"as_of": "data error", "market_open": False,
                   "rule": "", "rows": [], "error": str(e)}
    with _lock:
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
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(165px,1fr));gap:10px}
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
<div class="toolbar">
  <span id="status"></span>
  <button onclick="refresh()">Refresh</button>
  <button onclick="copyExcel()">Copy for Excel</button>
  <span id="msg"></span>
</div>
<div class="grid" id="grid"></div>
<div class="foot">Auto-refreshes every 30s · prices may be ~15 min delayed · "Copy for Excel" copies a table you can paste into a spreadsheet.</div>
<script>
let LAST={rows:[]};
function pctSpan(v){if(v===null)return '<span class="muted">—</span>';var s=(v>=0?"+":"")+v.toFixed(2)+"%";return '<span class="'+(v>=0?'up':'dn')+'">'+s+'</span>';}
function money(v){return v===null?'<span class="muted">—</span>':'$'+v.toFixed(2);}
async function refresh(){
 try{
  const r=await fetch('/api/quotes',{cache:'no-store'});const d=await r.json();LAST=d;
  document.getElementById('asof').textContent='As of '+d.as_of;
  document.getElementById('rule').textContent=d.rule?('Highlight rule: '+d.rule):'';
  document.getElementById('status').innerHTML=d.market_open?'<span class="open">● Market open</span>':'<span class="closed">● Market closed</span>';
  const rows=d.rows.slice().sort((a,b)=>(b.from_low??-99)-(a.from_low??-99));
  document.getElementById('grid').innerHTML=rows.map(card).join('');
 }catch(e){document.getElementById('asof').textContent='could not load data';}
}
function card(t){
 const cls=t.near?'card near':'card';
 return '<div class="'+cls+'"><span class="tk">'+t.ticker+'</span><span class="pr">'+money(t.price)+'</span>'+
  '<div class="row">from low: '+pctSpan(t.from_low)+'</div>'+
  '<div class="row">pre-mkt/last chg: '+pctSpan(t.change)+'</div>'+
  '<div class="row muted">3-min avg: '+(t.avg3===null?'—':'$'+t.avg3.toFixed(2))+' · '+(t.as_of||'')+'</div></div>';
}
function copyExcel(){
 const h=["Ticker","Price","% From Low","Prev Close","Change %","3-min Avg","As Of"];
 const lines=[h.join("\\t")].concat(LAST.rows.map(t=>[t.ticker,t.price??"",t.from_low??"",t.prev_close??"",t.change??"",t.avg3??"",t.as_of||""].join("\\t")));
 const text=lines.join("\\n");
 navigator.clipboard.writeText(text).then(()=>{document.getElementById('msg').textContent='Copied! Paste into Excel.';
   setTimeout(()=>document.getElementById('msg').textContent='',3000);})
 .catch(()=>{document.getElementById('msg').textContent='Copy failed — try the Refresh then again.';});
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
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Stock Watch running on http://localhost:{PORT}  (Ctrl+C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
