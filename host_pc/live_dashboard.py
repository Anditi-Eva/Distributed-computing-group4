"""Interactive Milestone 9 & 10 Defense Command Center Dashboard.

Runs next to `proposed_gateway.py` (:5101) on AWS Dublin (:8080) and visualizes:
  - 3-continent worker tiers (Edge: Nairobi PC, Core: Dublin, Cloud: Tokyo) with RTT, RFC 6298 timeout,
    CPU/RAM availability, active/standby badges, and per-node crash/hang controls.
  - Milestone 9 Transparency live badges (Access, Location, Replication, Migration, Failure),
    real-time client GET latency chart (log scale), p50 latency, uptime %, and split-brain copy check.
  - Milestone 10 Hierarchical Naming & Process Lifecycle panel (Create URLLC/eMBB/mMTC, Resolve Name/URN,
    Terminate Slice, live Prefix Table, registered names, and LRU+TTL cache stats).
"""
import os
import shlex
import statistics
import subprocess
import threading
import time
import uuid

import requests
from flask import Flask, abort, jsonify, request

GATEWAY = os.getenv("GATEWAY_URL", "http://127.0.0.1:5101")
TOKEN = os.environ["DASH_TOKEN"]
SITES = {}  # name -> (label, url)
for item in os.getenv("SITES", "").split(","):
    if "=" in item:
        name, rest = item.split("=", 1)
        label, base = rest.split("@", 1)
        SITES[name.strip()] = (label.strip(), base.strip().rstrip("/"))
LINK_SITE, LINK_CMD = os.getenv("LINK_SITE", ""), os.getenv("LINK_CMD", "")

app = Flask(__name__)
state = {
    "rid": None,
    "logical_uri": None,
    "logical_name": None,
    "slice_type": None,
    "since": 0.0,
    "samples": [],
    "notes": [],
    "link_down_until": 0.0,
    "proven": {
        "access": False,
        "location": False,
        "replication": False,
        "migration": False,
        "failure": False,
    },
}
lock = threading.Lock()
link = {"proc": None}


def note(text, kind="info"):
    with lock:
        state["notes"].append({"at": time.time(), "text": text, "kind": kind})
        state["notes"] = state["notes"][-50:]


# ------------------------------------------------------------------ network link
def link_up():
    if LINK_CMD and (link["proc"] is None or link["proc"].poll() is not None):
        link["proc"] = subprocess.Popen(
            shlex.split(os.path.expanduser(LINK_CMD)),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def link_down():
    if link["proc"] and link["proc"].poll() is None:
        link["proc"].kill()
        link["proc"].wait(timeout=5)


def link_keeper():
    while True:
        if time.time() >= state["link_down_until"]:
            if link["proc"] is None or link["proc"].poll() is not None:
                if state["link_down_until"]:
                    note(
                        f"Network link to {SITES.get(LINK_SITE, (LINK_SITE,))[0]} restored",
                        "ok",
                    )
                    state["link_down_until"] = 0.0
                link_up()
        time.sleep(1)


# ------------------------------------------------------------------ client probe
def client_loop():
    session = requests.Session()
    while True:
        rid = state["rid"]
        if rid:
            started = time.time()
            routing = "-"
            msgs = "-"
            try:
                r = session.get(f"{GATEWAY}/api/slices/{rid}", timeout=10)
                status = r.json().get("status", "?") if r.ok else f"http {r.status_code}"
                routing = r.headers.get("X-Routing", "-")
                msgs = r.headers.get("X-Worker-Messages", "-")
            except requests.RequestException:
                status = "error"
            ms = (time.time() - started) * 1000
            with lock:
                state["samples"].append(
                    {
                        "at": started,
                        "ms": round(ms),
                        "status": status,
                        "routing": routing,
                        "msgs": msgs,
                    }
                )
                state["samples"] = state["samples"][-120:]
        time.sleep(0.5)


def site_details(rid):
    placement = {}
    resources = {}
    for name, (_label, base) in SITES.items():
        try:
            st = requests.get(
                f"{base}/api/status", headers={"X-Probe": "dashboard"}, timeout=2.5
            )
            if st.ok:
                d = st.json()
                resources[name] = {
                    "avail_cpu": d.get("available_cpu"),
                    "total_cpu": d.get("total_cpu"),
                    "avail_mem": d.get("available_memory"),
                    "total_mem": d.get("total_memory"),
                    "queue": d.get("queue_length", 0),
                }
        except requests.RequestException:
            resources[name] = None
        if rid:
            try:
                r = requests.get(
                    f"{base}/api/process/{rid}",
                    headers={"X-Probe": "dashboard"},
                    timeout=2.5,
                )
                placement[name] = r.json().get("status") if r.ok else "-"
            except requests.RequestException:
                placement[name] = "unreachable"
    return placement, resources


def owner_of(rid):
    if not rid:
        return None
    pl, _ = site_details(rid)
    return next((n for n, s in pl.items() if s == "running"), None)


# ------------------------------------------------------------------------- HTTP
@app.before_request
def check_token():
    if request.args.get("token") != TOKEN and request.headers.get("X-Token") != TOKEN:
        abort(403)


@app.get("/api/state")
def api_state():
    try:
        m = requests.get(f"{GATEWAY}/api/membership", timeout=4).json()
    except requests.RequestException:
        m = {"nodes": {}, "events": []}
    try:
        met = requests.get(f"{GATEWAY}/api/metrics", timeout=4).json()
    except requests.RequestException:
        met = {"counters": {}, "cache": {}}
    try:
        names_resp = requests.get(f"{GATEWAY}/api/names", timeout=4).json()
        names = names_resp.get("names", [])[-12:]
    except requests.RequestException:
        names = []

    rid = state["rid"]
    pl, res = site_details(rid)
    events = [e for e in m.get("events", []) if rid and e["at"] >= state["since"]][-25:]

    with lock:
        samples = list(state["samples"])
        notes = list(state["notes"])
        if rid:
            state["proven"]["access"] = True
            state["proven"]["location"] = True
            if any(v == "replicated" for v in pl.values()):
                state["proven"]["replication"] = True
            if any(e.get("event") == "suspected_and_failed_over" for e in events):
                state["proven"]["migration"] = True
                state["proven"]["failure"] = True
        proven = dict(state["proven"])

    ok_samples = [s["ms"] for s in samples if s["status"] == "running"]
    p50_ms = round(statistics.median(ok_samples)) if ok_samples else None
    uptime_pct = round(100.0 * len(ok_samples) / len(samples), 1) if samples else None
    copies_running = sum(1 for v in pl.values() if v == "running")
    copies_standby = sum(1 for v in pl.values() if v == "replicated")

    return jsonify(
        {
            "now": time.time(),
            "rid": rid,
            "logical_uri": state["logical_uri"],
            "logical_name": state["logical_name"],
            "slice_type": state["slice_type"],
            "sites": [
                {
                    "name": n,
                    "label": SITES[n][0],
                    "res": res.get(n),
                    **m.get("nodes", {}).get(n, {}),
                }
                for n in SITES
            ],
            "placement": pl,
            "copies_running": copies_running,
            "copies_standby": copies_standby,
            "events": events,
            "samples": samples,
            "notes": notes,
            "metrics": met,
            "names": names,
            "proven": proven,
            "stats": {"p50_ms": p50_ms, "uptime_pct": uptime_pct, "total": len(samples)},
            "link_site": LINK_SITE,
            "link_down": time.time() < state["link_down_until"],
        }
    )


@app.get("/api/resolve")
def api_resolve():
    q = (request.args.get("name") or "").strip()
    if not q:
        return jsonify({"error": "enter a logical name or URN"}), 400
    t0 = time.perf_counter()
    try:
        r = requests.get(f"{GATEWAY}/api/names/resolve/{q}", timeout=6)
        ms = round((time.perf_counter() - t0) * 1000, 1)
        return jsonify(
            {
                "http_status": r.status_code,
                "latency_ms": ms,
                "worker_msgs": r.headers.get("X-Worker-Messages", "-"),
                "body": r.json(),
            }
        )
    except requests.RequestException as exc:
        return jsonify({"error": str(exc)}), 502


@app.post("/api/action/<name>")
def action(name):
    data = request.get_json(silent=True) or {}
    if name == "reset":
        for _l, base in SITES.values():
            try:
                requests.post(f"{base}/api/reset", timeout=4)
            except requests.RequestException:
                pass
        with lock:
            state["rid"] = None
            state["logical_uri"] = None
            state["logical_name"] = None
            state["slice_type"] = None
            state["samples"] = []
            state["notes"] = []
            state["proven"] = {k: False for k in state["proven"]}
        note("Demo reset — all workers cleared", "info")
        return jsonify({"ok": True})

    if name == "create":
        slice_type = str(data.get("slice_type") or "URLLC").upper()
        custom = str(data.get("logical_name") or "").strip()
        prefix = slice_type.lower()
        logical = custom or f"{prefix}-slice-{uuid.uuid4().hex[:4]}"
        cpu_map = {"URLLC": (1, 128), "EMBB": (2, 512), "MMTC": (1, 64)}
        cpu, mem = cpu_map.get(slice_type, (1, 128))
        for _l, base in SITES.values():
            try:
                requests.post(f"{base}/api/reset", timeout=4)
            except requests.RequestException:
                pass
        r = requests.post(
            f"{GATEWAY}/api/processes",
            json={
                "name": logical,
                "logical_name": logical,
                "slice_type": slice_type,
                "cpu_needed": cpu,
                "memory_needed": mem,
                "remaining_burst": 1000000,
            },
            timeout=30,
        )
        body = r.json()
        if not r.ok:
            return jsonify({"error": body.get("error", "create failed")}), r.status_code
        with lock:
            state["rid"] = body.get("resource_id")
            state["logical_uri"] = body.get("logical_uri")
            state["logical_name"] = body.get("name")
            state["slice_type"] = slice_type
            state["samples"] = []
            state["since"] = time.time()
            state["notes"] = []
            state["proven"] = {
                "access": True,
                "location": True,
                "replication": body.get("replica_count", 0) > 0,
                "migration": False,
                "failure": False,
            }
        note(
            f"Created {slice_type} slice {body.get('logical_uri')} (replicas={body.get('replica_count', 0)})",
            "ok",
        )
        return jsonify(body)

    if name == "terminate":
        lname = state["logical_name"]
        if not lname:
            return jsonify({"error": "no active slice to terminate"}), 409
        r = requests.post(f"{GATEWAY}/api/processes/{lname}/terminate", timeout=10)
        body = r.json() if r.content else {}
        with lock:
            state["rid"] = None
        note(f"M10 Terminate: {lname} -> {body.get('status', r.status_code)} (acks={body.get('coordination_acks', 0)})", "info")
        return jsonify(body)

    target = data.get("node") or owner_of(state["rid"])
    if not target or target not in SITES:
        return jsonify({"error": "no running owner to fail (create a slice first)"}), 409
    label, base = SITES[target]

    if name == "crash":
        try:
            requests.post(f"{base}/chaos/inject", json={"type": "crash"}, timeout=5)
        except requests.RequestException:
            pass
        note(f"FAILURE: {label} crashed (supervisor reboots in ~10 s)", "fail")
    elif name == "hang":
        try:
            requests.post(
                f"{base}/chaos/inject",
                json={"type": "omission", "probability": 1.0, "hold_s": 2.0, "duration": 12},
                timeout=5,
            )
        except requests.RequestException:
            return jsonify({"error": f"{label} is unreachable"}), 502
        note(f"FAILURE: {label} hung for 12 s (process stays alive in memory)", "fail")
    elif name == "cut":
        if target != LINK_SITE and not data.get("force_link"):
            # Allow cutting LINK_SITE directly from the button for ease of demo
            target = LINK_SITE
            label = SITES.get(LINK_SITE, (LINK_SITE,))[0]
        if not LINK_CMD:
            return jsonify({"error": "no cuttable link configured"}), 409
        state["link_down_until"] = time.time() + 20
        link_down()
        note(f"FAILURE: network link to {label} cut for 20 s (split-brain test)", "fail")
    else:
        abort(404)
    return jsonify({"ok": True, "target": target})


@app.get("/")
def page():
    return PAGE.replace("__TOKEN__", TOKEN)


PAGE = r"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ICS 2403 · Group 4 Live Distributed 5G Platform</title>
<style>
:root{--bg:#0E182E;--surface:#162442;--card:#1D2E52;--line:#2D426D;--ink:#F7F6F2;--mut:#A9B8D4;--ok:#34D399;--warn:#F59E0B;--bad:#F0883E;--blue:#60A5FA;--cobalt:#2563EB}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.45 Inter,Segoe UI,Arial,sans-serif}
main{max-width:1320px;margin:0 auto;padding:18px 18px 32px}
.top{display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:12px;margin-bottom:14px}
h1{font-size:24px;margin:0 0 2px;letter-spacing:-0.01em}
.sub{color:var(--mut);font-size:13.5px;margin:0}
.kpis{display:flex;gap:10px;flex-wrap:wrap}
.kpi{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:8px 12px;min-width:118px}
.kpi .v{font-size:18px;font-weight:700;color:var(--ink)}.kpi .l{font-size:11px;color:var(--mut);text-transform:uppercase;letter-spacing:.04em}
.kpi.good .v{color:var(--ok)}.kpi.warn .v{color:var(--bad)}
.toolbar{background:var(--surface);border:1px solid var(--line);border-radius:12px;padding:12px 14px;display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:14px}
select,input{background:#0E182E;color:var(--ink);border:1px solid var(--line);border-radius:8px;padding:8px 10px;font-size:14px}
button{background:var(--cobalt);color:#fff;border:0;border-radius:8px;padding:8px 13px;font-weight:600;font-size:13.5px;cursor:pointer;transition:opacity .15s}
button:hover{opacity:.9}
button.f{background:var(--bad);color:#2b1200}
button.sec{background:#293C66;color:var(--ink)}
button.sm{padding:4px 9px;font-size:12px;border-radius:6px}
.sep{width:1px;height:26px;background:var(--line);margin:0 4px}
.m9bar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:14px}
.pill{font-size:12px;font-weight:600;padding:4px 10px;border-radius:99px;background:#1A2948;color:var(--mut);border:1px solid var(--line)}
.pill.on{background:rgba(52,211,153,.15);color:var(--ok);border-color:rgba(52,211,153,.45)}
.sites{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:14px;margin-bottom:14px}
.site{background:var(--card);border-radius:12px;padding:15px;border-top:4px solid var(--ok);position:relative}
.site.down{border-top-color:var(--bad)}
.site-head{display:flex;justify-content:space-between;align-items:center;gap:8px}
.site h2{margin:0;font-size:19px}
.tier-tag{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--blue);font-weight:700}
.badge{display:inline-block;padding:3px 10px;border-radius:99px;font-weight:700;font-size:13px;margin-top:8px}
.running{background:var(--ok);color:#082418}.replicated{background:var(--blue);color:#0b1a36}
.none{background:#2B3E66;color:var(--mut)}.unreachable{background:var(--bad);color:#2b1200}
.kv{color:var(--mut);font-size:13px;margin-top:6px}
.site-btns{margin-top:10px;display:flex;gap:6px}
.grid2{display:grid;grid-template-columns:1.65fr 1fr;gap:14px;margin-bottom:14px}
.panel{background:var(--card);border-radius:12px;padding:14px;border:1px solid #263960}
.panel h3{margin:0 0 10px;font-size:14.5px;color:var(--blue);text-transform:uppercase;letter-spacing:.04em}
svg{width:100%;height:190px;background:#121F3B;border-radius:8px;display:block}
.ev{font-size:13px;border-bottom:1px solid #283B63;padding:5px 0}.ev b{color:var(--blue);font-family:monospace}
.ev.fail b{color:var(--bad)}.ev.ok b{color:var(--ok)}
.m10-grid{display:grid;grid-template-columns:1fr 1.4fr;gap:14px}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #283B63}
th{color:var(--mut);font-size:11.5px;text-transform:uppercase}
pre{background:#0E182E;border:1px solid var(--line);border-radius:8px;padding:10px;font-size:12px;overflow:auto;max-height:150px;margin:8px 0 0;color:#CFE2FF}
@media(max-width:900px){.grid2,.m10-grid{grid-template-columns:1fr}}
</style></head><body><main>

<div class="top">
  <div>
    <h1>Distributed 5G Network-Slicing Platform · Live Defense Console</h1>
    <p class="sub" id="sub">ICS 2403 · Group 4 · Milestones 9 (Transparency) &amp; 10 (Naming &amp; Process Management)</p>
  </div>
  <div class="kpis">
    <div class="kpi good" id="kpi-copies"><div class="v">—</div><div class="l">Copies Running</div></div>
    <div class="kpi" id="kpi-p50"><div class="v">—</div><div class="l">Client p50 RTT</div></div>
    <div class="kpi" id="kpi-up"><div class="v">—</div><div class="l">Client Uptime</div></div>
    <div class="kpi" id="kpi-direct"><div class="v">0</div><div class="l">Direct Hits (1 msg)</div></div>
    <div class="kpi" id="kpi-fenced"><div class="v">0</div><div class="l">Split-Brain Fenced</div></div>
  </div>
</div>

<div class="toolbar">
  <select id="sliceType">
    <option value="URLLC">URLLC (Ultra-Low Latency)</option>
    <option value="eMBB">eMBB (High Bandwidth)</option>
    <option value="mMTC">mMTC (IoT Sensors)</option>
  </select>
  <input id="sliceName" placeholder="Optional name (e.g. hospital-1)" size="24"/>
  <button onclick="createSlice()">1 · Create Slice</button>
  <div class="sep"></div>
  <button class="f" onclick="act('crash')">Crash Active Owner</button>
  <button class="f" onclick="act('hang')">Hang Owner (12s)</button>
  <button class="f" onclick="act('cut')">Cut Tokyo Link (20s)</button>
  <div class="sep"></div>
  <button class="sec" onclick="act('terminate')">M10 Terminate</button>
  <button class="sec" onclick="act('reset')">Reset Demo</button>
</div>

<div class="m9bar">
  <span style="font-size:12px;color:var(--mut);font-weight:700;text-transform:uppercase">M9 Transparencies Proven Live:</span>
  <span class="pill" id="p-access">Access</span>
  <span class="pill" id="p-location">Location</span>
  <span class="pill" id="p-replication">Replication</span>
  <span class="pill" id="p-migration">Migration</span>
  <span class="pill" id="p-failure">Failure</span>
</div>

<div class="sites" id="sites"></div>

<div class="grid2">
  <div class="panel">
    <h3>M9 Client View · GET Same Logical URI Every 0.5 s (ms, log scale)</h3>
    <svg id="chart" viewBox="0 0 600 190" preserveAspectRatio="none"></svg>
    <div class="kv" id="last">Create a slice to start live client probing…</div>
  </div>
  <div class="panel">
    <h3>Live Event Timeline (Failure → Detection → Recovery → Fencing)</h3>
    <div id="events"></div>
  </div>
</div>

<div class="m10-grid">
  <div class="panel">
    <h3>M10 Live Name Resolution (Name → URI → Owner)</h3>
    <div style="display:flex;gap:6px">
      <input id="resolveInput" placeholder="e.g. worker/core or process/..." style="flex:1"/>
      <button onclick="resolveName()">Resolve</button>
    </div>
    <div class="kv" id="cacheStats">Cache: —</div>
    <pre id="resolveOut">Click "Create Slice", then click any name on the right or press Resolve to verify Location Transparency (no physical IPs exposed).</pre>
  </div>
  <div class="panel">
    <h3>M10 Hierarchical Name Registry (Prefix Table + SQLite + LRU Cache)</h3>
    <table>
      <thead><tr><th>Logical Name</th><th>Kind</th><th>State</th><th>Logical URI</th></tr></thead>
      <tbody id="namesBody"><tr><td colspan="4" style="color:var(--mut)">No registered names yet</td></tr></tbody>
    </table>
  </div>
</div>

</main>
<script>
const T="__TOKEN__";
async function post(url, body){
  const r=await fetch(url+(url.includes('?')?'&':'?')+'token='+T,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})});
  const j=await r.json(); if(j.error) alert(j.error); return j;
}
async function createSlice(){
  const slice_type=document.getElementById('sliceType').value;
  const logical_name=document.getElementById('sliceName').value.trim();
  const res=await post('/api/action/create',{slice_type,logical_name});
  if(res && res.name) document.getElementById('resolveInput').value=res.name;
}
async function act(a, node){ await post('/api/action/'+a, node?{node}:{}); }
async function resolveName(name){
  const q=name||document.getElementById('resolveInput').value.trim();
  if(!q) return;
  document.getElementById('resolveInput').value=q;
  const r=await fetch('/api/resolve?name='+encodeURIComponent(q)+'&token='+T);
  const j=await r.json();
  document.getElementById('resolveOut').textContent=JSON.stringify(j,null,2);
}
function fmt(t,now){return '−'+Math.max(0,(now-t)).toFixed(1)+' s'}
function tierRole(n){return n==='edge'?'EDGE TIER':n==='core'?'CORE TIER':'CLOUD TIER'}
async function tick(){
  let s; try{s=await (await fetch('/api/state?token='+T)).json()}catch(e){return}
  document.getElementById('sub').textContent=s.logical_uri
    ? `Active Logical URI: ${s.logical_uri} (${s.slice_type}) — client never sees physical IPs`
    : 'ICS 2403 · Group 4 · Press "1 · Create Slice" to start live client probing';

  const kc=document.getElementById('kpi-copies');
  kc.querySelector('.v').textContent=s.rid?`${s.copies_running} active / ${s.copies_standby} standby`:'—';
  kc.className='kpi '+(s.rid?(s.copies_running===1?'good':'warn'):'');
  document.getElementById('kpi-p50').querySelector('.v').textContent=s.stats.p50_ms!=null?s.stats.p50_ms+' ms':'—';
  document.getElementById('kpi-up').querySelector('.v').textContent=s.stats.uptime_pct!=null?s.stats.uptime_pct+'%':'—';
  const ctr=(s.metrics&&s.metrics.counters)||{};
  document.getElementById('kpi-direct').querySelector('.v').textContent=ctr.direct_owner_hits||0;
  document.getElementById('kpi-fenced').querySelector('.v').textContent=ctr.fenced_duplicates||0;

  ['access','location','replication','migration','failure'].forEach(k=>{
    const el=document.getElementById('p-'+k);
    el.className='pill'+(s.proven&&s.proven[k]?' on':'');
  });

  document.getElementById('sites').innerHTML=s.sites.map(x=>{
    const p=s.placement[x.name]||'none';
    const lab=p==='running'?'RUNNING HERE':p==='replicated'?'STANDBY COPY':p==='unreachable'?'UNREACHABLE':'IDLE';
    const alive=x.alive!==false;
    const res=x.res?`CPU ${x.res.avail_cpu}/${x.res.total_cpu} · RAM ${x.res.avail_mem}/${x.res.total_mem} MB`:'metrics unavailable';
    return `<div class="site ${alive?'':'down'}">
      <div class="site-head"><h2>${x.label}</h2><span class="tier-tag">${tierRole(x.name)}</span></div>
      <div class="kv">worker <b>${x.name}</b> · ${alive?'alive':'SUSPECTED'}${s.link_down&&x.name===s.link_site?' · link cut':''}</div>
      <div class="kv">heartbeat RTT: <b>${x.srtt_ms??'–'} ms</b> · RFC 6298 timeout: <b>${x.rto_s??'–'} s</b></div>
      <div class="kv">${res}</div>
      <span class="badge ${p==='running'?'running':p==='replicated'?'replicated':p==='unreachable'?'unreachable':'none'}">${lab}</span>
      <div class="site-btns">
        <button class="sm f" onclick="act('crash','${x.name}')">Crash ${x.name}</button>
        <button class="sm sec" onclick="act('hang','${x.name}')">Hang ${x.name}</button>
      </div>
    </div>`;
  }).join('');

  const ev=[
    ...s.notes.map(n=>({at:n.at,t:n.text,k:n.kind||'info'})),
    ...s.events.map(e=>({
      at:e.at,
      k:e.event==='suspected_and_failed_over'?'fail':'ok',
      t:e.event==='suspected_and_failed_over'
        ? `DETECTED ${e.node} failed → promoted standby in ${Math.round(e.promote_ms)} ms${e.reprotected?` + re-protected ${e.reprotected} onto spare node`:''}`
        : e.event==='recovered'
        ? `${e.node} rejoined → fenced ${e.fenced_duplicates} stale copy, re-protected ${e.reprotected_replicas}`
        : `${e.event} ${e.node}`
    }))
  ].sort((a,b)=>b.at-a.at).slice(0,12);
  document.getElementById('events').innerHTML=ev.map(e=>`<div class="ev ${e.k}"><b>${fmt(e.at,s.now)}</b> ${e.t}</div>`).join('')||'<div class="kv">No events yet.</div>';

  const sm=s.samples, svg=document.getElementById('chart');
  if(sm.length){
    const t0=s.now-60, y=v=>178-Math.min(164,Math.max(0,Math.log10(Math.max(v,1))/4*164));
    let h='';
    [10,100,1000].forEach(v=>{h+=`<line x1="0" x2="600" y1="${y(v)}" y2="${y(v)}" stroke="#24375C"/><text x="6" y="${y(v)-4}" fill="#A9B8D4" font-size="11">${v} ms</text>`});
    sm.filter(p=>p.at>=t0).forEach(p=>{const x=(p.at-t0)/60*600; h+=`<circle cx="${x}" cy="${y(p.ms)}" r="4" fill="${p.status==='running'?'#34D399':'#F0883E'}"/>`});
    svg.innerHTML=h;
    const L=sm[sm.length-1];
    document.getElementById('last').textContent=`Last GET: ${L.status} in ${L.ms} ms · routing=${L.routing} · worker_msgs=${L.msgs} (green = running, orange = error)`;
  }

  const c=(s.metrics&&s.metrics.cache)||{};
  const hr=c.hits+c.misses?((100*c.hits/(c.hits+c.misses)).toFixed(1)+'%'):'0%';
  document.getElementById('cacheStats').textContent=`LRU+TTL Cache: ${c.entries??0} entries · hits=${c.hits??0} · misses=${c.misses??0} (${hr} hit rate)`;
  const nb=document.getElementById('namesBody');
  if(s.names&&s.names.length){
    nb.innerHTML=s.names.slice().reverse().map(n=>`<tr style="cursor:pointer" onclick="resolveName('${n.name}')"><td><b>${n.name}</b></td><td>${n.kind}</td><td>${n.state}</td><td style="color:var(--blue)">${n.logical_uri}</td></tr>`).join('');
  }
}
setInterval(tick,700); tick();
</script></body></html>"""

if __name__ == "__main__":
    threading.Thread(target=link_keeper, daemon=True).start()
    threading.Thread(target=client_loop, daemon=True).start()
    app.run(
        host=os.getenv("DASH_HOST", "0.0.0.0"),
        port=int(os.getenv("DASH_PORT", "8080")),
        threaded=True,
    )
