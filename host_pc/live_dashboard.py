"""Live dashboard for the multi-site demo (runs next to the proposed gateway).

    DASH_TOKEN=secret SITES="core=Dublin@http://127.0.0.1:5002,cloud1=Tokyo@http://127.0.0.1:5003" \
    LINK_CMD="ssh -i ~/.ssh/ics_cloud -N -L 5003:127.0.0.1:5003 ubuntu@<cloud-private-ip>" \
    LINK_SITE=cloud1 python live_dashboard.py

Shows every site (alive/suspected, heartbeat RTT, adaptive timeout), where the demo slice process
really runs, the gateway's failure/recovery events, and a client that asks for the slice through
the gateway twice a second. Buttons inject faults: crash or hang the owner, or cut the network link
to LINK_SITE (the dashboard owns that SSH tunnel). Every request needs ?token=DASH_TOKEN.
"""
import os
import shlex
import subprocess
import threading
import time
import uuid

import requests
from flask import Flask, abort, jsonify, request

GATEWAY = os.getenv("GATEWAY_URL", "http://127.0.0.1:5101")
TOKEN = os.environ["DASH_TOKEN"]
SITES = {}                                   # name -> (label, url)
for item in os.getenv("SITES", "").split(","):
    if "=" in item:
        name, rest = item.split("=", 1)
        label, base = rest.split("@", 1)
        SITES[name.strip()] = (label.strip(), base.strip().rstrip("/"))
LINK_SITE, LINK_CMD = os.getenv("LINK_SITE", ""), os.getenv("LINK_CMD", "")

app = Flask(__name__)
state = {"rid": None, "since": 0.0, "samples": [], "notes": [], "link_down_until": 0.0}
lock = threading.Lock()
link = {"proc": None}


def note(text):
    with lock:
        state["notes"].append({"at": time.time(), "text": text})
        state["notes"] = state["notes"][-40:]


# ------------------------------------------------------------------ network link
def link_up():
    if LINK_CMD and (link["proc"] is None or link["proc"].poll() is not None):
        link["proc"] = subprocess.Popen(shlex.split(os.path.expanduser(LINK_CMD)),
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def link_down():
    if link["proc"] and link["proc"].poll() is None:
        link["proc"].kill()
        link["proc"].wait(timeout=5)


def link_keeper():
    while True:
        if time.time() >= state["link_down_until"]:
            if link["proc"] is None or link["proc"].poll() is not None:
                if state["link_down_until"]:
                    note(f"network link to {SITES.get(LINK_SITE, (LINK_SITE,))[0]} restored")
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
            try:
                r = session.get(f"{GATEWAY}/api/slices/{rid}", timeout=10)
                status = r.json().get("status", "?") if r.ok else f"http {r.status_code}"
            except requests.RequestException:
                status = "error"
            ms = (time.time() - started) * 1000
            with lock:
                state["samples"].append({"at": started, "ms": round(ms), "status": status})
                state["samples"] = state["samples"][-120:]
        time.sleep(0.5)


def placement(rid):
    out = {}
    for name, (_label, base) in SITES.items():
        try:
            r = requests.get(f"{base}/api/process/{rid}", headers={"X-Probe": "dashboard"}, timeout=3)
            out[name] = r.json().get("status") if r.ok else "-"
        except requests.RequestException:
            out[name] = "unreachable"
    return out


def owner():
    rid = state["rid"]
    if not rid:
        return None
    return next((n for n, s in placement(rid).items() if s == "running"), None)


# ------------------------------------------------------------------------- HTTP
@app.before_request
def check_token():
    if request.args.get("token") != TOKEN and request.headers.get("X-Token") != TOKEN:
        abort(403)


@app.get("/api/state")
def api_state():
    try:
        m = requests.get(f"{GATEWAY}/api/membership", timeout=5).json()
    except requests.RequestException:
        m = {"nodes": {}, "events": []}
    rid = state["rid"]
    with lock:
        samples, notes = list(state["samples"]), list(state["notes"])
    return jsonify({
        "now": time.time(), "rid": rid,
        "sites": [{"name": n, "label": SITES[n][0], **m["nodes"].get(n, {})} for n in SITES],
        "placement": placement(rid) if rid else {},
        "events": [e for e in m["events"] if rid and e["at"] >= state["since"]][-20:], "samples": samples, "notes": notes,
        "link_site": LINK_SITE, "link_down": time.time() < state["link_down_until"]})


@app.post("/api/action/<name>")
def action(name):
    if name == "create":
        for base in [b for _l, b in SITES.values()]:
            try:
                requests.post(f"{base}/api/reset", timeout=5)
            except requests.RequestException:
                pass
        r = requests.post(f"{GATEWAY}/api/processes", json={
            "name": "urllc-robot-arm", "logical_name": f"urllc-robot-arm-{uuid.uuid4().hex[:4]}",
            "slice_type": "URLLC", "cpu_needed": 1, "memory_needed": 128,
            "remaining_burst": 1000000}, timeout=30).json()
        with lock:
            state["rid"], state["samples"], state["since"] = r.get("resource_id"), [], time.time()
            state["notes"] = []
        note(f"client created a URLLC slice: {r.get('logical_uri')}")
        return jsonify(r)
    who = owner()
    if who is None:
        return jsonify({"error": "no running owner to fail"}), 409
    label, base = SITES[who]
    if name == "crash":
        try:
            requests.post(f"{base}/chaos/inject", json={"type": "crash"}, timeout=5)
        except requests.RequestException:
            pass
        note(f"FAILURE: worker in {label} crashed (it reboots in 10 s)")
    elif name == "hang":
        requests.post(f"{base}/chaos/inject", json={"type": "omission", "probability": 1.0,
                                                     "hold_s": 2.0, "duration": 12}, timeout=5)
        note(f"FAILURE: worker in {label} hung for 12 s")
    elif name == "cut":
        if who != LINK_SITE:
            return jsonify({"error": f"the link that can be cut is to {SITES[LINK_SITE][0]}; "
                                     f"the process is in {label}"}), 409
        state["link_down_until"] = time.time() + 20
        link_down()
        note(f"FAILURE: network link to {label} cut for 20 s (its worker keeps running)")
    else:
        abort(404)
    return jsonify({"ok": True, "owner": who})


@app.get("/")
def page():
    return PAGE.replace("__TOKEN__", TOKEN)


PAGE = r"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Slice platform live</title><style>
:root{--bg:#14213D;--card:#1E2D4F;--ink:#F7F6F2;--mut:#B9C4D6;--ok:#4CC38A;--bad:#F0883E;--blue:#8FB3F2}
body{margin:0;background:var(--bg);color:var(--ink);font:16px/1.4 Inter,Segoe UI,Arial,sans-serif}
main{max-width:1200px;margin:0 auto;padding:20px 16px}
h1{font-size:26px;margin:0 0 4px}.sub{color:var(--mut);margin:0 0 18px}
.sites{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px}
.site{background:var(--card);border-radius:10px;padding:16px;border-top:5px solid var(--ok)}
.site.down{border-top-color:var(--bad)}.site h2{margin:0;font-size:22px}
.badge{display:inline-block;padding:3px 10px;border-radius:99px;font-weight:600;margin-top:10px;font-size:15px}
.running{background:var(--ok);color:#10251a}.replicated{background:var(--blue);color:#0e1d3a}
.none{background:#33415f;color:var(--mut)}.unreachable{background:var(--bad);color:#2b1200}
.kv{color:var(--mut);font-size:14px;margin-top:8px}
.row{display:grid;grid-template-columns:2fr 1fr;gap:14px;margin-top:14px}
.panel{background:var(--card);border-radius:10px;padding:14px}.panel h3{margin:0 0 8px;font-size:16px;color:var(--mut)}
button{background:var(--blue);color:#0e1d3a;border:0;border-radius:8px;padding:10px 14px;font-weight:700;font-size:15px;margin:0 8px 8px 0;cursor:pointer}
button.f{background:var(--bad);color:#2b1200}
.ev{font-size:14px;border-bottom:1px solid #2c3d63;padding:4px 0}.ev b{color:var(--blue)}
svg{width:100%;height:180px;background:#17264A;border-radius:8px}
@media(max-width:800px){.row{grid-template-columns:1fr}}
</style></head><body><main>
<h1>Distributed 5G slice platform, live</h1>
<p class="sub" id="sub">Group 4 · ICS 2403 · proposed gateway with heartbeat failure detection</p>
<div>
<button onclick="act('create')">1 · Create URLLC slice</button>
<button class="f" onclick="act('crash')">Crash the owner</button>
<button class="f" onclick="act('hang')">Hang the owner</button>
<button class="f" onclick="act('cut')">Cut network link</button>
</div>
<div class="sites" id="sites"></div>
<div class="row">
<div class="panel"><h3>What the client sees: GET on the same logical URI every 0.5 s (ms, log scale)</h3><svg id="chart" viewBox="0 0 600 180" preserveAspectRatio="none"></svg>
<div class="kv" id="last"></div></div>
<div class="panel"><h3>Timeline</h3><div id="events"></div></div>
</div></main>
<script>
const T="__TOKEN__";
async function act(a){const r=await fetch('/api/action/'+a+'?token='+T,{method:'POST'});const j=await r.json();if(j.error)alert(j.error)}
function fmt(t,now){return '−'+Math.max(0,(now-t)).toFixed(1)+' s'}
async function tick(){
 let s;try{s=await (await fetch('/api/state?token='+T)).json()}catch(e){return}
 document.getElementById('sub').textContent=s.rid?('Slice process '+s.rid.slice(0,8)+'… — the client only knows its logical name'):'Press 1 to create a slice';
 document.getElementById('sites').innerHTML=s.sites.map(x=>{
  const p=s.placement[x.name]||'none';const lab=p==='running'?'RUNNING HERE':p==='replicated'?'standby copy':p==='unreachable'?'unreachable':'—';
  const alive=x.alive!==false;
  return `<div class="site ${alive?'':'down'}"><h2>${x.label}</h2><div class="kv">worker ${x.name} · ${alive?'alive':'SUSPECTED'}${s.link_down&&x.name===s.link_site?' · link cut':''}</div>
  <div class="kv">heartbeat RTT ${x.srtt_ms??'–'} ms · timeout ${x.rto_s??'–'} s</div>
  <span class="badge ${p==='running'?'running':p==='replicated'?'replicated':p==='unreachable'?'unreachable':'none'}">${lab}</span></div>`}).join('');
 const ev=[...s.notes.map(n=>({at:n.at,t:n.text})),...s.events.map(e=>({at:e.at,t:e.event==='suspected_and_failed_over'?`DETECTED ${e.node} failed → promoted standby in ${Math.round(e.promote_ms)} ms, name rebound`:e.event==='recovered'?`${e.node} rejoined: fenced ${e.fenced_duplicates}, re-protected ${e.reprotected_replicas}`:e.event+' '+e.node}))].sort((a,b)=>b.at-a.at).slice(0,12);
 document.getElementById('events').innerHTML=ev.map(e=>`<div class="ev"><b>${fmt(e.at,s.now)}</b> ${e.t}</div>`).join('');
 const sm=s.samples, svg=document.getElementById('chart');
 if(sm.length){const t0=s.now-60,y=v=>170-Math.min(160,Math.max(0,Math.log10(Math.max(v,1))/4*160));
  let h='';[10,100,1000].forEach(v=>{h+=`<line x1="0" x2="600" y1="${y(v)}" y2="${y(v)}" stroke="#2c3d63"/><text x="4" y="${y(v)-3}" fill="#B9C4D6" font-size="11">${v} ms</text>`});
  sm.filter(p=>p.at>=t0).forEach(p=>{const x=(p.at-t0)/60*600;h+=`<circle cx="${x}" cy="${y(p.ms)}" r="3.5" fill="${p.status==='running'?'#4CC38A':'#F0883E'}"/>`});
  svg.innerHTML=h;const L=sm[sm.length-1];document.getElementById('last').textContent=`last request: ${L.status} in ${L.ms} ms (green = running, orange = anything else)`}
}
setInterval(tick,700);tick();
</script></body></html>"""

if __name__ == "__main__":
    threading.Thread(target=link_keeper, daemon=True).start()
    threading.Thread(target=client_loop, daemon=True).start()
    app.run(host=os.getenv("DASH_HOST", "0.0.0.0"), port=int(os.getenv("DASH_PORT", "8080")), threaded=True)
