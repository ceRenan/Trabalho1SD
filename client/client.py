import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from common import config
from common.net import recv_message, send_message
from common.util import status_name, type_name
from protos import datacenter_pb2 as pb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DEFAULT_PROCESSES = [
    ("gateway",    "Gateway DCIM",              ["gateway.gateway"]),
    ("crac-01",    "Fonte A: CRAC (Sala 1)",    ["sources.crac", "--id", "crac-01", "--room", "SALA-1"]),
    ("sensor-R01", "Fonte B: Sensor Rack R01",  ["sources.rack_sensor", "--rack", "R01", "--base-temp", "23.5"]),
    ("sensor-R02", "Fonte B: Sensor Rack R02",  ["sources.rack_sensor", "--rack", "R02", "--base-temp", "25.0"]),
    ("sensor-R03", "Fonte B: Sensor Rack R03",  ["sources.rack_sensor", "--rack", "R03", "--base-temp", "28.0"]),
    ("pdu-R01",    "Fonte C: PDU Rack R01",     ["sources.pdu", "--rack", "R01", "--base-kw", "4.5"]),
    ("pdu-R02",    "Fonte C: PDU Rack R02",     ["sources.pdu", "--rack", "R02", "--base-kw", "6.0"]),
    ("pdu-R03",    "Fonte C: PDU Rack R03",     ["sources.pdu", "--rack", "R03", "--base-kw", "7.5"]),
]


class GatewayConnection:
    def __init__(self, host: str, port: int, timeout: float = 15.0):
        self.host, self.port, self.timeout = host, port, timeout
        self.sock = None
        self._next_id = 0

    def connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)

    def close(self) -> None:
        if self.sock:
            self.sock.close()
            self.sock = None

    def request(self, **body) -> pb.GatewayResponse:
        self._next_id += 1
        req = pb.ClientRequest(request_id=self._next_id, **body)
        for attempt in (1, 2):
            try:
                if self.sock is None:
                    self.connect()
                send_message(self.sock, req)
                return recv_message(self.sock, pb.GatewayResponse)
            except (ConnectionError, OSError) as exc:
                self.close()
                if attempt == 2:
                    raise ConnectionError(f"Gateway indisponivel em {self.host}:{self.port} ({exc})")
        raise AssertionError()


def fmt_settings(state: pb.DeviceState) -> str:
    which = state.WhichOneof("settings")
    base = f"periodo {state.report_interval_s}s"
    if which == "ac":
        return f"setpoint {state.ac.setpoint_c:.1f}C, vent {state.ac.fan_speed_pct}%, {base}"
    if which == "sensor":
        return f"limiar {state.sensor.alert_threshold_c:.1f}C, {base}"
    if which == "pdu":
        return f"capacidade {state.pdu.rated_capacity_kw:.1f}kW, {base}"
    return base


def fmt_packet(packet: pb.DataPacket) -> str:
    which = packet.WhichOneof("payload")
    if which == "ac":
        a = packet.ac
        return f"insuflamento {a.supply_air_c:.1f}C, retorno {a.return_air_c:.1f}C, {a.power_kw:.2f}kW"
    if which == "environment":
        e = packet.environment
        return f"{e.temperature_c:.1f}C, umidade {e.humidity_pct:.0f}%"
    if which == "pdu":
        p = packet.pdu
        return f"{p.power_kw:.2f}kW, {p.current_a:.1f}A, {p.energy_kwh:.4f}kWh"
    if packet.source_id:
        return "(heartbeat sem dados)"
    return "-"


class ProcessManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._procs = {}
        self._specs = {k: (label, cmd) for k, label, cmd in DEFAULT_PROCESSES}

    def _external_pids(self) -> dict:
        if not sys.platform.startswith("win"):
            return {}
        try:
            out = subprocess.check_output(
                ["powershell", "-NoProfile", "-Command",
                 "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
                 "ForEach-Object { \"$($_.ProcessId)|$($_.CommandLine)\" }"],
                text=True, timeout=2.5, creationflags=subprocess.CREATE_NO_WINDOW
            )
        except Exception:
            return {}
        found = {}
        for line in out.splitlines():
            if "|" not in line:
                continue
            pid_str, cmdline = line.split("|", 1)
            if "gateway.gateway" in cmdline:
                found["gateway"] = int(pid_str)
            elif "sources.crac" in cmdline and "crac-01" in cmdline:
                found["crac-01"] = int(pid_str)
            elif "sources.rack_sensor" in cmdline:
                for r in ("R01", "R02", "R03"):
                    if f"--rack {r}" in cmdline:
                        found[f"sensor-{r}"] = int(pid_str)
            elif "sources.pdu" in cmdline:
                for r in ("R01", "R02", "R03"):
                    if f"--rack {r}" in cmdline:
                        found[f"pdu-{r}"] = int(pid_str)
        return found

    def start(self, key: str) -> bool:
        with self._lock:
            if key not in self._specs:
                return False
            proc = self._procs.get(key)
            if proc and proc.poll() is None:
                return True
            if key in self._external_pids():
                return True
            _, args = self._specs[key]
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            creationflags = subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0
            self._procs[key] = subprocess.Popen(
                [sys.executable, "-m", *args],
                cwd=ROOT,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=creationflags,
            )
            return True

    def stop(self, key: str) -> bool:
        with self._lock:
            proc = self._procs.pop(key, None)
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                return True
            ext_pid = self._external_pids().get(key)
            if ext_pid:
                subprocess.run(["taskkill", "/F", "/PID", str(ext_pid)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                return True
            return False

    def start_all(self) -> None:
        self.start("gateway")
        for key, _, _ in DEFAULT_PROCESSES:
            if key != "gateway":
                self.start(key)

    def stop_all(self) -> None:
        for key in list(self._specs):
            self.stop(key)

    def status_list(self, gw_connected: bool = False, active_source_ids=()):
        with self._lock:
            ext = self._external_pids()
            out = []
            for key, label, _ in DEFAULT_PROCESSES:
                proc = self._procs.get(key)
                running = (proc is not None and proc.poll() is None) or (key in ext)
                if key == "gateway" and gw_connected:
                    running = True
                elif key in active_source_ids:
                    running = True
                pid = (proc.pid if (proc and proc.poll() is None) else ext.get(key))
                out.append({"id": key, "label": label, "running": running, "pid": pid})
            return out


def serialize_source(s: pb.SourceSummary) -> dict:
    p = s.last_packet
    which = p.WhichOneof("payload")
    metrics = {}
    if which == "ac":
        metrics = {"supply_air_c": round(p.ac.supply_air_c, 2),
                   "return_air_c": round(p.ac.return_air_c, 2),
                   "setpoint_c": round(p.ac.setpoint_c, 1),
                   "fan_speed_pct": p.ac.fan_speed_pct,
                   "power_kw": round(p.ac.power_kw, 2)}
    elif which == "environment":
        metrics = {"temperature_c": round(p.environment.temperature_c, 2),
                   "humidity_pct": round(p.environment.humidity_pct, 1),
                   "alert_threshold_c": round(p.environment.alert_threshold_c, 1)}
    elif which == "pdu":
        metrics = {"power_kw": round(p.pdu.power_kw, 2),
                   "current_a": round(p.pdu.current_a, 1),
                   "voltage_v": round(p.pdu.voltage_v, 1),
                   "energy_kwh": round(p.pdu.energy_kwh, 4)}

    st = s.state
    settings = {"report_interval_s": st.report_interval_s,
                "powered_on": st.powered_on,
                "failure_simulated": st.failure_simulated}
    if st.WhichOneof("settings") == "ac":
        settings["setpoint_c"] = round(st.ac.setpoint_c, 1)
        settings["fan_speed_pct"] = st.ac.fan_speed_pct
    elif st.WhichOneof("settings") == "sensor":
        settings["alert_threshold_c"] = round(st.sensor.alert_threshold_c, 1)
    elif st.WhichOneof("settings") == "pdu":
        settings["rated_capacity_kw"] = round(st.pdu.rated_capacity_kw, 1)
        settings["overload_pct"] = round(st.pdu.overload_pct, 1)

    return {
        "source_id": s.info.source_id,
        "source_type": pb.SourceType.Name(s.info.source_type),
        "type_label": type_name(s.info.source_type),
        "rack_id": s.info.rack_id,
        "endpoint": f"{s.info.ip}:{s.info.port}",
        "controllable": s.info.controllable,
        "status": status_name(s.status),
        "settings_str": fmt_settings(s.state),
        "settings": settings,
        "last_reading_str": fmt_packet(p),
        "metrics": metrics,
        "packets_received": s.packets_received,
        "packets_lost": s.packets_lost,
        "last_seen_ms": s.last_seen_ms,
        "last_event": pb.EventKind.Name(p.event) if p.source_id else "NO_EVENT",
        "last_event_desc": p.event_description,
    }


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>DCIM - Monitoramento e Controle</title>
<style>
  :root {
    --bg: #0f172a; --panel: #1e293b; --panel2: #273549; --border: #334155;
    --text: #f1f5f9; --muted: #94a3b8; --accent: #38bdf8;
    --ok: #22c55e; --warn: #f59e0b; --err: #ef4444; --off: #64748b;
  }
  * { box-sizing: border-box; font-family: 'Segoe UI', system-ui, sans-serif; }
  body { margin: 0; background: var(--bg); color: var(--text); }
  header {
    background: #0b1120; border-bottom: 1px solid var(--border);
    padding: 14px 24px; display: flex; align-items: center; justify-content: space-between;
    flex-wrap: wrap; gap: 12px;
  }
  header h1 { margin: 0; font-size: 1.15rem; display: flex; align-items: center; gap: 10px; }
  .pill { font-size: 0.78rem; padding: 4px 10px; border-radius: 999px; font-weight: 600; }
  .pill.ONLINE { background: rgba(34,197,94,0.18); color: #4ade80; border: 1px solid #22c55e55; }
  .pill.STANDBY { background: rgba(245,158,11,0.18); color: #fbbf24; border: 1px solid #f59e0b55; }
  .pill.FALHA { background: rgba(239,68,68,0.2); color: #f87171; border: 1px solid #ef444466; }
  .pill.OFFLINE { background: rgba(100,116,139,0.25); color: #cbd5e1; border: 1px solid #64748b66; }
  main { max-width: 1400px; margin: 0 auto; padding: 18px 24px; display: grid; gap: 18px; }
  .card { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
  .card h2 { margin: 0 0 12px 0; font-size: 0.98rem; color: var(--accent); display: flex; justify-content: space-between; align-items: center; }
  .grid-sources { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 14px; }
  .src-card { background: var(--panel2); border: 1px solid var(--border); border-radius: 6px; padding: 14px; display: flex; flex-direction: column; gap: 10px; }
  .src-head { display: flex; justify-content: space-between; align-items: flex-start; }
  .src-title { font-weight: 700; font-size: 1rem; }
  .src-sub { font-size: 0.78rem; color: var(--muted); }
  .metrics { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; background: #0f172a88; padding: 10px; border-radius: 6px; }
  .m-item { font-size: 0.8rem; color: var(--muted); }
  .m-val { font-size: 1.05rem; font-weight: 700; color: var(--text); display: block; }
  .controls { display: flex; flex-wrap: wrap; gap: 6px; padding-top: 6px; border-top: 1px solid var(--border); }
  button {
    background: #0284c7; color: white; border: none; padding: 6px 11px;
    border-radius: 5px; cursor: pointer; font-size: 0.8rem; font-weight: 600;
  }
  button:hover { background: #0369a1; }
  button.sec { background: #334155; }
  button.sec:hover { background: #475569; }
  button.danger { background: #b91c1c; }
  button.danger:hover { background: #991b1b; }
  button.ok { background: #15803d; }
  button.ok:hover { background: #166534; }
  input[type=number], select {
    background: #0f172a; color: var(--text); border: 1px solid var(--border);
    padding: 5px 8px; border-radius: 5px; width: 78px; font-size: 0.82rem;
  }
  .inline-ctrl { display: flex; align-items: center; gap: 4px; font-size: 0.78rem; color: var(--muted); }
  .two-col { display: grid; grid-template-columns: 1.1fr 1fr; gap: 18px; }
  @media (max-width: 980px) { .two-col { grid-template-columns: 1fr; } }
  table { width: 100%; border-collapse: collapse; font-size: 0.85rem; margin-top: 10px; }
  th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--border); }
  th { color: var(--muted); font-weight: 600; background: #0f172a66; }
  .summary-box { margin-top: 10px; padding: 10px 12px; background: #0f172a; border-left: 4px solid var(--accent); border-radius: 4px; font-size: 0.9rem; }
  .proc-bar { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
  .proc-chip { display: flex; align-items: center; gap: 6px; background: var(--panel2); border: 1px solid var(--border); padding: 5px 10px; border-radius: 5px; font-size: 0.78rem; }
  .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
  .dot.on { background: var(--ok); }
  .dot.off { background: var(--off); }
  canvas { width: 100%; height: 220px; background: #0f172a; border-radius: 6px; border: 1px solid var(--border); }
  .toast { position: fixed; bottom: 18px; right: 18px; background: #0284c7; color: #fff; padding: 10px 16px; border-radius: 6px; font-size: 0.85rem; display: none; }
  .notif-list { max-height: 210px; overflow-y: auto; display: flex; flex-direction: column; gap: 6px; }
  .notif-item { display: flex; justify-content: space-between; align-items: center; gap: 10px; padding: 8px 12px; border-radius: 5px; font-size: 0.82rem; background: var(--panel2); border-left: 4px solid var(--accent); }
  .notif-item.ALARM_RAISED { border-left-color: var(--err); background: rgba(239,68,68,0.1); }
  .notif-item.ALARM_CLEARED { border-left-color: var(--ok); background: rgba(34,197,94,0.08); }
  .notif-item.STATE_CHANGED { border-left-color: var(--accent); }
  .notif-item.OFFLINE { border-left-color: var(--warn); background: rgba(245,158,11,0.1); }
</style>
</head>
<body>
<header>
  <h1>DCIM - Monitoramento e Controle de Data Center <span id="gwBadge" class="pill OFFLINE">Conectando...</span></h1>
  <div style="display:flex; gap:8px; align-items:center;">
    <button class="ok" onclick="procAction('start_all')">Iniciar Todos</button>
    <button class="danger" onclick="procAction('stop_all')">Parar Todos</button>
    <button class="sec" onclick="refreshAll()">Atualizar</button>
  </div>
</header>

<main>
  <div class="card">
    <h2>
      <span>Processos Locais</span>
    </h2>
    <div id="procList" class="proc-bar"></div>
  </div>

  <div class="card">
    <h2>
      <span>Fontes de Dados Descobertas (Multicast / UDP / gRPC)</span>
      <span id="srcCount" style="font-size:0.8rem; color:var(--muted);"></span>
    </h2>
    <div id="sourcesGrid" class="grid-sources"></div>
  </div>

  <div class="two-col">
    <div class="card">
      <h2>Consultas Analiticas Agregadas (TCP)</h2>
      <div style="display:flex; flex-wrap:wrap; gap:6px; margin-bottom:8px;">
        <button onclick="runQuery('HOTTEST_RACK')">1. Rack Mais Quente</button>
        <button onclick="runQuery('PEAK_POWER')">2. Pico de Consumo (24h)</button>
        <button onclick="runQuery('AVERAGE_TEMPERATURE')">3. Temp. Media (1h)</button>
        <button onclick="runQuery('ENERGY_BY_RACK')">4. Energia por Rack</button>
        <button onclick="runQuery('ALERTS_BY_RACK')">5. Alertas por Rack</button>
      </div>
      <div id="queryTitle" style="font-weight:700; margin-top:6px; color:var(--accent);"></div>
      <div id="querySummary" class="summary-box">Selecione uma consulta acima.</div>
      <div id="queryTableWrap"></div>
    </div>

    <div class="card">
      <h2>
        <span>Serie Temporal de Leituras</span>
        <select id="chartSource" onchange="loadHistory()" style="width:170px;"></select>
      </h2>
      <canvas id="tsCanvas" width="600" height="220"></canvas>
      <div id="histSummary" style="font-size:0.78rem; color:var(--muted); margin-top:6px;"></div>
    </div>
  </div>

  <div class="card">
    <h2>
      <span>Registro de Eventos e Falhas <span id="notifCount" style="color:var(--muted); font-weight:normal; font-size:0.82rem;"></span></span>
      <button class="sec" style="padding:4px 10px; font-size:0.75rem;" onclick="clearNotifications()">Limpar Registro</button>
    </h2>
    <div id="notifList" class="notif-list"></div>
  </div>
</main>

<div id="toast" class="toast"></div>

<script>
let knownSources = [];
let activeQuery = 'HOTTEST_RACK';
const recentEvents = {};
const prevStatuses = {};
let notificationLog = JSON.parse(localStorage.getItem('dcim_notifications') || '[]');
const EVENT_KEEP_MS = 9000;
const EVENT_LABELS = {
  ALARM_RAISED:  { label: 'Alerta Critico',       bg: 'rgba(239,68,68,0.16)',  border: '#ef444466', color: '#f87171' },
  ALARM_CLEARED: { label: 'Operacao Normalizada', bg: 'rgba(34,197,94,0.16)',  border: '#22c55e66', color: '#4ade80' },
  STATE_CHANGED: { label: 'Alteracao de Estado',  bg: 'rgba(56,189,248,0.15)', border: '#38bdf866', color: '#7dd3fc' },
  OFFLINE:       { label: 'Perda de Comunicacao', bg: 'rgba(245,158,11,0.16)', border: '#f59e0b66', color: '#fbbf24' },
};

function pushNotification(source_id, rack_id, kind, desc) {
  const time = new Date().toLocaleTimeString('pt-BR');
  const id = `${source_id}|${kind}|${desc}`;
  if (notificationLog.length && notificationLog[0].dedup === id) return;
  notificationLog.unshift({ dedup: id, time, source_id, rack_id, kind, desc });
  if (notificationLog.length > 100) notificationLog.length = 100;
  localStorage.setItem('dcim_notifications', JSON.stringify(notificationLog));
  renderNotifications();
}

function clearNotifications() {
  notificationLog = [];
  localStorage.removeItem('dcim_notifications');
  renderNotifications();
}

function renderNotifications() {
  const el = document.getElementById('notifList');
  const cnt = document.getElementById('notifCount');
  if (!el) return;
  cnt.textContent = `(${notificationLog.length})`;
  if (!notificationLog.length) {
    el.innerHTML = `<div style="color:var(--muted); font-size:0.82rem; padding:6px;">Nenhuma falha ou evento registrado.</div>`;
    return;
  }
  el.innerHTML = notificationLog.map(n => {
    const info = EVENT_LABELS[n.kind] || EVENT_LABELS.STATE_CHANGED;
    return `
      <div class="notif-item ${n.kind}">
        <div>
          <b style="color:${info.color}">[${info.label}]</b>
          <b>${n.source_id}</b> <span style="color:var(--muted)">(${n.rack_id})</span> — ${n.desc}
        </div>
        <span style="color:var(--muted); font-size:0.76rem; white-space:nowrap;">${n.time}</span>
      </div>`;
  }).join('');
}

function showToast(msg, isErr=false) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.background = isErr ? '#b91c1c' : '#0284c7';
  t.style.display = 'block';
  clearTimeout(window._toastTimer);
  window._toastTimer = setTimeout(() => { t.style.display = 'none'; }, 4000);
}

async function api(path, opts={}) {
  const r = await fetch(path, opts);
  return await r.json();
}

async function procAction(action, key='') {
  const res = await api('/api/processes', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({action, id: key})
  });
  showToast(res.message || 'OK');
  setTimeout(refreshAll, 700);
}

function renderProcs(procs) {
  const el = document.getElementById('procList');
  el.innerHTML = procs.map(p => `
    <div class="proc-chip">
      <span class="dot ${p.running ? 'on' : 'off'}"></span>
      <span><b>${p.id}</b></span>
      <button class="${p.running ? 'danger' : 'ok'}" style="padding:3px 7px; font-size:0.72rem;"
        onclick="procAction('${p.running ? 'stop' : 'start'}', '${p.id}')">
        ${p.running ? 'Parar' : 'Iniciar'}
      </button>
    </div>
  `).join('');
}

function renderSources(sources) {
  const grid = document.getElementById('sourcesGrid');
  document.getElementById('srcCount').textContent = `${sources.length} fonte(s)`;
  if (!sources.length) {
    grid.innerHTML = `<div style="color:var(--muted); padding:12px;">Nenhuma fonte descoberta.</div>`;
    return;
  }

  const sel = document.getElementById('chartSource');
  const prev = sel.value;
  if (sources.map(s=>s.source_id).join(',') !== knownSources.map(s=>s.source_id).join(',')) {
    sel.innerHTML = sources.map(s => `<option value="${s.source_id}">${s.source_id} (${s.rack_id})</option>`).join('');
    if (prev) sel.value = prev;
    loadHistory();
  }
  knownSources = sources;

  grid.innerHTML = sources.map(s => {
    const m = s.metrics || {};
    let metricsHtml = '';
    if (s.source_type === 'PRECISION_AC') {
      metricsHtml = `
        <div class="m-item">Insuflamento<span class="m-val">${m.supply_air_c ?? '-'} C</span></div>
        <div class="m-item">Retorno<span class="m-val">${m.return_air_c ?? '-'} C</span></div>
        <div class="m-item">Ventilador<span class="m-val">${m.fan_speed_pct ?? s.settings.fan_speed_pct ?? '-'} %</span></div>
        <div class="m-item">Potencia<span class="m-val">${m.power_kw ?? '-'} kW</span></div>`;
    } else if (s.source_type === 'RACK_SENSOR') {
      metricsHtml = `
        <div class="m-item">Temperatura<span class="m-val">${m.temperature_c ?? '-'} C</span></div>
        <div class="m-item">Umidade<span class="m-val">${m.humidity_pct ?? '-'} %</span></div>
        <div class="m-item">Limiar Alerta<span class="m-val">${s.settings.alert_threshold_c ?? '-'} C</span></div>
        <div class="m-item">Periodo UDP<span class="m-val">${s.settings.report_interval_s} s</span></div>`;
    } else {
      metricsHtml = `
        <div class="m-item">Potencia<span class="m-val">${m.power_kw ?? '-'} kW</span></div>
        <div class="m-item">Corrente<span class="m-val">${m.current_a ?? '-'} A</span></div>
        <div class="m-item">Tensao<span class="m-val">${m.voltage_v ?? '-'} V</span></div>
        <div class="m-item">Energia Acum.<span class="m-val">${m.energy_kwh ?? '-'} kWh</span></div>`;
    }

    let ctrlHtml = `<span style="font-size:0.76rem; color:var(--muted);">Fonte nao controlavel (somente UDP)</span>`;
    if (s.controllable) {
      const powerBtn = s.settings.powered_on
        ? `<button class="sec" onclick="sendCmd('${s.source_id}','set_power',false)">Desligar</button>`
        : `<button class="ok" onclick="sendCmd('${s.source_id}','set_power',true)">Ligar</button>`;
      const failBtn = s.settings.failure_simulated
        ? `<button class="ok" onclick="sendCmd('${s.source_id}','set_failure',false)">Remover Falha</button>`
        : `<button class="danger" onclick="sendCmd('${s.source_id}','set_failure',true)">Simular Falha</button>`;

      let specific = '';
      if (s.source_type === 'PRECISION_AC') {
        specific = `
          <div class="inline-ctrl">Setpoint:
            <input type="number" step="0.5" id="sp_${s.source_id}" value="${s.settings.setpoint_c ?? 22}"/>
            <button onclick="sendInput('${s.source_id}','set_setpoint','sp_${s.source_id}')">OK</button>
          </div>
          <div class="inline-ctrl">Vent(%):
            <input type="number" step="5" id="fn_${s.source_id}" value="${s.settings.fan_speed_pct ?? 60}"/>
            <button onclick="sendInput('${s.source_id}','set_fan_speed','fn_${s.source_id}')">OK</button>
          </div>`;
      } else if (s.source_type === 'RACK_SENSOR') {
        specific = `
          <div class="inline-ctrl">Limiar(C):
            <input type="number" step="0.5" id="th_${s.source_id}" value="${s.settings.alert_threshold_c ?? 30}"/>
            <button onclick="sendInput('${s.source_id}','set_threshold','th_${s.source_id}')">OK</button>
          </div>`;
      }
      ctrlHtml = `
        ${powerBtn} ${failBtn}
        <div class="inline-ctrl">Periodo(s):
          <input type="number" id="iv_${s.source_id}" value="${s.settings.report_interval_s ?? 5}"/>
          <button onclick="sendInput('${s.source_id}','set_interval','iv_${s.source_id}')">OK</button>
        </div>
        ${specific}`;
    }

    if (prevStatuses[s.source_id] && prevStatuses[s.source_id] !== s.status && s.status === 'OFFLINE') {
      pushNotification(s.source_id, s.rack_id, 'OFFLINE', 'Fonte parou de responder (status OFFLINE)');
    }
    prevStatuses[s.source_id] = s.status;

    const pktKey = `${s.packets_received}:${s.last_event}:${s.last_event_desc}`;
    if (s.last_event && s.last_event !== 'NO_EVENT' && (!recentEvents[s.source_id] || recentEvents[s.source_id].key !== pktKey)) {
      recentEvents[s.source_id] = {
        key: pktKey,
        kind: s.last_event,
        desc: s.last_event_desc,
        time: new Date().toLocaleTimeString('pt-BR'),
        expiresAt: Date.now() + EVENT_KEEP_MS,
      };
      pushNotification(s.source_id, s.rack_id, s.last_event, s.last_event_desc);
    }

    let overLimitDesc = '';
    if (s.status === 'FALHA') {
      overLimitDesc = 'Falha operacional ativa no equipamento';
    } else if (s.settings.powered_on) {
      if (s.source_type === 'RACK_SENSOR' && m.temperature_c !== undefined) {
        const th = s.settings.alert_threshold_c ?? m.alert_threshold_c ?? 30;
        if (m.temperature_c > th) {
          overLimitDesc = `Temperatura acima do limite: ${m.temperature_c} C (limiar ${th} C)`;
        }
      } else if (s.source_type === 'PRECISION_AC' && m.supply_air_c !== undefined) {
        const lim = (s.settings.setpoint_c ?? m.setpoint_c ?? 22) + 5;
        if (m.supply_air_c > lim) {
          overLimitDesc = `Temperatura de insuflamento elevada: ${m.supply_air_c} C (limite ${lim.toFixed(1)} C)`;
        }
      } else if (s.source_type === 'PDU' && m.power_kw !== undefined) {
        const cap = s.settings.rated_capacity_kw ?? 10;
        const ovPct = s.settings.overload_pct ?? 80;
        const limitKw = cap * ovPct / 100.0;
        if (m.power_kw > limitKw) {
          overLimitDesc = `Sobrecarga eletrica: ${m.power_kw} kW (limite ${limitKw.toFixed(1)} kW)`;
        }
      }
    }

    const ev = recentEvents[s.source_id];
    let eventBanner = '';
    if (overLimitDesc) {
      const style = EVENT_LABELS.ALARM_RAISED;
      eventBanner = `
        <div style="font-size:0.78rem; padding:7px 10px; border-radius:5px; background:${style.bg}; border:1px solid ${style.border}; color:${style.color};">
          <b>${style.label}:</b> ${overLimitDesc}
        </div>`;
    } else if (ev && Date.now() < ev.expiresAt && ev.kind !== 'ALARM_RAISED') {
      const style = EVENT_LABELS[ev.kind] || EVENT_LABELS.STATE_CHANGED;
      eventBanner = `
        <div style="font-size:0.78rem; padding:7px 10px; border-radius:5px; background:${style.bg}; border:1px solid ${style.border}; color:${style.color};">
          <b>${style.label} (${ev.time}):</b> ${ev.desc}
        </div>`;
    }

    return `
      <div class="src-card">
        <div class="src-head">
          <div>
            <div class="src-title">${s.source_id} <span style="font-weight:normal; color:var(--muted);">(${s.rack_id})</span></div>
            <div class="src-sub">${s.type_label} | ${s.endpoint} | pkts=${s.packets_received}</div>
          </div>
          <span class="pill ${s.status}">${s.status}</span>
        </div>
        <div class="metrics">${metricsHtml}</div>
        ${eventBanner}
        <div class="controls">${ctrlHtml}</div>
      </div>`;
  }).join('');
}

async function sendCmd(source_id, action, value) {
  const res = await api('/api/command', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({source_id, action, value})
  });
  showToast(res.message || res.error, !res.success);
  refreshAll();
}

function sendInput(source_id, action, inputId) {
  const val = parseFloat(document.getElementById(inputId).value);
  sendCmd(source_id, action, val);
}

async function runQuery(type, silent=false) {
  activeQuery = type;
  const res = await api(`/api/query?type=${type}`);
  if (!res.success) {
    if (!silent) showToast(res.error || 'Erro na consulta', true);
    return;
  }
  document.getElementById('queryTitle').textContent = res.title;
  document.getElementById('querySummary').textContent = res.summary;
  if (res.rows && res.rows.length) {
    document.getElementById('queryTableWrap').innerHTML = `
      <table>
        <thead><tr><th>Item</th><th>Valor</th><th>Detalhes</th></tr></thead>
        <tbody>
          ${res.rows.map(r => `<tr><td><b>${r.label}</b></td><td>${r.value.toFixed(2)} ${r.unit}</td><td style="color:var(--muted)">${r.detail}</td></tr>`).join('')}
        </tbody>
      </table>`;
  } else {
    document.getElementById('queryTableWrap').innerHTML = '';
  }
}

async function loadHistory() {
  const sel = document.getElementById('chartSource');
  if (!sel.value) return;
  const res = await api(`/api/query?type=READING_HISTORY&source_id=${encodeURIComponent(sel.value)}&limit=40`);
  if (!res.success) return;
  document.getElementById('histSummary').textContent = res.summary;
  const srcObj = knownSources.find(s => s.source_id === sel.value);
  drawChart(res.rows || [], srcObj);
}

function isRowOverThreshold(r, src) {
  if (r.detail && r.detail.includes('ALARM_RAISED')) return true;
  if (!src) return false;
  if (src.source_type === 'RACK_SENSOR') {
    const m = r.detail && r.detail.match(/alert_threshold_c=([0-9.]+)/);
    const th = src.settings.alert_threshold_c ?? (m ? parseFloat(m[1]) : 30);
    return r.value > th;
  }
  if (src.source_type === 'PRECISION_AC') {
    const m = r.detail && r.detail.match(/setpoint_c=([0-9.]+)/);
    const sp = src.settings.setpoint_c ?? (m ? parseFloat(m[1]) : 22);
    return r.value > (sp + 5);
  }
  if (src.source_type === 'PDU') {
    const cap = src.settings.rated_capacity_kw ?? 10;
    const ov = src.settings.overload_pct ?? 80;
    return r.value > (cap * ov / 100.0);
  }
  return false;
}

function drawChart(rows, src) {
  const canvas = document.getElementById('tsCanvas');
  const ctx = canvas.getContext('2d');
  const W = canvas.width, H = canvas.height;
  ctx.clearRect(0, 0, W, H);
  if (!rows.length) {
    ctx.fillStyle = '#94a3b8';
    ctx.font = '13px sans-serif';
    ctx.fillText('Sem leituras disponiveis.', 20, H/2);
    return;
  }
  const vals = rows.map(r => r.value);
  const unit = rows[0].unit || '';
  let minV = Math.min(...vals), maxV = Math.max(...vals);
  if (maxV - minV < 1) { minV -= 1; maxV += 1; }
  const padL = 52, padR = 18, padT = 20, padB = 28;
  const plotW = W - padL - padR, plotH = H - padT - padB;

  ctx.strokeStyle = '#334155';
  ctx.fillStyle = '#94a3b8';
  ctx.font = '11px sans-serif';
  for (let i = 0; i <= 4; i++) {
    const y = padT + (plotH * i / 4);
    const v = (maxV - (maxV - minV) * i / 4).toFixed(1);
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(W - padR, y); ctx.stroke();
    ctx.fillText(`${v}${unit}`, 6, y + 4);
  }

  ctx.strokeStyle = '#38bdf8';
  ctx.lineWidth = 2.2;
  ctx.beginPath();
  rows.forEach((r, idx) => {
    const x = padL + (rows.length === 1 ? plotW/2 : (idx / (rows.length - 1)) * plotW);
    const y = padT + (1 - (r.value - minV) / (maxV - minV)) * plotH;
    if (idx === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  ctx.stroke();

  rows.forEach((r, idx) => {
    const x = padL + (rows.length === 1 ? plotW/2 : (idx / (rows.length - 1)) * plotW);
    const y = padT + (1 - (r.value - minV) / (maxV - minV)) * plotH;
    const isAlert = isRowOverThreshold(r, src);
    ctx.fillStyle = isAlert ? '#ef4444' : '#38bdf8';
    ctx.beginPath(); ctx.arc(x, y, isAlert ? 4.2 : 3.2, 0, Math.PI * 2); ctx.fill();
  });

  ctx.fillStyle = '#94a3b8';
  ctx.fillText(rows[0].label, padL, H - 8);
  ctx.fillText(rows[rows.length - 1].label, W - padR - 75, H - 8);
}

async function refreshAll() {
  try {
    const st = await api('/api/state');
    renderProcs(st.processes || []);
    const badge = document.getElementById('gwBadge');
    if (st.gateway_connected) {
      badge.textContent = 'Gateway Online';
      badge.className = 'pill ONLINE';
      renderSources(st.sources || []);
      loadHistory();
      if (activeQuery) runQuery(activeQuery, true);
    } else {
      badge.textContent = 'Gateway Offline';
      badge.className = 'pill OFFLINE';
      renderSources([]);
    }
  } catch (e) {
    console.error(e);
  }
}

setInterval(() => {
  if (document.activeElement && document.activeElement.tagName === 'INPUT') return;
  refreshAll();
}, 2500);
renderNotifications();
refreshAll();
</script>
</body>
</html>
"""


class DashboardServer:
    def __init__(self, gw_host: str, gw_port: int, proc_mgr: ProcessManager):
        self.conn = GatewayConnection(gw_host, gw_port, timeout=3.0)
        self.lock = threading.Lock()
        self.proc_mgr = proc_mgr

    def request_gw(self, **body) -> pb.GatewayResponse:
        with self.lock:
            return self.conn.request(**body)


def make_handler(dashboard: DashboardServer):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def _send_json(self, data: dict, status: int = 200):
            raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path == "/":
                raw = HTML_PAGE.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                return

            if parsed.path == "/api/state":
                try:
                    resp = dashboard.request_gw(list_sources=pb.ListSourcesRequest(refresh=False))
                    sources = [serialize_source(s) for s in resp.sources.sources] if resp.success else []
                    active_ids = {s["source_id"] for s in sources if s["status"] != "OFFLINE"}
                    procs = dashboard.proc_mgr.status_list(gw_connected=True, active_source_ids=active_ids)
                    self._send_json({"gateway_connected": True, "sources": sources, "processes": procs})
                except Exception as exc:
                    procs = dashboard.proc_mgr.status_list(gw_connected=False)
                    self._send_json({"gateway_connected": False, "error": str(exc),
                                     "sources": [], "processes": procs})
                return

            if parsed.path == "/api/query":
                qs = parse_qs(parsed.query)
                qname = qs.get("type", ["HOTTEST_RACK"])[0]
                source_id = qs.get("source_id", [""])[0]
                limit = int(qs.get("limit", ["25"])[0])
                try:
                    qtype = pb.QueryType.Value(qname)
                    resp = dashboard.request_gw(
                        query=pb.QueryRequest(type=qtype, source_id=source_id, limit=limit)
                    )
                    if not resp.success:
                        self._send_json({"success": False, "error": resp.error})
                        return
                    q = resp.query
                    self._send_json({
                        "success": True,
                        "title": q.title,
                        "summary": q.summary,
                        "rows": [{"label": r.label, "value": r.value, "unit": r.unit, "detail": r.detail}
                                 for r in q.rows],
                    })
                except Exception as exc:
                    self._send_json({"success": False, "error": str(exc)})
                return

            self.send_error(404)

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length).decode("utf-8") or "{}")

            if self.path == "/api/processes":
                action = body.get("action")
                pid = body.get("id", "")
                if action == "start_all":
                    dashboard.proc_mgr.start_all()
                    self._send_json({"success": True, "message": "Processos iniciados."})
                elif action == "stop_all":
                    dashboard.proc_mgr.stop_all()
                    self._send_json({"success": True, "message": "Processos encerrados."})
                elif action == "start":
                    dashboard.proc_mgr.start(pid)
                    self._send_json({"success": True, "message": f"Processo {pid} iniciado."})
                elif action == "stop":
                    dashboard.proc_mgr.stop(pid)
                    self._send_json({"success": True, "message": f"Processo {pid} encerrado."})
                else:
                    self._send_json({"success": False, "message": "Acao desconhecida."}, 400)
                return

            if self.path == "/api/command":
                source_id = body.get("source_id", "")
                action = body.get("action", "")
                value = body.get("value")
                cmd = pb.ControlCommand(source_id=source_id)
                if action == "set_power":
                    cmd.set_power.on = bool(value)
                elif action == "set_failure":
                    cmd.set_failure.active = bool(value)
                elif action == "set_interval":
                    cmd.set_interval.seconds = int(value)
                elif action == "set_setpoint":
                    cmd.set_setpoint.celsius = float(value)
                elif action == "set_fan_speed":
                    cmd.set_fan_speed.percent = int(value)
                elif action == "set_threshold":
                    cmd.set_threshold.celsius = float(value)
                else:
                    self._send_json({"success": False, "error": f"Acao invalida: {action}"}, 400)
                    return

                try:
                    resp = dashboard.request_gw(command=cmd)
                    self._send_json({
                        "success": resp.success,
                        "message": resp.command_reply.message if resp.success else resp.error,
                        "error": resp.error,
                    })
                except Exception as exc:
                    self._send_json({"success": False, "error": str(exc)})
                return

            self.send_error(404)

    return Handler


def main():
    parser = argparse.ArgumentParser(description="Cliente Web DCIM")
    parser.add_argument("--host", default="127.0.0.1", help="IP do Gateway")
    parser.add_argument("--port", type=int, default=config.GATEWAY_TCP_PORT, help="porta TCP do Gateway")
    parser.add_argument("--web-port", type=int, default=8080, help="porta HTTP local")
    parser.add_argument("--no-autostart", action="store_true", help="nao inicia processos automaticamente")
    parser.add_argument("--no-browser", action="store_true", help="nao abre navegador automaticamente")
    args = parser.parse_args()

    proc_mgr = ProcessManager()
    if not args.no_autostart:
        print("Iniciando processos em segundo plano...")
        proc_mgr.start_all()

    dashboard = DashboardServer(args.host, args.port, proc_mgr)
    httpd = HTTPServer(("127.0.0.1", args.web_port), make_handler(dashboard))
    url = f"http://127.0.0.1:{args.web_port}"
    print(f"Servidor Web disponivel em: {url}")

    if not args.no_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nEncerrando...")
    finally:
        proc_mgr.stop_all()
        dashboard.conn.close()


if __name__ == "__main__":
    main()
