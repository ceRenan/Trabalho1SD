import time
from collections import defaultdict
from datetime import datetime
from statistics import mean

from common.util import status_name
from gateway.storage import metric_names
from protos import datacenter_pb2 as pb

DEFAULT_WINDOWS = {
    pb.HOTTEST_RACK: 0,
    pb.PEAK_POWER: 24 * 3600,
    pb.AVERAGE_TEMPERATURE: 3600,
    pb.ENERGY_BY_RACK: 24 * 3600,
    pb.ALERTS_BY_RACK: 3600,
    pb.READING_HISTORY: 0,
}
STALE_AFTER_S = 120

_MAIN_METRIC = {
    pb.PRECISION_AC: ("supply_air_c", "C"),
    pb.RACK_SENSOR: ("temperature_c", "C"),
    pb.PDU: ("power_kw", "kW"),
}


def _hhmmss(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000).strftime("%d/%m %H:%M:%S")


def _window_text(window_s: int) -> str:
    if window_s % 3600 == 0:
        return f"{window_s // 3600} h"
    if window_s % 60 == 0:
        return f"{window_s // 60} min"
    return f"{window_s} s"


def hottest_rack(store, registry, _window_s):
    now_ms = time.time() * 1000
    latest = store.latest(pb.RACK_SENSOR)
    rows = []
    fresh = []
    for sid, r in latest.items():
        age = (now_ms - r.timestamp_ms) / 1000
        temp = r.metrics["temperature_c"]
        stale = age > STALE_AFTER_S or registry.status_of(sid) == pb.OFFLINE
        rows.append(pb.QueryRow(
            label=f"Rack {r.rack_id}", value=temp, unit="C", timestamp_ms=r.timestamp_ms,
            detail=(f"{sid} | umidade {r.metrics.get('humidity_pct', 0):.0f}% | ha {age:.0f}s | "
                    f"{status_name(registry.status_of(sid))}{' (dado antigo)' if stale else ''}")))
        if not stale:
            fresh.append((temp, r))
    rows.sort(key=lambda row: row.value, reverse=True)
    if fresh:
        temp, r = max(fresh, key=lambda x: x[0])
        summary = f"Rack mais quente agora: {r.rack_id} com {temp:.1f} C ({r.source_id})"
    elif rows:
        summary = "Nenhum sensor com leitura recente; exibindo ultimas leituras conhecidas."
    else:
        summary = "Ainda nao ha leituras de temperatura."
    return "Rack mais quente no momento", rows, summary


def peak_power(store, _registry, window_s):
    readings = store.readings(pb.PDU, window_s)
    per_rack = {}
    for r in readings:
        p = r.metrics["power_kw"]
        if r.rack_id not in per_rack or p > per_rack[r.rack_id].metrics["power_kw"]:
            per_rack[r.rack_id] = r
    rows = [pb.QueryRow(label=f"Rack {rack}", value=r.metrics["power_kw"], unit="kW",
                        timestamp_ms=r.timestamp_ms,
                        detail=f"{r.source_id} | em {_hhmmss(r.timestamp_ms)} | corrente {r.metrics.get('current_a', 0):.1f} A")
            for rack, r in per_rack.items()]
    rows.sort(key=lambda row: row.value, reverse=True)
    if rows:
        top = rows[0]
        summary = (f"Pico de consumo nas ultimas {_window_text(window_s)}: {top.value:.2f} kW no "
                   f"{top.label} em {_hhmmss(top.timestamp_ms)} ({len(readings)} leituras analisadas)")
    else:
        summary = "Nenhuma leitura de PDU na janela."
    return f"Pico de consumo de energia (PDU) - ultimas {_window_text(window_s)}", rows, summary


def average_temperature(store, _registry, window_s):
    readings = store.readings(pb.RACK_SENSOR, window_s)
    by_rack = defaultdict(list)
    for r in readings:
        by_rack[r.rack_id].append(r.metrics["temperature_c"])
    rows = []
    for rack, temps in sorted(by_rack.items()):
        rows.append(pb.QueryRow(
            label=f"Rack {rack}", value=mean(temps), unit="C",
            detail=f"min {min(temps):.1f} | max {max(temps):.1f} | {len(temps)} amostras"))
    if rows:
        overall = mean(row.value for row in rows)
        samples = mean(t for temps in by_rack.values() for t in temps)
        summary = (f"Temperatura media do data center nas ultimas {_window_text(window_s)}: "
                   f"{overall:.2f} C (media por rack; media simples das {len(readings)} amostras = {samples:.2f} C)")
    else:
        summary = "Nenhuma leitura de temperatura na janela."
    return f"Temperatura media - ultimas {_window_text(window_s)}", rows, summary


def energy_by_rack(store, _registry, window_s):
    readings = store.readings(pb.PDU, window_s)
    by_source = defaultdict(list)
    for r in readings:
        by_source[r.source_id].append(r)
    rows = []
    total = 0.0
    for sid, rs in sorted(by_source.items()):
        energy = sum(max(0.0, b.metrics["energy_kwh"] - a.metrics["energy_kwh"]) for a, b in zip(rs, rs[1:]))
        avg_kw = mean(r.metrics["power_kw"] for r in rs)
        total += energy
        rows.append(pb.QueryRow(label=f"Rack {rs[-1].rack_id}", value=energy, unit="kWh",
                                detail=f"{sid} | potencia media {avg_kw:.2f} kW | {len(rs)} leituras"))
    rows.sort(key=lambda row: row.value, reverse=True)
    summary = (f"Energia total consumida pelos racks nas ultimas {_window_text(window_s)}: {total:.4f} kWh"
               if rows else "Nenhuma leitura de PDU na janela.")
    return f"Energia por rack - ultimas {_window_text(window_s)}", rows, summary


def alerts_by_rack(store, _registry, window_s):
    readings = store.readings(None, window_s)
    alerts = defaultdict(list)
    for r in readings:
        if r.event == pb.ALARM_RAISED:
            alerts[(r.rack_id, r.source_id)].append(r)
    rows = [pb.QueryRow(label=f"{rack} / {sid}", value=len(rs), unit="alertas",
                        timestamp_ms=rs[-1].timestamp_ms,
                        detail=f"ultimo em {_hhmmss(rs[-1].timestamp_ms)}: {rs[-1].event_description}")
            for (rack, sid), rs in alerts.items()]
    rows.sort(key=lambda row: row.value, reverse=True)
    total = sum(int(row.value) for row in rows)
    summary = (f"{total} alertas nas ultimas {_window_text(window_s)}; mais critico: {rows[0].label}"
               if rows else f"Nenhum alerta nas ultimas {_window_text(window_s)}.")
    return f"Alertas por rack/fonte - ultimas {_window_text(window_s)}", rows, summary


def reading_history(store, registry, window_s, source_id, limit):
    entry = registry.get(source_id)
    types = [entry.info.source_type] if entry else list(_MAIN_METRIC)
    limit = limit or 20
    for source_type in types:
        history = store.history(source_type, source_id, limit, window_s)
        if history:
            break
    else:
        return f"Historico de {source_id}", [], f"Nenhuma leitura persistida para '{source_id}'."
    metric, unit = _MAIN_METRIC[source_type]
    names = metric_names(source_type)
    rows = []
    for r in history:
        details = ", ".join(f"{n}={r.metrics[n]:g}" for n in names if n in r.metrics)
        if r.event != pb.NO_EVENT:
            details += f" | {pb.EventKind.Name(r.event)}: {r.event_description}"
        rows.append(pb.QueryRow(label=_hhmmss(r.timestamp_ms), value=r.metrics.get(metric, 0.0),
                                unit=unit, detail=details, timestamp_ms=r.timestamp_ms))
    summary = f"Ultimas {len(rows)} leituras de {source_id}."
    return f"Historico de {source_id}", rows, summary


_HANDLERS = {
    pb.HOTTEST_RACK: hottest_rack,
    pb.PEAK_POWER: peak_power,
    pb.AVERAGE_TEMPERATURE: average_temperature,
    pb.ENERGY_BY_RACK: energy_by_rack,
    pb.ALERTS_BY_RACK: alerts_by_rack,
}


def run_query(store, registry, query: pb.QueryRequest) -> pb.QueryResponse:
    window_s = query.window_seconds or DEFAULT_WINDOWS.get(query.type, 3600)
    if query.type == pb.READING_HISTORY:
        title, rows, summary = reading_history(store, registry, window_s, query.source_id, query.limit)
    elif query.type in _HANDLERS:
        title, rows, summary = _HANDLERS[query.type](store, registry, window_s)
    else:
        raise ValueError(f"tipo de consulta desconhecido: {query.type}")
    return pb.QueryResponse(type=query.type, title=title, rows=rows, summary=summary)
