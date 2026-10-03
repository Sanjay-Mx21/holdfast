"""Generates deploy/grafana/dashboards/mission-control.json (design doc 12.4).

Run from anywhere: python3 deploy/grafana/mission-control.py. Edit this file,
not the JSON, so the 34 panels stay consistent.
"""
import json
import os

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

DS = {"type": "prometheus", "uid": "prometheus"}
EV = '{event=~"$event"}'
panels = []
pid = 0
y = 0


def nid():
    global pid
    pid += 1
    return pid


def row(title):
    global y
    panels.append({"id": nid(), "type": "row", "title": title, "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})
    y += 1


def target(expr, legend="", ref="A", instant=False):
    t = {"refId": ref, "datasource": DS, "expr": expr, "legendFormat": legend}
    if instant:
        t["instant"] = True
    return t


def stat(title, exprs, x, w, h=4, unit="short", desc="", thresholds=None, no_value=None, mappings=None, decimals=None):
    defaults = {"unit": unit, "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": thresholds or [{"color": "blue", "value": None}]}}
    if no_value:
        defaults["noValue"] = no_value
    if mappings:
        defaults["mappings"] = mappings
    if decimals is not None:
        defaults["decimals"] = decimals
    return {"id": nid(), "type": "stat", "title": title, "description": desc, "datasource": DS,
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "background" if thresholds else "value", "graphMode": "area",
                        "textMode": "value_and_name" if len(exprs) > 1 else "value", "justifyMode": "center"},
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "targets": [target(e, l, chr(65 + i)) for i, (e, l) in enumerate(exprs)]}


def series(title, exprs, x, w, h=8, unit="short", desc="", stack=False, thresholds=None):
    custom = {"fillOpacity": 10, "lineWidth": 2, "showPoints": "never"}
    if stack:
        custom["stacking"] = {"mode": "normal"}
    defaults = {"unit": unit, "custom": custom}
    if thresholds:
        defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
        custom["thresholdsStyle"] = {"mode": "dashed"}
    return {"id": nid(), "type": "timeseries", "title": title, "description": desc, "datasource": DS,
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "targets": [target(e, l, chr(65 + i)) for i, (e, l) in enumerate(exprs)]}


def add(*ps, height):
    global y
    panels.extend(ps)
    y += height


# Grey with no data (nothing measured yet: never a false green), green at 0,
# red from 1.
green_red = [{"color": "#6e7079", "value": None}, {"color": "green", "value": 0}, {"color": "red", "value": 1}]

# Row 1: the queue ---------------------------------------------------------
row("The queue: who is waiting, and how fast they get in")
state_map = [{"type": "value", "options": {
    "PRE": {"text": "PRE (draw open)", "color": "blue"}, "OPEN": {"text": "OPEN", "color": "green"},
    "FROZEN": {"text": "FROZEN (paused)", "color": "orange"}, "SOLD_OUT": {"text": "SOLD OUT", "color": "purple"},
    "CLOSED": {"text": "CLOSED", "color": "text"}}}]
state = stat("Queue state", [(f'max by (state) (holdfast_queue_state{EV}) == 1', "{{state}}")], 0, 4,
             desc="The state the status document shows, set by the admission leader every tick (holdfastctl freezes show too).",
             mappings=state_map)
state["options"]["textMode"] = "name"
state["options"]["colorMode"] = "background"
add(state,
    stat("In the queue", [(f'sum(max by (event) (holdfast_queue_size{EV}))', "")], 4, 4,
         desc="Everyone who joined, admitted or not."),
    stat("Admitted up to", [(f'sum(max by (event) (holdfast_queue_admitted_up_to{EV}))', "")], 8, 4,
         desc="admittedUpTo: the highest rank allowed into checkout."),
    stat("Active sessions", [(f'sum(max by (event) (holdfast_queue_active_sessions{EV}))', "active"),
                             (f'sum(max by (event) (holdfast_queue_max_sessions{EV}))', "budget")], 12, 4,
         desc="Checkout sessions in use against the Little's Law budget; also capped by units left x 1.3 (P17)."),
    stat("Admission rate", [(f'sum(rate(holdfast_queue_admitted_total{EV}[1m]))', "")], 16, 4, unit="reqps",
         desc="People admitted per second (one-minute rate)."),
    stat("Proof-of-work difficulty", [('max(holdfast_queue_pow_difficulty)', "")], 20, 4, unit="none",
         desc="Bits of the last challenge issued; rises one bit per doubling of the challenge rate above the surge rate.",
         thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 19}, {"color": "red", "value": 22}]),
    height=4)
add(series("Queue size and admittedUpTo", [(f'max by (event) (holdfast_queue_size{EV})', "queue {{event}}"),
                                           (f'max by (event) (holdfast_queue_admitted_up_to{EV})', "admitted {{event}}")], 0, 12),
    series("Joins and admissions per second", [('sum by (result) (rate(holdfast_queue_joins_total[1m]))', "join {{result}}"),
                                               (f'sum(rate(holdfast_queue_admitted_total{EV}[1m]))', "admitted")], 12, 12, unit="reqps",
           desc="Join outcomes (including proof-of-work and policy refusals) and admissions."),
    height=8)

# Row 2: the sale -----------------------------------------------------------
row("The sale: units, holds, bookings, payments")
add(stat("Units available", [(f'sum(holdfast_inventory_available{EV})', "")], 0, 6,
         desc="Units neither held nor sold, as inventory-svc last saw them."),
    stat("Holds per second", [('sum(rate(holdfast_holds_total{result="held"}[1m]))', "")], 6, 6, unit="reqps"),
    stat("Bookings confirmed", [('sum(increase(holdfast_booking_saga_outcomes_total{status="CONFIRMED"}[$__range]))', "")], 12, 6,
         desc="Bookings the saga confirmed in the dashboard's time range.", decimals=0),
    stat("Capture to confirm, p99", [('histogram_quantile(0.99, sum by (le) (rate(holdfast_capture_to_confirm_seconds_bucket[5m])))', "")],
         18, 6, unit="s", desc="From the payment's capture to the booking's confirmation. SLO: p99 at most 5 s.",
         thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 3}, {"color": "red", "value": 5}],
         no_value="no confirmations yet"),
    height=4)
add(series("Holds per second by result", [('sum by (result) (rate(holdfast_holds_total[1m]))', "{{result}}")], 0, 8, unit="reqps", stack=True),
    series("Bookings per second by outcome", [('sum(rate(holdfast_bookings_created_total[1m]))', "created"),
                                              ('sum by (status) (rate(holdfast_booking_saga_outcomes_total[1m]))', "{{status}}")], 8, 8, unit="reqps",
           desc="Bookings created, and the saga's decisions: CONFIRMED, CANCELLED, REFUND_REQUIRED, REFUNDED."),
    series("Payments", [('sum by (via) (rate(holdfast_payment_captures_total[1m]))', "captures by {{via}}"),
                        ('max(holdfast_psp_breaker_state)', "PSP breaker (0 closed, 0.5 half-open, 1 open)")], 16, 8,
           desc="Captures by how they were learned (webhook or poll), and the payment provider's circuit breaker."),
    height=8)

# Row 3: correctness --------------------------------------------------------
row("Correctness: these must stay at zero")
tiles = []
names = {"I1": "I1 no oversell", "I2": "I2 no double charge", "I3": "I3 money safety",
         "I4": "I4 per-user cap", "I5": "I5 no lost units"}
for i, inv in enumerate(["I1", "I2", "I3", "I4", "I5"]):
    tiles.append(stat(names[inv], [(f'max(holdfast_invariant_violations{{invariant="{inv}"}})', "")], i * 4, 4,
                      desc="Published by the invariant auditor (Phase 5, task 5.1). Green at 0; until the auditor exists the tile shows no data.",
                      thresholds=green_red, no_value="auditor: Phase 5"))
tiles.append(stat("Reconciliation mismatches", [('sum(increase(holdfast_recon_mismatch_total[$__range]))', "")], 20, 4,
                  desc="Found by the reconciler (Phase 5, task 5.1).", thresholds=green_red, no_value="reconciler: Phase 5"))
add(*tiles, height=4)
add(series("Outbox lag", [('max by (schema) (holdfast_outbox_lag_seconds)', "{{schema}}")], 0, 8, unit="s",
           desc="Age of the oldest unpublished outbox row per service. Alert above 30 s.",
           thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 30}]),
    series("Kafka consumer lag", [('max by (group, topic) (holdfast_kafka_consumer_lag)', "{{group}} {{topic}}")], 8, 8,
           desc="Messages each consumer group has yet to process."),
    series("Money and late events", [('sum(increase(holdfast_payment_amount_mismatch_total[5m]))', "amount mismatches"),
                                     ('sum by (result) (increase(holdfast_late_confirm_total[5m]))', "late capture {{result}}")], 16, 8,
           desc="Captures whose amount differs from the intent (must be 0), and captures that arrived after cancellation."),
    height=8)

# Row 4: health -------------------------------------------------------------
row("Health: latency, errors, saturation")
lat = 'histogram_quantile({q}, sum by (le, job) (rate(holdfast_http_request_duration_seconds_bucket[1m])))'
errors = series("Server errors (5xx share)", [('(sum by (job) (rate(holdfast_http_requests_total{code=~"5.."}[1m])) or 0 * sum by (job) (rate(holdfast_http_requests_total[1m]))) / sum by (job) (rate(holdfast_http_requests_total[1m]))', "{{job}}")],
                18, 6, unit="percentunit")
errors["fieldConfig"]["defaults"].update({"min": 0, "max": 1})  # a share: 0 to 100 %
add(series("Latency p50 by service", [(lat.format(q=0.5), "{{job}}")], 0, 6, unit="s"),
    series("Latency p95 by service", [(lat.format(q=0.95), "{{job}}")], 6, 6, unit="s"),
    series("Latency p99 by service", [(lat.format(q=0.99), "{{job}}")], 12, 6, unit="s",
           desc="Per-route detail: the service dashboards. Join SLO: p99 150 ms."),
    errors,
    height=8)
valkey = series("Valkey CPU", [('rate(redis_cpu_sys_seconds_total[1m]) + rate(redis_cpu_user_seconds_total[1m])', "cores"),
                               ('rate(redis_commands_processed_total[1m])', "commands/s")], 0, 12,
                desc="From redis_exporter (works with Valkey). Valkey runs commands on one thread: near 1 core is saturation.")
# Cores (0 to 1) and commands (thousands a second) need separate axes.
valkey["fieldConfig"]["overrides"] = [{
    "matcher": {"id": "byName", "options": "commands/s"},
    "properties": [{"id": "custom.axisPlacement", "value": "right"}, {"id": "unit", "value": "ops"}],
}]
add(valkey,
    series("PostgreSQL load", [('sum(rate(pg_stat_database_xact_commit{datname="holdfast"}[1m]) + rate(pg_stat_database_xact_rollback{datname="holdfast"}[1m]))', "transactions/s"),
                               ('sum(pg_stat_database_numbackends{datname="holdfast"})', "connections")], 12, 12,
           desc="From postgres-exporter. It cannot read the server's CPU, so this shows transactions per second and connections instead (the design asked for CPU)."),
    height=8)

dashboard = {
    "uid": "holdfast-mission-control",
    "title": "HoldFast / Mission control",
    "description": "One screen for a live sale (design doc 12.4): the queue, the sale, correctness and health.",
    "tags": ["holdfast"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"},
    "templating": {"list": [{
        "name": "event", "label": "Event", "type": "query", "datasource": DS,
        "query": {"query": "label_values(holdfast_queue_size, event)", "refId": "events"},
        "definition": "label_values(holdfast_queue_size, event)",
        "refresh": 2, "includeAll": True, "multi": True, "allValue": ".*",
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "sort": 1,
    }]},
    "panels": panels,
}
with open('deploy/grafana/dashboards/mission-control.json', 'w', newline='\n') as f:
    json.dump(dashboard, f, indent=2)
    f.write('\n')
print('panels', len(panels))
