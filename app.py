#!/usr/bin/env python3
"""Dynatrace -> Claude API -> Microsoft Teams | RCA Bot v1.3
Yenilikler:
  - 4xx / 5xx hata orani metrikleri (get_service_metrics)
  - Yanit suresi metrigi (latency peak/avg)
  - Etkilenen servis entity ID otomatik cozumleme
  - Claude prompt metrik verisiyle zenginlestirildi
  - Teams karta "Show Metrics" butonu eklendi (4xx/5xx/latency + trace bulgusu)
  - OPEN/RESOLVED renk mantigi webhook_state ile duzeltildi
"""
import os, re, json, logging, requests
from flask import Flask, request, jsonify
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)
app = Flask(__name__)

DT_BASE_URL = os.environ.get("DT_BASE_URL", "").rstrip("/")
DT_API_TOKEN = os.environ.get("DT_API_TOKEN", "")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY", "")
TEAMS_WEBHOOK = os.environ.get("TEAMS_WEBHOOK", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")


def dt_headers():
    return {
        "Authorization": "Api-Token " + DT_API_TOKEN,
        "Content-Type": "application/json",
    }


def find_problem_by_display_id(display_id):
    try:
        r = requests.get(
            DT_BASE_URL + "/api/v2/problems",
            headers=dt_headers(),
            params={"from": "now-2h", "pageSize": 50, "sort": "-startTime"},
            timeout=15,
        )
        r.raise_for_status()
        for p in r.json().get("problems", []):
            if p.get("displayId") == display_id:
                return p
    except Exception as e:
        log.warning("Problem listesi alinamadi: %s", e)
    return {}


def get_problem_details(internal_id):
    try:
        r = requests.get(
            DT_BASE_URL + "/api/v2/problems/" + internal_id,
            headers=dt_headers(),
            timeout=15,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("Problem detayi alinamadi: %s", e)
    return {}


def get_recent_logs(start_ts):
    try:
        from_t = datetime.fromtimestamp(
            start_ts / 1000 - 120, tz=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        to_t = datetime.fromtimestamp(start_ts / 1000 + 600, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        r = requests.get(
            DT_BASE_URL + "/api/v2/logs/search",
            headers=dt_headers(),
            params={"from": from_t, "to": to_t, "query": "error", "limit": 8},
            timeout=15,
        )
        return [
            i.get("content", "")[:200]
            for i in r.json().get("results", [])
            if i.get("content")
        ]
    except Exception as e:
        log.warning("Log sorgusu basarisiz: %s", e)
    return []


def clean_placeholders(text):
    return re.sub(r"\{[A-Za-z][A-Za-z0-9]*\}", "", str(text or ""))


def get_root_cause_name(problem, details_text):
    rc = (problem.get("rootCauseEntity") or {}).get("name", "")
    if rc:
        return rc
    affected = problem.get("affectedEntities", [])
    if affected:
        name = affected[0].get("name", "")
        if name:
            return name
    clean = clean_placeholders(details_text)
    for part in clean.split("|"):
        if "Root Cause" in part:
            val = part.split(":", 1)[-1].strip().split("|")[0].strip()
            if (
                val
                and len(val) > 2
                and val.lower() not in ("n/a", "none", "unknown", "")
            ):
                return val
    title = problem.get("title", "")
    if title:
        return title
    return "Unknown Service"


# ── YENİ: METRİK FONKSİYONLARI ─────────────────────────────────────────────


def resolve_service_entity_id(service_name):
    """Servis adi ile Dynatrace entity ID sini bul"""
    try:
        r = requests.get(
            DT_BASE_URL + "/api/v2/entities",
            headers=dt_headers(),
            params={
                "entitySelector": 'type(SERVICE),entityName.equals("%s")'
                % service_name,
                "fields": "entityId",
                "pageSize": 5,
            },
            timeout=15,
        )
        r.raise_for_status()
        items = r.json().get("entities", [])
        if items:
            return items[0]["entityId"]["id"]
    except Exception as e:
        log.warning("Entity ID cozumlenemedi (%s): %s", service_name, e)
    return None


def _query_metric(entity_id, metric_selector, from_ts, to_ts, resolution="5m"):
    """Tek bir metrik sorgular, deger listesi doner"""
    try:
        r = requests.get(
            DT_BASE_URL + "/api/v2/metrics/query",
            headers=dt_headers(),
            params={
                "metricSelector": metric_selector,
                "entitySelector": 'entityId("%s")' % entity_id,
                "from": from_ts,
                "to": to_ts,
                "resolution": resolution,
            },
            timeout=20,
        )
        r.raise_for_status()
        result = r.json().get("result", [])
        if result:
            data = result[0].get("data", [])
            if data:
                return data[0].get("values", [])
    except Exception as e:
        log.warning("Metrik sorgusu basarisiz (%s): %s", metric_selector, e)
    return []


def get_service_metrics(service_name, start_ts):
    """
    Verilen servis icin 4xx, 5xx ve latency metriklerini ceker.
    Donus: {"fivexx_peak", "fivexx_avg", "fourxx_peak", "fourxx_avg",
            "latency_peak", "latency_avg", "summary", "entity_id"}
    """
    if not start_ts:
        return {}
    entity_id = resolve_service_entity_id(service_name)
    if not entity_id:
        return {}

    from_ts = datetime.fromtimestamp(start_ts / 1000 - 1800, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    to_ts = datetime.fromtimestamp(start_ts / 1000 + 3600, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    fivexx = _query_metric(
        entity_id, "builtin:service.errors.fivexx.rate", from_ts, to_ts
    )
    fourxx = _query_metric(
        entity_id, "builtin:service.errors.fourxx.rate", from_ts, to_ts
    )
    latency = _query_metric(entity_id, "builtin:service.response.time", from_ts, to_ts)

    def clean(lst):
        return [v for v in lst if v is not None]

    def peak(lst):
        return round(max(lst), 2) if lst else 0

    def avg(lst):
        return round(sum(lst) / len(lst), 2) if lst else 0

    fivexx_c = clean(fivexx)
    fourxx_c = clean(fourxx)
    latency_c = clean(latency)

    fivexx_peak = peak(fivexx_c)
    fourxx_peak = peak(fourxx_c)
    fivexx_avg = avg(fivexx_c)
    fourxx_avg = avg(fourxx_c)
    latency_avg = round(avg(latency_c) / 1000, 1) if latency_c else 0  # us->ms
    latency_peak = round(peak(latency_c) / 1000, 1) if latency_c else 0

    parts = []
    if fivexx_peak > 0:
        parts.append("5xx: zirve %s%% / ort %s%%" % (fivexx_peak, fivexx_avg))
    if fourxx_peak > 0:
        parts.append("4xx: zirve %s%% / ort %s%%" % (fourxx_peak, fourxx_avg))
    if latency_peak > 0:
        parts.append("Latency: zirve %sms / ort %sms" % (latency_peak, latency_avg))

    return {
        "entity_id": entity_id,
        "fivexx_peak": fivexx_peak,
        "fivexx_avg": fivexx_avg,
        "fourxx_peak": fourxx_peak,
        "fourxx_avg": fourxx_avg,
        "latency_peak": latency_peak,
        "latency_avg": latency_avg,
        "summary": " | ".join(parts) if parts else "Hata ve gecikme tespit edilmedi.",
    }


# ── CLAUDE PROMPT (metrik dahil) ─────────────────────────────────────────────


def ask_claude(problem, details_text, logs, rc_name, metrics=None):
    evidences = problem.get("evidenceDetails", {}).get("details", [])
    ev_lines = []
    for ev in evidences[:5]:
        props = {p["key"]: p["value"] for p in ev.get("data", {}).get("properties", [])}
        desc = props.get("dt.event.description", "")[:150]
        ev_lines.append(
            "- [%s] %s: %s"
            % (ev.get("evidenceType", ""), ev.get("entity", {}).get("name", ""), desc)
        )

    impacts = problem.get("impactAnalysis", {}).get("impacts", [])
    total_calls = sum(
        i.get("numberOfPotentiallyAffectedServiceCalls", 0) for i in impacts
    )
    affected = [e["name"] for e in problem.get("affectedEntities", [])]
    ns = problem.get("k8s.namespace.name")
    namespace = (ns[0] if isinstance(ns, list) and ns else ns) or "N/A"
    log_section = "\n".join(logs[:5]) if logs else "No logs."
    clean_details = clean_placeholders(details_text)[:600]

    metrics_section = "No metric data available."
    if metrics and metrics.get("summary"):
        m = metrics
        metrics_section = (
            "Service: %s | EntityID: %s\n"
            "5xx Error Rate: peak=%s%% avg=%s%%\n"
            "4xx Error Rate: peak=%s%% avg=%s%%\n"
            "Response Time:  peak=%sms avg=%sms"
        ) % (
            rc_name,
            m.get("entity_id", "N/A"),
            m.get("fivexx_peak", 0),
            m.get("fivexx_avg", 0),
            m.get("fourxx_peak", 0),
            m.get("fourxx_avg", 0),
            m.get("latency_peak", 0),
            m.get("latency_avg", 0),
        )

    prompt = (
        "You are an experienced SRE. Analyze this Dynatrace problem and produce ITIL v4 RCA.\n\n"
        "PROBLEM: %s | %s | Severity: %s | Status: %s\n"
        "Root Cause Service: %s | Affected: %s | Namespace: %s | Calls: %s\n\n"
        "EVIDENCE:\n%s\n\n"
        "LOGS:\n%s\n\n"
        "HTTP ERROR & LATENCY METRICS (distributed tracing data):\n%s\n\n"
        "DETAILS:\n%s\n\n"
        "Respond ONLY with this JSON. Write ALL text values in Turkish:\n"
        '{"root_cause":"2-3 cumle teknik kok neden",'
        '"confidence":"HIGH or MEDIUM or LOW",'
        '"hypotheses":["H1","H2","H3"],'
        '"contributing_factors":["F1","F2"],'
        '"immediate_actions":["A1","A2","A3"],'
        '"preventive_actions":["P1","P2"],'
        '"severity_assessment":"Critical or High or Medium or Low",'
        '"repeat_risk":"High or Medium or Low",'
        '"itil_category":"Infrastructure Failure or Application Error or Performance Degradation or Security",'
        '"trace_finding":"4xx/5xx ve latency metriklerinden en onemli bulgu (1 cumle)"}'
    ) % (
        problem.get("displayId", "N/A"),
        problem.get("title", "N/A"),
        problem.get("severityLevel", "N/A"),
        problem.get("status", "N/A"),
        rc_name,
        ", ".join(affected[:5]) if affected else "None",
        namespace,
        str(total_calls),
        "\n".join(ev_lines) if ev_lines else "No evidence.",
        log_section,
        metrics_section,
        clean_details,
    )

    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 1800,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    response.raise_for_status()
    raw = response.json()["content"][0]["text"].strip()
    if "```json" in raw:
        raw = raw.split("```json")[1].split("```")[0].strip()
    elif "```" in raw:
        raw = raw.split("```")[1].split("```")[0].strip()
    return json.loads(raw)


# ── TEAMS ADAPTIVE CARD (Show Metrics butonu eklendi) ────────────────────────


def build_teams_card(problem, rca, dt_url, rc_name, webhook_state="OPEN", metrics=None):
    display_id = problem.get("displayId", "N/A")
    title = problem.get("title", "Unknown")
    status = problem.get("status", "OPEN")
    severity = problem.get("severityLevel", "")
    ns = problem.get("k8s.namespace.name")
    namespace = (ns[0] if isinstance(ns, list) and ns else ns) or ""

    is_resolved = (status == "CLOSED") or (webhook_state == "RESOLVED")
    state_label = "CLOSED" if is_resolved else "OPEN"
    state_color = "good" if is_resolved else "attention"
    state_icon = "OK" if is_resolved else "!!"

    root_cause = rca.get("root_cause", "Analiz tamamlanamadi.")
    confidence = rca.get("confidence", "MEDIUM")
    severity_ass = rca.get("severity_assessment", "")
    repeat_risk = rca.get("repeat_risk", "")
    itil_cat = rca.get("itil_category", "")
    trace_finding = rca.get("trace_finding", "")

    hyp_text = "\n".join(
        "%d. %s" % (i + 1, h) for i, h in enumerate(rca.get("hypotheses", []))
    )
    imm_text = "\n".join(
        "%d. %s" % (i + 1, a) for i, a in enumerate(rca.get("immediate_actions", []))
    )
    prev_text = "\n".join(
        "%d. %s" % (i + 1, a) for i, a in enumerate(rca.get("preventive_actions", []))
    )
    con_text = "\n".join("- %s" % c for c in rca.get("contributing_factors", []))

    m = metrics or {}
    m_5xx_peak = ("%s%%" % m["fivexx_peak"]) if m.get("fivexx_peak") else "-"
    m_5xx_avg = ("%s%%" % m["fivexx_avg"]) if m.get("fivexx_avg") else "-"
    m_4xx_peak = ("%s%%" % m["fourxx_peak"]) if m.get("fourxx_peak") else "-"
    m_4xx_avg = ("%s%%" % m["fourxx_avg"]) if m.get("fourxx_avg") else "-"
    m_lat_peak = ("%s ms" % m["latency_peak"]) if m.get("latency_peak") else "-"
    m_lat_avg = ("%s ms" % m["latency_avg"]) if m.get("latency_avg") else "-"
    m_summary = m.get("summary", "Metrik verisi alinamadi.")

    main_facts = [
        {"title": "State", "value": state_label},
        {"title": "Problem ID", "value": display_id},
        {"title": "Severity", "value": severity},
        {"title": "Root Cause", "value": rc_name},
    ]
    if namespace:
        main_facts.append({"title": "Namespace", "value": namespace})

    # Show Metrics kart icerigi
    metrics_card_body = [
        {
            "type": "TextBlock",
            "text": "Distributed Tracing & HTTP Metrikler",
            "weight": "Bolder",
            "size": "Medium",
            "color": "Accent",
            "spacing": "None",
        },
        {
            "type": "TextBlock",
            "text": rc_name,
            "size": "Small",
            "isSubtle": True,
            "spacing": "None",
        },
        {
            "type": "TextBlock",
            "text": "Trace Bulgusu",
            "weight": "Bolder",
            "size": "Small",
            "spacing": "Medium",
        },
        {
            "type": "TextBlock",
            "text": trace_finding if trace_finding else m_summary,
            "wrap": True,
            "size": "Small",
        },
        {
            "type": "TextBlock",
            "text": "HTTP Hata Oranlari",
            "weight": "Bolder",
            "size": "Small",
            "color": "Attention",
            "spacing": "Medium",
        },
        {
            "type": "FactSet",
            "facts": [
                {"title": "5xx Zirve", "value": m_5xx_peak},
                {"title": "5xx Ortalama", "value": m_5xx_avg},
                {"title": "4xx Zirve", "value": m_4xx_peak},
                {"title": "4xx Ortalama", "value": m_4xx_avg},
            ],
        },
        {
            "type": "TextBlock",
            "text": "Yanit Suresi (Latency)",
            "weight": "Bolder",
            "size": "Small",
            "color": "Warning",
            "spacing": "Medium",
        },
        {
            "type": "FactSet",
            "facts": [
                {"title": "Zirve Gecikme", "value": m_lat_peak},
                {"title": "Ortalama Gecikme", "value": m_lat_avg},
            ],
        },
        {
            "type": "ActionSet",
            "actions": [
                {
                    "type": "Action.OpenUrl",
                    "title": "Distributed Traces Ac",
                    "url": (dt_url or "").replace(
                        "problemdetails", "distributed-tracing"
                    )
                    or dt_url,
                }
            ],
        },
    ]

    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "ColumnSet",
                            "columns": [
                                {
                                    "type": "Column",
                                    "width": "auto",
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": state_icon,
                                            "weight": "bolder",
                                            "size": "large",
                                            "color": state_color,
                                        }
                                    ],
                                },
                                {
                                    "type": "Column",
                                    "width": "stretch",
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": "Dynatrace Notification",
                                            "weight": "bolder",
                                            "size": "large",
                                            "color": state_color,
                                        },
                                        {
                                            "type": "TextBlock",
                                            "text": title,
                                            "wrap": True,
                                            "spacing": "None",
                                        },
                                    ],
                                },
                            ],
                        },
                        {"type": "FactSet", "spacing": "Medium", "facts": main_facts},
                        {
                            "type": "ActionSet",
                            "actions": [
                                {
                                    "type": "Action.OpenUrl",
                                    "title": "View in Dynatrace",
                                    "url": dt_url,
                                },
                                {
                                    "type": "Action.ShowCard",
                                    "title": "Show Details",
                                    "card": {
                                        "type": "AdaptiveCard",
                                        "body": [
                                            {
                                                "type": "TextBlock",
                                                "text": root_cause,
                                                "wrap": True,
                                            }
                                        ],
                                    },
                                },
                                {
                                    "type": "Action.ShowCard",
                                    "title": "Show RCA Analysis",
                                    "card": {
                                        "type": "AdaptiveCard",
                                        "body": [
                                            {
                                                "type": "TextBlock",
                                                "text": "Root Cause Analysis",
                                                "weight": "Bolder",
                                                "size": "Medium",
                                                "color": "Attention",
                                                "spacing": "None",
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": root_cause,
                                                "wrap": True,
                                                "size": "Small",
                                                "spacing": "Small",
                                            },
                                            {
                                                "type": "FactSet",
                                                "spacing": "Medium",
                                                "facts": [
                                                    {
                                                        "title": "Root Cause Service",
                                                        "value": rc_name,
                                                    },
                                                    {
                                                        "title": "Confidence",
                                                        "value": confidence,
                                                    },
                                                    {
                                                        "title": "Severity",
                                                        "value": severity_ass,
                                                    },
                                                    {
                                                        "title": "Repeat Risk",
                                                        "value": repeat_risk,
                                                    },
                                                    {
                                                        "title": "ITIL Category",
                                                        "value": itil_cat,
                                                    },
                                                ],
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": "Hipotezler",
                                                "weight": "Bolder",
                                                "size": "Small",
                                                "spacing": "Medium",
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": hyp_text,
                                                "wrap": True,
                                                "size": "Small",
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": "Katkida Bulunan Faktorler",
                                                "weight": "Bolder",
                                                "size": "Small",
                                                "spacing": "Medium",
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": con_text,
                                                "wrap": True,
                                                "size": "Small",
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": "Acil Aksiyonlar",
                                                "weight": "Bolder",
                                                "size": "Small",
                                                "color": "Attention",
                                                "spacing": "Medium",
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": imm_text,
                                                "wrap": True,
                                                "size": "Small",
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": "Onleyici Aksiyonlar",
                                                "weight": "Bolder",
                                                "size": "Small",
                                                "spacing": "Medium",
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": prev_text,
                                                "wrap": True,
                                                "size": "Small",
                                            },
                                        ],
                                    },
                                },
                                # ── YENİ BUTON ──────────────────────────────────────────
                                {
                                    "type": "Action.ShowCard",
                                    "title": "Show Metrics",
                                    "card": {
                                        "type": "AdaptiveCard",
                                        "body": metrics_card_body,
                                    },
                                },
                            ],
                        },
                    ],
                },
            }
        ],
    }


# ── ORKESTRATÖR ──────────────────────────────────────────────────────────────


def process_problem(display_id, problem_title, state, dt_url, details_text):
    log.info("Isleniyor: %s - %s (%s)", display_id, problem_title, state)
    basic_info = find_problem_by_display_id(display_id)
    problem = {}
    if basic_info.get("problemId"):
        problem = get_problem_details(basic_info["problemId"])
    if not problem:
        log.warning("Webhook verisi kullaniliyor: %s", display_id)
        problem = {
            "displayId": display_id,
            "title": problem_title,
            "status": state,
            "severityLevel": "ERROR",
            "impactLevel": "SERVICES",
            "affectedEntities": [],
            "rootCauseEntity": None,
        }

    rc_name = get_root_cause_name(problem, details_text)
    start_ts = problem.get("startTime", 0)
    logs_data = get_recent_logs(start_ts) if start_ts else []

    # YENİ: metrik verisi çek
    metrics = {}
    if rc_name and rc_name not in ("Unknown Service",) and start_ts:
        log.info("Metrik sorgusu basladi: %s -> %s", display_id, rc_name)
        metrics = get_service_metrics(rc_name, start_ts)
        log.info("Metrik: %s -> %s", display_id, metrics.get("summary", "alinamadi"))

    log.info("Claude sorgu: %s (rc: %s)", display_id, rc_name)
    rca = ask_claude(problem, details_text, logs_data, rc_name, metrics)
    log.info(
        "RCA OK: %s confidence=%s trace_finding=%s",
        display_id,
        rca.get("confidence"),
        bool(rca.get("trace_finding")),
    )

    card = build_teams_card(problem, rca, dt_url, rc_name, state, metrics)
    r = requests.post(
        TEAMS_WEBHOOK,
        headers={"Content-Type": "application/json"},
        json=card,
        timeout=15,
    )
    if r.ok:
        log.info("Teams OK: %s", display_id)
        return True
    log.error("Teams hata: %s %s", r.status_code, r.text[:200])
    return False


# ── FLASK ENDPOINTLERİ ───────────────────────────────────────────────────────


@app.route("/webhook/dynatrace-rca", methods=["POST"])
def dynatrace_webhook():
    if WEBHOOK_SECRET:
        if request.headers.get("X-Webhook-Token", "") != WEBHOOK_SECRET:
            return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    log.info("Webhook alindi: %s", json.dumps(data)[:200])
    display_id = data.get("ProblemID", "")
    problem_title = data.get("ProblemTitle", "")
    state = data.get("State", "")
    dt_url = data.get("URL", "")
    details_text = data.get("Details", "")
    if state not in ("OPEN", "RESOLVED"):
        log.info("Atlandi: %s state=%s", display_id, state)
        return jsonify({"status": "skipped", "reason": "unknown state"}), 200
    if not display_id:
        return jsonify({"error": "ProblemID eksik"}), 400
    ok = process_problem(display_id, problem_title, state, dt_url, details_text)
    return jsonify(
        {"status": "ok" if ok else "teams_error", "problem": display_id, "state": state}
    ), (200 if ok else 500)


@app.route("/health", methods=["GET"])
def health():
    missing = [
        k
        for k in ["DT_BASE_URL", "DT_API_TOKEN", "CLAUDE_API_KEY", "TEAMS_WEBHOOK"]
        if not os.environ.get(k)
    ]
    if missing:
        return jsonify({"status": "degraded", "missing_env": missing}), 200
    return jsonify({"status": "ok", "version": "1.3.0"}), 200


@app.route("/", methods=["GET"])
def root():
    return jsonify({"service": "Dynatrace RCA Bot", "version": "1.3.0"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
