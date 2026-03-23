#!/usr/bin/env python3
"""Dynatrace -> Claude API -> Microsoft Teams | RCA Bot v1.2"""
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
    """Cozumlenmemis Dynatrace placeholder {XYZ} degerlerini temizle"""
    return re.sub(r"\{[A-Za-z][A-Za-z0-9]*\}", "", str(text or ""))


def get_root_cause_name(problem, details_text):
    """Gercek root cause servis adini belirle: DT API > affected > details text"""
    # 1. Dynatrace API rootCauseEntity
    rc = (problem.get("rootCauseEntity") or {}).get("name", "")
    if rc:
        return rc
    # 2. Affected entities listesinin ilk elemani
    affected = problem.get("affectedEntities", [])
    if affected:
        name = affected[0].get("name", "")
        if name:
            return name
    # 3. Details metnindeki "Root Cause: XXX" kismindan parse et
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
    # 4. Problem basligi
    title = problem.get("title", "")
    if title:
        return title
    return "Unknown Service"


def ask_claude(problem, details_text, logs, rc_name):
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
    prompt = (
        "You are an experienced SRE. Analyze this Dynatrace problem and produce ITIL v4 RCA.\n\n"
        "PROBLEM: %s | %s | Severity: %s | Status: %s\n"
        "Root Cause Service: %s | Affected: %s | Namespace: %s | Calls: %s\n\n"
        "EVIDENCE:\n%s\n\nLOGS:\n%s\n\nDETAILS:\n%s\n\n"
        "Respond ONLY with this JSON (no extra text). Write ALL text values in Turkish:\n"
        '{"root_cause":"2-3 sentence technical root cause",'
        '"confidence":"HIGH or MEDIUM or LOW",'
        '"hypotheses":["H1","H2","H3"],'
        '"contributing_factors":["F1","F2"],'
        '"immediate_actions":["A1","A2","A3"],'
        '"preventive_actions":["P1","P2"],'
        '"severity_assessment":"Critical or High or Medium or Low",'
        '"repeat_risk":"High or Medium or Low",'
        '"itil_category":"Infrastructure Failure or Application Error or Performance Degradation or Security"}'
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
            "max_tokens": 1500,
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


def build_teams_card(problem, rca, dt_url, rc_name, webhook_state="OPEN"):
    display_id = problem.get("displayId", "N/A")
    title = problem.get("title", "Unknown")
    status = problem.get("status", "OPEN")
    severity = problem.get("severityLevel", "")
    is_resolved = (status == "CLOSED") or (webhook_state == "RESOLVED")
    state_label = "CLOSED" if is_resolved else "OPEN"
    state_color = "good" if is_resolved else "attention"
    state_icon = "OK" if is_resolved else "!!"
    root_cause = rca.get("root_cause", "Analysis incomplete.")
    confidence = rca.get("confidence", "MEDIUM")
    severity_ass = rca.get("severity_assessment", "")
    repeat_risk = rca.get("repeat_risk", "")
    itil_cat = rca.get("itil_category", "")
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
                        {
                            "type": "FactSet",
                            "spacing": "Medium",
                            "facts": [
                                {"title": "State", "value": state_label},
                                {"title": "Problem ID", "value": display_id},
                                {"title": "Severity", "value": severity},
                                {"title": "Root Cause", "value": rc_name},
                            ],
                        },
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
                                                "text": "Hypotheses",
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
                                                "text": "Contributing Factors",
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
                                                "text": "Immediate Actions",
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
                                                "text": "Preventive Actions",
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
                            ],
                        },
                    ],
                },
            }
        ],
    }


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
    log.info("Claude sorgu: %s (rc: %s)", display_id, rc_name)
    rca = ask_claude(problem, details_text, logs_data, rc_name)
    log.info("RCA OK: %s confidence=%s", display_id, rca.get("confidence"))
    card = build_teams_card(problem, rca, dt_url, rc_name, state)
    card = build_teams_card(problem, rca, dt_url, rc_name)
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
    # OPEN ve RESOLVED her ikisi de isleniyor
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
    return jsonify({"status": "ok", "version": "1.2.0"}), 200


@app.route("/", methods=["GET"])
def root():
    return jsonify({"service": "Dynatrace RCA Bot", "version": "1.2.0"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
