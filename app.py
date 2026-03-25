#!/usr/bin/env python3
"""Dynatrace -> Claude API -> Microsoft Teams | RCA Bot v1.4
- Metrik API cagrisi kaldirildi (Railway ic aga erisemiyor)
- Claude analizi: evidence + log + problem detayi uzerinden yapilir
- Teams kart: Show RCA Analysis (hipotez, aksiyon, ITIL) - temiz ve net
- OPEN kirmizi !! / RESOLVED yesil OK
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
    """
    Kisa, temiz servis adini dondur (entity lookup ve kart icin).
    Oncelik: rootCauseEntity > affectedEntities[0] > details parse > title parse
    """
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
        part = part.strip()
        if "Root Cause" in part:
            val = part.split(":", 1)[-1].strip()
            if val and len(val) > 2 and "on Web service" not in val and "{" not in val:
                return val
    title = problem.get("title", "")
    if title:
        for prefix in (
            "on Web service ",
            "on service ",
            "on web service ",
            "on Web request service ",
            "on web request service ",
        ):
            if prefix in title.lower():
                idx = title.lower().index(prefix)
                candidate = title[idx + len(prefix) :].strip()
                if candidate:
                    return candidate
        return title
    return "Unknown Service"


# ── CLAUDE PROMPT ─────────────────────────────────────────────────────────────


def ask_claude(problem, details_text, logs, rc_name):
    evidences = problem.get("evidenceDetails", {}).get("details", [])
    ev_lines = []
    for ev in evidences[:5]:
        props = {p["key"]: p["value"] for p in ev.get("data", {}).get("properties", [])}
        # Error rate / response time baseline bilgilerini de ekle
        ev_desc = props.get("dt.event.description", "")[:200]
        ev_lines.append(
            "- [%s] %s: %s"
            % (
                ev.get("evidenceType", ""),
                ev.get("entity", {}).get("name", ""),
                ev_desc,
            )
        )
        # Sayisal evidence varsa ekle (error rate, throughput vs)
        before = props.get("dt.event.baseline.error_rate_reference", "")
        after = props.get("dt.event.baseline.error_rate", "")
        if before and after:
            try:
                ev_lines.append(
                    "  baseline: %.4f%% -> simdiki: %.4f%% (%.1fx artis)"
                    % (
                        float(before) * 100,
                        float(after) * 100,
                        float(after) / float(before) if float(before) > 0 else 0,
                    )
                )
            except Exception:
                pass

    impacts = problem.get("impactAnalysis", {}).get("impacts", [])
    total_calls = sum(
        i.get("numberOfPotentiallyAffectedServiceCalls", 0) for i in impacts
    )
    affected = [e["name"] for e in problem.get("affectedEntities", [])]
    ns = problem.get("k8s.namespace.name")
    namespace = (ns[0] if isinstance(ns, list) and ns else ns) or "N/A"
    log_section = "\n".join(logs[:5]) if logs else "Log yok."
    clean_details = clean_placeholders(details_text)[:800]

    prompt = (
        "Sen deneyimli bir SRE uzmanisın. Asagidaki Dynatrace problemi icin ITIL v4 RCA analizi yap.\n\n"
        "PROBLEM: %s | %s | Severity: %s | Status: %s\n"
        "Root Cause Servis: %s | Etkilenen: %s | Namespace: %s | Etkilenen Cagri: %s\n\n"
        "EVIDENCE (hata orani baseline verileri dahil):\n%s\n\n"
        "LOG KAYITLARI:\n%s\n\n"
        "DETAYLAR:\n%s\n\n"
        "SADECE asagidaki JSON formatinda yanit ver (fazladan metin ekleme). "
        "Tum metin degerlerini Turkce yaz:\n"
        '{"root_cause":"2-3 cumle teknik kok neden. Evidence baseline verilerini kullan (ornek: hata orani X%%den Y%%e yukseldi)","'
        'confidence":"HIGH or MEDIUM or LOW","'
        'hypotheses":["H1","H2","H3"],"'
        'contributing_factors":["F1","F2"],"'
        'immediate_actions":["A1 - kubectl komutu ile","A2","A3"],"'
        'preventive_actions":["P1","P2"],"'
        'severity_assessment":"Critical or High or Medium or Low","'
        'repeat_risk":"High or Medium or Low","'
        'itil_category":"Infrastructure Failure or Application Error or Performance Degradation or Security"}'
    ) % (
        problem.get("displayId", "N/A"),
        problem.get("title", "N/A"),
        problem.get("severityLevel", "N/A"),
        problem.get("status", "N/A"),
        rc_name,
        ", ".join(affected[:5]) if affected else "Belirsiz",
        namespace,
        str(total_calls),
        "\n".join(ev_lines) if ev_lines else "Evidence yok.",
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


# ── TEAMS ADAPTIVE CARD ───────────────────────────────────────────────────────


def build_teams_card(problem, rca, dt_url, rc_name, webhook_state="OPEN"):
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

    main_facts = [
        {"title": "State", "value": state_label},
        {"title": "Problem ID", "value": display_id},
        {"title": "Severity", "value": severity},
        {"title": "Root Cause", "value": rc_name},
    ]
    if namespace:
        main_facts.append({"title": "Namespace", "value": namespace})

    # Evidence verilerini Show Details icin hazirla
    evidences = problem.get("evidenceDetails", {}).get("details", [])
    ev_summary_lines = []
    for ev in evidences[:3]:
        props = {p["key"]: p["value"] for p in ev.get("data", {}).get("properties", [])}
        desc = props.get("dt.event.description", "")[:150]
        before = props.get("dt.event.baseline.error_rate_reference", "")
        after = props.get("dt.event.baseline.error_rate", "")
        if desc:
            ev_summary_lines.append(desc)
        if before and after:
            try:
                ev_summary_lines.append(
                    "Hata orani: %.3f%% → %.3f%%"
                    % (float(before) * 100, float(after) * 100)
                )
            except Exception:
                pass
    evidence_text = "\n".join(ev_summary_lines) if ev_summary_lines else root_cause

    rca_card_body = [
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
            "spacing": "Small",
            "facts": [
                {"title": "Root Cause Servis", "value": rc_name},
                {"title": "Confidence", "value": confidence},
                {"title": "Severity", "value": severity_ass},
                {"title": "Repeat Risk", "value": repeat_risk},
                {"title": "ITIL Kategori", "value": itil_cat},
            ],
        },
        {
            "type": "TextBlock",
            "text": "Hipotezler",
            "weight": "Bolder",
            "size": "Small",
            "spacing": "Medium",
        },
        {"type": "TextBlock", "text": hyp_text, "wrap": True, "size": "Small"},
        {
            "type": "TextBlock",
            "text": "Katkida Bulunan Faktorler",
            "weight": "Bolder",
            "size": "Small",
            "spacing": "Medium",
        },
        {"type": "TextBlock", "text": con_text, "wrap": True, "size": "Small"},
        {
            "type": "TextBlock",
            "text": "Acil Aksiyonlar",
            "weight": "Bolder",
            "size": "Small",
            "color": "Attention",
            "spacing": "Medium",
        },
        {"type": "TextBlock", "text": imm_text, "wrap": True, "size": "Small"},
        {
            "type": "TextBlock",
            "text": "Onleyici Aksiyonlar",
            "weight": "Bolder",
            "size": "Small",
            "spacing": "Medium",
        },
        {"type": "TextBlock", "text": prev_text, "wrap": True, "size": "Small"},
        # Evidence / baseline verisi
        {
            "type": "TextBlock",
            "text": "Dynatrace Evidence",
            "weight": "Bolder",
            "size": "Small",
            "color": "Accent",
            "spacing": "Medium",
        },
        {
            "type": "TextBlock",
            "text": evidence_text,
            "wrap": True,
            "size": "Small",
            "isSubtle": True,
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
                                        "body": rca_card_body,
                                    },
                                },
                            ],
                        },
                    ],
                },
            }
        ],
    }


# ── ORKESTRATÖR ───────────────────────────────────────────────────────────────


def process_problem(display_id, problem_title, state, dt_url, details_text):
    log.info("Isleniyor: %s - %s (%s)", display_id, problem_title, state)

    # 1. Problem detayini DT API'den cekmeye calis (ic ag erisimi yoksa fallback)
    basic_info = find_problem_by_display_id(display_id)
    problem = {}
    if basic_info.get("problemId"):
        problem = get_problem_details(basic_info["problemId"])

    if not problem:
        log.warning("Webhook verisi kullaniliyor: %s", display_id)
        # Details metninden servis adini parse et
        fallback_name = problem_title
        for part in clean_placeholders(details_text).split("|"):
            part = part.strip()
            if "Root Cause" in part:
                val = part.split(":", 1)[-1].strip()
                if (
                    val
                    and len(val) > 2
                    and "on Web service" not in val
                    and "{" not in val
                ):
                    fallback_name = val
                    break
        problem = {
            "displayId": display_id,
            "title": problem_title,
            "status": state,
            "severityLevel": "ERROR",
            "impactLevel": "SERVICES",
            "affectedEntities": [
                {"name": fallback_name, "entityId": {"id": "", "type": "SERVICE"}}
            ],
            "rootCauseEntity": None,
            "startTime": int(datetime.now(timezone.utc).timestamp() * 1000) - 3600000,
        }

    # 2. Root cause servis adini belirle
    rc_name = get_root_cause_name(problem, details_text)
    start_ts = problem.get("startTime", 0)

    # 3. Log verisi (ic ag yoksa bos donecek - normal)
    logs_data = get_recent_logs(start_ts) if start_ts else []

    # 4. Claude ile RCA uret (evidence + log yeterli, metrik API yok)
    log.info("Claude sorgu: %s (rc: %s)", display_id, rc_name)
    rca = ask_claude(problem, details_text, logs_data, rc_name)
    log.info("RCA OK: %s confidence=%s", display_id, rca.get("confidence"))

    # 5. Teams'e gonder
    card = build_teams_card(problem, rca, dt_url, rc_name, state)
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


# ── FLASK ENDPOINTLERİ ────────────────────────────────────────────────────────


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
    return jsonify({"status": "ok", "version": "1.4.0"}), 200


@app.route("/", methods=["GET"])
def root():
    return jsonify({"service": "Dynatrace RCA Bot", "version": "1.4.0"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
