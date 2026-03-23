#!/usr/bin/env python3
"""
Dynatrace → Claude API → Microsoft Teams
RCA Bot - Webhook Sunucusu

Dynatrace'ten gelen problem bildirimleri:
1. Dynatrace API'den detay çekilir
2. Claude'a RCA analizi yaptırılır
3. Teams'e Adaptive Card gönderilir
"""

import os
import json
import logging
import requests
from flask import Flask, request, jsonify
from datetime import datetime, timezone

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ── Ortam değişkenleri ────────────────────────────────────────────────────────
DT_BASE_URL    = os.environ.get("DT_BASE_URL", "").rstrip("/")
DT_API_TOKEN   = os.environ.get("DT_API_TOKEN", "")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY", "")
TEAMS_WEBHOOK  = os.environ.get("TEAMS_WEBHOOK", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")   # opsiyonel güvenlik

# ── Dynatrace API ─────────────────────────────────────────────────────────────
def dt_headers():
    return {"Authorization": f"Api-Token {DT_API_TOKEN}", "Content-Type": "application/json"}

def find_problem_by_display_id(display_id: str) -> dict:
    """Display ID (P-XXXXX) ile internal problem ID'yi bulur."""
    try:
        r = requests.get(
            f"{DT_BASE_URL}/api/v2/problems",
            headers=dt_headers(),
            params={"from": "now-2h", "pageSize": 50, "sort": "-startTime"},
            timeout=15
        )
        r.raise_for_status()
        for p in r.json().get("problems", []):
            if p.get("displayId") == display_id:
                return p
    except Exception as e:
        log.warning(f"Problem listesi alınamadı: {e}")
    return {}

def get_problem_details(internal_id: str) -> dict:
    """Internal ID ile tam problem detayını çeker."""
    try:
        r = requests.get(
            f"{DT_BASE_URL}/api/v2/problems/{internal_id}",
            headers=dt_headers(),
            timeout=15
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"Problem detayı alınamadı: {e}")
    return {}

def get_recent_logs(start_ts: int) -> list[str]:
    """Problem başlangıcı civarındaki hata loglarını çeker."""
    try:
        from_t = datetime.fromtimestamp(start_ts / 1000 - 120, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        to_t   = datetime.fromtimestamp(start_ts / 1000 + 600, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        r = requests.get(
            f"{DT_BASE_URL}/api/v2/logs/search",
            headers=dt_headers(),
            params={"from": from_t, "to": to_t, "query": "error", "limit": 8},
            timeout=15
        )
        results = r.json().get("results", [])
        return [item.get("content", "")[:200] for item in results if item.get("content")]
    except Exception as e:
        log.warning(f"Log sorgusu başarısız: {e}")
    return []

# ── Claude API ────────────────────────────────────────────────────────────────
def ask_claude(problem: dict, details_text: str, logs: list[str]) -> dict:
    """Dynatrace verilerini Claude'a gönderir, JSON RCA alır."""

    # Kanıt özeti
    evidences = problem.get("evidenceDetails", {}).get("details", [])
    ev_lines = []
    for ev in evidences[:5]:
        props = {p["key"]: p["value"] for p in ev.get("data", {}).get("properties", [])}
        desc = props.get("dt.event.description", "")[:150]
        ev_lines.append(f"- [{ev.get('evidenceType')}] {ev.get('entity', {}).get('name', '')}: {desc}")

    # Etki özeti
    impacts = problem.get("impactAnalysis", {}).get("impacts", [])
    total_calls = sum(i.get("numberOfPotentiallyAffectedServiceCalls", 0) for i in impacts)

    rc_name   = (problem.get("rootCauseEntity") or {}).get("name", "Belirlenmedi")
    affected  = [e["name"] for e in problem.get("affectedEntities", [])]
    namespace = ""
    if isinstance(problem.get("k8s.namespace.name"), list):
        namespace = problem["k8s.namespace.name"][0] if problem["k8s.namespace.name"] else ""

    log_section = "\n".join(logs[:5]) if logs else "Log bulunamadı."

    prompt = f"""Sen deneyimli bir Site Reliability Engineer'sın.
Aşağıdaki Dynatrace problem verilerini analiz ederek ITIL v4 ve PMI PMBOK prensiplerine göre
kapsamlı bir RCA (Root Cause Analysis) üret.

═══ PROBLEM BİLGİLERİ ═══
Problem ID    : {problem.get('displayId', 'N/A')}
Başlık        : {problem.get('title', 'N/A')}
Önem          : {problem.get('severityLevel', 'N/A')}
Etki Seviyesi : {problem.get('impactLevel', 'N/A')}
Durum         : {problem.get('status', 'N/A')}
Root Cause    : {rc_name}
Etkilenen     : {', '.join(affected[:5]) if affected else 'Yok'}
Namespace     : {namespace or 'N/A'}
Etkilenen Çağrı: {total_calls:,}

═══ DYNATRACE KANIT DETAYLARI ═══
{chr(10).join(ev_lines) if ev_lines else 'Kanıt yok.'}

═══ İLGİLİ LOG KAYITLARI ═══
{log_section}

═══ PROBLEM DETAY METNİ ═══
{details_text[:600]}

GÖREV: Yukarıdaki verileri analiz ederek SADECE aşağıdaki JSON formatında yanıt ver.
Başka hiçbir metin ekleme, sadece JSON bloğu döndür.

{{
  "root_cause": "Kök nedenin net ve teknik 2-3 cümlelik açıklaması",
  "confidence": "YÜKSEK veya ORTA veya DÜŞÜK",
  "hypotheses": [
    "En olası hipotez — kanıta dayalı",
    "İkinci olası hipotez",
    "Üçüncü olası hipotez"
  ],
  "contributing_factors": [
    "Katkıda bulunan faktör 1",
    "Katkıda bulunan faktör 2"
  ],
  "immediate_actions": [
    "Şimdi yapılacak aksiyon 1 (kubectl/komut içerebilir)",
    "Şimdi yapılacak aksiyon 2",
    "Şimdi yapılacak aksiyon 3"
  ],
  "preventive_actions": [
    "Tekrarı önleyen uzun vadeli aksiyon 1",
    "Tekrarı önleyen uzun vadeli aksiyon 2"
  ],
  "severity_assessment": "Kritik veya Yüksek veya Orta veya Düşük",
  "repeat_risk": "Yüksek veya Orta veya Düşük",
  "itil_category": "Altyapı Arızası veya Uygulama Hatası veya Performans Bozulması veya Güvenlik"
}}"""

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
            "messages": [{"role": "user", "content": prompt}]
        },
        timeout=60
    )
    response.raise_for_status()

    raw = response.json()["content"][0]["text"].strip()

    # JSON bloğunu güvenli çıkar
    if "```json" in raw:
        raw = raw.split("```json")[1].split("```")[0].strip()
    elif "```" in raw:
        raw = raw.split("```")[1].split("```")[0].strip()

    return json.loads(raw)

# ── Teams Adaptive Card ───────────────────────────────────────────────────────
def build_teams_card(problem: dict, rca: dict, dt_url: str) -> dict:
    """Teams Adaptive Card payload'unu oluşturur."""

    display_id = problem.get("displayId", "N/A")
    title      = problem.get("title", "Bilinmiyor")
    status     = problem.get("status", "OPEN")
    severity   = problem.get("severityLevel", "")
    rc_name    = (problem.get("rootCauseEntity") or {}).get("name", "Belirlenmedi")

    state_label = "ÇÖZÜLDÜ" if status == "CLOSED" else "AÇIK"
    state_color = "good"    if status == "CLOSED" else "attention"
    state_icon  = "OK"      if status == "CLOSED" else "!!"

    # RCA alanları
    root_cause   = rca.get("root_cause", "Analiz tamamlanamadı.")
    confidence   = rca.get("confidence", "ORTA")
    severity_ass = rca.get("severity_assessment", "")
    repeat_risk  = rca.get("repeat_risk", "")
    itil_cat     = rca.get("itil_category", "")

    hypotheses   = rca.get("hypotheses", [])
    imm_actions  = rca.get("immediate_actions", [])
    prev_actions = rca.get("preventive_actions", [])
    contrib      = rca.get("contributing_factors", [])

    hyp_text  = "\n".join(f"{i+1}. {h}" for i, h in enumerate(hypotheses))
    imm_text  = "\n".join(f"{i+1}. {a}" for i, a in enumerate(imm_actions))
    prev_text = "\n".join(f"{i+1}. {a}" for i, a in enumerate(prev_actions))
    con_text  = "\n".join(f"- {c}" for c in contrib)

    return {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "contentUrl": None,
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    # Header
                    {
                        "type": "ColumnSet",
                        "columns": [
                            {
                                "type": "Column", "width": "auto",
                                "items": [{"type": "TextBlock", "text": state_icon,
                                           "weight": "bolder", "size": "large",
                                           "color": state_color}]
                            },
                            {
                                "type": "Column", "width": "stretch",
                                "items": [
                                    {"type": "TextBlock", "text": "Dynatrace Notification",
                                     "weight": "bolder", "size": "large", "color": state_color},
                                    {"type": "TextBlock", "text": title, "wrap": True, "spacing": "None"}
                                ]
                            }
                        ]
                    },
                    # Metadata
                    {
                        "type": "FactSet",
                        "spacing": "Medium",
                        "facts": [
                            {"title": "State",      "value": state_label},
                            {"title": "Problem ID", "value": display_id},
                            {"title": "Severity",   "value": severity},
                            {"title": "Root Cause", "value": rc_name},
                        ]
                    },
                    # Actions
                    {
                        "type": "ActionSet",
                        "actions": [
                            {
                                "type": "Action.OpenUrl",
                                "title": "View in Dynatrace",
                                "url": dt_url
                            },
                            {
                                "type": "Action.ShowCard",
                                "title": "Show Details",
                                "card": {
                                    "type": "AdaptiveCard",
                                    "body": [
                                        {"type": "TextBlock", "text": root_cause,
                                         "wrap": True, "size": "Small"}
                                    ]
                                }
                            },
                            {
                                "type": "Action.ShowCard",
                                "title": "Show RCA Analysis",
                                "card": {
                                    "type": "AdaptiveCard",
                                    "body": [
                                        # RCA Başlık
                                        {"type": "TextBlock", "text": "Root Cause Analysis",
                                         "weight": "Bolder", "size": "Medium",
                                         "color": "Attention", "spacing": "None"},
                                        # Özet
                                        {"type": "TextBlock", "text": root_cause,
                                         "wrap": True, "size": "Small", "spacing": "Small"},
                                        # Metadata
                                        {"type": "FactSet", "spacing": "Medium", "facts": [
                                            {"title": "Guven",     "value": confidence},
                                            {"title": "Onem",      "value": severity_ass},
                                            {"title": "Tekrar Riski", "value": repeat_risk},
                                            {"title": "ITIL Kategori", "value": itil_cat},
                                        ]},
                                        # Hipotezler
                                        {"type": "TextBlock", "text": "Hipotezler",
                                         "weight": "Bolder", "size": "Small",
                                         "spacing": "Medium"},
                                        {"type": "TextBlock", "text": hyp_text,
                                         "wrap": True, "size": "Small", "spacing": "Small"},
                                        # Katkıda Bulunan Faktörler
                                        {"type": "TextBlock", "text": "Katkida Bulunan Faktorler",
                                         "weight": "Bolder", "size": "Small",
                                         "spacing": "Medium"},
                                        {"type": "TextBlock", "text": con_text,
                                         "wrap": True, "size": "Small", "spacing": "Small"},
                                        # Acil Aksiyonlar
                                        {"type": "TextBlock", "text": "Acil Aksiyonlar",
                                         "weight": "Bolder", "size": "Small",
                                         "color": "Attention", "spacing": "Medium"},
                                        {"type": "TextBlock", "text": imm_text,
                                         "wrap": True, "size": "Small", "spacing": "Small"},
                                        # Önleyici Aksiyonlar
                                        {"type": "TextBlock", "text": "Onleyici Aksiyonlar",
                                         "weight": "Bolder", "size": "Small",
                                         "spacing": "Medium"},
                                        {"type": "TextBlock", "text": prev_text,
                                         "wrap": True, "size": "Small", "spacing": "Small"},
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
        }]
    }

# ── Webhook Endpoint ──────────────────────────────────────────────────────────
@app.route("/webhook/dynatrace-rca", methods=["POST"])
def dynatrace_webhook():
    """Dynatrace'ten gelen POST isteğini işler."""

    # Opsiyonel secret kontrolü
    if WEBHOOK_SECRET:
        token = request.headers.get("X-Webhook-Token", "")
        if token != WEBHOOK_SECRET:
            log.warning("Yetkisiz webhook isteği reddedildi.")
            return jsonify({"error": "Unauthorized"}), 401

    data = request.json or {}
    log.info(f"Webhook alındı: {json.dumps(data)[:200]}")

    display_id    = data.get("ProblemID", "")
    problem_title = data.get("ProblemTitle", "")
    state         = data.get("State", "")
    dt_url        = data.get("URL", "")
    details_text  = data.get("Details", "")

    # Sadece OPEN problemleri işle
    if state != "OPEN":
        log.info(f"Atlandı: {display_id} — state={state}")
        return jsonify({"status": "skipped", "reason": "not OPEN", "id": display_id}), 200

    if not display_id:
        return jsonify({"error": "ProblemID eksik"}), 400

    log.info(f"İşleniyor: {display_id} — {problem_title}")

    # 1. Dynatrace'ten problem detayı çek
    basic_info = find_problem_by_display_id(display_id)
    problem    = {}
    if basic_info.get("problemId"):
        problem = get_problem_details(basic_info["problemId"])

    # API'den alınamazsa webhook payload'ından temel bilgi yap
    if not problem:
        log.warning(f"API'den detay alınamadı, webhook verisi kullanılıyor: {display_id}")
        problem = {
            "displayId":     display_id,
            "title":         problem_title,
            "status":        state,
            "severityLevel": data.get("Severity", ""),
            "affectedEntities": [],
            "rootCauseEntity":  None,
        }

    # 2. Log çek
    start_ts = problem.get("startTime", 0)
    logs     = get_recent_logs(start_ts) if start_ts else []

    # 3. Claude'dan RCA al
    log.info(f"Claude'a sorgu gönderiliyor: {display_id}")
    rca = ask_claude(problem, details_text, logs)
    log.info(f"RCA alındı: confidence={rca.get('confidence')}, severity={rca.get('severity_assessment')}")

    # 4. Teams'e Adaptive Card gönder
    card = build_teams_card(problem, rca, dt_url)
    r = requests.post(
        TEAMS_WEBHOOK,
        headers={"Content-Type": "application/json"},
        json=card,
        timeout=15
    )

    if r.ok:
        log.info(f"Teams bildirimi gönderildi: {display_id}")
        return jsonify({"status": "ok", "problem": display_id}), 200
    else:
        log.error(f"Teams hatası: {r.status_code} — {r.text[:200]}")
        return jsonify({"status": "teams_error", "code": r.status_code}), 500

# ── Health check ──────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    missing = [k for k in ["DT_BASE_URL", "DT_API_TOKEN", "CLAUDE_API_KEY", "TEAMS_WEBHOOK"]
               if not os.environ.get(k)]
    if missing:
        return jsonify({"status": "degraded", "missing_env": missing}), 200
    return jsonify({"status": "ok", "version": "1.0.0"}), 200

@app.route("/", methods=["GET"])
def root():
    return jsonify({"service": "Dynatrace RCA Bot", "status": "running"}), 200

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)