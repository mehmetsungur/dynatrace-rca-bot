#!/usr/bin/env python3
"""Dynatrace -> Claude API -> Microsoft Teams | RCA Bot v1.7
- 4xx/5xx hata adetleri + peak oran + max/avg response time (tek API cagrisi)
- Yeni DT token ile metrik API erisimi aktif
- Entity ID: rootCauseEntity > affectedEntities[0] zincirleme
- HTTP metrikleri erisilemezse sessizce atlanir (fallback safe)
- Onceki v1.5 ozellikleri korundu (baseline, downstream, pattern, timeline)
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
            params={"from": from_t, "to": to_t, "query": "error", "limit": 10},
            timeout=15,
        )
        return [
            i.get("content", "")[:250]
            for i in r.json().get("results", [])
            if i.get("content")
        ]
    except Exception as e:
        log.warning("Log sorgusu basarisiz: %s", e)
    return []


def get_http_metrics(entity_id, start_ts, end_ts):
    """
    Etkilenen servis icin 4xx/5xx adet + oran + max/avg response time ceker.
    Tek API cagrisiyla 5 metrik birden alinir (verimli).
    entity_id : "SERVICE-XXXX"
    start_ts  : Unix ms — problem baslangici (0 ise son 1 saat)
    end_ts    : Unix ms — problem bitisi  (-1 ise simdi)
    Donus: {fivexx_count, fivexx_peak_rate, fourxx_count, fourxx_peak_rate,
            total_errors, response_time_max_ms, response_time_avg_ms} veya {}
    """
    if not entity_id:
        return {}
    try:
        # Zaman araligi: problem baslangicından 30 dk once → bitis + 5 dk
        if start_ts and start_ts > 0:
            from_dt = datetime.fromtimestamp(
                max(start_ts / 1000 - 1800, 0), tz=timezone.utc
            )
        else:
            from_dt = datetime.fromtimestamp(
                datetime.now(timezone.utc).timestamp() - 3600, tz=timezone.utc
            )
        if end_ts and end_ts > 0:
            to_dt = datetime.fromtimestamp(end_ts / 1000 + 300, tz=timezone.utc)
        else:
            to_dt = datetime.now(timezone.utc)

        from_str = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_str = to_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        es = 'entityId("%s")' % entity_id

        # Tek cagriyla 5 metrik birden
        COMBINED = (
            "builtin:service.errors.fivexx.rate,"
            "builtin:service.errors.fourxx.rate,"
            "builtin:service.requestCount.total,"
            "builtin:service.errors.total.count,"
            "builtin:service.response.time:max,"
            "builtin:service.response.time"
        )
        r = requests.get(
            DT_BASE_URL + "/api/v2/metrics/query",
            headers=dt_headers(),
            params={
                "metricSelector": COMBINED,
                "entitySelector": es,
                "from": from_str,
                "to": to_str,
                "resolution": "5m",
            },
            timeout=25,
        )
        r.raise_for_status()

        # Sonuclari metrik ID'ye gore indeksle
        raw = {}
        for series in r.json().get("result", []):
            mid = series.get("metricId", "")
            data = series.get("data", [{}])
            vals = data[0].get("values", []) if data else []
            raw[mid] = [v for v in vals if v is not None]

        def _peak(lst):
            return round(max(lst), 2) if lst else 0

        def _sum(lst):
            return int(sum(lst)) if lst else 0

        def _avg(lst):
            return round(sum(lst) / len(lst), 2) if lst else 0

        fivexx_rates = raw.get("builtin:service.errors.fivexx.rate", [])
        fourxx_rates = raw.get("builtin:service.errors.fourxx.rate", [])
        total_counts = raw.get("builtin:service.requestCount.total", [])
        error_counts = raw.get("builtin:service.errors.total.count", [])
        rt_max_vals = raw.get("builtin:service.response.time:max", [])
        rt_avg_vals = raw.get("builtin:service.response.time", [])

        # 5xx / 4xx ADET = her dilimde toplam_istek × oran / 100
        fivexx_count = 0
        fourxx_count = 0
        for i, total in enumerate(total_counts):
            if i < len(fivexx_rates):
                fivexx_count += int(round(total * fivexx_rates[i] / 100))
            if i < len(fourxx_rates):
                fourxx_count += int(round(total * fourxx_rates[i] / 100))

        rt_max_us = _peak(rt_max_vals)
        rt_avg_us = _avg(rt_avg_vals)

        result = {
            "fivexx_count": fivexx_count,
            "fivexx_peak_rate": _peak(fivexx_rates),
            "fourxx_count": fourxx_count,
            "fourxx_peak_rate": _peak(fourxx_rates),
            "total_errors": _sum(error_counts),
            "response_time_max_ms": round(rt_max_us / 1000, 1) if rt_max_us else 0,
            "response_time_avg_ms": round(rt_avg_us / 1000, 1) if rt_avg_us else 0,
        }
        log.info(
            "HTTP metrikleri OK: 5xx=%d(peak %.2f%%) 4xx=%d(peak %.2f%%) rt_max=%.1fms rt_avg=%.1fms",
            result["fivexx_count"],
            result["fivexx_peak_rate"],
            result["fourxx_count"],
            result["fourxx_peak_rate"],
            result["response_time_max_ms"],
            result["response_time_avg_ms"],
        )
        return result
    except Exception as e:
        log.warning("HTTP metrik sorgusu basarisiz (%s): %s", entity_id, e)
        return {}


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


def _format_ms(us_val):
    """Mikrosaniyeyi okunabilir formata cevir"""
    try:
        ms = float(us_val) / 1000.0
        if ms >= 1000:
            return "%.2f sn" % (ms / 1000.0)
        return "%.0f ms" % ms
    except Exception:
        return str(us_val)


def _pct(val):
    """Float orani yuzde stringe cevir"""
    try:
        return "%.4f%%" % (float(val) * 100)
    except Exception:
        return str(val)


def extract_evidence_data(problem):
    """
    Dynatrace evidence'indan yapılandırılmış veri çıkar:
    - Baseline karşılaştırmaları (error rate, response time, throughput)
    - Downstream etki
    - Timestamp bilgileri
    - Event tipleri
    """
    evidences = problem.get("evidenceDetails", {}).get("details", [])
    impacts = problem.get("impactAnalysis", {}).get("impacts", [])
    start_ts = problem.get("startTime", 0)
    end_ts = problem.get("endTime", -1)

    # Zaman bilgisi
    start_str = ""
    end_str = ""
    duration_str = ""
    if start_ts:
        start_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
        start_str = start_dt.strftime("%H:%M UTC")
        if end_ts and end_ts > 0:
            end_dt = datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc)
            end_str = end_dt.strftime("%H:%M UTC")
            dur_min = int((end_ts - start_ts) / 60000)
            duration_str = "%d dakika" % dur_min

    # Evidence satırlari (Claude icin zengin metin)
    ev_lines = []
    # Evidence ozeti (kart icin)
    ev_card_lines = []
    baseline_facts = []

    for ev in evidences:
        ev_type = ev.get("evidenceType", "")
        ev_name = ev.get("displayName", "")
        ent_name = ev.get("entity", {}).get("name", "")
        props = {p["key"]: p["value"] for p in ev.get("data", {}).get("properties", [])}
        desc = props.get("dt.event.description", "")[:300]
        rc_rel = ev.get("rootCauseRelevant", False)
        unit = ev.get("unit", "")

        # TRANSACTIONAL evidence - daha fazla sayisal veri var
        before_val = ev.get("valueBeforeChangePoint")
        after_val = ev.get("valueAfterChangePoint")

        line = "- [%s][%s] %s: %s" % (
            ev_type,
            "ROOT_CAUSE" if rc_rel else "CONTEXT",
            ent_name,
            desc,
        )
        ev_lines.append(line)

        if before_val is not None and after_val is not None:
            if "MicroSecond" in unit or "response" in ev_name.lower():
                b_fmt = _format_ms(before_val)
                a_fmt = _format_ms(after_val)
                try:
                    ratio = (
                        float(after_val) / float(before_val)
                        if float(before_val) > 0
                        else 0
                    )
                    change = (
                        "%.0f%% artis" % ((ratio - 1) * 100)
                        if ratio > 1
                        else "%.0f%% azalis" % ((1 - ratio) * 100)
                    )
                except Exception:
                    change = ""
                ev_lines.append("  %s: %s → %s (%s)" % (ev_name, b_fmt, a_fmt, change))
                ev_card_lines.append(
                    "%s: %s → %s (%s)" % (ev_name, b_fmt, a_fmt, change)
                )
                baseline_facts.append(
                    {"title": ev_name, "value": "%s → %s (%s)" % (b_fmt, a_fmt, change)}
                )
            elif "PerMinute" in unit or "throughput" in ev_name.lower():
                ev_lines.append(
                    "  %s throughput: %.1f/dk → %.1f/dk"
                    % (ev_name, float(before_val), float(after_val))
                )
                ev_card_lines.append(
                    "%s: %.1f/dk → %.1f/dk"
                    % (ev_name, float(before_val), float(after_val))
                )
                baseline_facts.append(
                    {
                        "title": ev_name,
                        "value": "%.1f → %.1f /dk"
                        % (float(before_val), float(after_val)),
                    }
                )

        # Error rate baseline
        err_ref = props.get("dt.event.baseline.error_rate_reference", "")
        err_cur = props.get("dt.event.baseline.error_rate", "")
        if err_ref and err_cur:
            try:
                b_pct = float(err_ref) * 100
                a_pct = float(err_cur) * 100
                ratio = a_pct / b_pct if b_pct > 0 else 0
                ev_lines.append(
                    "  Hata orani: %.4f%% → %.4f%% (%.1fx artis)"
                    % (b_pct, a_pct, ratio)
                )
                ev_card_lines.append(
                    "Hata orani: %.4f%% → %.4f%% (%.1fx artis)" % (b_pct, a_pct, ratio)
                )
                baseline_facts.append(
                    {
                        "title": "Hata Orani",
                        "value": "%.4f%% → %.4f%% (%.1fx)" % (b_pct, a_pct, ratio),
                    }
                )
            except Exception:
                pass

        # Response time P50/P90 (event bazli)
        rt_p50 = props.get("dt.event.baseline.response_time_p50", "")
        rt_p90 = props.get("dt.event.baseline.response_time_p90", "")
        rt_p50_ref = props.get("dt.event.baseline.response_time_p50_reference", "")
        rt_p90_ref = props.get("dt.event.baseline.response_time_p90_reference", "")
        if rt_p90 and rt_p90_ref:
            try:
                p90_cur = float(rt_p90)
                p90_ref = float(rt_p90_ref)
                chg = ((p90_cur - p90_ref) / p90_ref * 100) if p90_ref > 0 else 0
                ev_lines.append(
                    "  P90 yanit suresi: %s → %s (%+.1f%%)"
                    % (_format_ms(p90_ref), _format_ms(p90_cur), chg)
                )
                ev_card_lines.append(
                    "P90: %s → %s (%+.1f%%)"
                    % (_format_ms(p90_ref), _format_ms(p90_cur), chg)
                )
                baseline_facts.append(
                    {
                        "title": "P90 Yanit Suresi",
                        "value": "%s → %s (%+.1f%%)"
                        % (_format_ms(p90_ref), _format_ms(p90_cur), chg),
                    }
                )
            except Exception:
                pass
        if rt_p50 and rt_p50_ref:
            try:
                p50_cur = float(rt_p50)
                p50_ref = float(rt_p50_ref)
                chg = ((p50_cur - p50_ref) / p50_ref * 100) if p50_ref > 0 else 0
                ev_lines.append(
                    "  P50 yanit suresi: %s → %s (%+.1f%%)"
                    % (_format_ms(p50_ref), _format_ms(p50_cur), chg)
                )
                ev_card_lines.append(
                    "P50: %s → %s (%+.1f%%)"
                    % (_format_ms(p50_ref), _format_ms(p50_cur), chg)
                )
                baseline_facts.append(
                    {
                        "title": "P50 Yanit Suresi",
                        "value": "%s → %s (%+.1f%%)"
                        % (_format_ms(p50_ref), _format_ms(p50_cur), chg),
                    }
                )
            except Exception:
                pass

        # Affected load
        load = props.get("dt.event.baseline.affected_load", "")
        total_load = props.get("dt.event.baseline.total_load", "")
        if load and total_load:
            ev_lines.append("  Etkilenen yuk: %s / %s istek" % (load, total_load))

    # Downstream etki
    downstream_lines = []
    total_downstream = 0
    for imp in impacts:
        imp_name = imp.get("impactedEntity", {}).get("name", "")
        imp_calls = imp.get("numberOfPotentiallyAffectedServiceCalls", 0)
        total_downstream += imp_calls
        downstream_lines.append("%s: %d cagri etkilendi" % (imp_name[:60], imp_calls))

    return {
        "ev_lines": ev_lines,
        "ev_card_lines": ev_card_lines,
        "baseline_facts": baseline_facts,
        "downstream_lines": downstream_lines,
        "total_downstream": total_downstream,
        "start_str": start_str,
        "end_str": end_str,
        "duration_str": duration_str,
    }


# ── CLAUDE PROMPT ─────────────────────────────────────────────────────────────


def ask_claude(problem, details_text, logs, rc_name):
    ev_data = extract_evidence_data(problem)

    affected = [e["name"] for e in problem.get("affectedEntities", [])]
    ns = problem.get("k8s.namespace.name")
    namespace = (ns[0] if isinstance(ns, list) and ns else ns) or "N/A"
    log_section = (
        "\n".join(logs[:6]) if logs else "Log alinamadi (ic ag erisim kisitli)."
    )
    clean_det = clean_placeholders(details_text)[:800]
    impacts = problem.get("impactAnalysis", {}).get("impacts", [])
    total_calls = sum(
        i.get("numberOfPotentiallyAffectedServiceCalls", 0) for i in impacts
    )

    downstream_block = (
        "\n".join(ev_data["downstream_lines"])
        if ev_data["downstream_lines"]
        else "Downstream etki yok."
    )
    evidence_block = (
        "\n".join(ev_data["ev_lines"]) if ev_data["ev_lines"] else "Evidence alinamadi."
    )

    prompt = """Sen kıdemli bir SRE ve Site Reliability Engineer'sın. ITIL v4 ve PMI PMBOK standartlarinda Root Cause Analysis uretiyorsun.

PROBLEM OZETI:
- ID: {display_id} | Baslik: {title}
- Severity: {severity} | Status: {status}
- Root Cause Servis: {rc_name}
- Etkilenen Servisler: {affected}
- Namespace: {namespace}
- Baslangic: {start_str} | Bitis: {end_str} | Sure: {duration}
- Toplam Etkilenen Cagri: {total_calls}

DYNATRACE EVIDENCE (Baseline karsilastirma verileri):
{evidence_block}

DOWNSTREAM ETKI:
{downstream_block}

LOG KAYITLARI:
{log_section}

DETAYLAR:
{details}

KRITIK TALIMAT: Asagidaki JSON formatinda DETAYLI analiz uret. Turkce yaz. Her alan icin gercek sayisal verileri kullan.
Evidence'daki baseline sayilarini mutlaka belirt (ornek: "P90 yanit suresi 3.89s'den 9.51s'ye yukseldi, %144 artis").
Sadece JSON dondur, baska metin ekleme:

{{
  "root_cause": "3-4 cumle. Gercek baseline sayilarini kullanarak teknik kok nedeni acikla. Evidence'daki P50/P90/hata orani degerlerini yuzde artis ile belirt.",
  "confidence": "HIGH veya MEDIUM veya LOW",
  "severity_assessment": "Critical veya High veya Medium veya Low",
  "repeat_risk": "High veya Medium veya Low",
  "itil_category": "Infrastructure Failure veya Application Error veya Performance Degradation veya Security",
  "pattern_analysis": "Tek cumle: P50 mi P90 mu etkilendi, sporadic mi surekli mi, yuk bagimliligi var mi. Bu pattern ne anlama geliyor.",
  "blast_radius": "Etkilenen servis sayisi, downstream cagri sayisi ve kullanici etkisini belirt.",
  "hypotheses": [
    "H1: En yuksek olasilikli hipotez - gercek metrik degerlerle destekle",
    "H2: Ikinci hipotez",
    "H3: Ucuncu hipotez"
  ],
  "contributing_factors": [
    "F1: Gercek bir katkida bulunan faktor",
    "F2: Ikinci faktor"
  ],
  "immediate_actions": [
    "A1: kubectl get pods -n {namespace} --field-selector=status.phase!=Running ile basla",
    "A2: Ikinci acil aksiyon - spesifik komut veya adimla",
    "A3: Ucuncu acil aksiyon"
  ],
  "preventive_actions": [
    "P1: Tekrari onleyecek mimari veya konfigurasyon degisikligi",
    "P2: Izleme ve erken uyari iyilestirmesi"
  ],
  "timeline": "Sorunun zaman akisi: ne zaman basladi, ne kadar surdu, ne zaman cozuldu."
}}""".format(
        display_id=problem.get("displayId", "N/A"),
        title=problem.get("title", "N/A"),
        severity=problem.get("severityLevel", "N/A"),
        status=problem.get("status", "N/A"),
        rc_name=rc_name,
        affected=", ".join(affected[:5]) if affected else "Belirsiz",
        namespace=namespace,
        start_str=ev_data["start_str"] or "Bilinmiyor",
        end_str=ev_data["end_str"] or "Devam ediyor",
        duration=ev_data["duration_str"] or "Belirsiz",
        total_calls=str(total_calls),
        evidence_block=evidence_block,
        downstream_block=downstream_block,
        log_section=log_section,
        details=clean_det,
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
            "max_tokens": 2500,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=90,
    )
    response.raise_for_status()
    raw = response.json()["content"][0]["text"].strip()
    if "```json" in raw:
        raw = raw.split("```json")[1].split("```")[0].strip()
    elif "```" in raw:
        raw = raw.split("```")[1].split("```")[0].strip()
    return json.loads(raw)


# ── TEAMS ADAPTIVE CARD ───────────────────────────────────────────────────────


def _severity_color(sev):
    """Severity seviyesine gore Adaptive Card rengi"""
    return {
        "critical": "Attention",
        "high": "Attention",
        "medium": "Warning",
        "low": "Good",
    }.get((sev or "").lower(), "Accent")


def _risk_emoji(risk):
    return {"High": "🔴", "Medium": "🟡", "Low": "🟢"}.get(risk, "⚪")


def _confidence_emoji(conf):
    return {"HIGH": "✅", "MEDIUM": "⚠️", "LOW": "❓"}.get(conf, "⚪")


def _col_metric(label, value, color="Accent"):
    """Tek metrik kolonu — ust label kucuk, alt deger buyuk"""
    return {
        "type": "Column",
        "width": "stretch",
        "items": [
            {
                "type": "TextBlock",
                "text": label,
                "size": "Small",
                "color": "Default",
                "isSubtle": True,
                "weight": "Default",
                "spacing": "None",
            },
            {
                "type": "TextBlock",
                "text": str(value),
                "size": "Large",
                "weight": "Bolder",
                "color": color,
                "spacing": "None",
            },
        ],
    }


def build_teams_card(
    problem, rca, dt_url, rc_name, webhook_state="OPEN", http_metrics=None
):
    display_id = problem.get("displayId", "N/A")
    title = problem.get("title", "Unknown")
    status = problem.get("status", "OPEN")
    severity = problem.get("severityLevel", "")
    ns = problem.get("k8s.namespace.name")
    namespace = (ns[0] if isinstance(ns, list) and ns else ns) or ""

    is_resolved = (status == "CLOSED") or (webhook_state == "RESOLVED")
    state_color = "good" if is_resolved else "attention"
    header_emoji = "✅" if is_resolved else "🔴"
    state_badge = "RESOLVED" if is_resolved else "OPEN"

    # RCA alanlari
    root_cause = rca.get("root_cause", "Analiz tamamlanamadi.")
    confidence = rca.get("confidence", "MEDIUM")
    severity_ass = rca.get("severity_assessment", "")
    repeat_risk = rca.get("repeat_risk", "")
    itil_cat = rca.get("itil_category", "")
    pattern_analysis = rca.get("pattern_analysis", "")
    blast_radius = rca.get("blast_radius", "")
    timeline_text = rca.get("timeline", "")

    hyp_list = rca.get("hypotheses", [])
    imm_list = rca.get("immediate_actions", [])
    prev_list = rca.get("preventive_actions", [])
    con_list = rca.get("contributing_factors", [])

    # Evidence & metrik verisi
    ev_data = extract_evidence_data(problem)
    baseline_facts = ev_data["baseline_facts"]
    downstream_lines = ev_data["downstream_lines"]
    ev_card_text = (
        "\n".join(ev_data["ev_card_lines"]) if ev_data["ev_card_lines"] else ""
    )
    m = http_metrics or {}

    # ─── ANA KART BODY ───────────────────────────────────────────────────────

    main_body = []

    # 1. Başlık çizgisi — emoji + durum + problem adı
    main_body.append(
        {
            "type": "ColumnSet",
            "spacing": "None",
            "columns": [
                {
                    "type": "Column",
                    "width": "auto",
                    "verticalContentAlignment": "Center",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": header_emoji,
                            "size": "ExtraLarge",
                            "spacing": "None",
                        }
                    ],
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "verticalContentAlignment": "Center",
                    "spacing": "Small",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "Dynatrace  ·  %s" % state_badge,
                            "weight": "Bolder",
                            "size": "Medium",
                            "color": state_color,
                            "spacing": "None",
                        },
                        {
                            "type": "TextBlock",
                            "text": title,
                            "wrap": True,
                            "size": "Small",
                            "spacing": "None",
                            "color": "Default",
                            "isSubtle": True,
                        },
                    ],
                },
            ],
        }
    )

    # 2. Separator
    main_body.append(
        {
            "type": "TextBlock",
            "text": "─────────────────────────",
            "color": "Default",
            "isSubtle": True,
            "size": "Small",
            "spacing": "Small",
        }
    )

    # 3. Kimlik bilgisi satırı — 3 sütun yan yana
    id_cols = [
        {
            "type": "Column",
            "width": "stretch",
            "items": [
                {
                    "type": "TextBlock",
                    "text": "PROBLEM ID",
                    "size": "Small",
                    "isSubtle": True,
                    "weight": "Default",
                    "spacing": "None",
                },
                {
                    "type": "TextBlock",
                    "text": display_id,
                    "weight": "Bolder",
                    "size": "Small",
                    "spacing": "None",
                    "color": "Accent",
                },
            ],
        },
        {
            "type": "Column",
            "width": "stretch",
            "items": [
                {
                    "type": "TextBlock",
                    "text": "SEVERITY",
                    "size": "Small",
                    "isSubtle": True,
                    "weight": "Default",
                    "spacing": "None",
                },
                {
                    "type": "TextBlock",
                    "text": severity or "N/A",
                    "weight": "Bolder",
                    "size": "Small",
                    "spacing": "None",
                    "color": (
                        "Attention" if severity in ("ERROR", "PERFORMANCE") else "Good"
                    ),
                },
            ],
        },
        {
            "type": "Column",
            "width": "stretch",
            "items": [
                {
                    "type": "TextBlock",
                    "text": "SURE",
                    "size": "Small",
                    "isSubtle": True,
                    "weight": "Default",
                    "spacing": "None",
                },
                {
                    "type": "TextBlock",
                    "text": ev_data["duration_str"] if ev_data["duration_str"] else "—",
                    "weight": "Bolder",
                    "size": "Small",
                    "spacing": "None",
                },
            ],
        },
    ]
    if namespace:
        id_cols.append(
            {
                "type": "Column",
                "width": "stretch",
                "items": [
                    {
                        "type": "TextBlock",
                        "text": "NAMESPACE",
                        "size": "Small",
                        "isSubtle": True,
                        "weight": "Default",
                        "spacing": "None",
                    },
                    {
                        "type": "TextBlock",
                        "text": namespace,
                        "weight": "Bolder",
                        "size": "Small",
                        "spacing": "None",
                        "color": "Accent",
                    },
                ],
            }
        )
    main_body.append({"type": "ColumnSet", "columns": id_cols, "spacing": "Small"})

    # 4. Root cause satiri
    main_body.append(
        {
            "type": "TextBlock",
            "text": "🎯  ROOT CAUSE: %s" % rc_name,
            "weight": "Bolder",
            "size": "Small",
            "color": "Attention",
            "spacing": "Small",
            "wrap": True,
        }
    )

    # 5. HTTP Metrik Kutulari — ana kartta büyük rakamlar
    has_metrics = m and (
        m.get("fivexx_count", 0) > 0
        or m.get("fourxx_count", 0) > 0
        or m.get("response_time_max_ms", 0) > 0
    )
    if has_metrics:
        main_body.append(
            {
                "type": "TextBlock",
                "text": "─────────────────────────",
                "color": "Default",
                "isSubtle": True,
                "size": "Small",
                "spacing": "Small",
            }
        )
        main_body.append(
            {
                "type": "TextBlock",
                "text": "HTTP & Latency Metrikleri",
                "weight": "Bolder",
                "size": "Small",
                "color": "Warning",
                "spacing": "None",
            }
        )

        metric_cols = []
        if m.get("fivexx_count", 0) > 0 or m.get("fivexx_peak_rate", 0) > 0:
            metric_cols.append(
                _col_metric(
                    "5xx HATA", "%d istek" % m.get("fivexx_count", 0), "Attention"
                )
            )
            metric_cols.append(
                _col_metric(
                    "5xx PEAK", "%.1f%%" % m.get("fivexx_peak_rate", 0), "Attention"
                )
            )
        if m.get("fourxx_count", 0) > 0 or m.get("fourxx_peak_rate", 0) > 0:
            metric_cols.append(
                _col_metric(
                    "4xx HATA", "%d istek" % m.get("fourxx_count", 0), "Warning"
                )
            )
            metric_cols.append(
                _col_metric(
                    "4xx PEAK", "%.1f%%" % m.get("fourxx_peak_rate", 0), "Warning"
                )
            )
        if m.get("response_time_max_ms", 0) > 0:
            metric_cols.append(
                _col_metric(
                    "MAX RT", _format_ms(m["response_time_max_ms"] * 1000), "Attention"
                )
            )
        if m.get("response_time_avg_ms", 0) > 0:
            metric_cols.append(
                _col_metric(
                    "ORT RT", _format_ms(m["response_time_avg_ms"] * 1000), "Accent"
                )
            )

        # En fazla 4 kolon yan yana (Teams limiti)
        if metric_cols:
            main_body.append(
                {"type": "ColumnSet", "columns": metric_cols[:4], "spacing": "Small"}
            )
            if len(metric_cols) > 4:
                main_body.append(
                    {
                        "type": "ColumnSet",
                        "columns": metric_cols[4:],
                        "spacing": "Small",
                    }
                )

    # ─── RCA ANALİZ KARTI (Show RCA Analysis) ────────────────────────────────
    rca_body = []

    # Başlık
    rca_body.append(
        {
            "type": "TextBlock",
            "text": "🔍  Root Cause Analysis — %s" % display_id,
            "weight": "Bolder",
            "size": "Medium",
            "color": "Attention",
            "spacing": "None",
        }
    )

    # Özet metin
    rca_body.append(
        {
            "type": "TextBlock",
            "text": root_cause,
            "wrap": True,
            "size": "Small",
            "spacing": "Small",
        }
    )

    # RCA badge satırı — confidence / severity / risk / ITIL yan yana
    rca_body.append(
        {
            "type": "ColumnSet",
            "spacing": "Small",
            "columns": [
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "CONFIDENCE",
                            "size": "Small",
                            "isSubtle": True,
                            "spacing": "None",
                        },
                        {
                            "type": "TextBlock",
                            "text": "%s %s"
                            % (_confidence_emoji(confidence), confidence),
                            "weight": "Bolder",
                            "size": "Small",
                            "spacing": "None",
                        },
                    ],
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "SEVERITY",
                            "size": "Small",
                            "isSubtle": True,
                            "spacing": "None",
                        },
                        {
                            "type": "TextBlock",
                            "text": severity_ass or "N/A",
                            "weight": "Bolder",
                            "size": "Small",
                            "spacing": "None",
                            "color": _severity_color(severity_ass),
                        },
                    ],
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "REPEAT RISK",
                            "size": "Small",
                            "isSubtle": True,
                            "spacing": "None",
                        },
                        {
                            "type": "TextBlock",
                            "text": "%s %s"
                            % (_risk_emoji(repeat_risk), repeat_risk or "N/A"),
                            "weight": "Bolder",
                            "size": "Small",
                            "spacing": "None",
                        },
                    ],
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "ITIL",
                            "size": "Small",
                            "isSubtle": True,
                            "spacing": "None",
                        },
                        {
                            "type": "TextBlock",
                            "text": (itil_cat or "N/A").replace(" ", "\n"),
                            "weight": "Bolder",
                            "size": "Small",
                            "spacing": "None",
                            "wrap": True,
                        },
                    ],
                },
            ],
        }
    )

    # Separator
    rca_body.append(
        {
            "type": "TextBlock",
            "text": "─────────────────────────",
            "color": "Default",
            "isSubtle": True,
            "size": "Small",
            "spacing": "Small",
        }
    )

    # HTTP Metrik kutulari (RCA kart icinde tekrar buyuk göster)
    if has_metrics:
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "📊  HTTP Hata & Latency",
                "weight": "Bolder",
                "size": "Small",
                "color": "Warning",
                "spacing": "None",
            }
        )
        metric_rows = []
        if m.get("fivexx_count", 0) > 0 or m.get("fivexx_peak_rate", 0) > 0:
            metric_rows.append(
                {
                    "type": "ColumnSet",
                    "spacing": "Small",
                    "columns": [
                        _col_metric(
                            "5xx Hata Adedi",
                            "%d istek" % m.get("fivexx_count", 0),
                            "Attention",
                        ),
                        _col_metric(
                            "5xx Peak Oran",
                            "%.2f%%" % m.get("fivexx_peak_rate", 0),
                            "Attention",
                        ),
                        _col_metric(
                            "Toplam Hata",
                            "%d istek" % m.get("total_errors", 0),
                            "Default",
                        ),
                    ],
                }
            )
        if m.get("fourxx_count", 0) > 0 or m.get("fourxx_peak_rate", 0) > 0:
            metric_rows.append(
                {
                    "type": "ColumnSet",
                    "spacing": "Small",
                    "columns": [
                        _col_metric(
                            "4xx Hata Adedi",
                            "%d istek" % m.get("fourxx_count", 0),
                            "Warning",
                        ),
                        _col_metric(
                            "4xx Peak Oran",
                            "%.2f%%" % m.get("fourxx_peak_rate", 0),
                            "Warning",
                        ),
                        _col_metric(" ", " ", "Default"),
                    ],
                }
            )
        if m.get("response_time_max_ms", 0) > 0 or m.get("response_time_avg_ms", 0) > 0:
            metric_rows.append(
                {
                    "type": "ColumnSet",
                    "spacing": "Small",
                    "columns": [
                        _col_metric(
                            "Max Yanit Suresi",
                            _format_ms(m.get("response_time_max_ms", 0) * 1000),
                            "Attention",
                        ),
                        _col_metric(
                            "Ort Yanit Suresi",
                            _format_ms(m.get("response_time_avg_ms", 0) * 1000),
                            "Accent",
                        ),
                        _col_metric(" ", " ", "Default"),
                    ],
                }
            )
        rca_body.extend(metric_rows)
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "─────────────────────────",
                "color": "Default",
                "isSubtle": True,
                "size": "Small",
                "spacing": "Small",
            }
        )

    # Baseline verileri
    if baseline_facts:
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "📈  Dynatrace Baseline Karsilastirmasi",
                "weight": "Bolder",
                "size": "Small",
                "color": "Accent",
                "spacing": "None",
            }
        )
        rca_body.append(
            {"type": "FactSet", "facts": baseline_facts, "spacing": "Small"}
        )
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "─────────────────────────",
                "color": "Default",
                "isSubtle": True,
                "size": "Small",
                "spacing": "Small",
            }
        )

    # Timeline & Pattern
    if timeline_text or pattern_analysis:
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "⏱  Zaman Akisi & Hata Paterni",
                "weight": "Bolder",
                "size": "Small",
                "color": "Accent",
                "spacing": "None",
            }
        )
        if timeline_text:
            rca_body.append(
                {
                    "type": "TextBlock",
                    "text": timeline_text,
                    "wrap": True,
                    "size": "Small",
                    "spacing": "Small",
                }
            )
        if pattern_analysis:
            rca_body.append(
                {
                    "type": "TextBlock",
                    "text": pattern_analysis,
                    "wrap": True,
                    "size": "Small",
                    "isSubtle": True,
                    "spacing": "Small",
                }
            )
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "─────────────────────────",
                "color": "Default",
                "isSubtle": True,
                "size": "Small",
                "spacing": "Small",
            }
        )

    # Downstream etki
    if downstream_lines or blast_radius:
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "💥  Etki Alani (%d cagri)" % ev_data["total_downstream"],
                "weight": "Bolder",
                "size": "Small",
                "spacing": "None",
            }
        )
        if blast_radius:
            rca_body.append(
                {
                    "type": "TextBlock",
                    "text": blast_radius,
                    "wrap": True,
                    "size": "Small",
                    "spacing": "Small",
                }
            )
        if downstream_lines:
            for dl in downstream_lines[:4]:
                rca_body.append(
                    {
                        "type": "TextBlock",
                        "text": "▸  " + dl,
                        "wrap": True,
                        "size": "Small",
                        "isSubtle": True,
                        "spacing": "None",
                    }
                )
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "─────────────────────────",
                "color": "Default",
                "isSubtle": True,
                "size": "Small",
                "spacing": "Small",
            }
        )

    # Hipotezler
    rca_body.append(
        {
            "type": "TextBlock",
            "text": "🧪  Hipotezler",
            "weight": "Bolder",
            "size": "Small",
            "spacing": "None",
        }
    )
    for i, h in enumerate(hyp_list[:3]):
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "H%d  %s" % (i + 1, h),
                "wrap": True,
                "size": "Small",
                "spacing": "Small",
                "color": "Accent" if i == 0 else "Default",
            }
        )

    # Katkida bulunan faktorler
    if con_list:
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "⚙️  Katkida Bulunan Faktorler",
                "weight": "Bolder",
                "size": "Small",
                "spacing": "Medium",
            }
        )
        for c in con_list[:3]:
            rca_body.append(
                {
                    "type": "TextBlock",
                    "text": "•  " + c,
                    "wrap": True,
                    "size": "Small",
                    "isSubtle": True,
                    "spacing": "None",
                }
            )

    rca_body.append(
        {
            "type": "TextBlock",
            "text": "─────────────────────────",
            "color": "Default",
            "isSubtle": True,
            "size": "Small",
            "spacing": "Small",
        }
    )

    # Acil aksiyonlar
    rca_body.append(
        {
            "type": "TextBlock",
            "text": "🚨  Acil Aksiyonlar",
            "weight": "Bolder",
            "size": "Small",
            "color": "Attention",
            "spacing": "None",
        }
    )
    for i, a in enumerate(imm_list[:3]):
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "%d.  %s" % (i + 1, a),
                "wrap": True,
                "size": "Small",
                "spacing": "Small",
            }
        )

    # Onleyici aksiyonlar
    if prev_list:
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "🛡  Onleyici Aksiyonlar",
                "weight": "Bolder",
                "size": "Small",
                "spacing": "Medium",
            }
        )
        for i, p in enumerate(prev_list[:3]):
            rca_body.append(
                {
                    "type": "TextBlock",
                    "text": "%d.  %s" % (i + 1, p),
                    "wrap": True,
                    "size": "Small",
                    "isSubtle": True,
                    "spacing": "Small",
                }
            )

    # Evidence (varsa ve baseline yoksa)
    if ev_card_text and not baseline_facts:
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "─────────────────────────",
                "color": "Default",
                "isSubtle": True,
                "size": "Small",
                "spacing": "Small",
            }
        )
        rca_body.append(
            {
                "type": "TextBlock",
                "text": "📋  Dynatrace Evidence",
                "weight": "Bolder",
                "size": "Small",
                "color": "Accent",
                "spacing": "None",
            }
        )
        rca_body.append(
            {
                "type": "TextBlock",
                "text": ev_card_text,
                "wrap": True,
                "size": "Small",
                "isSubtle": True,
                "spacing": "Small",
            }
        )

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
                    "body": main_body,
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
                                        "size": "Default",
                                    }
                                ],
                            },
                        },
                        {
                            "type": "Action.ShowCard",
                            "title": "Show RCA Analysis",
                            "card": {"type": "AdaptiveCard", "body": rca_body},
                        },
                    ],
                },
            }
        ],
    }


# ── ORKESTRATÖR ───────────────────────────────────────────────────────────────


def process_problem(display_id, problem_title, state, dt_url, details_text):
    log.info("Isleniyor: %s - %s (%s)", display_id, problem_title, state)

    basic_info = find_problem_by_display_id(display_id)
    problem = {}
    if basic_info.get("problemId"):
        problem = get_problem_details(basic_info["problemId"])

    if not problem:
        log.warning("Webhook verisi kullaniliyor: %s", display_id)
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

    rc_name = get_root_cause_name(problem, details_text)
    start_ts = problem.get("startTime", 0)
    end_ts = problem.get("endTime", -1)
    logs_data = get_recent_logs(start_ts) if start_ts else []

    # HTTP metrikleri: entity ID'yi problem'dan al
    http_metrics = {}
    entity_id = ""
    rc_entity = problem.get("rootCauseEntity") or {}
    if rc_entity.get("entityId", {}).get("id"):
        entity_id = rc_entity["entityId"]["id"]
    elif problem.get("affectedEntities"):
        entity_id = problem["affectedEntities"][0].get("entityId", {}).get("id", "")
    if entity_id:
        http_metrics = get_http_metrics(entity_id, start_ts, end_ts)

    log.info("Claude sorgu: %s (rc: %s)", display_id, rc_name)
    rca = ask_claude(problem, details_text, logs_data, rc_name)
    log.info(
        "RCA OK: %s confidence=%s pattern=%s",
        display_id,
        rca.get("confidence"),
        bool(rca.get("pattern_analysis")),
    )

    card = build_teams_card(problem, rca, dt_url, rc_name, state, http_metrics)
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
    return jsonify({"status": "ok", "version": "1.7.0"}), 200


@app.route("/", methods=["GET"])
def root():
    return jsonify({"service": "Dynatrace RCA Bot", "version": "1.5.0"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
