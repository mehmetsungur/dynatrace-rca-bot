#!/usr/bin/env python3
"""
Lokal test: sunucu çalışırken gerçek bir Dynatrace webhook simüle eder.
Kullanım: python test_webhook.py
"""
import requests, json

# Lokal sunucu URL'si
BASE = "http://localhost:8080"

# Test payload — gerçek Dynatrace formatı
payload = {
    "State":        "OPEN",
    "ProblemID":    "P-26032870",
    "ProblemTitle": "Failure rate increase",
    "URL":          "https://apm.viennalife.com.tr/e/726736a4-9ea5-4b9e-ad00-ced85772d72e/#problems/problemdetails;pid=-7514893602154755174_1773830340000V2",
    "Details":      "State: OPEN | Duration: 6min | Root Cause: UserController | Impacted: UserController, AuthController | Tags: atouchv2 ---RCA--- Error rate exceeded baseline on UserController. validateUser endpoint failure rate 25x above normal. ---ACTIONS--- 1. kubectl get pods -n atouchv2 | grep -v Running 2. kubectl describe deployment UserController 3. Check Dynatrace metrics"
}

print("1. Health check...")
r = requests.get(f"{BASE}/health")
print(f"   Status: {r.status_code} — {r.json()}")

print("\n2. Webhook test (OPEN problem)...")
r = requests.post(f"{BASE}/webhook/dynatrace-rca",
                  json=payload,
                  headers={"Content-Type": "application/json"})
print(f"   Status: {r.status_code}")
print(f"   Response: {json.dumps(r.json(), indent=2, ensure_ascii=False)}")

print("\n3. RESOLVED test (atlanmalı)...")
payload["State"] = "RESOLVED"
r = requests.post(f"{BASE}/webhook/dynatrace-rca", json=payload)
print(f"   Status: {r.status_code} — {r.json()}")
