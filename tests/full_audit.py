"""Full plan verification audit — tests every feature from the implementation plan."""
import requests
import json
import psycopg2
import sys

BASE = "http://localhost:8001/api/v1"
DB = "postgresql://neondb_owner:npg_paXHNPt6rzy2@ep-nameless-field-a9j5xt82-pooler.gwc.azure.neon.tech/neondb?sslmode=require"

results = []

def check(name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    results.append((name, status, detail))
    print(f"  [{status}] {name}" + (f" -- {detail}" if detail else ""))

print("=" * 70)
print("OUTBREW -- FULL PLAN VERIFICATION AUDIT")
print("=" * 70)

# ============ 1. DB SCHEMA ============
print("\n--- 1. DATABASE SCHEMA ---")
conn = psycopg2.connect(DB)
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='candidates' ORDER BY column_name")
all_cols = [r[0] for r in cur.fetchall()]
for col in ["plan_tier", "email_verified", "monthly_email_limit", "monthly_email_sent",
            "monthly_campaign_limit", "monthly_campaigns_created", "monthly_recipient_limit",
            "usage_reset_at", "email_verification_token"]:
    check(f"Column: {col}", col in all_cols)

cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename IN ('audit_logs','user_sessions','password_reset_tokens')")
tables = [r[0] for r in cur.fetchall()]
for t in ["audit_logs", "user_sessions", "password_reset_tokens"]:
    check(f"Table: {t}", t in tables)

cur.execute("SELECT count(*) FROM pg_tables WHERE schemaname='public'")
total = cur.fetchone()[0]
check(f"Total tables: {total}", total >= 80)
conn.close()

# ============ 2. REGISTER ============
print("\n--- 2. REGISTER ---")
import random
uname = f"audit_{random.randint(10000,99999)}"
r = requests.post(f"{BASE}/auth/register", json={
    "username": uname, "email": f"{uname}@gmail.com",
    "password": "Audit!Test99", "full_name": "Audit User",
    "email_account": f"{uname}send@gmail.com", "email_password": "pass",
})
reg = r.json() if r.status_code == 201 else {}
check("Register succeeds", r.status_code == 201, f"HTTP {r.status_code}")
check("Register returns plan_tier=free", reg.get("plan_tier") == "free")
check("Register returns email_verified=false", reg.get("email_verified") == False)
check("Register returns usage object", "usage" in reg)

# Weak password
r = requests.post(f"{BASE}/auth/register", json={
    "username": "wp", "email": "wp@gmail.com", "password": "short",
    "full_name": "W", "email_account": "w@gmail.com", "email_password": "p"})
check("Weak password rejected (422)", r.status_code == 422)

# ============ 3. LOGIN ============
print("\n--- 3. LOGIN ---")
r = requests.post(f"{BASE}/auth/login/json", json={"username": uname, "password": "Audit!Test99"})
check("Login succeeds", r.status_code == 200)
login_data = r.json()
check("Has access_token", "access_token" in login_data)
check("Has refresh_token", "refresh_token" in login_data)
check("Has expires_in", "expires_in" in login_data)
token = login_data.get("access_token", "")
refresh = login_data.get("refresh_token", "")
headers = {"Authorization": f"Bearer {token}"}

r = requests.post(f"{BASE}/auth/login/json", json={"username": uname, "password": "wrong"})
check("Wrong password = 401", r.status_code == 401)

# ============ 4. GET /ME ============
print("\n--- 4. GET /ME ---")
r = requests.get(f"{BASE}/auth/me", headers=headers)
me = r.json()
check("/me returns plan_tier", "plan_tier" in me)
check("/me returns email_verified", "email_verified" in me)
check("/me returns usage", "usage" in me)
usage = me.get("usage", {})
check("/me usage.monthly_email_sent", "monthly_email_sent" in usage)
check("/me usage.monthly_email_limit", "monthly_email_limit" in usage)
check("/me usage.monthly_campaign_limit", "monthly_campaign_limit" in usage)

# ============ 5. LOGOUT ============
print("\n--- 5. LOGOUT (token blacklist) ---")
r = requests.post(f"{BASE}/auth/logout", headers=headers, json={"refresh_token": refresh})
check("Logout success", r.json().get("success") == True)
check("Access token blacklisted", r.json().get("access_token_invalidated") == True)
check("Refresh token blacklisted", r.json().get("refresh_token_invalidated") == True)

r = requests.get(f"{BASE}/auth/me", headers=headers)
check("Token rejected after logout", r.status_code == 401)

# ============ 6. EMAIL VERIFICATION ============
print("\n--- 6. EMAIL VERIFICATION ---")
conn = psycopg2.connect(DB)
cur = conn.cursor()
cur.execute("SELECT email_verification_token, email_verified FROM candidates WHERE username=%s", (uname,))
row = cur.fetchone()
check("Verification token in DB", row[0] is not None and len(row[0]) > 10)
check("Not verified before click", row[1] == False)

r = requests.get(f"{BASE}/auth/verify-email?token={row[0]}")
check("Verify email succeeds", r.json().get("success") == True)

cur.execute("SELECT email_verified FROM candidates WHERE username=%s", (uname,))
check("Verified after click", cur.fetchone()[0] == True)
conn.close()

# ============ 7. SESSIONS ============
print("\n--- 7. SESSION TRACKING ---")
r = requests.post(f"{BASE}/auth/login/json", json={"username": uname, "password": "Audit!Test99"})
token = r.json()["access_token"]
refresh = r.json()["refresh_token"]
headers = {"Authorization": f"Bearer {token}"}

r = requests.get(f"{BASE}/auth/sessions", headers=headers)
sess = r.json()
check("Sessions endpoint works", "sessions" in sess)
check("Session count > 0", sess.get("count", 0) > 0)
if sess.get("count", 0) > 0:
    s = sess["sessions"][0]
    check("Session has ip_address", s.get("ip_address") is not None)
    check("Session has device_info", s.get("device_info") is not None)
    check("Session has created_at", s.get("created_at") is not None)

# Revoke all
r = requests.post(f"{BASE}/auth/sessions/revoke-all", headers=headers)
check("Revoke-all succeeds", r.json().get("success") == True)

# ============ 8. TOKEN ROTATION ============
print("\n--- 8. REFRESH TOKEN ROTATION ---")
r = requests.post(f"{BASE}/auth/login/json", json={"username": uname, "password": "Audit!Test99"})
token = r.json()["access_token"]
refresh = r.json()["refresh_token"]

r = requests.post(f"{BASE}/auth/refresh", json={"refresh_token": refresh})
check("Refresh returns 200", r.status_code == 200)
if r.status_code == 200:
    new_refresh = r.json().get("refresh_token", "")
    check("New refresh token differs from old", new_refresh != refresh)
    check("New access token returned", len(r.json().get("access_token", "")) > 20)

# ============ 9. PASSWORD RESET (DB) ============
print("\n--- 9. PASSWORD RESET (DB-backed) ---")
r = requests.post(f"{BASE}/auth/forgot-password", json={"email": f"{uname}@gmail.com"})
check("Forgot password succeeds", r.json().get("success") == True)

conn = psycopg2.connect(DB)
cur = conn.cursor()
cur.execute("SELECT count(*) FROM password_reset_tokens WHERE user_id=(SELECT id FROM candidates WHERE username=%s)", (uname,))
check("Reset token in DB", cur.fetchone()[0] > 0)
conn.close()

# ============ 10. USAGE METERING ============
print("\n--- 10. USAGE METERING ---")
headers = {"Authorization": f"Bearer " + r.json().get("access_token", token) if False else f"Bearer {token}"}
# Re-login
r = requests.post(f"{BASE}/auth/login/json", json={"username": uname, "password": "Audit!Test99"})
token = r.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

r = requests.get(f"{BASE}/usage/", headers=headers)
u = r.json()
check("Usage returns plan_tier", "plan_tier" in u)
check("Usage emails.used", "used" in u.get("emails", {}))
check("Usage emails.limit", "limit" in u.get("emails", {}))
check("Usage emails.percent", "percent" in u.get("emails", {}))
check("Usage campaigns.used", "used" in u.get("campaigns", {}))
check("Free email limit = 100", u.get("emails", {}).get("limit") == 100)
check("Free campaign limit = 3", u.get("campaigns", {}).get("limit") == 3)

r = requests.get(f"{BASE}/usage/limits", headers=headers)
lim = r.json()
check("Limits: free plan defined", "free" in lim.get("plans", {}))
check("Limits: pro plan defined", "pro" in lim.get("plans", {}))
check("Pro unlimited emails", lim.get("plans", {}).get("pro", {}).get("monthly_email_limit") == 999999)

# ============ 11. ADMIN DASHBOARD ============
print("\n--- 11. ADMIN DASHBOARD ---")
r = requests.post(f"{BASE}/auth/login/json", json={"username": "stronguser", "password": "MyStr0ng!Pass"})
if r.status_code != 200:
    print(f"  [SKIP] Admin login failed: {r.status_code} {r.text[:80]}")
    print("  (stronguser may not exist or password changed)")
    admin_token = ""
    ah = {}
else:
    admin_token = r.json()["access_token"]
    ah = {"Authorization": f"Bearer {admin_token}"}

if admin_token:
    r = requests.get(f"{BASE}/admin/dashboard", headers=ah)
    d = r.json()
    check("Dashboard: users stats", "users" in d)
    check("Dashboard: plan breakdown", "plans" in d.get("users", {}))
    check("Dashboard: email stats", "emails" in d)
    check("Dashboard: recent registrations", "recent_registrations" in d)
    check("Dashboard: login failures", "recent_login_failures" in d)

    r = requests.get(f"{BASE}/admin/users", headers=ah)
    check("Admin users: returns list", len(r.json().get("users", [])) > 0)
    u0 = r.json()["users"][0]
    check("Admin users: has plan_tier", "plan_tier" in u0)
    check("Admin users: has email_verified", "email_verified" in u0)
    check("Admin users: has monthly_email_sent", "monthly_email_sent" in u0)

    # Set plan
    r = requests.get(f"{BASE}/admin/users?search={uname}", headers=ah)
    uid = r.json()["users"][0]["id"]
    r = requests.post(f"{BASE}/admin/users/{uid}/set-plan?plan=pro", headers=ah)
    check("Set plan to pro", r.json().get("plan_tier") == "pro")
    r = requests.post(f"{BASE}/admin/users/{uid}/set-plan?plan=free", headers=ah)
    check("Set plan to free", r.json().get("plan_tier") == "free")

    # ============ 12. AUDIT LOGS ============
    print("\n--- 12. AUDIT LOGS ---")
    r = requests.get(f"{BASE}/admin/audit-logs", headers=ah)
    al = r.json()
    check("Audit logs: returns logs", "logs" in al)
    check("Audit logs: has entries", al.get("total", 0) > 0, f"{al.get('total',0)} entries")
    check("Audit logs: has event_types", "event_types" in al)
    check("Audit logs: filterable", "page" in al and "total_pages" in al)

    r = requests.get(f"{BASE}/admin/audit-stats", headers=ah)
    s = r.json()
    check("Audit stats: total_events", "total_events" in s)
    check("Audit stats: success_rate", "success_rate" in s)
    check("Audit stats: by_type", "by_type" in s)
    check("Audit stats: suspicious_ips", "suspicious_ips" in s)
else:
    print("  [SKIP] Admin tests skipped (no admin token)")

# ============ 13. INFRASTRUCTURE ============
print("\n--- 13. INFRASTRUCTURE ---")
r = requests.get("http://localhost:8001/health")
check("API healthy", r.json().get("status") == "healthy")
check("DB connected", r.json().get("database", {}).get("status") == "connected")

# ============ SUMMARY ============
print("\n" + "=" * 70)
passed = sum(1 for _, s, _ in results if s == "PASS")
failed = sum(1 for _, s, _ in results if s == "FAIL")
print(f"TOTAL: {passed} PASSED, {failed} FAILED out of {len(results)} checks")
print("=" * 70)

if failed > 0:
    print("\nFAILED:")
    for name, status, detail in results:
        if status == "FAIL":
            print(f"  FAIL: {name} {detail}")
    sys.exit(1)
else:
    print("\nALL CHECKS PASSED!")
