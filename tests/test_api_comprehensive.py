"""
Comprehensive API Test Suite — Tests every auth, admin, usage, session endpoint
with edge cases, error paths, and security checks.

Run inside the outbrew-api container:
  docker exec outbrew-api python3 /tmp/test_api_comprehensive.py

Categories:
  A. Auth — Register, Login, Logout, Password
  B. Email Verification
  C. Sessions — Create, List, Revoke
  D. Token Rotation
  E. Usage Metering & Limits
  F. Admin Dashboard & User Management
  G. Audit Logs
  H. Security Edge Cases
  I. CRUD Operations
  J. Infrastructure
"""
import requests
import json
import time
import random
import string
import hashlib
import psycopg2

BASE = "http://localhost:8001/api/v1"
DB_URL = "postgresql://neondb_owner:npg_paXHNPt6rzy2@ep-nameless-field-a9j5xt82-pooler.gwc.azure.neon.tech/neondb?sslmode=require"

passed = 0
failed = 0
skipped = 0
failures = []

def ok(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        failures.append(f"{name}: {detail}")
        print(f"  [FAIL] {name} -- {detail}")

def skip(name, reason=""):
    global skipped
    skipped += 1
    print(f"  [SKIP] {name} -- {reason}")

def rand_str(n=8):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=n))

print("=" * 72)
print("  OUTBREW COMPREHENSIVE API TEST SUITE")
print("=" * 72)

# ═══════════════════════════════════════════════════════════════
# A. AUTH — REGISTER
# ═══════════════════════════════════════════════════════════════
print("\n╔══ A. AUTH: REGISTER ══╗")

# A1: Successful registration
u1 = f"test_{rand_str()}"
r = requests.post(f"{BASE}/auth/register", json={
    "username": u1, "email": f"{u1}@gmail.com", "password": "Test!Pass99",
    "full_name": "Test User", "email_account": f"{u1}s@gmail.com", "email_password": "p"
})
ok("A1: Register valid user", r.status_code == 201)
ok("A1: Returns id", "id" in r.json())
ok("A1: plan_tier=free", r.json().get("plan_tier") == "free")
ok("A1: email_verified=false", r.json().get("email_verified") == False)
ok("A1: Has usage", r.json().get("usage") is not None)
ok("A1: usage.monthly_email_limit=100", r.json().get("usage", {}).get("monthly_email_limit") == 100)

# A2: Duplicate username
r = requests.post(f"{BASE}/auth/register", json={
    "username": u1, "email": f"dup_{u1}@gmail.com", "password": "Test!Pass99",
    "full_name": "Dup", "email_account": "d@gmail.com", "email_password": "p"
})
ok("A2: Duplicate username rejected", r.status_code == 400)
ok("A2: Error message", "already registered" in r.json().get("detail", "").lower())

# A3: Duplicate email
r = requests.post(f"{BASE}/auth/register", json={
    "username": f"dup_{rand_str()}", "email": f"{u1}@gmail.com", "password": "Test!Pass99",
    "full_name": "Dup", "email_account": "d@gmail.com", "email_password": "p"
})
ok("A3: Duplicate email rejected", r.status_code == 400)

# A4-A7: Password validation
for label, pwd, expect in [
    ("A4: Too short", "Sh0rt!", 422),
    ("A5: No number", "Password!!", 422),
    ("A6: No special char", "Password123", 422),
    ("A7: No letter", "12345678!", 422),
]:
    r = requests.post(f"{BASE}/auth/register", json={
        "username": rand_str(), "email": f"{rand_str()}@gmail.com", "password": pwd,
        "full_name": "X", "email_account": "x@gmail.com", "email_password": "p"
    })
    ok(f"{label} ({pwd}) rejected", r.status_code == expect, f"got {r.status_code}")

# A8: Missing required fields
r = requests.post(f"{BASE}/auth/register", json={"username": "x"})
ok("A8: Missing fields rejected", r.status_code == 422)

# A9: Empty body
r = requests.post(f"{BASE}/auth/register", json={})
ok("A9: Empty body rejected", r.status_code == 422)

# ═══════════════════════════════════════════════════════════════
# A. AUTH — LOGIN
# ═══════════════════════════════════════════════════════════════
print("\n╔══ A. AUTH: LOGIN ══╗")

# A10: Valid login
r = requests.post(f"{BASE}/auth/login/json", json={"username": u1, "password": "Test!Pass99"})
ok("A10: Login succeeds", r.status_code == 200)
ok("A10: Has access_token", "access_token" in r.json())
ok("A10: Has refresh_token", "refresh_token" in r.json())
ok("A10: Has expires_in", "expires_in" in r.json())
ok("A10: token_type=bearer", r.json().get("token_type") == "bearer")
tok = r.json().get("access_token", "")
ref = r.json().get("refresh_token", "")
h = {"Authorization": f"Bearer {tok}"}

# A11: Wrong password
r = requests.post(f"{BASE}/auth/login/json", json={"username": u1, "password": "wrong"})
ok("A11: Wrong password = 401", r.status_code == 401)

# A12: Non-existent user
r = requests.post(f"{BASE}/auth/login/json", json={"username": "nonexistent_user_xyz", "password": "x"})
ok("A12: Non-existent user = 401", r.status_code == 401)

# A13: Empty credentials
r = requests.post(f"{BASE}/auth/login/json", json={"username": "", "password": ""})
ok("A13: Empty credentials rejected", r.status_code in [401, 422])

# A14: GET /me
r = requests.get(f"{BASE}/auth/me", headers=h)
ok("A14: /me returns 200", r.status_code == 200)
me = r.json()
ok("A14: Has username", me.get("username") == u1)
ok("A14: Has plan_tier", "plan_tier" in me)
ok("A14: Has email_verified", "email_verified" in me)
ok("A14: Has usage", "usage" in me)

# A15: /me without token
r = requests.get(f"{BASE}/auth/me")
ok("A15: /me without token = 401", r.status_code == 401)

# A16: /me with invalid token
r = requests.get(f"{BASE}/auth/me", headers={"Authorization": "Bearer invalid.token.here"})
ok("A16: /me with bad token = 401", r.status_code == 401)

# ═══════════════════════════════════════════════════════════════
# A. AUTH — LOGOUT
# ═══════════════════════════════════════════════════════════════
print("\n╔══ A. AUTH: LOGOUT ══╗")

# Login fresh for logout test
r = requests.post(f"{BASE}/auth/login/json", json={"username": u1, "password": "Test!Pass99"})
tok2 = r.json()["access_token"]
ref2 = r.json()["refresh_token"]
h2 = {"Authorization": f"Bearer {tok2}"}

# A17: Logout
r = requests.post(f"{BASE}/auth/logout", headers=h2, json={"refresh_token": ref2})
ok("A17: Logout success", r.json().get("success") == True)
ok("A17: Access blacklisted", r.json().get("access_token_invalidated") == True)
ok("A17: Refresh blacklisted", r.json().get("refresh_token_invalidated") == True)

# A18: Token rejected after logout
r = requests.get(f"{BASE}/auth/me", headers=h2)
ok("A18: Token dead after logout", r.status_code == 401)

# A19: Refresh token rejected after logout
r = requests.post(f"{BASE}/auth/refresh", json={"refresh_token": ref2})
ok("A19: Refresh token dead after logout", r.status_code == 401)

# A20: Logout without body (just access token)
r = requests.post(f"{BASE}/auth/login/json", json={"username": u1, "password": "Test!Pass99"})
tok3 = r.json()["access_token"]
r = requests.post(f"{BASE}/auth/logout", headers={"Authorization": f"Bearer {tok3}"}, json={})
ok("A20: Logout without refresh_token", r.json().get("success") == True)

# ═══════════════════════════════════════════════════════════════
# B. EMAIL VERIFICATION
# ═══════════════════════════════════════════════════════════════
print("\n╔══ B. EMAIL VERIFICATION ══╗")

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

# B1: Token exists in DB
cur.execute("SELECT email_verification_token, email_verified FROM candidates WHERE username=%s", (u1,))
row = cur.fetchone()
ok("B1: Verification token in DB", row[0] is not None and len(row[0]) > 10)
ok("B1: Not verified yet", row[1] == False)
vtoken = row[0]

# B2: Invalid token
r = requests.get(f"{BASE}/auth/verify-email?token=totally_invalid_token")
ok("B2: Invalid token = 400", r.status_code == 400)

# B3: Valid verification
r = requests.get(f"{BASE}/auth/verify-email?token={vtoken}")
ok("B3: Verify succeeds", r.json().get("success") == True)

# B4: Check DB
cur.execute("SELECT email_verified, email_verification_token FROM candidates WHERE username=%s", (u1,))
row = cur.fetchone()
ok("B4: email_verified=True in DB", row[0] == True)
ok("B4: Token cleared after use", row[1] is None)

# B5: Re-verify (already verified)
r = requests.get(f"{BASE}/auth/verify-email?token={vtoken}")
ok("B5: Already-used token = 400", r.status_code == 400)

# B6: Resend verification (anti-enumeration)
r = requests.post(f"{BASE}/auth/resend-verification", json={"email": "nonexistent@xyz.com"})
ok("B6: Resend for unknown email = success (anti-enum)", r.json().get("success") == True)

conn.close()

# ═══════════════════════════════════════════════════════════════
# C. SESSIONS
# ═══════════════════════════════════════════════════════════════
print("\n╔══ C. SESSION TRACKING ══╗")

# Login fresh
r = requests.post(f"{BASE}/auth/login/json", json={"username": u1, "password": "Test!Pass99"})
tok = r.json()["access_token"]
h = {"Authorization": f"Bearer {tok}"}

# C1: List sessions
r = requests.get(f"{BASE}/auth/sessions", headers=h)
ok("C1: Sessions endpoint", r.status_code == 200)
ok("C1: Has sessions array", "sessions" in r.json())
ok("C1: Count > 0", r.json().get("count", 0) > 0)

sess = r.json()["sessions"]
if sess:
    s0 = sess[0]
    ok("C2: Session has id", "id" in s0)
    ok("C2: Session has ip_address", "ip_address" in s0)
    ok("C2: Session has device_info", "device_info" in s0)
    ok("C2: Session has user_agent", "user_agent" in s0)
    ok("C2: Session has created_at", "created_at" in s0)
    ok("C2: Session has last_active_at", "last_active_at" in s0)

# C3: Create second session by logging in again
r = requests.post(f"{BASE}/auth/login/json", json={"username": u1, "password": "Test!Pass99"})
tok_b = r.json()["access_token"]
h_b = {"Authorization": f"Bearer {tok_b}"}

r = requests.get(f"{BASE}/auth/sessions", headers=h_b)
count_before = r.json().get("count", 0)
ok("C3: Multiple sessions tracked", count_before >= 2)

# C4: Revoke specific session
if r.json().get("sessions"):
    oldest_id = r.json()["sessions"][-1]["id"]
    r = requests.delete(f"{BASE}/auth/sessions/{oldest_id}", headers=h_b)
    ok("C4: Revoke session", r.json().get("success") == True)

    r = requests.get(f"{BASE}/auth/sessions", headers=h_b)
    ok("C4: Count decreased", r.json().get("count", 0) < count_before)

# C5: Revoke non-existent session
r = requests.delete(f"{BASE}/auth/sessions/999999", headers=h_b)
ok("C5: Revoke non-existent = 404", r.status_code == 404)

# C6: Revoke all sessions
r = requests.post(f"{BASE}/auth/sessions/revoke-all", headers=h_b)
ok("C6: Revoke-all success", r.json().get("success") == True)
ok("C6: Returns revoked count", "revoked_count" in r.json())

# ═══════════════════════════════════════════════════════════════
# D. TOKEN ROTATION
# ═══════════════════════════════════════════════════════════════
print("\n╔══ D. TOKEN ROTATION ══╗")

r = requests.post(f"{BASE}/auth/login/json", json={"username": u1, "password": "Test!Pass99"})
tok = r.json()["access_token"]
ref = r.json()["refresh_token"]
h = {"Authorization": f"Bearer {tok}"}

# D1: Refresh returns new tokens
r = requests.post(f"{BASE}/auth/refresh", json={"refresh_token": ref})
ok("D1: Refresh = 200", r.status_code == 200)
new_ref = r.json().get("refresh_token", "")
new_tok = r.json().get("access_token", "")
ok("D1: New refresh differs", new_ref != ref)
ok("D1: New access token issued", len(new_tok) > 20)

# D2: Old refresh token blacklisted
r = requests.post(f"{BASE}/auth/refresh", json={"refresh_token": ref})
ok("D2: Old refresh rejected", r.status_code == 401)

# D3: New refresh works
r = requests.post(f"{BASE}/auth/refresh", json={"refresh_token": new_ref})
ok("D3: New refresh works", r.status_code == 200)

# D4: Invalid refresh token
r = requests.post(f"{BASE}/auth/refresh", json={"refresh_token": "totally.invalid.token"})
ok("D4: Invalid refresh = 401", r.status_code == 401)

# ═══════════════════════════════════════════════════════════════
# E. USAGE METERING
# ═══════════════════════════════════════════════════════════════
print("\n╔══ E. USAGE METERING ══╗")

# Re-login
r = requests.post(f"{BASE}/auth/login/json", json={"username": u1, "password": "Test!Pass99"})
tok = r.json()["access_token"]
h = {"Authorization": f"Bearer {tok}"}

# E1: GET /usage/
r = requests.get(f"{BASE}/usage/", headers=h)
ok("E1: Usage endpoint", r.status_code == 200)
u = r.json()
ok("E1: Has plan_tier", "plan_tier" in u)
ok("E1: Has emails.used", "used" in u.get("emails", {}))
ok("E1: Has emails.limit", "limit" in u.get("emails", {}))
ok("E1: Has emails.remaining", "remaining" in u.get("emails", {}))
ok("E1: Has emails.percent", "percent" in u.get("emails", {}))
ok("E1: Has campaigns", "campaigns" in u)
ok("E1: Has recipients", "recipients" in u)

# E2: Free plan limits
ok("E2: Free email limit=100", u.get("emails", {}).get("limit") == 100)
ok("E2: Free campaign limit=3", u.get("campaigns", {}).get("limit") == 3)
ok("E2: Free recipient limit=100", u.get("recipients", {}).get("limit") == 100)

# E3: GET /usage/limits
r = requests.get(f"{BASE}/usage/limits", headers=h)
ok("E3: Limits endpoint", r.status_code == 200)
lim = r.json()
ok("E3: Has current_plan", "current_plan" in lim)
ok("E3: Has plans.free", "free" in lim.get("plans", {}))
ok("E3: Has plans.pro", "pro" in lim.get("plans", {}))
ok("E3: Pro email=999999", lim["plans"]["pro"]["monthly_email_limit"] == 999999)

# E4: Usage without auth
r = requests.get(f"{BASE}/usage/")
ok("E4: Usage without auth = 401", r.status_code == 401)

# ═══════════════════════════════════════════════════════════════
# F. PASSWORD RESET
# ═══════════════════════════════════════════════════════════════
print("\n╔══ F. PASSWORD RESET (DB) ══╗")

# F1: Forgot password
r = requests.post(f"{BASE}/auth/forgot-password", json={"email": f"{u1}@gmail.com"})
ok("F1: Forgot password success", r.json().get("success") == True)

# F2: Anti-enumeration (unknown email)
r = requests.post(f"{BASE}/auth/forgot-password", json={"email": "doesnt.exist.xyz@gmail.com"})
ok("F2: Unknown email still returns success", r.json().get("success") == True)

# F3: Token in DB
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
cur.execute("SELECT count(*) FROM password_reset_tokens WHERE user_id=(SELECT id FROM candidates WHERE username=%s)", (u1,))
ok("F3: Reset token stored in DB", cur.fetchone()[0] > 0)
conn.close()

# F4: Invalid reset token
r = requests.post(f"{BASE}/auth/reset-password", json={"token": "invalid", "new_password": "NewPass!123"})
ok("F4: Invalid reset token = 400", r.status_code == 400)

# F5: Change password
r = requests.post(f"{BASE}/auth/login/json", json={"username": u1, "password": "Test!Pass99"})
tok = r.json()["access_token"]
h = {"Authorization": f"Bearer {tok}"}

r = requests.post(f"{BASE}/auth/change-password", headers=h, json={
    "current_password": "Test!Pass99", "new_password": "NewP@ss456"
})
ok("F5: Change password success", r.status_code == 200)

# F6: Old password no longer works
r = requests.post(f"{BASE}/auth/login/json", json={"username": u1, "password": "Test!Pass99"})
ok("F6: Old password rejected", r.status_code == 401)

# F7: New password works
r = requests.post(f"{BASE}/auth/login/json", json={"username": u1, "password": "NewP@ss456"})
ok("F7: New password works", r.status_code == 200)

# F8: Change with wrong current password
tok = r.json()["access_token"]
h = {"Authorization": f"Bearer {tok}"}
r = requests.post(f"{BASE}/auth/change-password", headers=h, json={
    "current_password": "wrong", "new_password": "Another!123"
})
ok("F8: Wrong current password = 400", r.status_code == 400)

# F9: Change to weak password
r = requests.post(f"{BASE}/auth/change-password", headers=h, json={
    "current_password": "NewP@ss456", "new_password": "weak"
})
ok("F9: Weak new password rejected", r.status_code == 422)

# ═══════════════════════════════════════════════════════════════
# G. ADMIN (needs super_admin)
# ═══════════════════════════════════════════════════════════════
print("\n╔══ G. ADMIN DASHBOARD ══╗")

r = requests.post(f"{BASE}/auth/login/json", json={"username": "stronguser", "password": "MyStr0ng!Pass"})
if r.status_code == 200:
    at = r.json()["access_token"]
    ah = {"Authorization": f"Bearer {at}"}

    # G1: Dashboard
    r = requests.get(f"{BASE}/admin/dashboard", headers=ah)
    ok("G1: Dashboard 200", r.status_code == 200)
    d = r.json()
    ok("G1: Has users.total", "total" in d.get("users", {}))
    ok("G1: Has users.plans", "plans" in d.get("users", {}))
    ok("G1: Has emails", "emails" in d)
    ok("G1: Has recent_registrations", "recent_registrations" in d)
    ok("G1: Has recent_login_failures", "recent_login_failures" in d)
    ok("G1: Has applications", "applications" in d)
    ok("G1: Has campaigns", "campaigns" in d)
    ok("G1: Has recipients", "recipients" in d)

    # G2: Users list
    r = requests.get(f"{BASE}/admin/users", headers=ah)
    ok("G2: Users list 200", r.status_code == 200)
    ok("G2: Has users array", "users" in r.json())
    ok("G2: Has total", "total" in r.json())
    ok("G2: Has pagination", "total_pages" in r.json())
    if r.json().get("users"):
        u0 = r.json()["users"][0]
        ok("G2: User has plan_tier", "plan_tier" in u0)
        ok("G2: User has email_verified", "email_verified" in u0)
        ok("G2: User has monthly_email_sent", "monthly_email_sent" in u0)

    # G3: Users search
    r = requests.get(f"{BASE}/admin/users?search={u1}", headers=ah)
    ok("G3: Search works", r.json().get("total", 0) >= 1)

    # G4: Users filter by plan
    r = requests.get(f"{BASE}/admin/users?plan_tier=free", headers=ah)
    ok("G4: Filter by plan", r.status_code == 200)

    # G5: Set plan
    r = requests.get(f"{BASE}/admin/users?search={u1}", headers=ah)
    uid = r.json()["users"][0]["id"] if r.json().get("users") else None
    if uid:
        r = requests.post(f"{BASE}/admin/users/{uid}/set-plan?plan=pro", headers=ah)
        ok("G5: Set plan pro", r.json().get("plan_tier") == "pro")
        ok("G5: Pro limits applied", r.json().get("monthly_email_limit") == 999999)

        r = requests.post(f"{BASE}/admin/users/{uid}/set-plan?plan=free", headers=ah)
        ok("G5: Set plan free", r.json().get("plan_tier") == "free")
        ok("G5: Free limits restored", r.json().get("monthly_email_limit") == 100)

    # G6: Invalid plan
    if uid:
        r = requests.post(f"{BASE}/admin/users/{uid}/set-plan?plan=invalid", headers=ah)
        ok("G6: Invalid plan rejected", r.status_code == 400)

    # G7: Non-existent user
    r = requests.post(f"{BASE}/admin/users/999999/set-plan?plan=pro", headers=ah)
    ok("G7: Non-existent user = 404", r.status_code == 404)

    # G8: Audit logs
    r = requests.get(f"{BASE}/admin/audit-logs", headers=ah)
    ok("G8: Audit logs 200", r.status_code == 200)
    al = r.json()
    ok("G8: Has logs array", "logs" in al)
    ok("G8: Has total", "total" in al)
    ok("G8: Has event_types", "event_types" in al)
    ok("G8: Total > 0", al.get("total", 0) > 0)

    # G9: Audit logs filtered
    r = requests.get(f"{BASE}/admin/audit-logs?event_type=login_success", headers=ah)
    ok("G9: Filter by event_type", r.status_code == 200)

    # G10: Audit stats
    r = requests.get(f"{BASE}/admin/audit-stats", headers=ah)
    ok("G10: Audit stats 200", r.status_code == 200)
    st = r.json()
    ok("G10: Has total_events", "total_events" in st)
    ok("G10: Has success_rate", "success_rate" in st)
    ok("G10: Has by_type", "by_type" in st)
    ok("G10: Has suspicious_ips", "suspicious_ips" in st)

    # G11: Non-admin access denied
    r = requests.post(f"{BASE}/auth/login/json", json={"username": u1, "password": "NewP@ss456"})
    if r.status_code == 200:
        non_admin_h = {"Authorization": f"Bearer {r.json()['access_token']}"}
        r = requests.get(f"{BASE}/admin/dashboard", headers=non_admin_h)
        ok("G11: Non-admin denied dashboard", r.status_code == 403)
        r = requests.get(f"{BASE}/admin/users", headers=non_admin_h)
        ok("G11: Non-admin denied users", r.status_code == 403)
        r = requests.get(f"{BASE}/admin/audit-logs", headers=non_admin_h)
        ok("G11: Non-admin denied audit", r.status_code == 403)
else:
    skip("G: Admin tests", f"Login failed: {r.status_code}")

# ═══════════════════════════════════════════════════════════════
# H. SECURITY EDGE CASES
# ═══════════════════════════════════════════════════════════════
print("\n╔══ H. SECURITY EDGE CASES ══╗")

# H1: SQL injection in username
r = requests.post(f"{BASE}/auth/login/json", json={"username": "'; DROP TABLE candidates;--", "password": "x"})
ok("H1: SQL injection blocked", r.status_code in [401, 422])

# H2: XSS in registration
r = requests.post(f"{BASE}/auth/register", json={
    "username": f"xss_{rand_str()}", "email": f"xss_{rand_str()}@gmail.com",
    "password": "Safe!Pass99", "full_name": "<script>alert(1)</script>",
    "email_account": f"xss@gmail.com", "email_password": "p"
})
ok("H2: XSS in name stored safely", r.status_code == 201)

# H3: Very long username
r = requests.post(f"{BASE}/auth/register", json={
    "username": "a" * 200, "email": "long@gmail.com", "password": "Test!Pass99",
    "full_name": "X", "email_account": "l@gmail.com", "email_password": "p"
})
ok("H3: Long username rejected", r.status_code == 422)

# H4: Invalid email format
r = requests.post(f"{BASE}/auth/register", json={
    "username": rand_str(), "email": "not-an-email", "password": "Test!Pass99",
    "full_name": "X", "email_account": "x@gmail.com", "email_password": "p"
})
ok("H4: Invalid email rejected", r.status_code == 422)

# H5: Health endpoint no auth required
r = requests.get(f"{BASE}/health/")
ok("H5: Health no auth", r.status_code == 200)

# H6: Health live/ready no auth
r = requests.get(f"{BASE}/health/live")
ok("H6: Live no auth", r.status_code == 200)
r = requests.get(f"{BASE}/health/ready")
ok("H6: Ready no auth", r.status_code == 200)

# ═══════════════════════════════════════════════════════════════
# I. CRUD OPERATIONS
# ═══════════════════════════════════════════════════════════════
print("\n╔══ I. CRUD OPERATIONS ══╗")

r = requests.post(f"{BASE}/auth/login/json", json={"username": u1, "password": "NewP@ss456"})
tok = r.json()["access_token"]
h = {"Authorization": f"Bearer {tok}"}

# I1: Create application
r = requests.post(f"{BASE}/applications/", headers=h, json={
    "company_name": "TestCorp", "recruiter_email": f"hr{rand_str()}@test.com",
    "position_title": "Developer"
})
ok("I1: Create application", r.status_code in [200, 201])

# I2: List applications
r = requests.get(f"{BASE}/applications/", headers=h)
ok("I2: List applications", r.status_code == 200)

# I3: Create template
r = requests.post(f"{BASE}/email-templates/", headers=h, json={
    "name": f"Test Template {rand_str()}", "category": "initial_application",
    "subject_template": "Test {{position}}", "body_template_html": "<p>Hi</p>"
})
ok("I3: Create template", r.status_code in [200, 201])

# I4: List templates
r = requests.get(f"{BASE}/email-templates/", headers=h)
ok("I4: List templates", r.status_code == 200)

# I5: Create recipient
r = requests.post(f"{BASE}/recipients/", headers=h, json={
    "email": f"r{rand_str()}@test.com", "company": "TestCorp"
})
ok("I5: Create recipient", r.status_code in [200, 201])

# I6: List recipients
r = requests.get(f"{BASE}/recipients/", headers=h)
ok("I6: List recipients", r.status_code == 200)

# I7: Follow-up sequences
r = requests.get(f"{BASE}/follow-up/sequences", headers=h)
ok("I7: Follow-up sequences", r.status_code == 200)

# I8: Extraction jobs
r = requests.get(f"{BASE}/extraction/jobs", headers=h)
ok("I8: Extraction jobs", r.status_code == 200)

# I9: Companies
r = requests.get(f"{BASE}/companies/", headers=h)
ok("I9: Companies list", r.status_code == 200)

# I10: Analytics
r = requests.get(f"{BASE}/analytics/dashboard", headers=h)
ok("I10: Analytics", r.status_code == 200)

# ═══════════════════════════════════════════════════════════════
# J. INFRASTRUCTURE
# ═══════════════════════════════════════════════════════════════
print("\n╔══ J. INFRASTRUCTURE ══╗")

r = requests.get("http://localhost:8001/health")
ok("J1: API healthy", r.json().get("status") == "healthy")
ok("J1: DB connected", r.json().get("database", {}).get("status") == "connected")

r = requests.get("http://localhost:8001/")
ok("J2: Root endpoint", r.json().get("status") == "operational")

r = requests.get("http://localhost:8001/api/docs")
ok("J3: Swagger docs", r.status_code == 200)

r = requests.get("http://localhost:8001/api/openapi.json")
ok("J4: OpenAPI spec", r.status_code == 200)
paths = r.json().get("paths", {})
ok("J4: Has 380+ paths", len(paths) >= 380, f"found {len(paths)}")

# ═══════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 72)
total = passed + failed + skipped
print(f"  RESULTS: {passed} PASSED, {failed} FAILED, {skipped} SKIPPED / {total} total")
print("=" * 72)

if failures:
    print("\n  FAILURES:")
    for f in failures:
        print(f"    - {f}")

import sys
sys.exit(0 if failed == 0 else 1)
