"""Initialize database with all tables"""
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.database import init_db, Base, engine
from app.models.candidate import Candidate, UserRole
from app.core.auth import get_password_hash
from app.core.encryption import encrypt_value

print("=" * 50)
print("Initializing Outbrew Database")
print("=" * 50)

# Create all tables
init_db()

# Create database session
from app.core.database import SessionLocal
db = SessionLocal()

# Check if we have users
existing_users = db.query(Candidate).count()

if existing_users == 0:
    print("\n[SETUP] Creating default users...")

    # Create Super Admin
    admin = Candidate(
        username="admin",
        email="admin@outbrew.app",
        hashed_password=get_password_hash("admin123"),
        full_name="Super Admin",
        role=UserRole.SUPER_ADMIN,
        email_account="admin@outbrew.app",
        email_password="",
        smtp_host="smtp.gmail.com",
        smtp_port=587,
        is_active=True
    )
    db.add(admin)

    # Create Pragya
    # NOTE: Email app password should be set via /auth/register or admin API for security
    # Using environment variable or placeholder for initial setup
    pragya_email_password = os.environ.get("PRAGYA_EMAIL_PASSWORD", "")
    pragya = Candidate(
        username="pragya",
        email="pragyapandey2709@gmail.com",
        hashed_password=get_password_hash("pragya123"),
        full_name="Pragya Pandey",
        role=UserRole.PRAGYA,
        email_account="pragyapandey2709@gmail.com",
        email_password=encrypt_value(pragya_email_password) if pragya_email_password else "",
        smtp_host="smtp.gmail.com",
        smtp_port=587,
        title="Machine Learning Engineer",
        is_active=True
    )
    db.add(pragya)

    db.commit()

    print("[OK] Default users created:")
    print("   - Username: admin, Password: admin123")
    print("   - Username: pragya, Password: pragya123")
else:
    print(f"\n[INFO] Database already has {existing_users} users")

db.close()

print("\n" + "=" * 50)
print("Database initialization complete!")
print("=" * 50)
