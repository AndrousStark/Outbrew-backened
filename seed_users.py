"""
Database Seed Script - Add Default Users

This script creates the following users if they don't exist:
1. Pragya (normal user) - username: pragya, password: pragya123
2. Super Admin - username: admin, password: admin123

Run this script from the backend directory:
    python seed_users.py
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.database import SessionLocal, init_db
from app.core.auth import get_password_hash
from app.core.encryption import encrypt_value
from app.models.candidate import Candidate, UserRole


def seed_users():
    """Seed default users into the database"""

    # Initialize database tables first
    print("=" * 60)
    print("Outbrew - Database Seeder")
    print("=" * 60)

    print("\n[1/4] Initializing database...")
    init_db()

    # Create database session
    db = SessionLocal()

    try:
        # ============================================
        # User 1: Pragya (Normal User)
        # ============================================
        print("\n[2/4] Checking for user 'pragya'...")

        pragya = db.query(Candidate).filter(
            Candidate.username == "pragya",
            Candidate.deleted_at.is_(None)
        ).first()

        if pragya:
            print("   [OK] User 'pragya' already exists (ID: {})".format(pragya.id))
        else:
            print("   --> Creating user 'pragya'...")
            # Get email password from environment variable for security
            # Set PRAGYA_EMAIL_PASSWORD env var or configure via admin API after creation
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
                title="Software Engineer",
                is_active=True
            )
            db.add(pragya)
            db.commit()
            db.refresh(pragya)
            print("   [OK] User 'pragya' created successfully (ID: {})".format(pragya.id))
            if not pragya_email_password:
                print("   [WARN] No PRAGYA_EMAIL_PASSWORD env var set - configure via admin API")

        # ============================================
        # User 2: Super Admin
        # ============================================
        print("\n[3/4] Checking for user 'admin'...")

        admin = db.query(Candidate).filter(
            Candidate.username == "admin",
            Candidate.deleted_at.is_(None)
        ).first()

        if admin:
            print("   [OK] User 'admin' already exists (ID: {})".format(admin.id))
        else:
            print("   --> Creating user 'admin'...")
            # Admin email password from env var or empty (admin may not need email sending)
            admin_email_password = os.environ.get("ADMIN_EMAIL_PASSWORD", "")
            admin = Candidate(
                username="admin",
                email="admin@hrresume.local",
                hashed_password=get_password_hash("admin123"),
                full_name="Super Admin",
                role=UserRole.SUPER_ADMIN,
                email_account="admin@hrresume.local",
                email_password=encrypt_value(admin_email_password) if admin_email_password else "",
                smtp_host="smtp.gmail.com",
                smtp_port=587,
                title="System Administrator",
                is_active=True
            )
            db.add(admin)
            db.commit()
            db.refresh(admin)
            print("   [OK] User 'admin' created successfully (ID: {})".format(admin.id))

        # ============================================
        # Summary
        # ============================================
        print("\n[4/4] Seeding complete!")
        print("=" * 60)
        print("\nUser Credentials:")
        print("-" * 40)
        print("Normal User:")
        print("   Username: pragya")
        print("   Password: pragya123")
        print("   Email:    pragyapandey2709@gmail.com")
        print("-" * 40)
        print("Super Admin:")
        print("   Username: admin")
        print("   Password: admin123")
        print("   Email:    admin@hrresume.local")
        print("=" * 60)

        # List all users
        print("\nAll Users in Database:")
        all_users = db.query(Candidate).filter(Candidate.deleted_at.is_(None)).all()
        for user in all_users:
            print("   [{}] {} ({}) - {}".format(user.id, user.username, user.role.value, user.email))

        print("\n[SUCCESS] Database seeding completed successfully!")

    except Exception as e:
        print("\n[ERROR] Error during seeding: {}".format(e))
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    seed_users()
