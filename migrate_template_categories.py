"""Migrate template categories to new frontend-aligned format"""
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()

print("=" * 60)
print("Migrating Template Categories")
print("=" * 60)

try:
    # Check current templates
    result = db.execute(text("SELECT id, name, category FROM email_templates"))
    templates = result.fetchall()

    print(f"\nFound {len(templates)} templates:")
    for t in templates:
        print(f"  - ID {t[0]}: {t[1]} (category: {t[2]})")

    if not templates:
        print("\n[INFO] No templates found. Nothing to migrate.")
        db.close()
        sys.exit(0)

    # Update ai_generated → application
    result = db.execute(
        text("UPDATE email_templates SET category = 'application' WHERE category = 'ai_generated'")
    )
    db.commit()
    print(f"\n[SUCCESS] Updated {result.rowcount} templates: ai_generated → application")

    # Update initial_application → application
    result = db.execute(
        text("UPDATE email_templates SET category = 'application' WHERE category = 'initial_application'")
    )
    db.commit()
    print(f"[SUCCESS] Updated {result.rowcount} templates: initial_application → application")

    # Update follow_up → followup
    result = db.execute(
        text("UPDATE email_templates SET category = 'followup' WHERE category = 'follow_up'")
    )
    db.commit()
    print(f"[SUCCESS] Updated {result.rowcount} templates: follow_up → followup")

    # Update networking → outreach
    result = db.execute(
        text("UPDATE email_templates SET category = 'outreach' WHERE category = 'networking'")
    )
    db.commit()
    print(f"[SUCCESS] Updated {result.rowcount} templates: networking → outreach")

    # Check final result
    result = db.execute(text("SELECT id, name, category FROM email_templates"))
    templates = result.fetchall()

    print(f"\nFinal template categories:")
    category_counts = {}
    for t in templates:
        cat = t[2]
        category_counts[cat] = category_counts.get(cat, 0) + 1
        print(f"  - ID {t[0]}: {t[1]} (category: {cat})")

    print(f"\nCategory distribution:")
    for cat, count in category_counts.items():
        print(f"  - {cat}: {count} templates")

    print("\n" + "=" * 60)
    print("Migration Complete!")
    print("=" * 60)

except Exception as e:
    print(f"\n[ERROR] Migration failed: {e}")
    db.rollback()
finally:
    db.close()
