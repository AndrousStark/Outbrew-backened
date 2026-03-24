"""Create sample email templates for Pragya"""
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()

print("=" * 60)
print("Creating Sample Email Templates")
print("=" * 60)

try:
    # Get Pragya using raw SQL to avoid import issues
    result = db.execute(text("SELECT id, full_name FROM candidates WHERE username = 'pragya'"))
    pragya_row = result.fetchone()

    if not pragya_row:
        print("[ERROR] Pragya user not found!")
        db.close()
        sys.exit(1)

    pragya_id = pragya_row[0]
    pragya_name = pragya_row[1]
    print(f"\n[OK] Found user: {pragya_name} (ID: {pragya_id})")

    # Check existing templates
    result = db.execute(text("SELECT COUNT(*) FROM email_templates WHERE candidate_id = :id"), {"id": pragya_id})
    existing = result.scalar()
    print(f"[INFO] Existing templates: {existing}")

    if existing > 0:
        print("[INFO] Templates already exist. Skipping creation.")
        db.close()
        sys.exit(0)

    # Create 5 AI-generated templates with different tones
    templates = [
        {
            "name": "Professional ML Engineer Application",
            "description": "Professional tone for machine learning engineering positions",
            "category": "application",
            "tone": "professional",
            "subject": "Application for {{position_title}} Position - Machine Learning Engineer with Published Research",
            "body": """<p>Dear {{recruiter_name}},</p>

<p>I am writing to express my strong interest in the {{position_title}} position at {{company_name}}. With a proven track record in machine learning and AI, including published research achieving 99.93% accuracy in medical diagnosis systems, I am confident in my ability to contribute significantly to your team.</p>

<p>My experience includes:</p>
<ul>
<li>Developing ColNet, a deep learning model for COVID-19 detection with 99.93% accuracy</li>
<li>16+ years of Python expertise with TensorFlow, PyTorch, and Scikit-learn</li>
<li>8+ years working with React for building ML-powered applications</li>
<li>Full-stack development experience at Clarivate, DRDO, and Tech Mahindra</li>
</ul>

<p>I am particularly impressed by {{company_name}}'s work in {{company_focus_area}}, and I believe my background in {{matching_skills}} aligns well with your requirements.</p>

<p>I would welcome the opportunity to discuss how my skills and experience can contribute to your team's success. Please find my resume attached for your review.</p>

<p>Thank you for your consideration.</p>

<p>Best regards,<br>
Pragya Pandey<br>
Machine Learning Engineer<br>
pragyapandey2709@gmail.com</p>"""
        },
        {
            "name": "Enthusiastic AI Engineer Outreach",
            "description": "Enthusiastic and energetic tone showcasing excitement",
            "category": "application",
            "tone": "enthusiastic",
            "subject": "Excited to Apply: {{position_title}} at {{company_name}}!",
            "body": """<p>Hi {{recruiter_name}}!</p>

<p>I'm thrilled to apply for the {{position_title}} role at {{company_name}}! Your company's innovative work in {{company_focus_area}} really resonates with me, and I'm genuinely excited about the possibility of contributing to your team.</p>

<p>Here's what I bring to the table:</p>
<ul>
<li><strong>Published Research!</strong> My ColNet project achieved 99.93% accuracy in COVID-19 detection - it's been incredibly rewarding to see AI make a real-world impact in healthcare!</li>
<li><strong>16+ years of Python mastery</strong> - I absolutely love working with TensorFlow, PyTorch, and building ML pipelines!</li>
<li><strong>Full-stack expertise</strong> - From React frontends to Python backends, I enjoy building complete AI-powered applications!</li>
<li><strong>Proven track record</strong> - Delivered successful projects at Clarivate, DRDO, and Tech Mahindra!</li>
</ul>

<p>I'm particularly drawn to {{company_name}} because of your commitment to {{company_value}}. I'd love to bring my passion for machine learning and my technical skills to help drive your AI initiatives forward!</p>

<p>Can we schedule a call to discuss how I can contribute to your team? I'm eager to learn more about the role and share how my experience aligns with your needs!</p>

<p>Looking forward to connecting!</p>

<p>Cheers,<br>
Pragya Pandey<br>
Machine Learning Engineer<br>
pragyapandey2709@gmail.com</p>"""
        },
        {
            "name": "Story-Driven Career Journey",
            "description": "Narrative approach highlighting career journey and achievements",
            "category": "application",
            "tone": "story_driven",
            "subject": "From Medical AI Research to {{company_name}} - My Journey as an ML Engineer",
            "body": """<p>Dear {{recruiter_name}},</p>

<p>Three years ago, during the height of the COVID-19 pandemic, I found myself with a unique challenge: how could I use my machine learning expertise to make a tangible difference in people's lives?</p>

<p>That question led me to develop ColNet, a deep learning system for COVID-19 detection that achieved 99.93% accuracy. Seeing my research published and knowing it could help diagnose patients faster and more accurately was a defining moment in my career - it showed me the real power of AI when applied thoughtfully.</p>

<p>Since then, my journey has taken me through diverse and challenging environments:</p>
<ul>
<li>At <strong>Clarivate</strong>, I worked on information analytics systems that processed massive datasets</li>
<li>At <strong>DRDO</strong> (Defence Research), I built robust AI systems for critical defense applications</li>
<li>At <strong>Tech Mahindra</strong>, I scaled ML solutions for enterprise clients</li>
</ul>

<p>Each experience taught me something invaluable: Clarivate showed me the importance of data quality, DRDO taught me about building fail-safe systems, and Tech Mahindra honed my ability to deliver practical AI solutions that businesses can actually use.</p>

<p>Now, I'm excited about the {{position_title}} opportunity at {{company_name}} because your work in {{company_focus_area}} represents the kind of impactful, innovative AI application that drives me. With my 16+ years in Python, expertise in TensorFlow and PyTorch, and track record of shipping production ML systems, I'm ready to contribute to your mission.</p>

<p>I'd love to discuss how my journey and experience can add value to your team. What challenges are you currently tackling in {{company_focus_area}}?</p>

<p>Best regards,<br>
Pragya Pandey<br>
Machine Learning Engineer<br>
pragyapandey2709@gmail.com</p>"""
        },
        {
            "name": "Value-First ROI Focus",
            "description": "Business-focused approach highlighting ROI and value delivery",
            "category": "application",
            "tone": "value_first",
            "subject": "Driving Measurable ML Impact: {{position_title}} Application",
            "body": """<p>Dear {{recruiter_name}},</p>

<p>I'm reaching out regarding the {{position_title}} position at {{company_name}} because I believe I can deliver significant, measurable value to your AI initiatives from day one.</p>

<p><strong>The Value I Bring:</strong></p>

<p><strong>1. Proven ROI in Production ML Systems</strong></p>
<ul>
<li>Developed ColNet: 99.93% accuracy in COVID-19 detection, reducing diagnosis time by 80%</li>
<li>Published research demonstrating real-world applicability and impact</li>
<li>System designed for production deployment, not just academic exercises</li>
</ul>

<p><strong>2. Full-Stack Efficiency</strong></p>
<ul>
<li>16+ years Python expertise = faster model development and debugging</li>
<li>8+ years React experience = can build end-to-end ML applications without additional frontend resources</li>
<li>TensorFlow, PyTorch, Scikit-learn mastery = flexible toolset for any ML challenge</li>
</ul>

<p><strong>3. Enterprise-Scale Delivery</strong></p>
<ul>
<li>Clarivate: Built analytics systems processing millions of data points daily</li>
<li>DRDO: Delivered mission-critical AI systems with 99.9% uptime requirements</li>
<li>Tech Mahindra: Scaled ML solutions for Fortune 500 clients</li>
</ul>

<p><strong>How This Translates to {{company_name}}:</strong></p>
<p>Based on your focus on {{company_focus_area}}, I can immediately contribute by:</p>
<ul>
<li>Accelerating your ML model development cycle with proven pipelines</li>
<li>Building production-ready systems that scale from prototype to deployment</li>
<li>Reducing time-to-market for AI features through full-stack capability</li>
<li>Bringing best practices from defense, analytics, and enterprise environments</li>
</ul>

<p>Companies like {{company_name}} succeed when AI delivers measurable business outcomes, not just impressive accuracy metrics. I focus on building systems that drive real ROI.</p>

<p>Let's discuss how I can create value for your team. Are you available for a 30-minute call this week?</p>

<p>Best regards,<br>
Pragya Pandey<br>
Machine Learning Engineer<br>
pragyapandey2709@gmail.com<br>
Portfolio: github.com/pragya-ml-engineer</p>"""
        },
        {
            "name": "Consultant-Style Problem Solver",
            "description": "Consultative approach offering solutions and insights",
            "category": "application",
            "tone": "consultant",
            "subject": "Solving {{company_name}}'s ML Challenges: {{position_title}} Opportunity",
            "body": """<p>Dear {{recruiter_name}},</p>

<p>I've been following {{company_name}}'s progress in {{company_focus_area}}, and I noticed you're hiring for a {{position_title}}. Based on my analysis of your technical stack and market positioning, I believe you're likely facing several interesting ML challenges:</p>

<p><strong>Challenge #1: Model Accuracy vs. Inference Speed</strong></p>
<p>My Approach: In my ColNet project (99.93% accuracy for COVID-19 detection), I optimized model architecture to maintain high accuracy while reducing inference time by 60%. This involved strategic pruning, quantization, and architectural choices that prioritized production performance.</p>

<p><strong>Challenge #2: Scaling ML Infrastructure</strong></p>
<p>My Experience: At Clarivate and Tech Mahindra, I built ML pipelines that processed millions of data points daily. Key lessons: async processing, intelligent caching, and modular model serving can 10x your throughput without proportional cost increases.</p>

<p><strong>Challenge #3: Cross-Functional Collaboration</strong></p>
<p>My Advantage: With 16+ years in Python (ML/backend) and 8+ years in React (frontend), I bridge the gap between data science and engineering teams. This means faster iteration cycles and better product-ML alignment.</p>

<p><strong>What I'd Focus On in the First 90 Days:</strong></p>
<ol>
<li><strong>Weeks 1-2: Diagnostic Phase</strong><br>
   - Audit existing ML pipelines for bottlenecks<br>
   - Identify quick wins for accuracy or performance improvements<br>
   - Map technical debt and prioritize based on business impact</li>

<li><strong>Weeks 3-6: Quick Wins</strong><br>
   - Implement 2-3 high-impact optimizations<br>
   - Establish ML best practices and documentation<br>
   - Set up monitoring and observability for model performance</li>

<li><strong>Weeks 7-12: Strategic Initiatives</strong><br>
   - Drive major ML feature development aligned with product roadmap<br>
   - Mentor junior team members on ML engineering best practices<br>
   - Build scalable infrastructure for next-generation models</li>
</ol>

<p><strong>Why This Matters to {{company_name}}:</strong></p>
<p>You're not just hiring an ML engineer; you're investing in someone who can:</p>
<ul>
<li>Diagnose ML problems quickly (DRDO experience with mission-critical systems)</li>
<li>Ship production ML features fast (full-stack capability)</li>
<li>Think strategically about ML ROI (enterprise consulting mindset)</li>
<li>Scale systems as your company grows (proven track record across 3 major orgs)</li>
</ul>

<p>I'd welcome a conversation about the specific ML challenges you're prioritizing for 2026. What would success look like for this role in the first 6 months?</p>

<p>Looking forward to exploring how we can work together.</p>

<p>Best regards,<br>
Pragya Pandey<br>
Machine Learning Engineer & Consultant<br>
pragyapandey2709@gmail.com<br>
LinkedIn: linkedin.com/in/pragya-pandey-ml</p>"""
        }
    ]

    print(f"\n[INFO] Creating {len(templates)} sample templates...")

    for i, tmpl_data in enumerate(templates, 1):
        # Insert using raw SQL to avoid ORM import issues
        insert_sql = text("""
            INSERT INTO email_templates (
                candidate_id, name, description, category, language, tone,
                subject_template, body_template_html, is_default, is_active, times_used
            ) VALUES (
                :candidate_id, :name, :description, :category, :language, :tone,
                :subject, :body, :is_default, :is_active, :times_used
            )
        """)

        db.execute(insert_sql, {
            "candidate_id": pragya_id,
            "name": tmpl_data["name"],
            "description": tmpl_data["description"],
            "category": tmpl_data["category"],
            "language": "english",
            "tone": tmpl_data["tone"],
            "subject": tmpl_data["subject"],
            "body": tmpl_data["body"],
            "is_default": (i == 1),  # First template is default
            "is_active": True,
            "times_used": 0
        })
        print(f"  [{i}/5] Created: {tmpl_data['name']} ({tmpl_data['tone']})")

    db.commit()

    # Verify
    result = db.execute(text("SELECT COUNT(*) FROM email_templates WHERE candidate_id = :id"), {"id": pragya_id})
    final_count = result.scalar()
    print(f"\n[SUCCESS] Created {final_count} templates for Pragya!")

    print("\n" + "=" * 60)
    print("Sample Templates Created Successfully!")
    print("=" * 60)

except Exception as e:
    print(f"\n[ERROR] Failed to create templates: {e}")
    import traceback
    traceback.print_exc()
    db.rollback()
finally:
    db.close()
