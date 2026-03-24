"""Email Preview and Editing Endpoints"""
import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr
from typing import Optional
from jinja2.sandbox import SandboxedEnvironment

from app.api.dependencies import get_db, get_current_candidate

logger = logging.getLogger(__name__)
from app.models.application import Application
from app.models.candidate import Candidate
from app.models.company import Company

router = APIRouter()


class EmailPreviewRequest(BaseModel):
    application_id: int
    custom_subject: Optional[str] = None
    custom_body: Optional[str] = None
    use_template: bool = True


class EmailEditRequest(BaseModel):
    subject: str
    body_html: str


class EmailPreviewResponse(BaseModel):
    from_email: str
    from_name: str
    to_email: str
    to_name: str
    subject: str
    body_html: str
    body_text: str
    company_name: str
    position_title: str
    recruiter_name: str


@router.post("/preview", response_model=EmailPreviewResponse)
def preview_email(
    request: EmailPreviewRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Generate email preview before sending

    Can use:
    - Default template with dynamic data
    - Custom subject and body
    - Existing saved email from application
    """
    # Get application (scoped to current user)
    application = db.query(Application).filter(
        Application.id == request.application_id,
        Application.candidate_id == current_candidate.id,
        Application.deleted_at.is_(None)
    ).first()

    if not application:
        raise HTTPException(status_code=404, detail="Application not found")

    # Use authenticated candidate
    candidate = current_candidate

    # Get company
    company = db.query(Company).filter(
        Company.id == application.company_id,
        Company.deleted_at.is_(None)
    ).first()

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Determine email content
    if request.custom_subject and request.custom_body:
        # Use custom content
        subject = request.custom_subject
        body_html = request.custom_body
    elif application.email_subject and application.email_body_html:
        # Use saved email
        subject = application.email_subject
        body_html = application.email_body_html
    elif request.use_template:
        # Generate from template
        subject = f"Application for {application.position_title} Position at {company.name}"

        # Email template (Jinja2 syntax)
        email_template = """<html>
<head>
    <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
        .container { max-width: 600px; margin: 0 auto; padding: 20px; }
        .header { background-color: #f4f4f4; padding: 20px; text-align: center; }
        .content { padding: 20px; }
        .footer { background-color: #f4f4f4; padding: 10px; text-align: center; font-size: 12px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>Job Application</h2>
        </div>
        <div class="content">
            <p>Dear {{ recruiter_name }},</p>

            <p>I hope this email finds you well. I am writing to express my strong interest in the <strong>{{ position_title }}</strong> position at <strong>{{ company_name }}</strong>.</p>

            <p>With my background and skills, I believe I would be a great fit for your team. I am particularly drawn to {{ company_name }}'s work in {{ industry }}.</p>

            {% if alignment_text %}
            <p><strong>Why I'm a great fit:</strong></p>
            <p>{{ alignment_text }}</p>
            {% endif %}

            <p>I have attached my resume for your review. I would welcome the opportunity to discuss how my experience and skills can contribute to your team's success.</p>

            <p>Thank you for considering my application. I look forward to hearing from you.</p>

            <p>Best regards,<br>
            <strong>{{ candidate_name }}</strong><br>
            {{ candidate_email }}</p>
        </div>
        <div class="footer">
            <p>This is a professional job application email.</p>
        </div>
    </div>
</body>
</html>"""

        # Render template
        template = SandboxedEnvironment().from_string(email_template)
        body_html = template.render(
            recruiter_name=application.recruiter_name or "Hiring Manager",
            position_title=application.position_title or "the advertised position",
            company_name=company.name,
            industry=company.industry or "your industry",
            alignment_text=application.alignment_text,
            candidate_name=candidate.full_name,
            candidate_email=candidate.email
        )
    else:
        raise HTTPException(status_code=400, detail="No email content available")

    # Generate plain text version (strip HTML tags for simplicity)
    import re
    body_text = re.sub('<[^<]+?>', '', body_html)
    body_text = re.sub(r'\n\s*\n', '\n\n', body_text)

    return {
        "from_email": candidate.email_account,
        "from_name": candidate.full_name,
        "to_email": application.recruiter_email,
        "to_name": application.recruiter_name or "Hiring Manager",
        "subject": subject,
        "body_html": body_html,
        "body_text": body_text.strip(),
        "company_name": company.name,
        "position_title": application.position_title or "Position",
        "recruiter_name": application.recruiter_name or "Hiring Manager"
    }


@router.put("/{application_id}/save-email")
def save_email_draft(
    application_id: int,
    email: EmailEditRequest,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Save email draft to application"""
    application = db.query(Application).filter(
        Application.id == application_id,
        Application.candidate_id == current_candidate.id,
        Application.deleted_at.is_(None)
    ).first()

    if not application:
        raise HTTPException(status_code=404, detail="Application not found")

    # Save email content
    application.email_subject = email.subject
    application.email_body_html = email.body_html

    try:
        db.commit()
        db.refresh(application)
        logger.info(f"[EmailPreview] Saved email draft for application {application_id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[EmailPreview] Failed to save email draft for application {application_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to save email draft")

    return {
        "message": "Email draft saved successfully",
        "application_id": application.id,
        "subject": application.email_subject
    }


@router.get("/{application_id}/template")
def get_email_template(
    application_id: int,
    db: Session = Depends(get_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get available email templates for an application"""
    application = db.query(Application).filter(
        Application.id == application_id,
        Application.candidate_id == current_candidate.id,
        Application.deleted_at.is_(None)
    ).first()

    if not application:
        raise HTTPException(status_code=404, detail="Application not found")

    # Return template options
    return {
        "templates": [
            {
                "id": "professional",
                "name": "Professional Introduction",
                "description": "Formal professional introduction with resume attachment"
            },
            {
                "id": "enthusiastic",
                "name": "Enthusiastic Application",
                "description": "Enthusiastic tone emphasizing passion and fit"
            },
            {
                "id": "concise",
                "name": "Concise Application",
                "description": "Brief and to-the-point application"
            },
            {
                "id": "custom",
                "name": "Custom Email",
                "description": "Start from blank template"
            }
        ],
        "has_saved_draft": bool(application.email_subject and application.email_body_html)
    }
