import json
import os
import tempfile
from typing import List, Optional
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pydantic import BaseModel
from PyPDF2 import PdfReader
from .resume_scorer import score_resume, enhance_job_requirements

router = APIRouter(prefix="/ai", tags=["AI"])


class ResumeScoringRequest(BaseModel):
    """Resume Scoring Request Schema"""
    resume_text: str
    job_description: str
    target_skills: List[str] = []


class ResumeScoringResponse(BaseModel):
    """Resume Scoring Response Schema"""
    success: bool
    data: Optional[dict] = None
    error: Optional[str] = None
    message: str


class JobRequirementsEnhancementRequest(BaseModel):
    """Job Requirements Enhancement Request Schema"""
    job_requirements: Optional[str] = None
    job_title: Optional[str] = None
    industry: Optional[str] = None
    job_skills: Optional[str] = None
    gender: Optional[str] = None
    years_experience: Optional[str] = None
    age: Optional[str] = None
    education: Optional[str] = None
    working_type: Optional[str] = None


class JobRequirementsEnhancementResponse(BaseModel):
    """Job Requirements Enhancement Response Schema"""
    success: bool
    enhanced_requirements: Optional[str] = None
    error: Optional[str] = None
    message: str


async def extract_text_from_upload(upload_file: UploadFile) -> str:
    content = await upload_file.read()
    file_extension = os.path.splitext(upload_file.filename)[1].lower()
    with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
        temp_file.write(content)
        temp_path = temp_file.name
    try:
        if file_extension == ".pdf":
            reader = PdfReader(temp_path)
            text = ""
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text
            if not text.strip():
                raise HTTPException(status_code=400, detail="Could not extract text from PDF")
            return text
        elif file_extension == ".txt":
            with open(temp_path, "r", encoding="utf-8") as f:
                text = f.read()
            if not text.strip():
                raise HTTPException(status_code=400, detail="TXT file is empty")
            return text
        else:
            raise HTTPException(status_code=400, detail="Only PDF and TXT files are supported")
    finally:
        os.unlink(temp_path)



@router.post("/score-resume", response_model=ResumeScoringResponse)
async def score_resume_endpoint(
    resume_text: str = Form(..., description="Resume text"),
    job_description: str = Form(..., description="Job description text"),
    target_skills: List[str] = Form(None, description="Array list of target skills")
) -> ResumeScoringResponse:
    """
    Score a resume against a job description and target skills.
    
    Args:
        resume_text: text input of resume
        job_description: Text description of the job requirements
        target_skills: Array list of skills to check for
    
    Returns:
        Detailed scoring results including skills match, experience score, education score, and overall assessment
    """
    try:
        if target_skills is None:
            target_skills = []
        
        result = await score_resume(resume_text, job_description, target_skills)
        
        return ResumeScoringResponse(
            success=True,
            data=result,
            message="Resume berhasil dinilai"
        )
                
    except Exception as e:
        return ResumeScoringResponse(
            success=False,
            error=str(e),
            message="Gagal menilai resume"
        )
    

@router.post("/score-pdf", response_model=ResumeScoringResponse)
async def score_pdf_endpoint(
    resume: UploadFile = File(..., description="Resume file (PDF or text)"),
    job_description: str = Form(..., description="Job description text"),
    target_skills: str = Form("[]", description="JSON string of target skills")
) -> ResumeScoringResponse:
    """
    Score a resume (PDF or TXT) against a job description and target skills.
    """
    try:
        if not resume.filename:
            raise HTTPException(status_code=400, detail="No file provided")
        
        # Parse target_skills from JSON string
        try:
            target_skills_list = json.loads(target_skills) if target_skills else []
        except json.JSONDecodeError:
            target_skills_list = []
        
        # Extract text from file
        resume_text = await extract_text_from_upload(resume)
        
        # Score the resume
        result = await score_resume(resume_text, job_description, target_skills_list)
        
        return ResumeScoringResponse(
            success=True,
            data=result,
            message="Resume berhasil dinilai"
        )
    except HTTPException:
        raise
    except Exception as e:
        return ResumeScoringResponse(
            success=False,
            error=str(e),
            message="Gagal menilai resume"
        )


@router.post("/enhance_job_requirements", response_model=JobRequirementsEnhancementResponse)
async def enhance_job_requirements_endpoint(
    request: JobRequirementsEnhancementRequest
) -> JobRequirementsEnhancementResponse:
    """
    Enhance job requirements with bullet point formatting and 500 character limit.
    Uses additional context information when available to provide more complete requirements.
    
    Args:
        request: JobRequirementsEnhancementRequest containing:
            - job_requirements: The main job requirements text (optional, not used)
            - job_title: Job title (optional)
            - industry: Industry sector (optional)
            - job_skills: Required skills (optional)
            - gender: Gender preference (optional)
            - years_experience: Years of experience required (optional)
            - age: Age requirements (optional)
            - education: Education requirements (optional)
            - working_type: Working arrangement (optional)
    
    Returns:
        Enhanced job requirements with bullet point formatting and maximum 500 characters
    """
    try:
        enhanced_requirements = await enhance_job_requirements(
            job_requirements=request.job_requirements,
            job_title=request.job_title,
            industry=request.industry,
            job_skills=request.job_skills,
            gender=request.gender,
            years_experience=request.years_experience,
            age=request.age,
            education=request.education,
            working_type=request.working_type
        )
        
        return JobRequirementsEnhancementResponse(
            success=True,
            enhanced_requirements=enhanced_requirements,
            message="Persyaratan pekerjaan berhasil ditingkatkan"
        )
                
    except Exception as e:
        return JobRequirementsEnhancementResponse(
            success=False,
            error=str(e),
            message="Gagal meningkatkan persyaratan pekerjaan"
        )


@router.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "service": "Resume Scoring API",
        "version": "1.0.0",
        "endpoints": {
            "score_resume": "/score-resume",
            "score_pdf": "/score-pdf",
            "enhance_job_requirements": "/enhance_job_requirements",
            "docs": "/docs"
        }
    }


@router.get("/test-agents")
async def test_agents():
    """Test endpoint to debug agents library behavior"""
    try:
        from .resume_scorer import safe_runner_run, skill_extractor_agent
        import json
        
        test_input = json.dumps({
            "resume_text": "Test resume text",
            "target_skills": ["Python", "JavaScript"]
        })
        
        result = await safe_runner_run(skill_extractor_agent, test_input)
        
        return {
            "success": True,
            "message": "Agents test successful",
            "result_type": str(type(result)),
            "has_final_output": hasattr(result, 'final_output')
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "message": "Agents test failed"
        }