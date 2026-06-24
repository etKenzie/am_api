import asyncio
import json
import os
import shutil
import tempfile
from typing import List, Optional

from fastapi import APIRouter, UploadFile, File, Form, Query, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from dotenv import load_dotenv

from .resume_scorer import score_resume, enhance_job_requirements
from .resume_extractor import extract_resume_text_from_upload
from .interview_scorer import InterviewQAItem, score_interview
from .interview_zip import process_interview_zip
from .transcribe import ALLOWED_EXTENSIONS, save_upload_to_temp, transcribe_file

# Load environment variables
load_dotenv()

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


class TranscribeResponse(BaseModel):
    """Transcription Response Schema"""
    job_name: str
    status: str
    language_code: str
    media_format: str
    transcription: Optional[str] = None
    error: Optional[str] = None


class InterviewScoringRequest(BaseModel):
    """
    Request body for POST /ai/score-interview.

    You must send one Q&A object per interview question. Each video from /ai/transcribe
    maps to one qa_pairs entry: put the interviewer question in `question` and the
    candidate transcript in `answer`.
    """

    job_description: str = Field(
        ...,
        min_length=1,
        description="Deskripsi pekerjaan / persyaratan posisi yang dilamar kandidat.",
        examples=[
            "Bertanggung jawab atas rekrutmen, disiplin karyawan, administrasi SDM, "
            "dan pengembangan kebijakan HR."
        ],
    )
    job_title: Optional[str] = Field(
        None,
        description="Judul posisi (opsional, membantu penilaian job fit).",
        examples=["Staff HRD"],
    )
    target_skills: List[str] = Field(
        default_factory=list,
        description="Skill yang dibutuhkan untuk posisi ini (opsional).",
        examples=[["rekrutmen", "manajemen SDM", "administrasi"]],
    )
    resume_text: Optional[str] = Field(
        None,
        description="Teks CV kandidat (opsional). Jika diisi, skor consistency_score akan dihitung.",
    )
    qa_pairs: List[InterviewQAItem] = Field(
        ...,
        min_length=1,
        description=(
            "Daftar pertanyaan dan jawaban wawancara. Wajib. "
            "Satu item = satu pertanyaan. Urutkan dengan question_number mulai 1."
        ),
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "job_title": "Staff HRD",
                "job_description": (
                    "Bertanggung jawab atas rekrutmen, disiplin karyawan, "
                    "administrasi SDM, dan pengembangan kebijakan HR."
                ),
                "target_skills": ["rekrutmen", "manajemen SDM", "administrasi"],
                "resume_text": "Pengalaman 3 tahun sebagai HRD di perusahaan manufaktur...",
                "qa_pairs": [
                    {
                        "question_number": 1,
                        "question": (
                            "HALO PERKENALKAN SAYA FANDY. SAYA YANG AKAN MELAKUKAN WAWANCARA "
                            "DENGAN ANDA. APAKAH ANDA SUDAH SIAP? SILAKAN PERKENALKAN DIRI ANDA"
                        ),
                        "answer": "",
                    },
                    {
                        "question_number": 2,
                        "question": "Boleh diceritakan, apa kesibukan Anda saat ini?",
                        "answer": (
                            "Saat ini saya ee sedang menganggur, selama ee selama saya menganggur, "
                            "saya fokus memperbaiki CV saya dan memperluas network jaringan ter- "
                            "terutama di- di media sosial seperti LinkedIn. Saya juga sempat "
                            "beberapa kali melakukan tes interview yang membantu saya untuk "
                            "memahami berbagai macam jenis industri. uh dan juga saya uh di rumah, "
                            "uh juga jualan-jualan uh bahan baking ataupun baking."
                        ),
                    },
                    {
                        "question_number": 3,
                        "question": (
                            "Pada pekerjaan yang Anda jalani saat ini tentunya pernah terjadi "
                            "kendala. Kendala apa ya pernah Anda hadapi dan bagaimana cara Anda "
                            "menyelesaikan kendala tersebut?"
                        ),
                        "answer": (
                            "ee kendala yang saya hadapi, terutama berhubungan selama saya bekerja "
                            "sebagai HRD yaitu ee proses uh kehadiran karyawan, terus kedisiplinan "
                            "karyawan. serta perekrutan karyawan. ee Untuk me- menghadapi kendala "
                            "tersebut seperti proses perekrutan karyawan, saya melakukan ee iklan "
                            "lowongan ee iklan lowongan kerja, melakukan ee. walk in interview yang "
                            "bekerjasama baik dengan swasta maupun pemerintah, terus me melakukan "
                            "proses rekrutmen, terus uh untuk me- seperti kedisiplinan karyawan, "
                            "ya, tidak seperti integritas karyawan, terus mengelola beberagam macam "
                            "uh Semua latar belakang. uh yang berhubungan dengan ee karyawan"
                        ),
                    },
                    {
                        "question_number": 4,
                        "question": "Bagaimana cara Anda menghadapi tekanan dalam pekerjaan Anda saat ini?",
                        "answer": (
                            "ee Pertama saya melakukan keseimbangan waktu. antara pekerjaan, "
                            "aktivitas sosial dan untuk diri sendiri, terus menjaga kesehatan dengan "
                            "berolahraga, istirahat yang cukup, terus melakukan uh relaksasi, terus "
                            "uh melakukan pemahaman terhadap diri sendiri. Fokus terhadap pekerjaan, "
                            "hindari pikiran negatif, komunikasi dengan rekan kerja maupun atasan. "
                            "serta mencari dukungan dengan tim ee atasan, teman dan keluarga"
                        ),
                    },
                    {
                        "question_number": 5,
                        "question": (
                            "Keahlian apa saja yang Anda miliki dan kuasai dari pekerjaan saat ini?"
                        ),
                        "answer": (
                            "ee saya memiliki keahlian dalam bidang administrasi. Kenapa? Karena "
                            "administrasi sangat membantu karir saya di bidang HRD. untuk mempelajari "
                            "manajemen, SDM, meningkatkan keterampilan, cara berkomunikasi, dan cara "
                            "uh Kim kepemimpinan."
                        ),
                    },
                    {
                        "question_number": 6,
                        "question": "Apa yang memotivasi Anda untuk melamar di posisi ini?",
                        "answer": (
                            "Saya tertarik di dunia HRD karena uh dunia HRD ini mencakup uh bidang "
                            "manajemen, uh kep- kepemimpinan, uh SDM. Serta ee karakteristik juga ee "
                            "untuk perusahaan."
                        ),
                    },
                    {
                        "question_number": 7,
                        "question": "Apa harapan Anda dalam 3 tahun ke depan?",
                        "answer": "Apa ya.",
                    },
                ],
            }
        }
    )


class InterviewScoringResponse(BaseModel):
    """Interview Scoring Response Schema"""
    success: bool
    data: Optional[dict] = None
    error: Optional[str] = None
    message: str


class ProcessInterviewZipItem(BaseModel):
    question_number: int
    question: str
    answer: str


class ProcessInterviewZipResponse(BaseModel):
    success: bool
    qa_pairs: Optional[List[ProcessInterviewZipItem]] = None
    error: Optional[str] = None
    message: str


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
    resume: UploadFile = File(
        ...,
        description="Resume file: PDF, TXT, or image (JPG, PNG, WEBP, etc.)",
    ),
    job_description: str = Form(..., description="Job description text"),
    target_skills: str = Form("[]", description="JSON string of target skills")
) -> ResumeScoringResponse:
    """
    Score a resume file against a job description and target skills.

    Supported uploads:
    - PDF (text-based)
    - TXT
    - Images: JPG, JPEG, PNG, WEBP, GIF, BMP (uses vision OCR for photos/scans)
    """
    try:
        if not resume.filename:
            raise HTTPException(status_code=400, detail="No file provided")
        
        # Parse target_skills from JSON string
        try:
            target_skills_list = json.loads(target_skills) if target_skills else []
        except json.JSONDecodeError:
            target_skills_list = []
        
        resume_text = await extract_resume_text_from_upload(resume)
        
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
            "transcribe": "/ai/transcribe",
            "process_interview_zip": "/ai/process-interview-zip",
            "score_interview": "/ai/score-interview",
            "docs": "/docs"
        }
    }


@router.post("/score-interview", response_model=InterviewScoringResponse)
async def score_interview_endpoint(
    request: InterviewScoringRequest,
) -> InterviewScoringResponse:
    """
    Score a job interview from structured Q&A pairs.

    **Required:** `job_description` and `qa_pairs` (min 1 item).

    **How to build qa_pairs:**
    - Upload the interview zip to `POST /ai/process-interview-zip`, or
    - Transcribe each video manually via `POST /ai/transcribe` and assemble pairs yourself.

    **Optional:** `resume_text` enables CV consistency scoring.
    """
    try:
        result = await score_interview(
            qa_pairs=request.qa_pairs,
            job_description=request.job_description,
            job_title=request.job_title,
            resume_text=request.resume_text,
            target_skills=request.target_skills,
        )
        return InterviewScoringResponse(
            success=True,
            data=result,
            message="Wawancara berhasil dinilai",
        )
    except Exception as e:
        return InterviewScoringResponse(
            success=False,
            error=str(e),
            message="Gagal menilai wawancara",
        )


@router.post("/process-interview-zip", response_model=ProcessInterviewZipResponse)
async def process_interview_zip_endpoint(
    file: UploadFile = File(
        ...,
        description=(
            "Interview zip containing questions_list.txt and "
            "Question{N}_*.mp4 video files"
        ),
    ),
    language_code: str = Query(
        "id-ID",
        description="Amazon Transcribe language code (default: id-ID for Indonesian)",
    ),
) -> ProcessInterviewZipResponse:
    """
    Process an interview zip into `qa_pairs` ready for `/ai/score-interview`.

    **Expected zip contents:**
    - `questions_list.txt` — numbered questions (1. ..., 2. ..., etc.)
    - `Question1_*.mp4`, `Question2_*.mp4`, ... — one video per question

    Videos are transcribed and matched to questions by number.

    Returns only `qa_pairs`: question_number, question, answer — ready to paste into `/ai/score-interview`.
    """
    if not file.filename or not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Upload a .zip file")

    zip_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as temp_zip:
            shutil.copyfileobj(file.file, temp_zip)
            zip_path = temp_zip.name

        qa_pairs = await process_interview_zip(
            zip_path=zip_path,
            language_code=language_code,
        )
        return ProcessInterviewZipResponse(
            success=True,
            qa_pairs=qa_pairs,
            message="Zip wawancara berhasil diproses",
        )
    except HTTPException:
        raise
    except Exception as e:
        return ProcessInterviewZipResponse(
            success=False,
            error=str(e),
            message="Gagal memproses zip wawancara",
        )
    finally:
        if zip_path and os.path.exists(zip_path):
            os.unlink(zip_path)


@router.post("/transcribe", response_model=TranscribeResponse)
async def transcribe_audio(
    file: UploadFile = File(..., description="Audio file to transcribe (MP3, MP4, etc.)"),
    language_code: str = Query("en-US", description="Language code for transcription (default: en-US)"),
) -> TranscribeResponse:
    """
    Upload an audio/video file and get transcription using Amazon Transcribe.

    The file is uploaded to S3 (required by Amazon Transcribe), a transcription job
    is started, and the endpoint waits for completion before returning the result.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    file_ext = file.filename.split(".")[-1].lower() if "." in file.filename else ""
    if file_ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type. Allowed: {', '.join(ALLOWED_EXTENSIONS)}",
        )

    temp_file_path = None
    try:
        temp_file_path = save_upload_to_temp(file.file, file_ext)
        result = await asyncio.to_thread(
            transcribe_file, temp_file_path, file.filename, language_code
        )
        return TranscribeResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


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