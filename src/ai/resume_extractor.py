import base64
import os
import tempfile
from typing import Optional

from dotenv import load_dotenv
from fastapi import HTTPException, UploadFile
from openai import AsyncOpenAI
from PyPDF2 import PdfReader

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MODEL = os.getenv("MODEL_CHOICE", "gpt-4o")

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}
TEXT_EXTENSIONS = {".txt"}
PDF_EXTENSION = ".pdf"
SUPPORTED_RESUME_EXTENSIONS = IMAGE_EXTENSIONS | TEXT_EXTENSIONS | {PDF_EXTENSION}

MIME_BY_EXTENSION = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".bmp": "image/bmp",
}

RESUME_OCR_PROMPT = (
    "Extract all text from this resume/CV image. "
    "Return only the extracted text with no commentary. "
    "Preserve sections (experience, education, skills, contact info) where possible. "
    "If the document is in Indonesian, keep the original language."
)


def _extension(filename: Optional[str]) -> str:
    if not filename or "." not in filename:
        return ""
    return os.path.splitext(filename)[1].lower()


def _media_type(file_extension: str, content_type: Optional[str]) -> str:
    if content_type and content_type.startswith("image/"):
        return content_type
    return MIME_BY_EXTENSION.get(file_extension, "image/jpeg")


def _extract_text_from_pdf(file_path: str) -> str:
    reader = PdfReader(file_path)
    text = ""
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            text += page_text
    return text.strip()


async def _extract_text_from_image(file_path: str, media_type: str) -> str:
    with open(file_path, "rb") as image_file:
        encoded = base64.b64encode(image_file.read()).decode("utf-8")

    response = await client.chat.completions.create(
        model=MODEL,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": RESUME_OCR_PROMPT},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{media_type};base64,{encoded}"},
                    },
                ],
            }
        ],
        max_tokens=4096,
    )

    text = (response.choices[0].message.content or "").strip()
    if not text:
        raise HTTPException(
            status_code=400,
            detail="Could not extract text from resume image",
        )
    return text


async def extract_resume_text_from_upload(upload_file: UploadFile) -> str:
    """
    Extract resume text from PDF, TXT, or image uploads (JPEG, PNG, WEBP, etc.).

    Images and scanned resumes use OpenAI vision OCR.
    """
    file_extension = _extension(upload_file.filename)
    if file_extension not in SUPPORTED_RESUME_EXTENSIONS:
        supported = ", ".join(sorted(SUPPORTED_RESUME_EXTENSIONS))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type. Allowed: {supported}",
        )

    content = await upload_file.read()
    with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
        temp_file.write(content)
        temp_path = temp_file.name

    try:
        if file_extension == PDF_EXTENSION:
            text = _extract_text_from_pdf(temp_path)
            if not text:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Could not extract text from PDF. "
                        "If this is a scanned resume, upload a photo (JPG/PNG) instead."
                    ),
                )
            return text

        if file_extension in TEXT_EXTENSIONS:
            with open(temp_path, "r", encoding="utf-8") as text_file:
                text = text_file.read().strip()
            if not text:
                raise HTTPException(status_code=400, detail="TXT file is empty")
            return text

        media_type = _media_type(file_extension, upload_file.content_type)
        return await _extract_text_from_image(temp_path, media_type)
    finally:
        os.unlink(temp_path)
