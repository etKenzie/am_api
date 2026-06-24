import asyncio
import os
import re
import shutil
import tempfile
import zipfile
from typing import Dict, List, Optional, Tuple

from fastapi import HTTPException

from .transcribe import ALLOWED_EXTENSIONS, transcribe_file

VIDEO_EXTENSIONS = tuple(f".{ext}" for ext in ALLOWED_EXTENSIONS)
QUESTIONS_FILENAME = "questions_list.txt"
QUESTION_VIDEO_PATTERN = re.compile(r"Question(\d+)_", re.IGNORECASE)


def parse_questions_list(text: str) -> Dict[int, str]:
    """Parse numbered questions from questions_list.txt."""
    text = text.strip()
    if not text:
        return {}

    parts = re.split(r"(?m)^(\d+)\.\s*", text)
    questions: Dict[int, str] = {}

    index = 1
    while index < len(parts) - 1:
        number = int(parts[index])
        content = parts[index + 1].strip()
        lines = [line.strip() for line in content.split("\n") if line.strip()]
        questions[number] = " ".join(lines)
        index += 2

    return questions


def question_number_from_video(filename: str) -> Optional[int]:
    match = QUESTION_VIDEO_PATTERN.search(os.path.basename(filename))
    return int(match.group(1)) if match else None


def discover_interview_files(extract_dir: str) -> Tuple[str, Dict[int, str], Dict[int, str]]:
    questions_path: Optional[str] = None
    videos: Dict[int, str] = {}

    for root, _, files in os.walk(extract_dir):
        for filename in files:
            full_path = os.path.join(root, filename)
            lower_name = filename.lower()

            if lower_name == QUESTIONS_FILENAME:
                questions_path = full_path
                continue

            if not lower_name.endswith(VIDEO_EXTENSIONS):
                continue

            question_number = question_number_from_video(filename)
            if question_number is not None:
                videos[question_number] = full_path

    if not questions_path:
        raise HTTPException(
            status_code=400,
            detail=f"Zip must contain {QUESTIONS_FILENAME}",
        )

    if not videos:
        raise HTTPException(
            status_code=400,
            detail="Zip must contain at least one video named Question{N}_*.mp4",
        )

    with open(questions_path, "r", encoding="utf-8") as questions_file:
        questions = parse_questions_list(questions_file.read())

    if not questions:
        raise HTTPException(
            status_code=400,
            detail=f"Could not parse any questions from {QUESTIONS_FILENAME}",
        )

    return questions_path, questions, videos


def _validate_zip(zip_path: str, extract_dir: str) -> None:
    if not zipfile.is_zipfile(zip_path):
        raise HTTPException(status_code=400, detail="Uploaded file is not a valid zip archive")

    with zipfile.ZipFile(zip_path, "r") as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            target = os.path.realpath(os.path.join(extract_dir, member.filename))
            if not target.startswith(os.path.realpath(extract_dir) + os.sep):
                raise HTTPException(status_code=400, detail="Zip archive contains unsafe paths")
        archive.extractall(extract_dir)


async def _transcribe_video(
    video_path: str,
    language_code: str,
) -> Tuple[str, Optional[str]]:
    filename = os.path.basename(video_path)
    try:
        result = await asyncio.to_thread(
            transcribe_file, video_path, filename, language_code
        )
        return result["transcription"], None
    except HTTPException as exc:
        return "", exc.detail
    except Exception as exc:
        return "", str(exc)


async def process_interview_zip(
    zip_path: str,
    language_code: str = "id-ID",
    max_concurrent_transcriptions: int = 3,
) -> List[dict]:
    extract_dir = tempfile.mkdtemp(prefix="interview_zip_")

    try:
        _validate_zip(zip_path, extract_dir)
        _, questions, videos = discover_interview_files(extract_dir)

        all_numbers = sorted(set(questions) | set(videos))
        semaphore = asyncio.Semaphore(max_concurrent_transcriptions)

        async def transcribe_for_number(number: int) -> dict:
            video_path = videos.get(number)
            question = questions.get(number, f"Pertanyaan {number}")

            if not video_path:
                return {
                    "question_number": number,
                    "question": question,
                    "answer": "",
                }

            async with semaphore:
                answer, _error = await _transcribe_video(video_path, language_code)

            return {
                "question_number": number,
                "question": question,
                "answer": answer,
            }

        qa_pairs = list(
            await asyncio.gather(*(transcribe_for_number(n) for n in all_numbers))
        )

        return qa_pairs
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)
