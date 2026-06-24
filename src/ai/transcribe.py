import json
import os
import shutil
import tempfile
import time
import urllib.request
import uuid

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from fastapi import HTTPException

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "")

transcribe_client = boto3.client("transcribe", region_name=AWS_REGION)
s3_client = boto3.client("s3", region_name=AWS_REGION)

ALLOWED_EXTENSIONS = ["mp3", "mp4", "wav", "flac", "ogg", "amr", "webm", "m4a"]


def get_media_format(filename: str) -> str:
    ext = filename.lower().split(".")[-1]
    format_map = {
        "mp3": "mp3",
        "mp4": "mp4",
        "wav": "wav",
        "flac": "flac",
        "ogg": "ogg",
        "amr": "amr",
        "webm": "webm",
        "m4a": "mp4",
    }
    return format_map.get(ext, "mp3")


def upload_to_s3(file_path: str, s3_key: str) -> str:
    try:
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        return f"s3://{S3_BUCKET}/{s3_key}"
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {str(e)}")


def start_transcription_job(
    job_name: str, media_uri: str, media_format: str, language_code: str = "en-US"
) -> dict:
    try:
        return transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={"MediaFileUri": media_uri},
            MediaFormat=media_format,
            LanguageCode=language_code,
        )
    except ClientError as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to start transcription job: {str(e)}"
        )


def wait_for_job_completion(job_name: str, timeout: int = 300) -> dict:
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            raise HTTPException(status_code=408, detail="Transcription job timed out")

        try:
            response = transcribe_client.get_transcription_job(
                TranscriptionJobName=job_name
            )
            status = response["TranscriptionJob"]["TranscriptionJobStatus"]

            if status == "COMPLETED":
                return response
            if status == "FAILED":
                failure_reason = response["TranscriptionJob"].get(
                    "FailureReason", "Unknown error"
                )
                raise HTTPException(
                    status_code=500, detail=f"Transcription failed: {failure_reason}"
                )

            time.sleep(2)
        except ClientError as e:
            raise HTTPException(
                status_code=500, detail=f"Error checking job status: {str(e)}"
            )


def get_transcription_result(transcript_uri: str) -> str:
    try:
        with urllib.request.urlopen(transcript_uri) as response:
            transcript_data = json.loads(response.read().decode())
            return transcript_data["results"]["transcripts"][0]["transcript"]
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve transcription: {str(e)}"
        )


def transcribe_file(file_path: str, filename: str, language_code: str = "en-US") -> dict:
    if not S3_BUCKET:
        raise HTTPException(
            status_code=500,
            detail=(
                "S3_BUCKET environment variable must be set. "
                "Amazon Transcribe requires files to be stored in S3."
            ),
        )

    file_ext = filename.split(".")[-1].lower() if "." in filename else ""
    if file_ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type. Allowed: {', '.join(ALLOWED_EXTENSIONS)}",
        )

    job_name = f"transcribe-{uuid.uuid4().hex[:12]}"
    media_format = get_media_format(filename)
    s3_key = f"transcriptions/{job_name}.{file_ext}"

    media_uri = upload_to_s3(file_path, s3_key)
    start_transcription_job(job_name, media_uri, media_format, language_code)
    job_response = wait_for_job_completion(job_name)

    transcript_uri = job_response["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
    transcription_text = get_transcription_result(transcript_uri)

    return {
        "job_name": job_name,
        "status": "completed",
        "transcription": transcription_text,
        "language_code": language_code,
        "media_format": media_format,
    }


def save_upload_to_temp(upload_file, file_ext: str) -> str:
    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_ext}") as temp_file:
        shutil.copyfileobj(upload_file, temp_file)
        return temp_file.name
