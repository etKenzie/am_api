from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
import boto3
import os
import uuid
import time
from botocore.exceptions import ClientError
import tempfile
import shutil
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = FastAPI(title="Amazon Transcribe API", version="1.0.0")

# Initialize AWS clients
transcribe_client = boto3.client('transcribe', region_name=os.getenv('AWS_REGION', 'us-east-1'))
s3_client = boto3.client('s3', region_name=os.getenv('AWS_REGION', 'us-east-1'))

# Configuration
S3_BUCKET = os.getenv('S3_BUCKET', '')


def upload_to_s3(file_path: str, s3_key: str) -> str:
    """Upload file to S3 and return the S3 URI"""
    try:
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        return f"s3://{S3_BUCKET}/{s3_key}"
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {str(e)}")


def start_transcription_job(job_name: str, media_uri: str, media_format: str, language_code: str = 'en-US') -> dict:
    """Start an Amazon Transcribe job"""
    try:
        response = transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': media_uri} if media_uri.startswith('s3://') else {'MediaFileUri': media_uri},
            MediaFormat=media_format,
            LanguageCode=language_code
        )
        return response
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Failed to start transcription job: {str(e)}")


def wait_for_job_completion(job_name: str, timeout: int = 300) -> dict:
    """Wait for transcription job to complete"""
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            raise HTTPException(status_code=408, detail="Transcription job timed out")
        
        try:
            response = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
            status = response['TranscriptionJob']['TranscriptionJobStatus']
            
            if status == 'COMPLETED':
                return response
            elif status == 'FAILED':
                failure_reason = response['TranscriptionJob'].get('FailureReason', 'Unknown error')
                raise HTTPException(status_code=500, detail=f"Transcription failed: {failure_reason}")
            
            time.sleep(2)  # Wait 2 seconds before checking again
        except ClientError as e:
            raise HTTPException(status_code=500, detail=f"Error checking job status: {str(e)}")


def get_transcription_result(transcript_uri: str) -> str:
    """Download and parse transcription result"""
    import urllib.request
    import json
    
    try:
        with urllib.request.urlopen(transcript_uri) as response:
            transcript_data = json.loads(response.read().decode())
            return transcript_data['results']['transcripts'][0]['transcript']
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve transcription: {str(e)}")


def get_media_format(filename: str) -> str:
    """Determine media format from file extension"""
    ext = filename.lower().split('.')[-1]
    format_map = {
        'mp3': 'mp3',
        'mp4': 'mp4',
        'wav': 'wav',
        'flac': 'flac',
        'ogg': 'ogg',
        'amr': 'amr',
        'webm': 'webm',
        'm4a': 'mp4'
    }
    return format_map.get(ext, 'mp3')


@app.get("/")
async def root():
    return {"message": "Amazon Transcribe API", "status": "running"}


@app.post("/transcribe")
async def transcribe_audio(
    file: UploadFile = File(...),
    language_code: str = 'en-US'
):
    """
    Upload an audio/video file and get transcription using Amazon Transcribe.
    
    The file will be automatically uploaded to S3 (required by Amazon Transcribe).
    
    - **file**: Audio/video file (MP3, MP4, WAV, FLAC, etc.)
    - **language_code**: Language code (default: en-US)
    """
    # Validate file type
    allowed_extensions = ['mp3', 'mp4', 'wav', 'flac', 'ogg', 'amr', 'webm', 'm4a']
    file_ext = file.filename.split('.')[-1].lower() if '.' in file.filename else ''
    
    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type. Allowed: {', '.join(allowed_extensions)}"
        )
    
    # Amazon Transcribe requires files to be in S3, so S3_BUCKET is mandatory
    if not S3_BUCKET:
        raise HTTPException(
            status_code=500,
            detail="S3_BUCKET environment variable must be set. Amazon Transcribe requires files to be stored in S3."
        )
    
    # Generate unique job name
    job_name = f"transcribe-{uuid.uuid4().hex[:12]}"
    media_format = get_media_format(file.filename)
    
    temp_file_path = None
    try:
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_ext}") as temp_file:
            shutil.copyfileobj(file.file, temp_file)
            temp_file_path = temp_file.name
        
        # Upload to S3 (required by Amazon Transcribe)
        s3_key = f"transcriptions/{job_name}.{file_ext}"
        media_uri = upload_to_s3(temp_file_path, s3_key)
        
        # Start transcription job
        start_transcription_job(job_name, media_uri, media_format, language_code)
        
        # Wait for completion
        job_response = wait_for_job_completion(job_name)
        
        # Get transcription result
        transcript_uri = job_response['TranscriptionJob']['Transcript']['TranscriptFileUri']
        transcription_text = get_transcription_result(transcript_uri)
        
        # Clean up S3 file if needed (optional)
        # s3_client.delete_object(Bucket=S3_BUCKET, Key=s3_key)
        
        return JSONResponse(content={
            "job_name": job_name,
            "status": "completed",
            "transcription": transcription_text,
            "language_code": language_code,
            "media_format": media_format
        })
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    
    finally:
        # Clean up temporary file
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

