import os
import tempfile
import urllib.parse
import urllib.request
from typing import Tuple

from fastapi import HTTPException

MAX_DOWNLOAD_BYTES = 500 * 1024 * 1024  # 500 MB


def normalize_download_url(url: str) -> str:
    """Encode spaces and special characters in URL paths (e.g. unencoded filenames)."""
    url = url.strip()
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise HTTPException(status_code=400, detail="URL must start with http:// or https://")

    path = urllib.parse.quote(urllib.parse.unquote(parts.path), safe="/")
    query = (
        urllib.parse.quote(urllib.parse.unquote(parts.query), safe="=&?/:+-")
        if parts.query
        else parts.query
    )
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))


def filename_from_url(url: str) -> str:
    normalized = normalize_download_url(url)
    path = urllib.parse.urlparse(normalized).path
    name = os.path.basename(urllib.parse.unquote(path))
    if not name:
        raise HTTPException(status_code=400, detail="Could not determine filename from URL")
    return name


def download_url_to_temp(url: str) -> Tuple[str, str]:
    """Download a URL to a temporary file. Returns (path, filename)."""
    normalized_url = normalize_download_url(url)
    filename = filename_from_url(normalized_url)
    ext = os.path.splitext(filename)[1]

    request = urllib.request.Request(normalized_url, headers={"User-Agent": "AkuMaju-API/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            data = response.read(MAX_DOWNLOAD_BYTES + 1)
            if len(data) > MAX_DOWNLOAD_BYTES:
                raise HTTPException(
                    status_code=400,
                    detail=f"File exceeds maximum download size ({MAX_DOWNLOAD_BYTES // (1024 * 1024)} MB)",
                )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to download URL: {exc}") from exc

    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as temp_file:
        temp_file.write(data)
        return temp_file.name, filename
