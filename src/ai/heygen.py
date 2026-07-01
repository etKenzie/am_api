"""HeyGen v3 API client for avatar video generation (Indonesian interview questions)."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

BASE_URL = "https://api.heygen.com"

DEFAULT_AVATAR_LOOK_ID = "74dd6e182f0d415ab740c1097d49304b"
DEFAULT_INDONESIAN_VOICE_ID = "e4878ce9b703461695bf793c0df0d2b1"
DEFAULT_AVATAR_NAME = "Maya"
DEFAULT_VOICE_NAME = "Gadis - Natural"

POLL_INITIAL_INTERVAL = float(os.getenv("HEYGEN_POLL_INITIAL_INTERVAL", "5"))
POLL_MAX_INTERVAL = float(os.getenv("HEYGEN_POLL_MAX_INTERVAL", "10"))
POLL_TIMEOUT = float(os.getenv("HEYGEN_POLL_TIMEOUT", "600"))
HTTP_MAX_RETRIES = int(os.getenv("HEYGEN_HTTP_MAX_RETRIES", "5"))
HTTP_TIMEOUT = float(os.getenv("HEYGEN_HTTP_TIMEOUT", "30"))


class HeyGenAPIError(Exception):
    """Raised when the HeyGen API returns a non-success response."""

    def __init__(self, message: str, status_code: int | None = None, payload: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class HeyGenClient:
    def __init__(self, api_key: str, base_url: str = BASE_URL):
        if not api_key or api_key.startswith("YOUR_"):
            raise ValueError("Set HEYGEN_API_KEY before using text-to-speech.")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        retry_count: int = 0,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        data = None
        headers = {
            "X-Api-Key": self.api_key,
            "Accept": "application/json",
        }

        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"

        request = urllib.request.Request(url, data=data, headers=headers, method=method)

        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            payload = self._read_error_payload(exc)
            if self._should_retry_http(exc.code, retry_count):
                wait = self._retry_wait_seconds(exc, retry_count)
                time.sleep(wait)
                return self._request(method, path, body=body, retry_count=retry_count + 1)

            message = self._format_api_error(exc.code, payload)
            raise HeyGenAPIError(message, status_code=exc.code, payload=payload) from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            if retry_count < HTTP_MAX_RETRIES:
                wait = min(POLL_MAX_INTERVAL, POLL_INITIAL_INTERVAL * (2**retry_count))
                time.sleep(wait)
                return self._request(method, path, body=body, retry_count=retry_count + 1)
            raise HeyGenAPIError(f"Network failure after retries: {exc}") from exc

    @staticmethod
    def _read_error_payload(exc: urllib.error.HTTPError) -> Any:
        try:
            raw = exc.read().decode("utf-8")
            return json.loads(raw) if raw else None
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    @staticmethod
    def _format_api_error(status_code: int, payload: Any) -> str:
        if isinstance(payload, dict):
            error = payload.get("error")
            if isinstance(error, dict):
                code = error.get("code", "unknown_error")
                message = error.get("message", "Request failed")
                return f"HTTP {status_code} [{code}]: {message}"
        return f"HTTP {status_code}: unexpected API error"

    @staticmethod
    def _should_retry_http(status_code: int, retry_count: int) -> bool:
        if retry_count >= HTTP_MAX_RETRIES:
            return False
        return status_code in (408, 409, 425, 429, 500, 502, 503, 504)

    @staticmethod
    def _retry_wait_seconds(exc: urllib.error.HTTPError, retry_count: int) -> float:
        retry_after = exc.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            return float(retry_after)
        return min(POLL_MAX_INTERVAL, POLL_INITIAL_INTERVAL * (2**retry_count))

    def _get_paginated(
        self,
        path: str,
        params: dict[str, str],
        *,
        limit: int = 50,
        max_pages: int = 1,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        query = dict(params)
        query.setdefault("limit", str(limit))
        token: str | None = None
        pages = 0

        while pages < max_pages:
            pages += 1
            if token:
                query["token"] = token
            elif "token" in query:
                del query["token"]

            qs = urllib.parse.urlencode(query)
            response = self._request("GET", f"{path}?{qs}")
            batch = response.get("data")
            if isinstance(batch, list):
                items.extend(item for item in batch if isinstance(item, dict))

            if not response.get("has_more"):
                break
            token = response.get("next_token")
            if not token:
                break

        return items

    def list_indonesian_voices(self) -> list[dict[str, Any]]:
        return self._get_paginated(
            "/v3/voices",
            {"type": "public", "language": "Indonesian"},
            limit=50,
            max_pages=2,
        )

    def list_public_avatars(self) -> list[dict[str, Any]]:
        return self._get_paginated(
            "/v3/avatars/looks",
            {"ownership": "public"},
            limit=50,
            max_pages=2,
        )

    @staticmethod
    def _score_indonesian_voice(voice: dict[str, Any]) -> int:
        score = 0
        name = str(voice.get("name", "")).lower()
        gender = str(voice.get("gender", "")).lower()

        if gender == "female":
            score += 20
        if voice.get("support_locale"):
            score += 5
        if voice.get("support_pause"):
            score += 2

        for preferred in ("ardita", "gadis", "sari", "putri", "dewi", "rina"):
            if preferred in name:
                score += 15
                break

        for deprioritized in ("child", "kid", "boy"):
            if deprioritized in name:
                score -= 20

        return score

    @staticmethod
    def _score_interview_avatar(look: dict[str, Any]) -> int:
        score = 0
        name = str(look.get("name", "")).lower()
        gender = str(look.get("gender", "")).lower()
        tags = [str(tag).lower() for tag in look.get("tags", []) if tag]
        engines = [str(engine).lower() for engine in look.get("supported_api_engines", []) if engine]
        status = str(look.get("status") or "completed").lower()

        if status == "completed":
            score += 10
        if gender in ("female", "woman"):
            score += 15
        if any("avatar" in engine for engine in engines):
            score += 8

        professional_keywords = ("business", "office", "professional", "formal", "suit", "interview")
        if any(keyword in name for keyword in professional_keywords):
            score += 12
        if any(any(keyword in tag for keyword in professional_keywords) for tag in tags):
            score += 8

        for preferred in ("monica", "ann", "annie", "judy", "amanda", "kate", "jennifer"):
            if preferred in name:
                score += 10
                break

        return score

    def resolve_indonesian_defaults(self) -> tuple[str, str, str, str]:
        voices = self.list_indonesian_voices()
        if not voices:
            raise HeyGenAPIError("No public Indonesian voices found. Set HEYGEN_VOICE_ID manually.")

        best_voice = max(voices, key=self._score_indonesian_voice)
        voice_id = str(best_voice["voice_id"])
        voice_name = str(best_voice.get("name", voice_id))

        looks = self.list_public_avatars()
        if not looks:
            raise HeyGenAPIError("No public avatars found. Set HEYGEN_AVATAR_ID manually.")

        studio_looks = [look for look in looks if look.get("avatar_type") == "studio_avatar"]
        candidates = studio_looks or looks
        best_look = max(candidates, key=self._score_interview_avatar)
        avatar_id = str(best_look["id"])
        avatar_name = str(best_look.get("name", avatar_id))

        return avatar_id, voice_id, avatar_name, voice_name

    def create_avatar_video(
        self,
        *,
        avatar_id: str,
        voice_id: str,
        script: str,
        aspect_ratio: str = "16:9",
        resolution: str = "1080p",
        title: str | None = None,
        engine_type: str | None = None,
        voice_locale: str | None = "id-ID",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": "avatar",
            "avatar_id": avatar_id,
            "voice_id": voice_id,
            "script": script,
            "aspect_ratio": aspect_ratio,
            "resolution": resolution,
        }

        if title:
            payload["title"] = title
        if engine_type:
            payload["engine"] = {"type": engine_type}
        if voice_locale:
            payload["voice_settings"] = {"locale": voice_locale}

        response = self._request("POST", "/v3/videos", body=payload)
        data = response.get("data")
        if not isinstance(data, dict) or not data.get("video_id"):
            raise HeyGenAPIError("Create video response missing data.video_id", payload=response)
        return data

    def get_video(self, video_id: str) -> dict[str, Any]:
        response = self._request("GET", f"/v3/videos/{video_id}")
        data = response.get("data")
        if not isinstance(data, dict):
            raise HeyGenAPIError("Get video response missing data object", payload=response)
        return data

    def poll_until_complete(self, video_id: str) -> dict[str, Any]:
        deadline = time.monotonic() + POLL_TIMEOUT
        interval = POLL_INITIAL_INTERVAL

        while time.monotonic() < deadline:
            video = self.get_video(video_id)
            status = video.get("status", "unknown")

            if status == "completed":
                return video
            if status == "failed":
                code = video.get("failure_code", "unknown")
                message = video.get("failure_message", "Video generation failed.")
                raise HeyGenAPIError(f"Video failed [{code}]: {message}", payload=video)

            time.sleep(interval)
            interval = min(POLL_MAX_INTERVAL, interval * 2)

        raise HeyGenAPIError(
            f"Timed out after {POLL_TIMEOUT:.0f}s waiting for video {video_id} to complete."
        )


def _needs_default_resolution(value: str | None) -> bool:
    return not value or value.startswith("YOUR_")


def _resolve_avatar_voice(
    client: HeyGenClient,
    *,
    avatar_id: str | None,
    voice_id: str | None,
    auto_resolve: bool,
) -> tuple[str, str, str, str]:
    resolved_avatar = avatar_id or os.getenv("HEYGEN_AVATAR_ID", DEFAULT_AVATAR_LOOK_ID)
    resolved_voice = voice_id or os.getenv("HEYGEN_VOICE_ID", DEFAULT_INDONESIAN_VOICE_ID)
    avatar_name = DEFAULT_AVATAR_NAME
    voice_name = DEFAULT_VOICE_NAME

    if auto_resolve or _needs_default_resolution(resolved_avatar) or _needs_default_resolution(resolved_voice):
        catalog_avatar, catalog_voice, catalog_avatar_name, catalog_voice_name = (
            client.resolve_indonesian_defaults()
        )
        if auto_resolve or _needs_default_resolution(resolved_avatar):
            resolved_avatar = catalog_avatar
            avatar_name = catalog_avatar_name
        if auto_resolve or _needs_default_resolution(resolved_voice):
            resolved_voice = catalog_voice
            voice_name = catalog_voice_name

    return resolved_avatar, resolved_voice, avatar_name, voice_name


def generate_avatar_video(
    *,
    script: str,
    title: str | None = None,
    avatar_id: str | None = None,
    voice_id: str | None = None,
    aspect_ratio: str = "16:9",
    resolution: str = "1080p",
    engine_type: str | None = None,
    voice_locale: str = "id-ID",
    auto_resolve_defaults: bool = False,
    wait_for_completion: bool = True,
) -> dict[str, Any]:
    """Submit a HeyGen avatar video job and optionally wait until it completes."""
    api_key = os.getenv("HEYGEN_API_KEY", "")
    client = HeyGenClient(api_key)

    resolved_avatar, resolved_voice, avatar_name, voice_name = _resolve_avatar_voice(
        client,
        avatar_id=avatar_id,
        voice_id=voice_id,
        auto_resolve=auto_resolve_defaults,
    )

    created = client.create_avatar_video(
        avatar_id=resolved_avatar,
        voice_id=resolved_voice,
        script=script,
        aspect_ratio=aspect_ratio,
        resolution=resolution,
        title=title,
        engine_type=engine_type or os.getenv("HEYGEN_ENGINE_TYPE") or None,
        voice_locale=voice_locale,
    )

    video_id = str(created["video_id"])
    status = str(created.get("status", "queued"))
    result: dict[str, Any] = {
        "video_id": video_id,
        "status": status,
        "avatar_id": resolved_avatar,
        "voice_id": resolved_voice,
        "avatar_name": avatar_name,
        "voice_name": voice_name,
    }

    if wait_for_completion:
        completed = client.poll_until_complete(video_id)
        result.update(
            {
                "status": completed.get("status", "completed"),
                "video_url": completed.get("video_url"),
                "thumbnail_url": completed.get("thumbnail_url"),
                "duration": completed.get("duration"),
            }
        )
    else:
        result["video_url"] = created.get("video_url")
        result["thumbnail_url"] = created.get("thumbnail_url")
        result["duration"] = created.get("duration")

    return result


def get_video_status(video_id: str) -> dict[str, Any]:
    """Fetch current HeyGen video status by ID."""
    api_key = os.getenv("HEYGEN_API_KEY", "")
    client = HeyGenClient(api_key)
    video = client.get_video(video_id)
    return {
        "video_id": video_id,
        "status": video.get("status"),
        "video_url": video.get("video_url"),
        "thumbnail_url": video.get("thumbnail_url"),
        "duration": video.get("duration"),
        "failure_code": video.get("failure_code"),
        "failure_message": video.get("failure_message"),
    }
