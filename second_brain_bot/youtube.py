from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

import aiohttp


@dataclass(frozen=True)
class YouTubeTranscript:
    video_id: str
    url: str
    title: str
    language: str
    text: str


class YouTubeTranscriptUnavailable(RuntimeError):
    pass


YOUTUBE_HOSTS = {"youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be"}


def extract_youtube_video_id(value: str) -> str:
    parsed = urlparse(str(value or "").strip())
    host = parsed.netloc.lower()
    if host not in YOUTUBE_HOSTS:
        return ""
    if host == "youtu.be":
        candidate = parsed.path.strip("/").split("/", 1)[0]
        return candidate if _valid_video_id(candidate) else ""
    if parsed.path == "/watch":
        candidate = parse_qs(parsed.query).get("v", [""])[0]
        return candidate if _valid_video_id(candidate) else ""
    for prefix in ("/shorts/", "/embed/"):
        if parsed.path.startswith(prefix):
            candidate = parsed.path[len(prefix) :].split("/", 1)[0]
            return candidate if _valid_video_id(candidate) else ""
    return ""


def is_youtube_url(value: str) -> bool:
    return bool(extract_youtube_video_id(value))


def first_youtube_url(text: str) -> str:
    for raw_url in re.findall(r"https?://[^\s)>\]]+", text or ""):
        url = raw_url.rstrip(".,;!?")
        if is_youtube_url(url):
            return url
    return ""


def transcript_segments_to_text(segments: Any) -> str:
    lines: list[str] = []
    for segment in segments or []:
        if isinstance(segment, dict):
            text = str(segment.get("text") or "").strip()
        else:
            text = str(getattr(segment, "text", "") or "").strip()
        if text:
            lines.append(text)
    return "\n".join(lines)


def clean_transcript_text(text: str) -> str:
    paragraphs: list[list[str]] = [[]]
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line:
            _ensure_new_paragraph(paragraphs)
            continue
        if _is_time_range_line(line):
            _ensure_new_paragraph(paragraphs)
            continue
        line = _strip_timecode_prefix(line)
        line = re.sub(r"<[^>]+>", "", line)
        line = re.sub(r"\s+", " ", line).strip()
        if not line:
            continue
        paragraphs[-1].append(line)
    cleaned = "\n\n".join(" ".join(parts).strip() for parts in paragraphs if parts).strip()
    return re.sub(r"\n{3,}", "\n\n", cleaned)


class PublicYouTubeTranscriptFetcher:
    async def fetch(self, url: str) -> YouTubeTranscript:
        video_id = extract_youtube_video_id(url)
        if not video_id:
            raise YouTubeTranscriptUnavailable("Not a supported YouTube URL")
        try:
            # This path intentionally reads public caption tracks only. It does
            # not use OAuth, proxies, CAPTCHA solving, or video/audio downloads.
            transcript, title = await asyncio.gather(
                asyncio.to_thread(_fetch_transcript_sync, video_id),
                _fetch_oembed_title(url),
            )
        except Exception as exc:
            raise YouTubeTranscriptUnavailable(str(exc)[:300]) from exc
        text = clean_transcript_text(transcript_segments_to_text(_transcript_segments(transcript)))
        if not text:
            raise YouTubeTranscriptUnavailable("Transcript was empty")
        return YouTubeTranscript(
            video_id=video_id,
            url=url,
            title=title or f"YouTube Video {video_id}",
            language=str(getattr(transcript, "language_code", "") or ""),
            text=text,
        )


def _fetch_transcript_sync(video_id: str):
    from youtube_transcript_api import YouTubeTranscriptApi

    api = YouTubeTranscriptApi()
    try:
        return api.fetch(video_id, languages=["en", "uk", "ru"])
    except Exception:
        return api.fetch(video_id)


async def _fetch_oembed_title(url: str) -> str:
    endpoint = "https://www.youtube.com/oembed"
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(endpoint, params={"url": url, "format": "json"}) as response:
                if response.status >= 400:
                    return ""
                payload = await response.json()
                return str(payload.get("title") or "").strip()
    except Exception:
        return ""


def _transcript_segments(transcript: Any) -> Any:
    return getattr(transcript, "snippets", transcript)


def _valid_video_id(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_-]{11}", value or ""))


def _ensure_new_paragraph(paragraphs: list[list[str]]) -> None:
    if paragraphs and paragraphs[-1]:
        paragraphs.append([])


def _is_time_range_line(line: str) -> bool:
    return bool(re.fullmatch(r"\[?\d{1,2}:\d{2}(?::\d{2})?(?:[.,]\d+)?\]?\s*-->\s*\[?\d{1,2}:\d{2}(?::\d{2})?(?:[.,]\d+)?\]?", line))


def _strip_timecode_prefix(line: str) -> str:
    return re.sub(r"^\[?\d{1,2}:\d{2}(?::\d{2})?(?:[.,]\d+)?\]?\s+", "", line).strip()
