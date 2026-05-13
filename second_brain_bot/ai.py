from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

import aiohttp

from helpers.analytics_events import AnalyticsSink
from second_brain_bot.models import SearchResult

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProviderResult:
    provider: str
    model: str
    payload: dict[str, Any]
    text: str = ""


@dataclass(frozen=True)
class AIEnrichment:
    title: str = ""
    summary: str = ""
    suggested_folder: str = "00_Inbox"
    suggested_tags: list[str] = field(default_factory=list)
    entities: list[str] = field(default_factory=list)
    action_items: list[str] = field(default_factory=list)
    questions: list[str] = field(default_factory=list)
    provider: str = "local_fallback"
    verification_pending: bool = False


@dataclass(frozen=True)
class RelatedNoteSuggestion:
    note_id: str
    title: str
    reason: str
    confidence: float


class ModelProvider(Protocol):
    name: str

    async def complete_json(self, *, task: str, prompt: str, max_tokens: int = 800) -> ProviderResult:
        ...


class OpenAICompatibleProvider:
    def __init__(
        self,
        *,
        name: str,
        api_key: str,
        base_url: str,
        model: str,
        timeout_sec: float = 45.0,
        analytics_sink: AnalyticsSink | None = None,
    ) -> None:
        self.name = name
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_sec = timeout_sec
        self.analytics_sink = analytics_sink or AnalyticsSink()

    async def complete_json(self, *, task: str, prompt: str, max_tokens: int = 800) -> ProviderResult:
        started = time.monotonic()
        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout_sec)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": self.model,
                        "messages": [
                            {
                                "role": "system",
                                "content": "Return only valid JSON. Do not wrap JSON in markdown.",
                            },
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.2,
                        "max_tokens": max_tokens,
                    },
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
            content = str(data["choices"][0]["message"]["content"])
            payload = _parse_json_object(content)
            self._record("success", task, started)
            return ProviderResult(provider=self.name, model=self.model, payload=payload, text=content)
        except Exception as exc:
            self._record("failed", task, started, error=str(exc)[:160])
            raise

    def _record(self, event: str, task: str, started: float, *, error: str = "") -> None:
        try:
            self.analytics_sink.append_event(
                "second_brain_ai",
                {
                    "event": event,
                    "task": task,
                    "provider": self.name,
                    "model": self.model,
                    "latency_ms": int((time.monotonic() - started) * 1000),
                    "error": error,
                },
            )
            self.analytics_sink.add_daily_counters(
                "second_brain_ai",
                dimensions={"event": event, "provider": self.name, "task": task},
                counters={"calls": 1, "failures": 1 if event == "failed" else 0},
            )
        except Exception:
            return


class AIOrchestrator:
    def __init__(self, *, providers: dict[str, ModelProvider] | None = None, analytics_sink: AnalyticsSink | None = None) -> None:
        self.providers = providers or {}
        self.analytics_sink = analytics_sink or AnalyticsSink()
        self._provider_lock = asyncio.Lock()

    async def enrich_capture(
        self,
        text: str,
        *,
        image_bytes: bytes | None = None,
        preferred_provider: str | None = None,
        allow_web: bool = False,
    ) -> AIEnrichment:
        del image_bytes  # Hosted v1 enrichment intentionally receives text/caption only.
        prompt = _enrichment_prompt(text, allow_web=allow_web)
        for name in self._route(preferred_provider=preferred_provider, task="enrich"):
            provider = self.providers.get(name)
            if provider is None:
                continue
            try:
                async with self._provider_lock:
                    result = await provider.complete_json(task="enrich", prompt=prompt, max_tokens=900)
                return _enrichment_from_payload(result.payload, provider=result.provider, fallback_text=text)
            except Exception:
                LOGGER.warning("Second Brain AI provider failed for enrichment: %s", name)
                continue
        return local_enrichment(text)

    async def suggest_relations(self, note_text: str, candidates: list[SearchResult]) -> list[RelatedNoteSuggestion]:
        if not candidates:
            return []
        prompt = _relations_prompt(note_text, candidates)
        for name in self._route(preferred_provider="modal_glm", task="relations"):
            provider = self.providers.get(name)
            if provider is None:
                continue
            try:
                async with self._provider_lock:
                    result = await provider.complete_json(task="relations", prompt=prompt, max_tokens=800)
                return _relations_from_payload(result.payload, candidates)
            except Exception:
                LOGGER.warning("Second Brain AI provider failed for relation judging: %s", name)
                continue
        return local_relation_suggestions(note_text, candidates)

    async def ask(self, question: str, *, context: str, heavy: bool = True) -> ProviderResult:
        prompt = (
            "Answer the user's question using the collected Second Brain context first. "
            "Cite note titles or paths when possible. If the context does not support an answer, say so.\n\n"
            f"Question:\n{question}\n\nContext:\n{context[:12000]}"
        )
        preferred = "modal_glm" if heavy else "cerebras"
        for name in self._route(preferred_provider=preferred, task="ask"):
            provider = self.providers.get(name)
            if provider is None:
                continue
            try:
                async with self._provider_lock:
                    result = await provider.complete_json(task="ask", prompt=prompt, max_tokens=1200)
                text = str(result.payload.get("answer") or result.payload.get("text") or result.text)
                return ProviderResult(provider=result.provider, model=result.model, payload=result.payload, text=text)
            except Exception:
                LOGGER.warning("Second Brain AI provider failed for ask: %s", name)
                continue
        return ProviderResult(provider="local_fallback", model="local", payload={}, text=_local_answer(question, context))

    def _route(self, *, preferred_provider: str | None, task: str) -> list[str]:
        order: list[str] = []
        if preferred_provider:
            order.append(preferred_provider)
        elif task == "enrich":
            order.extend(["cerebras", "groq", "modal_glm"])
        elif task in {"ask", "relations"}:
            order.extend(["modal_glm", "cerebras", "groq"])
        order.extend(["modal_glm", "cerebras", "groq"])
        seen: set[str] = set()
        return [name for name in order if not (name in seen or seen.add(name))]


def local_enrichment(text: str) -> AIEnrichment:
    title = _first_line(text)
    keywords = _keywords(text)
    return AIEnrichment(
        title=title,
        summary=text.strip()[:280],
        suggested_folder="00_Inbox",
        suggested_tags=["inbox", *keywords[:4]],
        entities=keywords[:8],
        action_items=[],
        questions=[],
        provider="local_fallback",
    )


def local_relation_suggestions(note_text: str, candidates: list[SearchResult]) -> list[RelatedNoteSuggestion]:
    note_words = set(_keywords(note_text))
    suggestions: list[RelatedNoteSuggestion] = []
    for candidate in candidates:
        candidate_words = set(candidate.entities + candidate.tags + _keywords(candidate.body))
        overlap = note_words & candidate_words
        if not overlap:
            continue
        confidence = min(0.85, 0.45 + len(overlap) * 0.1)
        suggestions.append(
            RelatedNoteSuggestion(
                note_id=candidate.note_id,
                title=candidate.title,
                reason="Shared terms: " + ", ".join(sorted(overlap)[:5]),
                confidence=confidence,
            )
        )
    return suggestions[:5]


def _enrichment_prompt(text: str, *, allow_web: bool) -> str:
    return (
        "Enrich this personal Second Brain capture. Use model knowledge only unless web facts are already supplied. "
        "Return JSON with keys: title, summary, suggested_folder, suggested_tags, entities, action_items, questions. "
        "suggested_folder must be one of 00_Inbox, 01_Projects, 02_Areas, 03_Resources, 99_Archive. "
        f"Public web lookup allowed by policy: {allow_web}.\n\nCapture:\n{text[:8000]}"
    )


def _relations_prompt(note_text: str, candidates: list[SearchResult]) -> str:
    packed = [
        {
            "note_id": item.note_id,
            "title": item.title,
            "path": item.path,
            "tags": item.tags,
            "entities": item.entities,
            "excerpt": item.body[:700],
        }
        for item in candidates[:10]
    ]
    return (
        "Find genuinely useful links between the new note and existing notes. "
        "Return JSON {\"related_notes\": [{\"note_id\": str, \"reason\": str, \"confidence\": 0..1}]}. "
        "Only include notes with confidence >= 0.55.\n\n"
        f"New note:\n{note_text[:4000]}\n\nCandidates:\n{json.dumps(packed, ensure_ascii=False)}"
    )


def _parse_json_object(content: str) -> dict[str, Any]:
    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```(?:json)?\s*", "", content)
        content = re.sub(r"\s*```$", "", content)
    start = content.find("{")
    end = content.rfind("}")
    if start >= 0 and end >= start:
        content = content[start : end + 1]
    payload = json.loads(content)
    if not isinstance(payload, dict):
        raise ValueError("AI response was not a JSON object")
    return payload


def _enrichment_from_payload(payload: dict[str, Any], *, provider: str, fallback_text: str) -> AIEnrichment:
    return AIEnrichment(
        title=str(payload.get("title") or _first_line(fallback_text)),
        summary=str(payload.get("summary") or fallback_text[:280]),
        suggested_folder=str(payload.get("suggested_folder") or "00_Inbox"),
        suggested_tags=_string_list(payload.get("suggested_tags")),
        entities=_string_list(payload.get("entities")),
        action_items=_string_list(payload.get("action_items")),
        questions=_string_list(payload.get("questions")),
        provider=provider,
        verification_pending=bool(payload.get("verification_pending", False)),
    )


def _relations_from_payload(payload: dict[str, Any], candidates: list[SearchResult]) -> list[RelatedNoteSuggestion]:
    by_id = {item.note_id: item for item in candidates}
    rows = payload.get("related_notes") or []
    if not isinstance(rows, list):
        return []
    result: list[RelatedNoteSuggestion] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        note_id = str(row.get("note_id") or "")
        candidate = by_id.get(note_id)
        if candidate is None:
            continue
        try:
            confidence = float(row.get("confidence", 0))
        except (TypeError, ValueError):
            confidence = 0.0
        if confidence < 0.55:
            continue
        result.append(
            RelatedNoteSuggestion(
                note_id=note_id,
                title=candidate.title,
                reason=str(row.get("reason") or "Related note"),
                confidence=min(1.0, max(0.0, confidence)),
            )
        )
    return result[:8]


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()][:20]


def _first_line(text: str) -> str:
    stripped = (text or "").strip()
    if not stripped:
        return "Capture"
    return stripped.splitlines()[0][:80]


def _keywords(text: str) -> list[str]:
    words = re.findall(r"[\w'-]{3,}", (text or "").lower(), flags=re.UNICODE)
    stop = {"the", "and", "for", "this", "that", "with", "from", "about", "later", "need", "have"}
    result: list[str] = []
    for word in words:
        if word in stop or word in result:
            continue
        result.append(word)
    return result[:20]


def _local_answer(question: str, context: str) -> str:
    if not context.strip():
        return "I could not find collected notes that answer this yet."
    return f"Based on local notes, relevant context exists for: {question}\n\n{context[:1200]}"
