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

SECOND_BRAIN_SYSTEM_INSTRUCTIONS = """
You are Grotesk Brain: a private personal helper and second brain for one person.
Your job is to help the user capture, organize, distill, express, and retrieve their own knowledge.

Rules:
- Use the user's collected notes as the primary source of truth.
- If the notes do not support an answer, say that clearly instead of guessing.
- Do not reveal chain-of-thought, hidden reasoning, scratchpads, or <think> blocks.
- Keep answers concise, practical, and easy to scan in Telegram.
- Use short headings and bullets for lists.
- Use emojis rarely, only when they help identify the section before reading it.
- Prefer concrete next actions over abstract advice.
- Cite note titles or paths when useful.
- Separate facts from assumptions.
- Keep the user's visible note content in the user's language unless they ask otherwise.
- Never output raw JSON to the user unless they explicitly ask for JSON.
- Enrich captures only when the enrichment is useful and not noisy.
- Never invent missing specifics such as a scarf brand, product model, ticker, person, or source when the user did not provide enough evidence.
- Add compact high-confidence clarifications for recognizable tickers, acronyms, companies, products, methods, books, people, or concepts.
- When suggesting variants/options, attach a practical usefulness/confidence score from 1 to 100.
- For wellbeing or mental-performance captures, suggest evidence-informed methods without diagnosing the user or pretending to provide therapy.
""".strip()

TASK_SYSTEM_INSTRUCTIONS = {
    "ask": (
        SECOND_BRAIN_SYSTEM_INSTRUCTIONS
        + "\n\nTask: answering questions from the user's saved vault. Require evidence from notes, cite note titles or paths, "
        "separate assumptions from saved facts, and say clearly when the vault does not support an answer."
    ),
    "learn": (
        SECOND_BRAIN_SYSTEM_INSTRUCTIONS
        + "\n\nTask: creating learning sessions from saved notes. Teach clearly, explain why the concept matters, include examples, "
        "misconceptions, recall questions, flashcards, practice, and scored next steps."
    ),
    "enrich": (
        SECOND_BRAIN_SYSTEM_INSTRUCTIONS
        + "\n\nTask: cataloging captured notes. Preserve the raw capture, add only useful high-confidence enrichment, "
        "avoid noisy guesses, and return structured metadata for the vault."
    ),
    "relations": (
        SECOND_BRAIN_SYSTEM_INSTRUCTIONS
        + "\n\nTask: judging relationships between notes. Link only genuinely related notes and prefer precise reasons over broad topic overlap."
    ),
}

JSON_SYSTEM_INSTRUCTIONS = (
    TASK_SYSTEM_INSTRUCTIONS["enrich"]
    + "\n\nFor this task, return only valid JSON matching the requested schema. "
    "Do not wrap it in markdown. Do not include chain-of-thought."
)


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
    polished_text: str = ""
    suggested_folder: str = "3-Resources"
    suggested_tags: list[str] = field(default_factory=list)
    entities: list[str] = field(default_factory=list)
    aliases: list[str] = field(default_factory=list)
    note_type: str = "Concept"
    note_status: str = "Reference"
    parent_moc: str = ""
    moc_category: str = ""
    moc_description: str = ""
    related_links: list[str] = field(default_factory=list)
    action_items: list[str] = field(default_factory=list)
    questions: list[str] = field(default_factory=list)
    enrichment_notes: list[str] = field(default_factory=list)
    scored_suggestions: list[dict[str, Any]] = field(default_factory=list)
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

    async def complete_text(self, *, task: str, prompt: str, max_tokens: int = 1200) -> ProviderResult:
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
                                "content": self._system_instructions_for_task(task, json_mode=True),
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

    async def complete_text(self, *, task: str, prompt: str, max_tokens: int = 1200) -> ProviderResult:
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
                                "content": self._system_instructions_for_task(task),
                            },
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.2,
                        "max_tokens": max_tokens,
                    },
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
            content = str(data["choices"][0]["message"].get("content") or "")
            if not content.strip():
                raise ValueError("AI response was empty")
            self._record("success", task, started)
            return ProviderResult(provider=self.name, model=self.model, payload={}, text=content)
        except Exception as exc:
            self._record("failed", task, started, error=str(exc)[:160])
            raise

    def _system_instructions_for_task(self, task: str, *, json_mode: bool = False) -> str:
        base = TASK_SYSTEM_INSTRUCTIONS.get(task, SECOND_BRAIN_SYSTEM_INSTRUCTIONS)
        if json_mode:
            return base + "\n\nReturn only valid JSON matching the requested schema. Do not wrap it in markdown."
        return base

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
    def __init__(
        self,
        *,
        providers: dict[str, ModelProvider] | None = None,
        analytics_sink: AnalyticsSink | None = None,
        provider_cooldown_after: int = 2,
        provider_cooldown_sec: int = 180,
    ) -> None:
        self.providers = providers or {}
        self.analytics_sink = analytics_sink or AnalyticsSink()
        self._provider_lock = asyncio.Lock()
        self.provider_cooldown_after = max(1, int(provider_cooldown_after))
        self.provider_cooldown_sec = max(1, int(provider_cooldown_sec))
        self._provider_failures: dict[str, int] = {}
        self._provider_cooldown_until: dict[str, float] = {}

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
                self._record_provider_success(name)
                return _enrichment_from_payload(result.payload, provider=result.provider, fallback_text=text)
            except Exception:
                self._record_provider_failure(name)
                LOGGER.warning("Second Brain AI provider failed for enrichment: %s", name)
                continue
        return local_enrichment(text)

    async def suggest_relations(self, note_text: str, candidates: list[SearchResult]) -> list[RelatedNoteSuggestion]:
        if not candidates:
            return []
        prompt = _relations_prompt(note_text, candidates)
        preferred = "gemini" if "gemini" in self.providers else "modal_glm"
        for name in self._route(preferred_provider=preferred, task="relations"):
            provider = self.providers.get(name)
            if provider is None:
                continue
            try:
                async with self._provider_lock:
                    result = await provider.complete_json(task="relations", prompt=prompt, max_tokens=800)
                self._record_provider_success(name)
                return _relations_from_payload(result.payload, candidates)
            except Exception:
                self._record_provider_failure(name)
                LOGGER.warning("Second Brain AI provider failed for relation judging: %s", name)
                continue
        return local_relation_suggestions(note_text, candidates)

    async def ask(self, question: str, *, context: str, heavy: bool = True, task: str = "ask") -> ProviderResult:
        prompt = (
            "Answer the user's question using the collected Second Brain context first. "
            "Cite note titles or paths when possible. If the context does not support an answer, say so. "
            "Return a human-readable Telegram answer, not JSON. For task questions, use a short bullet list.\n"
            "Use this structure when useful:\n"
            "Answer - direct answer in the user's language.\n"
            "Evidence from notes - cite the note titles or paths that support the answer.\n"
            "Assumptions - separate any inference from saved facts.\n"
            "Confidence - one practical score from 1 to 100.\n"
            "Next actions - only concrete steps that follow from the notes.\n\n"
            f"Question:\n{question}\n\nContext:\n{context[:12000]}"
        )
        preferred = "gemini" if "gemini" in self.providers else ("modal_glm" if heavy else "cerebras")
        for name in self._route(preferred_provider=preferred, task="ask"):
            provider = self.providers.get(name)
            if provider is None:
                continue
            try:
                async with self._provider_lock:
                    result = await provider.complete_text(task=task, prompt=prompt, max_tokens=1200)
                self._record_provider_success(name)
                text = clean_human_response(str(result.text or result.payload.get("answer") or result.payload.get("text") or ""))
                return ProviderResult(provider=result.provider, model=result.model, payload=result.payload, text=text)
            except Exception:
                self._record_provider_failure(name)
                LOGGER.warning("Second Brain AI provider failed for ask: %s", name)
                continue
        return ProviderResult(provider="local_fallback", model="local", payload={}, text=_local_answer(question, context))

    def _route(self, *, preferred_provider: str | None, task: str) -> list[str]:
        order: list[str] = []
        if preferred_provider:
            order.append(preferred_provider)
        elif task == "enrich":
            order.extend(["gemini", "cerebras", "groq", "modal_glm"])
        elif task in {"ask", "relations"}:
            order.extend(["gemini", "modal_glm", "cerebras", "groq"])
        order.extend(["gemini", "modal_glm", "cerebras", "groq"])
        seen: set[str] = set()
        ordered = [name for name in order if not (name in seen or seen.add(name))]
        return [name for name in ordered if not self._provider_is_cooling_down(name)]

    def _provider_is_cooling_down(self, name: str) -> bool:
        until = self._provider_cooldown_until.get(name, 0.0)
        if until <= time.monotonic():
            self._provider_cooldown_until.pop(name, None)
            return False
        return True

    def _record_provider_success(self, name: str) -> None:
        self._provider_failures.pop(name, None)
        self._provider_cooldown_until.pop(name, None)

    def _record_provider_failure(self, name: str) -> None:
        failures = self._provider_failures.get(name, 0) + 1
        self._provider_failures[name] = failures
        if failures >= self.provider_cooldown_after:
            self._provider_cooldown_until[name] = time.monotonic() + self.provider_cooldown_sec
            try:
                self.analytics_sink.append_event(
                    "second_brain_ai_provider_cooldown",
                    {"provider": name, "failures": failures, "cooldown_sec": self.provider_cooldown_sec},
                )
            except Exception:
                return


def local_enrichment(text: str) -> AIEnrichment:
    title = _first_line(text)
    keywords = _keywords(text)
    return AIEnrichment(
        title=title,
        summary=text.strip()[:280],
        polished_text=_polish_capture_text(text),
        suggested_folder=_local_para_folder(text),
        suggested_tags=[f"#{item}" for item in keywords[:4]],
        entities=keywords[:8],
        aliases=[],
        note_type=_local_note_type(text),
        note_status=_local_note_status(text),
        parent_moc=_local_parent_moc(text),
        moc_category=_local_moc_category(text),
        moc_description=_local_moc_description(text),
        related_links=_local_related_links(text),
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
        f"{SECOND_BRAIN_SYSTEM_INSTRUCTIONS}\n\n"
        "Enrich this personal Second Brain capture. Use model knowledge only unless web facts are already supplied.\n"
        "Universal enrichment rules:\n"
        "- Preserve everything the user provided; never replace the raw capture.\n"
        "- Create polished_text by improving style, order, typo cleanup, and readability while preserving the user's meaning.\n"
        "- In polished_text, split inline numbered steps such as 1), 2), 3) onto separate lines.\n"
        "- In polished_text, use rare helpful emoji section markers only when they make the note easier to scan.\n"
        "- Do not add new facts, guesses, or model knowledge into polished_text; put extra context only in enrichment_notes or scored_suggestions.\n"
        "- Add useful context only when it is high-confidence and helpful for future recall or decisions.\n"
        "- Do not add noisy guesses. If the user says they want to buy a scarf, do not guess a brand or material unless provided.\n"
        "- If the user gives a recognizable ticker, acronym, company, product, method, book, person, or concept, add a compact definition.\n"
        "- If the capture is a goal/problem, suggest a few effective approaches and give each a score from 1 to 100.\n"
        "- Scores mean practical usefulness/confidence for this capture, not scientific certainty.\n"
        "- For health or mental wellbeing topics, keep it educational, do not diagnose, and prefer evidence-informed methods.\n"
        "- Keep enrichment small: usually 2-6 bullets.\n\n"
        "Vault catalog rules:\n"
        "- Choose one PARA root: 1-Projects, 2-Areas, 3-Resources, or 4-Incubator.\n"
        "- Create a descriptive searchable title, never a generic title like Another idea or Note.\n"
        "- Choose a parent MOC name like Investments MOC, Purchases MOC, Plans to Do MOC, Recipes MOC, or Software Knowledge MOC.\n"
        "- Choose a short moc_category folder name matching the MOC topic, such as Investments or Purchases.\n"
        "- Catalog metadata must be English-only: aliases, suggested_tags, entities, parent_moc, moc_category, related_links, "
        "action_items, questions, enrichment_notes labels, and scored_suggestions titles/reasons must use English names/terms.\n"
        "- Keep the user's visible note content in the user's language, but catalog/index/search metadata must be English.\n"
        "- type must be one of MOC, Concept, Plan, Purchase, Idea.\n"
        "- status must be one of Active, Incubating, Completed, Reference.\n"
        "- tags must be 2-4 English Obsidian tags with a # prefix, lowercase words, and hyphens instead of spaces.\n"
        "- related_links must be useful existing or likely MOC/concept note titles without brackets.\n\n"
        "Return JSON with keys: title, summary, polished_text, suggested_folder, suggested_tags, entities, aliases, "
        "note_type, note_status, parent_moc, moc_category, moc_description, related_links, action_items, questions, "
        "enrichment_notes, scored_suggestions. scored_suggestions must be a list of objects with title, score, reason. "
        "suggested_folder must be one of 1-Projects, 2-Areas, 3-Resources, 4-Incubator. "
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
        f"{SECOND_BRAIN_SYSTEM_INSTRUCTIONS}\n\n"
        "Find genuinely useful links between the new note and existing notes. "
        "Return JSON {\"related_notes\": [{\"note_id\": str, \"reason\": str, \"confidence\": 0..1}]}. "
        "Only include notes with confidence >= 0.55.\n\n"
        f"New note:\n{note_text[:4000]}\n\nCandidates:\n{json.dumps(packed, ensure_ascii=False)}"
    )


def _parse_json_object(content: str) -> dict[str, Any]:
    content = _strip_thinking(content)
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
        polished_text=str(
            payload.get("polished_text")
            or payload.get("cleaned_text")
            or payload.get("rewritten_capture")
            or _polish_capture_text(fallback_text)
        ).strip(),
        suggested_folder=str(payload.get("suggested_folder") or _local_para_folder(fallback_text)),
        suggested_tags=_string_list(payload.get("suggested_tags")),
        entities=_string_list(payload.get("entities")),
        aliases=_string_list(payload.get("aliases")),
        note_type=str(payload.get("note_type") or payload.get("type") or _local_note_type(fallback_text)),
        note_status=str(payload.get("note_status") or payload.get("status") or _local_note_status(fallback_text)),
        parent_moc=str(payload.get("parent_moc") or _local_parent_moc(fallback_text)),
        moc_category=str(payload.get("moc_category") or _local_moc_category(fallback_text)),
        moc_description=str(payload.get("moc_description") or _local_moc_description(fallback_text)),
        related_links=_string_list(payload.get("related_links")),
        action_items=_string_list(payload.get("action_items")),
        questions=_string_list(payload.get("questions")),
        enrichment_notes=_string_list(payload.get("enrichment_notes") or payload.get("useful_context")),
        scored_suggestions=_scored_suggestions(payload.get("scored_suggestions") or payload.get("suggestions")),
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


def _scored_suggestions(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    result: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, str):
            title, score, reason = item.strip(), 70, ""
        elif isinstance(item, dict):
            title = str(item.get("title") or item.get("name") or "").strip()
            reason = str(item.get("reason") or item.get("why") or "").strip()
            try:
                score = int(float(item.get("score", 70)))
            except (TypeError, ValueError):
                score = 70
        else:
            continue
        if not title:
            continue
        result.append({"title": title, "score": min(100, max(1, score)), "reason": reason})
    return result[:10]


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


def _local_para_folder(text: str) -> str:
    lowered = (text or "").lower()
    if any(word in lowered for word in ("buy", "wishlist", "purchase", "idea", "someday", "maybe", "strategy")):
        return "4-Incubator"
    if any(word in lowered for word in ("project", "deadline", "plan to", "need to")):
        return "1-Projects"
    if any(word in lowered for word in ("health", "investment", "wealth", "home", "routine")):
        return "2-Areas"
    return "3-Resources"


def _local_note_type(text: str) -> str:
    lowered = (text or "").lower()
    if any(word in lowered for word in ("buy", "purchase", "wishlist")):
        return "Purchase"
    if any(word in lowered for word in ("plan", "need to", "todo", "to do")):
        return "Plan"
    if any(word in lowered for word in ("idea", "strategy", "maybe")):
        return "Idea"
    return "Concept"


def _local_note_status(text: str) -> str:
    note_type = _local_note_type(text)
    if note_type in {"Idea", "Purchase"}:
        return "Incubating"
    if note_type == "Plan":
        return "Active"
    return "Reference"


def _local_parent_moc(text: str) -> str:
    lowered = (text or "").lower()
    if any(word in lowered for word in ("buy", "purchase", "wishlist", "scarf", "knife", "chair", "microphone")):
        return "Purchases MOC"
    if any(word in lowered for word in ("stock", "investment", "invest", "etf", "toloka")):
        return "Investments MOC"
    if any(word in lowered for word in ("earn", "business", "fba", "strategy", "money")):
        return "Business Ideas MOC"
    if any(word in lowered for word in ("city", "living", "move")):
        return "Life Planning MOC"
    if any(word in lowered for word in ("health", "mental", "workout")):
        return "Health MOC"
    return "Knowledge MOC"


def _local_moc_category(text: str) -> str:
    moc = _local_parent_moc(text)
    return moc.removesuffix(" MOC")


def _local_moc_description(text: str) -> str:
    moc = _local_parent_moc(text)
    if moc == "Purchases MOC":
        return "Tracks potential purchases, buying criteria, comparisons, and follow-up decisions."
    if moc == "Investments MOC":
        return "Tracks investment ideas, risks, theses, and research notes."
    if moc == "Business Ideas MOC":
        return "Tracks business and earning ideas that may become plans later."
    if moc == "Life Planning MOC":
        return "Tracks life-management decisions, location planning, and personal direction."
    if moc == "Health MOC":
        return "Tracks health, wellbeing, routines, and personal performance notes."
    return "Indexes reference notes and reusable knowledge."


def _local_related_links(text: str) -> list[str]:
    links: list[str] = []
    lowered = (text or "").lower()
    if any(word in lowered for word in ("buy", "purchase", "wishlist")):
        links.append("Things to Buy MOC")
    if any(word in lowered for word in ("plan", "need to", "todo", "to do")):
        links.append("Plans to Do MOC")
    return links


def _polish_capture_text(text: str) -> str:
    polished = (text or "").strip()
    if not polished:
        return ""
    # Local fallback only does mechanical readability cleanup; model-added knowledge belongs in enrichment notes.
    polished = re.sub(r"([:\.])\s+([1-9]\d*)[\).]\s+", r"\1\n\2. ", polished)
    polished = re.sub(r"\s+([1-9]\d*)[\).]\s+", r"\n\1. ", polished)
    polished = re.sub(r"\n{3,}", "\n\n", polished)
    return polished.strip()


def _local_answer(question: str, context: str) -> str:
    if not context.strip():
        return "I could not find collected notes that answer this yet."
    notes = _parse_context_notes(context)
    if not notes:
        excerpt = _truncate_clean(clean_note_excerpt(context), 600)
        return "🧠 I could not reach the AI models, so here is the most relevant saved context I found:\n\n" + excerpt
    lines = ["🧠 I could not reach the AI models, so here are the most relevant saved notes I found:"]
    for note in notes[:5]:
        lines.append("")
        lines.append(f"- {note['title']} ({note['path']})")
        excerpt = _fallback_note_excerpt(note["excerpt"])
        if excerpt:
            lines.append(f"  {excerpt}")
    return "\n".join(lines).strip()


def format_note_context(item: SearchResult, *, max_chars: int = 800) -> str:
    return (
        f"Title: {item.title}\n"
        f"Path: {item.path}\n"
        f"Tags: {', '.join(item.tags)}\n"
        f"Excerpt: {_truncate_clean(clean_note_excerpt(item.body), max_chars)}"
    )


def clean_note_excerpt(text: str) -> str:
    cleaned = str(text or "")
    cleaned = re.sub(r"^---\n.*?\n---\n", "", cleaned, flags=re.S)
    cleaned = re.sub(r"^#{1,6}\s+", "", cleaned, flags=re.M)
    cleaned = re.sub(r"`([^`]+)`", r"\1", cleaned)
    cleaned = re.sub(r"!\[\[([^\]]+)\]\]", r"[attachment: \1]", cleaned)
    cleaned = re.sub(r"\[\[([^\]]+)\]\]", r"\1", cleaned)
    cleaned = re.sub(r"\n{2,}", "\n", cleaned)
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    return cleaned.strip()


def _parse_context_notes(context: str) -> list[dict[str, str]]:
    notes: list[dict[str, str]] = []
    pattern = re.compile(
        r"(?:^|\n\n)Title:\s*(?P<title>.*?)\nPath:\s*(?P<path>.*?)\nTags:\s*(?P<tags>.*?)\nExcerpt:\s*(?P<excerpt>.*?)(?=\n\nTitle:|\Z)",
        flags=re.S,
    )
    for match in pattern.finditer(context.strip()):
        notes.append(
            {
                "title": match.group("title").strip() or "Untitled note",
                "path": match.group("path").strip(),
                "excerpt": clean_note_excerpt(match.group("excerpt")),
            }
        )
    return notes


def _truncate_clean(text: str, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(text) <= limit:
        return text
    candidate = text[: max(0, limit - 3)].rstrip()
    boundary = max(candidate.rfind(". "), candidate.rfind("; "), candidate.rfind("! "), candidate.rfind("? "))
    if boundary >= max(80, limit // 2):
        candidate = candidate[: boundary + 1]
    else:
        space = candidate.rfind(" ")
        if space >= max(40, limit // 3):
            candidate = candidate[:space]
    return candidate.rstrip(" ,;:-") + "..."


def _fallback_note_excerpt(excerpt: str) -> str:
    text = clean_note_excerpt(excerpt)
    summary = _extract_flat_section(text, "Executive Summary", stop_labels=("Source Capture", "Catalog", "Polished Capture"))
    if summary:
        return _truncate_clean(summary, 260)
    text = re.sub(r"\bParent:\s*.*?(?=\bRelated:|\bExecutive Summary\b|\bSource Capture\b|\Z)", "", text)
    text = re.sub(r"\bRelated:\s*.*?(?=\bExecutive Summary\b|\bSource Capture\b|\Z)", "", text)
    text = re.sub(r"\b(Source Capture|Catalog|Polished Capture|Executive Summary)\b", "", text)
    return _truncate_clean(text, 260)


def _extract_flat_section(text: str, label: str, *, stop_labels: tuple[str, ...]) -> str:
    pattern = re.escape(label) + r"\s+(?P<body>.*?)(?=" + "|".join(re.escape(item) for item in stop_labels) + r"|\Z)"
    match = re.search(pattern, text, flags=re.S)
    return re.sub(r"\s+", " ", match.group("body")).strip(" .:-") if match else ""


def clean_human_response(text: str) -> str:
    cleaned = _strip_thinking(text).strip()
    if not cleaned:
        return ""
    parsed = _try_parse_json(cleaned)
    if parsed is not None:
        converted = _json_to_human_text(parsed)
        if converted:
            return converted
    cleaned = re.sub(r"```(?:json)?\s*", "", cleaned)
    cleaned = cleaned.replace("```", "").strip()
    cleaned = _telegram_plaintext_markdown(cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned[:3900]


def _telegram_plaintext_markdown(text: str) -> str:
    text = re.sub(r"(?m)^\s*#{1,6}\s+", "", text)
    text = re.sub(r"(?m)^\s*[*+]\s+", "- ", text)
    text = re.sub(r"(?m)^\s*-\s{2,}", "- ", text)
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
    text = re.sub(r"__(.*?)__", r"\1", text)
    return text.strip()


def _strip_thinking(text: str) -> str:
    text = str(text or "")
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.IGNORECASE | re.S)
    text = re.sub(r"<analysis>.*?</analysis>", "", text, flags=re.IGNORECASE | re.S)
    return text


def _try_parse_json(text: str) -> dict[str, Any] | None:
    candidate = text.strip()
    if candidate.startswith("```"):
        candidate = re.sub(r"^```(?:json)?\s*", "", candidate)
        candidate = re.sub(r"\s*```$", "", candidate)
    start = candidate.find("{")
    end = candidate.rfind("}")
    if start >= 0 and end >= start:
        candidate = candidate[start : end + 1]
    try:
        payload = json.loads(candidate)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _json_to_human_text(payload: dict[str, Any]) -> str:
    if isinstance(payload.get("answer"), str):
        return clean_human_response(payload["answer"])
    if isinstance(payload.get("text"), str):
        return clean_human_response(payload["text"])
    action_items = payload.get("action_items")
    if isinstance(action_items, list):
        lines = ["🧭 Things to do"]
        for item in action_items[:20]:
            if isinstance(item, dict):
                task = str(item.get("task") or item.get("title") or "").strip()
                source = str(item.get("source") or "").strip()
            else:
                task = str(item).strip()
                source = ""
            if not task:
                continue
            line = f"- {task}"
            if source:
                line += f" — {source}"
            lines.append(line)
        return "\n".join(lines) if len(lines) > 1 else ""
    suggestions = payload.get("suggestions") or payload.get("scored_suggestions")
    if isinstance(suggestions, list):
        lines = ["🧠 Suggested options"]
        for item in _scored_suggestions(suggestions):
            line = f"- {item['title']} (Score: {item['score']}/100)"
            if item.get("reason"):
                line += f" — {item['reason']}"
            lines.append(line)
        return "\n".join(lines) if len(lines) > 1 else ""
    bullets = payload.get("bullets") or payload.get("items")
    if isinstance(bullets, list):
        lines = ["🧠 Summary"]
        lines.extend(f"- {str(item).strip()}" for item in bullets[:20] if str(item).strip())
        return "\n".join(lines) if len(lines) > 1 else ""
    return ""
