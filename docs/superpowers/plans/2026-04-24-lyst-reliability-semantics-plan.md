# LYST Reliability Semantics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make LYST run results, Cloudflare throttling, scheduler wake behavior, and exchange-rate cache tests deterministic and trustworthy.

**Architecture:** Add small focused helper modules instead of expanding `GroteskBotTg.py`. Keep existing service boundaries intact: `grotesk-lyst.service` remains the LYST runtime, `helpers/scheduler.py` remains the scheduler, `helpers/service_health.py` remains the canonical health snapshot writer. The plan changes behavior in safe increments with tests before implementation.

**Tech Stack:** Python 3.10+, `asyncio`, `unittest`, existing `helpers.service_health`, existing LYST helper package, no paid infrastructure, no new external services.

---

## Current Evidence And Root Cause Summary

The instance currently shows `grotesk-lyst` as process-healthy but operationally unreliable: health snapshots recorded `18` successful LYST runs and `31` failed LYST runs, with the latest failure being `Cloudflare challenge`. The logs also show a confusing pattern: a Cloudflare challenge marks the run as failed, but later lines can still say `LYST run completed` because partial already-scraped data is processed before finalization. This is technically explainable, but operationally ambiguous.

The local full test suite had one existing failure in `tests/test_lyst_pricing.py`: the test hardcodes `2026-04-21T10:00:00` as a fresh exchange-rate cache timestamp. On `2026-04-24`, that fixture is no longer fresh enough, so the async loader tries a network refresh and the test fails. The root cause is a date-sensitive test fixture, not a pricing algorithm failure.

The scheduler has duplicated logic across `run_scheduler`, `run_market_scheduler`, and `run_lyst_scheduler`. It logs repeated short sleeps around task boundaries and does not expose structured run IDs or scheduler state. This makes production logs harder to interpret during partial LYST failures.

## File Structure

Create:
- `helpers/lyst/outcome.py`: Typed LYST run outcome model and final-state helpers.
- `helpers/lyst/cloudflare_backoff.py`: Persistent adaptive cooldown/rate-limit state for Cloudflare responses.
- `tests/test_lyst_outcome.py`: Unit tests for outcome classification and status payload mapping.
- `tests/test_lyst_cloudflare_backoff.py`: Unit tests for cooldown escalation, expiry, persistence, and reset.
- `tests/test_scheduler_accounting.py`: Unit tests for scheduler run accounting/state transitions.

Modify:
- `GroteskBotTg.py`: Use typed LYST outcomes, update final log messages, record Cloudflare cooldowns, pass scheduler callbacks.
- `helpers/lyst/status.py`: Accept structured outcome details while preserving legacy `ok/note` status files.
- `helpers/lyst/models.py`: Leave unchanged in this plan. `helpers/lyst/outcome.py` owns final run semantics so the existing fetch/result models stay stable.
- `helpers/scheduler.py`: Add reusable run accounting helpers and reduce noisy sleep logs.
- `helpers/lyst/pricing.py`: Make cache freshness testable by injecting `now`.
- `tests/test_lyst_pricing.py`: Replace hardcoded date with deterministic current/frozen date logic.
- `tests/test_lyst_status.py`: Verify partial/Cloudflare outcomes are reported consistently.
- `tests/test_status_heartbeat.py`: Verify status text surfaces explicit LYST phase/note.

Do not modify:
- `grotesk_market_service.py`, `olx_scraper.py`, `shafa_scraper.py` unless a test proves a shared scheduler change requires it.
- Systemd service files unless deployment verification shows a missing environment variable is needed.

---

## Task 1: Fix Date-Sensitive LYST Pricing Test

**Files:**
- Modify: `helpers/lyst/pricing.py`
- Modify: `tests/test_lyst_pricing.py`

- [ ] **Step 1: Add a failing freshness-boundary test**

Add this test to `tests/test_lyst_pricing.py`:

```python
from datetime import datetime


def test_load_cached_exchange_rates_uses_injected_now_for_freshness(self):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "rates.json"
        path.write_text(
            '{"last_update":"2026-04-21T10:00:00","rates":{"EUR":0.04,"USD":0.025,"GBP":0.03}}',
            encoding="utf-8",
        )

        rates, is_fresh = pricing._load_cached_exchange_rates(
            path,
            now=datetime.fromisoformat("2026-04-22T09:00:00"),
        )

    self.assertTrue(is_fresh)
    self.assertEqual(rates["USD"], 0.025)
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```powershell
python -m unittest tests.test_lyst_pricing.LystPricingTests.test_load_cached_exchange_rates_uses_injected_now_for_freshness -v
```

Expected result: `TypeError` because `_load_cached_exchange_rates()` does not accept `now`.

- [ ] **Step 3: Make freshness deterministic**

Change the helper signature in `helpers/lyst/pricing.py`:

```python
def _load_cached_exchange_rates(exchange_rates_file: Path, *, now: datetime | None = None):
    with exchange_rates_file.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    cached_rates = data.get("rates")
    last_update = data.get("last_update")
    current_time = now or datetime.now()
    # The bot treats FX cache freshness as a calendar-day safeguard, not a strict
    # 24-hour SLA, so a previous-day cache is still acceptable before re-fetching.
    is_fresh = bool(last_update) and (current_time - datetime.fromisoformat(last_update)).days <= 1
    return cached_rates, is_fresh
```

- [ ] **Step 4: Replace hardcoded stale test dates**

Update both existing cache tests in `tests/test_lyst_pricing.py` to write `datetime.now().isoformat()` instead of `2026-04-21T10:00:00`:

```python
path.write_text(
    json.dumps(
        {
            "last_update": datetime.now().isoformat(),
            "rates": {"EUR": 0.04, "USD": 0.025, "GBP": 0.03},
        }
    ),
    encoding="utf-8",
)
```

Also add `import json` and `from datetime import datetime` at the top of the test file.

- [ ] **Step 5: Verify pricing tests**

Run:

```powershell
python -m unittest tests.test_lyst_pricing -v
```

Expected result: all `LystPricingTests` pass with no network call in the async fresh-cache test.

- [ ] **Step 6: Commit**

```powershell
git add helpers/lyst/pricing.py tests/test_lyst_pricing.py
git commit -m "Fix deterministic LYST exchange-rate cache tests"
```

---

## Task 2: Add Explicit LYST Outcome Semantics

**Files:**
- Create: `helpers/lyst/outcome.py`
- Create: `tests/test_lyst_outcome.py`
- Modify: `helpers/lyst/status.py`
- Modify: `tests/test_lyst_status.py`

- [ ] **Step 1: Write tests for outcome classification**

Create `tests/test_lyst_outcome.py`:

```python
import unittest

from helpers.lyst.outcome import LystRunOutcome, LystRunState


class LystOutcomeTests(unittest.TestCase):
    def test_full_success_has_ok_status(self):
        outcome = LystRunOutcome.full_success(items_seen=42, new_items=3)

        self.assertEqual(outcome.state, LystRunState.SUCCESS_FULL)
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.note, "")
        self.assertEqual(outcome.phase, "succeeded")

    def test_cloudflare_partial_success_is_not_ok(self):
        outcome = LystRunOutcome.cloudflare_partial(
            source_name="Main brands",
            country="US",
            page=3,
            items_seen=120,
            new_items=0,
        )

        self.assertEqual(outcome.state, LystRunState.FAILED_CLOUDFLARE)
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.phase, "failed_cloudflare")
        self.assertIn("Cloudflare challenge", outcome.note)
        self.assertIn("Main brands", outcome.note)
        self.assertEqual(outcome.service_state_fields()["lyst_cycle_phase"], "failed_cloudflare")

    def test_stalled_outcome_is_not_ok(self):
        outcome = LystRunOutcome.failed("stalled")

        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.phase, "failed")
        self.assertEqual(outcome.note, "stalled")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```powershell
python -m unittest tests.test_lyst_outcome -v
```

Expected result: import failure because `helpers.lyst.outcome` does not exist.

- [ ] **Step 3: Create the outcome model**

Create `helpers/lyst/outcome.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class LystRunState(str, Enum):
    SUCCESS_FULL = "success_full"
    SUCCESS_PARTIAL = "success_partial"
    FAILED_CLOUDFLARE = "failed_cloudflare"
    FAILED_STALLED = "failed_stalled"
    FAILED = "failed"
    SKIPPED_DISABLED = "skipped_disabled"


@dataclass(slots=True)
class LystRunOutcome:
    state: LystRunState
    note: str = ""
    items_seen: int = 0
    new_items: int = 0
    source_name: str = ""
    country: str = ""
    page: int | None = None

    @property
    def ok(self) -> bool:
        return self.state in {LystRunState.SUCCESS_FULL, LystRunState.SUCCESS_PARTIAL}

    @property
    def phase(self) -> str:
        if self.state == LystRunState.SUCCESS_FULL:
            return "succeeded"
        if self.state == LystRunState.SUCCESS_PARTIAL:
            return "succeeded_partial"
        if self.state == LystRunState.FAILED_CLOUDFLARE:
            return "failed_cloudflare"
        if self.state == LystRunState.FAILED_STALLED:
            return "failed_stalled"
        if self.state == LystRunState.SKIPPED_DISABLED:
            return "skipped_disabled"
        return "failed"

    @classmethod
    def full_success(cls, *, items_seen: int, new_items: int) -> "LystRunOutcome":
        return cls(LystRunState.SUCCESS_FULL, items_seen=items_seen, new_items=new_items)

    @classmethod
    def partial_success(cls, *, note: str, items_seen: int, new_items: int) -> "LystRunOutcome":
        return cls(LystRunState.SUCCESS_PARTIAL, note=note, items_seen=items_seen, new_items=new_items)

    @classmethod
    def cloudflare_partial(
        cls,
        *,
        source_name: str,
        country: str,
        page: int | None,
        items_seen: int,
        new_items: int,
    ) -> "LystRunOutcome":
        location = " ".join(part for part in (source_name, country, f"page {page}" if page else "") if part)
        note = f"Cloudflare challenge: {location}".strip()
        return cls(
            LystRunState.FAILED_CLOUDFLARE,
            note=note,
            items_seen=items_seen,
            new_items=new_items,
            source_name=source_name,
            country=country,
            page=page,
        )

    @classmethod
    def failed(cls, note: str) -> "LystRunOutcome":
        state = LystRunState.FAILED_STALLED if note == "stalled" else LystRunState.FAILED
        return cls(state, note=note or "failed")

    def service_state_fields(self) -> dict[str, object]:
        return {
            "lyst_last_run_ok": self.ok,
            "lyst_last_run_note": self.note,
            "lyst_cycle_phase": self.phase,
            "lyst_items_seen": self.items_seen,
            "lyst_new_items": self.new_items,
            "lyst_failure_source": self.source_name,
            "lyst_failure_country": self.country,
            "lyst_failure_page": self.page,
        }
```

- [ ] **Step 4: Extend status manager with structured finalization**

Add these methods to `helpers/lyst/status.py` inside `LystStatusManager`:

```python
    def set_state_fields(self, **fields: Any) -> None:
        self._reporter.set_state_fields(**fields)

    def finish_outcome(self, outcome, *, duration_seconds: float | None = None) -> None:
        if self._finished:
            return
        self._finished = True
        finished_at_utc = _utc_now_iso()
        state_fields = outcome.service_state_fields()
        state_fields["lyst_last_run_end_utc"] = finished_at_utc
        self._reporter.set_state_fields(**state_fields)
        if outcome.ok:
            self._reporter.record_success("lyst_run", duration_seconds=duration_seconds, note=outcome.note)
        else:
            self._reporter.record_failure("lyst_run", outcome.note, duration_seconds=duration_seconds)
        self._write_legacy(ok=outcome.ok, note=outcome.note, end_utc=finished_at_utc)
```

Keep `finish_success()` and `finish_failure()` as compatibility wrappers. Do not remove them in this task.

- [ ] **Step 5: Add status manager tests**

Add to `tests/test_lyst_status.py`:

```python
    def test_finish_outcome_records_cloudflare_phase(self) -> None:
        from helpers.lyst.outcome import LystRunOutcome

        self.manager.begin_cycle()
        self.manager.finish_outcome(
            LystRunOutcome.cloudflare_partial(
                source_name="Main brands",
                country="US",
                page=3,
                items_seen=120,
                new_items=0,
            ),
            duration_seconds=12.5,
        )

        snapshot = self._snapshot()
        service_state = snapshot["service_state"]
        self.assertEqual(service_state["lyst_last_run_ok"], False)
        self.assertEqual(service_state["lyst_cycle_phase"], "failed_cloudflare")
        self.assertEqual(service_state["lyst_failure_source"], "Main brands")
        self.assertEqual(snapshot["operation_stats"]["lyst_run"]["failure_count"], 1)
        self.assertIn("Cloudflare challenge", self.legacy_calls[-1]["note"])
```

- [ ] **Step 6: Verify outcome/status tests**

Run:

```powershell
python -m unittest tests.test_lyst_outcome tests.test_lyst_status -v
```

Expected result: all tests pass.

- [ ] **Step 7: Commit**

```powershell
git add helpers/lyst/outcome.py helpers/lyst/status.py tests/test_lyst_outcome.py tests/test_lyst_status.py
git commit -m "Add explicit LYST run outcome semantics"
```

---

## Task 3: Wire LYST Outcomes Into The Runtime

**Files:**
- Modify: `GroteskBotTg.py`
- Modify: `tests/test_lyst_runtime.py`
- Modify: `tests/test_status_heartbeat.py`

- [ ] **Step 1: Add import**

In `GroteskBotTg.py`, add:

```python
from helpers.lyst.outcome import LystRunOutcome
```

- [ ] **Step 2: Add tests for final log/status wording and outcome construction**

Add focused pure-helper tests to `tests/test_lyst_runtime.py`:

```python
def test_format_lyst_completion_message_distinguishes_cloudflare_failure(self):
    from GroteskBotTg import _format_lyst_completion_message
    from helpers.lyst.outcome import LystRunOutcome

    message = _format_lyst_completion_message(
        LystRunOutcome.cloudflare_partial(
            source_name="Main brands",
            country="US",
            page=3,
            items_seen=120,
            new_items=0,
        )
    )

    self.assertIn("failed_cloudflare", message)
    self.assertIn("Cloudflare challenge", message)
    self.assertNotEqual(message, "LYST run completed")


def test_build_lyst_run_outcome_prefers_cloudflare_failure_event(self):
    from GroteskBotTg import _build_lyst_run_outcome

    outcome = _build_lyst_run_outcome(
        run_failed=True,
        items_seen=120,
        new_items=0,
        cloudflare_event={"source_name": "Main brands", "country": "US", "page": 3},
        fallback_note="failed",
    )

    self.assertEqual(outcome.phase, "failed_cloudflare")
    self.assertIn("Main brands", outcome.note)
```

Run:

```powershell
python -m unittest tests.test_lyst_runtime -v
```

Expected result: import failure for `_format_lyst_completion_message`.

- [ ] **Step 3: Add completion message helper**

Add near `_finalize_lyst_cycle()` in `GroteskBotTg.py`:

```python
def _format_lyst_completion_message(outcome: LystRunOutcome) -> str:
    if outcome.ok:
        return (
            f"LYST run {outcome.phase}: "
            f"items_seen={outcome.items_seen}, new_items={outcome.new_items}"
        )
    return (
        f"LYST run {outcome.phase}: {outcome.note}; "
        f"items_seen={outcome.items_seen}, new_items={outcome.new_items}"
    )


def _build_lyst_run_outcome(
    *,
    run_failed: bool,
    items_seen: int,
    new_items: int,
    cloudflare_event: dict | None,
    fallback_note: str,
) -> LystRunOutcome:
    if cloudflare_event:
        return LystRunOutcome.cloudflare_partial(
            source_name=str(cloudflare_event.get("source_name") or ""),
            country=str(cloudflare_event.get("country") or ""),
            page=cloudflare_event.get("page"),
            items_seen=items_seen,
            new_items=new_items,
        )
    if run_failed:
        return LystRunOutcome.failed(fallback_note or "failed")
    return LystRunOutcome.full_success(items_seen=items_seen, new_items=new_items)
```

- [ ] **Step 4: Track Cloudflare failure context**

Add a global near the other LYST run globals in `GroteskBotTg.py`:

```python
LYST_LAST_CLOUDFLARE_EVENT = None
```

At the start of `run_lyst_cycle_impl`, reset it:

```python
    global LYST_LAST_PROGRESS_TS, LYST_ACTIVE_TASK, LYST_CYCLE_STARTED_IN_RESUME, LYST_RESUME_ENTRY_OUTCOMES, LYST_LAST_CLOUDFLARE_EVENT
    LYST_LAST_CLOUDFLARE_EVENT = None
```

Where `scrape_all_pages` currently handles `status == "cloudflare"`, record the exact source context before marking the run failed:

```python
            LYST_LAST_CLOUDFLARE_EVENT = {
                "source_name": url_name,
                "country": country,
                "page": page,
            }
```

If `scrape_all_pages` does not currently declare this global, add:

```python
    global LYST_LAST_CLOUDFLARE_EVENT
```

- [ ] **Step 5: Build outcome in `run_lyst_cycle_impl`**

Inside `run_lyst_cycle_impl`, replace the final success block:

```python
        await _finalize_lyst_resume_state()
        _touch_lyst_progress("finalize_run")
        _finalize_lyst_cycle()
        if status_manager is not None:
            status_manager.finish_success(duration_seconds=time.perf_counter() - started)
        logger.info("LYST run completed")
```

with:

```python
        await _finalize_lyst_resume_state()
        _touch_lyst_progress("finalize_run")
        fallback_note = "; ".join(sorted(set(LYST_RESUME_ENTRY_OUTCOMES.values()))) if LYST_RUN_FAILED else ""
        outcome = _build_lyst_run_outcome(
            run_failed=LYST_RUN_FAILED,
            items_seen=len(all_shoes),
            new_items=0,
            cloudflare_event=LYST_LAST_CLOUDFLARE_EVENT,
            fallback_note=fallback_note,
        )
        _finalize_lyst_cycle(issue=outcome.note if not outcome.ok else None)
        if status_manager is not None:
            status_manager.finish_outcome(outcome, duration_seconds=time.perf_counter() - started)
        logger.info(_format_lyst_completion_message(outcome))
```

Keep `new_items=0` in this task. Counting exact new items requires changing `helpers/lyst/processing.py`, which is a separate behavior change and not needed for correct success/failure semantics.

- [ ] **Step 6: Preserve failure paths**

In `except asyncio.CancelledError`, replace `finish_failure("stalled")` with:

```python
        if status_manager is not None:
            status_manager.finish_outcome(
                LystRunOutcome.failed("stalled"),
                duration_seconds=time.perf_counter() - started,
            )
```

In generic `except Exception as exc`, replace `finish_failure(exc)` with:

```python
        if status_manager is not None:
            status_manager.finish_outcome(
                LystRunOutcome.failed(str(exc)),
                duration_seconds=time.perf_counter() - started,
            )
```

- [ ] **Step 7: Update heartbeat status test**

In `tests/test_status_heartbeat.py`, add `lyst_items_seen`, `lyst_failure_source`, and `lyst_cycle_phase="failed_cloudflare"` to the snapshot and assert the text includes `Cloudflare challenge`. Do not require exact formatting unless the formatter already exposes phase; avoid brittle emoji/text tests.

- [ ] **Step 8: Verify LYST runtime/status tests**

Run:

```powershell
python -m unittest tests.test_lyst_runtime tests.test_status_heartbeat tests.test_lyst_status -v
```

Expected result: all tests pass.

- [ ] **Step 9: Commit**

```powershell
git add GroteskBotTg.py tests/test_lyst_runtime.py tests/test_status_heartbeat.py
git commit -m "Report explicit LYST run outcomes"
```

---

## Task 4: Add Persistent Adaptive Cloudflare Backoff

**Files:**
- Create: `helpers/lyst/cloudflare_backoff.py`
- Create: `tests/test_lyst_cloudflare_backoff.py`
- Modify: `helpers/runtime_paths.py`
- Modify: `config.py`

- [ ] **Step 1: Add tests for cooldown behavior**

Create `tests/test_lyst_cloudflare_backoff.py`:

```python
import tempfile
import unittest
from pathlib import Path

from helpers.lyst.cloudflare_backoff import CloudflareBackoff


class CloudflareBackoffTests(unittest.TestCase):
    def test_record_failure_escalates_cooldown(self):
        with tempfile.TemporaryDirectory() as tmp:
            backoff = CloudflareBackoff(Path(tmp) / "cf.json", base_cooldown_sec=60, max_cooldown_sec=600)

            first = backoff.record_failure("Main brands", "US", now_ts=1000)
            second = backoff.record_failure("Main brands", "US", now_ts=1010)

        self.assertEqual(first.cooldown_sec, 60)
        self.assertEqual(second.cooldown_sec, 120)
        self.assertTrue(second.blocked_until_ts > first.blocked_until_ts)

    def test_allows_after_cooldown_expires(self):
        with tempfile.TemporaryDirectory() as tmp:
            backoff = CloudflareBackoff(Path(tmp) / "cf.json", base_cooldown_sec=60, max_cooldown_sec=600)
            backoff.record_failure("Main brands", "US", now_ts=1000)

            self.assertFalse(backoff.should_allow("Main brands", "US", now_ts=1059))
            self.assertTrue(backoff.should_allow("Main brands", "US", now_ts=1061))

    def test_success_resets_source_country_penalty(self):
        with tempfile.TemporaryDirectory() as tmp:
            backoff = CloudflareBackoff(Path(tmp) / "cf.json", base_cooldown_sec=60, max_cooldown_sec=600)
            backoff.record_failure("Main brands", "US", now_ts=1000)
            backoff.record_success("Main brands", "US")

            self.assertTrue(backoff.should_allow("Main brands", "US", now_ts=1001))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```powershell
python -m unittest tests.test_lyst_cloudflare_backoff -v
```

Expected result: import failure.

- [ ] **Step 3: Implement backoff helper**

Create `helpers/lyst/cloudflare_backoff.py`:

```python
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class CloudflareBackoffDecision:
    key: str
    failure_count: int
    cooldown_sec: int
    blocked_until_ts: float


class CloudflareBackoff:
    def __init__(self, path: Path, *, base_cooldown_sec: int, max_cooldown_sec: int) -> None:
        self._path = path
        self._base = max(1, int(base_cooldown_sec))
        self._max = max(self._base, int(max_cooldown_sec))
        self._state = self._load()

    def _key(self, source_name: str, country: str) -> str:
        return f"{source_name.strip()}::{country.strip()}".lower()

    def should_allow(self, source_name: str, country: str, *, now_ts: float | None = None) -> bool:
        now = time.time() if now_ts is None else now_ts
        entry = self._state.get(self._key(source_name, country), {})
        return now >= float(entry.get("blocked_until_ts") or 0)

    def record_failure(self, source_name: str, country: str, *, now_ts: float | None = None) -> CloudflareBackoffDecision:
        now = time.time() if now_ts is None else now_ts
        key = self._key(source_name, country)
        entry = self._state.get(key, {})
        failure_count = int(entry.get("failure_count") or 0) + 1
        cooldown_sec = min(self._max, self._base * (2 ** (failure_count - 1)))
        blocked_until_ts = now + cooldown_sec
        self._state[key] = {
            "failure_count": failure_count,
            "cooldown_sec": cooldown_sec,
            "blocked_until_ts": blocked_until_ts,
            "source_name": source_name,
            "country": country,
            "updated_ts": now,
        }
        self._save()
        return CloudflareBackoffDecision(key, failure_count, cooldown_sec, blocked_until_ts)

    def record_success(self, source_name: str, country: str) -> None:
        self._state.pop(self._key(source_name, country), None)
        self._save()

    def snapshot(self) -> dict:
        return dict(self._state)

    def _load(self) -> dict:
        try:
            return json.loads(self._path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._path)
```

- [ ] **Step 4: Add runtime path and config**

In `helpers/runtime_paths.py`, add:

```python
LYST_CLOUDFLARE_BACKOFF_FILE = RUNTIME_STATUS_DIR / "lyst_cloudflare_backoff.json"
```

In `config.py`, add:

```python
LYST_CLOUDFLARE_BASE_COOLDOWN_SEC = int(os.getenv("LYST_CLOUDFLARE_BASE_COOLDOWN_SEC", "900" if IS_INSTANCE else "60"))
LYST_CLOUDFLARE_MAX_COOLDOWN_SEC = int(os.getenv("LYST_CLOUDFLARE_MAX_COOLDOWN_SEC", "7200" if IS_INSTANCE else "600"))
```

Use comments explaining that this protects the instance from immediately retrying blocked country/source pairs.

- [ ] **Step 5: Verify backoff tests**

Run:

```powershell
python -m unittest tests.test_lyst_cloudflare_backoff -v
```

Expected result: all tests pass.

- [ ] **Step 6: Commit**

```powershell
git add helpers/lyst/cloudflare_backoff.py helpers/runtime_paths.py config.py tests/test_lyst_cloudflare_backoff.py
git commit -m "Add persistent LYST Cloudflare backoff"
```

---

## Task 5: Apply Cloudflare Backoff In LYST Fetch Flow

**Files:**
- Modify: `GroteskBotTg.py`
- Modify: `tests/test_lyst_runtime.py`

- [ ] **Step 1: Add imports and global backoff instance**

In `GroteskBotTg.py`, import:

```python
from helpers.lyst.cloudflare_backoff import CloudflareBackoff
from helpers.runtime_paths import LYST_CLOUDFLARE_BACKOFF_FILE
```

Extend the existing config import with:

```python
LYST_CLOUDFLARE_BASE_COOLDOWN_SEC,
LYST_CLOUDFLARE_MAX_COOLDOWN_SEC,
```

Create the instance near other LYST globals:

```python
LYST_CLOUDFLARE_BACKOFF = CloudflareBackoff(
    LYST_CLOUDFLARE_BACKOFF_FILE,
    base_cooldown_sec=LYST_CLOUDFLARE_BASE_COOLDOWN_SEC,
    max_cooldown_sec=LYST_CLOUDFLARE_MAX_COOLDOWN_SEC,
)
```

- [ ] **Step 2: Add helper tests**

Add to `tests/test_lyst_runtime.py`:

```python
def test_should_skip_lyst_source_when_cloudflare_backoff_blocks_it(self):
    from GroteskBotTg import _should_skip_lyst_source_for_backoff

    class Backoff:
        def should_allow(self, source_name, country):
            return False

    self.assertTrue(_should_skip_lyst_source_for_backoff("Main brands", "US", Backoff()))
```

- [ ] **Step 3: Add skip helper**

Add to `GroteskBotTg.py`:

```python
def _should_skip_lyst_source_for_backoff(source_name: str, country: str, backoff=LYST_CLOUDFLARE_BACKOFF) -> bool:
    return not backoff.should_allow(source_name, country)
```

- [ ] **Step 4: Skip blocked source/country pairs before fetching**

In `scrape_all_pages(url, country, exchange_rates, ...)`, before the page loop starts or before each first page fetch, add:

```python
    if _should_skip_lyst_source_for_backoff(url_name, country):
        logger.warning("Skipping %s for %s due to active Cloudflare cooldown", url_name, country)
        LYST_RESUME_ENTRY_OUTCOMES[key] = "cloudflare_cooldown"
        return []
```

Use the existing variable names from `scrape_all_pages`; if `url_name` or `key` are named differently, use the names already present in that function.

- [ ] **Step 5: Record failure and success**

Where Cloudflare is detected in `scrape_all_pages`, after logging the challenge, add:

```python
            decision = LYST_CLOUDFLARE_BACKOFF.record_failure(url_name, country)
            logger.warning(
                "Cloudflare cooldown for %s %s: failures=%s cooldown=%ss",
                url_name,
                country,
                decision.failure_count,
                decision.cooldown_sec,
            )
```

When a source/country reaches a terminal page after collecting items or otherwise completes without Cloudflare, add:

```python
    LYST_CLOUDFLARE_BACKOFF.record_success(url_name, country)
```

Place the success reset only on clean completion, not on aborted/failed branches.

- [ ] **Step 6: Expose backoff snapshot in health state**

In `run_lyst_cycle_impl`, after `status_manager.begin_cycle()`, call the public wrapper added in Task 2:

```python
        status_manager.set_state_fields(lyst_cloudflare_backoff=LYST_CLOUDFLARE_BACKOFF.snapshot())
```

- [ ] **Step 7: Verify runtime tests**

Run:

```powershell
python -m unittest tests.test_lyst_runtime tests.test_lyst_cloudflare_backoff tests.test_lyst_status -v
```

Expected result: all tests pass.

- [ ] **Step 8: Commit**

```powershell
git add GroteskBotTg.py tests/test_lyst_runtime.py helpers/lyst/status.py
git commit -m "Throttle LYST retries after Cloudflare challenges"
```

---

## Task 6: Improve Scheduler Wake Logic And Run Accounting

**Files:**
- Modify: `helpers/scheduler.py`
- Create: `tests/test_scheduler_accounting.py`
- Modify: `GroteskBotTg.py`

- [ ] **Step 1: Add scheduler accounting tests**

Create `tests/test_scheduler_accounting.py`:

```python
import unittest

from helpers.scheduler import SchedulerRunAccountant


class SchedulerRunAccountantTests(unittest.TestCase):
    def test_run_ids_increment_and_state_transitions_are_recorded(self):
        accountant = SchedulerRunAccountant("lyst")

        run = accountant.start_run(now_ts=1000)
        accountant.finish_run(run, "success", now_ts=1015)

        self.assertEqual(run.run_id, 1)
        self.assertEqual(accountant.state, "idle")
        self.assertEqual(accountant.last_outcome, "success")
        self.assertEqual(accountant.last_duration_sec, 15)

    def test_sleep_log_is_only_needed_when_bucket_changes(self):
        accountant = SchedulerRunAccountant("lyst")

        self.assertTrue(accountant.should_log_sleep(100))
        self.assertFalse(accountant.should_log_sleep(95))
        self.assertTrue(accountant.should_log_sleep(30))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```powershell
python -m unittest tests.test_scheduler_accounting -v
```

Expected result: import failure for `SchedulerRunAccountant`.

- [ ] **Step 3: Add scheduler run accountant**

Add to `helpers/scheduler.py`:

```python
from dataclasses import dataclass


@dataclass(slots=True)
class SchedulerRun:
    name: str
    run_id: int
    started_ts: float


class SchedulerRunAccountant:
    def __init__(self, name: str) -> None:
        self.name = name
        self._next_run_id = 1
        self.state = "idle"
        self.last_outcome = ""
        self.last_duration_sec: int | None = None
        self._last_sleep_bucket: int | None = None

    def start_run(self, *, now_ts: float) -> SchedulerRun:
        run = SchedulerRun(self.name, self._next_run_id, now_ts)
        self._next_run_id += 1
        self.state = "running"
        return run

    def finish_run(self, run: SchedulerRun, outcome: str, *, now_ts: float) -> None:
        self.state = "idle"
        self.last_outcome = outcome
        self.last_duration_sec = max(0, int(now_ts - run.started_ts))

    def should_log_sleep(self, sleep_for: int) -> bool:
        bucket = max(0, int(sleep_for // 60))
        if self._last_sleep_bucket == bucket:
            return False
        self._last_sleep_bucket = bucket
        return True
```

- [ ] **Step 4: Use accountant in `run_lyst_scheduler`**

In `run_lyst_scheduler`, initialize:

```python
    accountant = SchedulerRunAccountant("lyst")
    active_run = None
```

When creating `lyst_task`, replace:

```python
                    lyst_task = asyncio.create_task(run_lyst())
                    lyst_task_started_ts = time.time()
```

with:

```python
                    active_run = accountant.start_run(now_ts=time.time())
                    logger.info("Starting LYST scheduler run #%s", active_run.run_id)
                    lyst_task = asyncio.create_task(run_lyst(), name=f"lyst_run_{active_run.run_id}")
                    lyst_task_started_ts = active_run.started_ts
```

When the task completes, after checking exception/cancelled state:

```python
                outcome = "cancelled" if lyst_task.cancelled() else "failed" if exc else "success"
                if active_run is not None:
                    accountant.finish_run(active_run, outcome, now_ts=time.time())
                    logger.info(
                        "Finished LYST scheduler run #%s outcome=%s duration=%ss",
                        active_run.run_id,
                        outcome,
                        accountant.last_duration_sec,
                    )
                    active_run = None
```

When logging sleep:

```python
            if accountant.should_log_sleep(sleep_for):
                logger.info("Sleeping for %s seconds before next Lyst check", sleep_for)
```

- [ ] **Step 5: Keep market scheduler behavior stable**

Do not refactor `run_market_scheduler` in the same commit unless tests are added for OLX/SHAFA. The original request focuses on LYST wake logic; changing market scheduling would increase regression risk.

- [ ] **Step 6: Verify scheduler tests**

Run:

```powershell
python -m unittest tests.test_scheduler_accounting tests.test_lyst_cycle tests.test_lyst_runtime -v
```

Expected result: all tests pass.

- [ ] **Step 7: Commit**

```powershell
git add helpers/scheduler.py tests/test_scheduler_accounting.py
git commit -m "Add LYST scheduler run accounting"
```

---

## Task 7: Integration Verification Without Network

**Files:**
- Modify tests only if a failure exposes a real missing test seam.

- [ ] **Step 1: Run focused no-network tests**

Run:

```powershell
python -m unittest tests.test_lyst_pricing tests.test_lyst_outcome tests.test_lyst_status tests.test_lyst_cloudflare_backoff tests.test_scheduler_accounting tests.test_lyst_runtime tests.test_status_heartbeat -v
```

Expected result: all listed tests pass.

- [ ] **Step 2: Run full local test suite**

Run:

```powershell
python -m unittest discover -s tests -v
```

Expected result: all tests pass. If any test tries real network unexpectedly, patch the test seam, not the production code, unless production code is actually wrong.

- [ ] **Step 3: Compile critical runtime files**

Run:

```powershell
python -m py_compile GroteskBotTg.py helpers\lyst\outcome.py helpers\lyst\cloudflare_backoff.py helpers\scheduler.py helpers\lyst\pricing.py helpers\lyst\status.py
```

Expected result: exit code `0`.

- [ ] **Step 4: Commit verification-only test fixes if needed**

Only commit if Step 1-3 required additional changes:

```powershell
git add tests helpers GroteskBotTg.py
git commit -m "Verify LYST reliability semantics"
```

---

## Task 8: Instance Deployment And Runtime Verification

**Files:**
- No direct remote edits unless deployment requires environment config.

- [ ] **Step 1: Ask for production approval**

Ask the user whether to commit/merge/deploy if not already approved. This project requires local-first changes, then commit, merge into production-bound state, pull on instance, and restart the affected service.

- [ ] **Step 2: Push production-bound branch**

Run:

```powershell
git status --short
git log --oneline -5
git push origin master
```

Expected result: push succeeds and local tree is clean.

- [ ] **Step 3: Pull on instance**

Run:

```powershell
python skills\instance-ops\scripts\instance_ops.py exec -- bash -lc 'cd /home/ubuntu/LystTgFirefox && git pull --ff-only origin master && git log --oneline -5'
```

Expected result: instance fast-forwards to the new commits.

- [ ] **Step 4: Run tests on instance**

Run:

```powershell
python skills\instance-ops\scripts\instance_ops.py exec -- bash -lc 'cd /home/ubuntu/LystTgFirefox && .venv/bin/python -m unittest tests.test_lyst_pricing tests.test_lyst_outcome tests.test_lyst_status tests.test_lyst_cloudflare_backoff tests.test_scheduler_accounting tests.test_lyst_runtime tests.test_status_heartbeat -v'
```

Expected result: all focused tests pass.

- [ ] **Step 5: Restart only LYST**

Run:

```powershell
python skills\instance-ops\scripts\instance_ops.py restart --service grotesk-lyst.service
```

Expected result: service restarts successfully.

- [ ] **Step 6: Verify service health and logs**

Run:

```powershell
python skills\instance-ops\scripts\instance_ops.py exec -- bash -lc 'systemctl is-active grotesk-lyst.service && sleep 5 && tail -c 4000 /home/ubuntu/LystTgFirefox/runtime_data/health/grotesk-lyst.json'
python skills\instance-ops\scripts\instance_ops.py logs --service grotesk-lyst.service --lines 120
```

Expected result:
- `grotesk-lyst.service` is `active`.
- Health JSON contains `lyst_cycle_phase`.
- If a Cloudflare challenge occurs, `lyst_cycle_phase` becomes `failed_cloudflare`, not generic `failed`.
- Logs say `LYST run failed_cloudflare` or `LYST run succeeded`, not misleading bare `LYST run completed`.

- [ ] **Step 7: Verify updater restart routing**

Run:

```powershell
python skills\instance-ops\scripts\instance_ops.py exec -- bash -lc 'cd /home/ubuntu/LystTgFirefox && .venv/bin/python deploy/restart_changed_services.py --from-ref HEAD~1 --to-ref HEAD --dry-run'
```

Expected result: LYST-related changes would restart `grotesk-lyst.service`; unrelated market services are not restarted unless their files changed.

---

## Self-Review And Improvements Applied

### Initial Plan Weaknesses Found

The first draft would have changed `run_market_scheduler` and `run_lyst_scheduler` together. That was not ideal because OLX/SHAFA are currently stable on the instance and the user asked specifically for LYST Cloudflare behavior plus scheduler wake logic. The final plan limits run-accounting changes to `run_lyst_scheduler` first.

The first draft treated Cloudflare backoff as in-memory only. That was not ideal because a service restart would erase the cooldown and immediately retry the same blocked source/country. The final plan persists cooldown state to `runtime_data/status/lyst_cloudflare_backoff.json`.

The first draft used generic `success_partial` for Cloudflare runs. That was not ideal because a Cloudflare challenge should not be marked successful even if partial data was processed. The final plan keeps partial scraped data but reports the run as `failed_cloudflare`.

The first draft wanted to count `new_items` immediately. That may not be available without refactoring `process_all_shoes`. The final plan avoids risky processing refactors and allows `new_items=0` until a later focused task adds a return count.

The first draft would have made status text assert exact formatting. That was brittle. The final plan asserts durable facts only: Cloudflare note, phase, source/country fields, and operation success/failure counters.

### Final Coverage Check

- Fix LYST success/failure semantics: Tasks 2 and 3.
- Add adaptive LYST rate limiting based on Cloudflare: Tasks 4 and 5.
- Improve scheduler wake logic and run accounting: Task 6.
- Fix date-sensitive LYST pricing test: Task 1.
- Verify no regressions locally and on instance: Tasks 7 and 8.

### Risk Controls

- Each behavior change starts with a failing test.
- Each task has a focused commit.
- No OLX/SHAFA scheduler refactor is included in the same change set.
- Cloudflare cooldown defaults are free and local-only; no paid proxy or infrastructure is introduced.
- Existing legacy status files remain supported through `LystStatusManager._write_legacy()`.
- Deployment restarts only `grotesk-lyst.service`.
