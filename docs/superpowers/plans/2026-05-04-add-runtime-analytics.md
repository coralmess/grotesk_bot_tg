# Runtime Analytics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add structured analytics for scrapers, image handling, service operations, and bot checks without increasing noisy human logs.

**Architecture:** Add one shared append-only analytics helper under `helpers/analytics_events.py`, then wire existing run summaries and service-health operations into it. Add focused event hooks for marketplace item lifecycle and image pipeline so future analysis can explain source quality, image quality, sends, failures, and partial Lyst coverage from machine-readable JSON files.

**Tech Stack:** Python stdlib JSON/Path/threading, existing unittest suite, existing runtime path conventions.

---

### Task 1: Shared Analytics Helper

**Files:**
- Create: `helpers/analytics_events.py`
- Modify: `helpers/runtime_paths.py`
- Test: `tests/test_analytics_events.py`

- [ ] Write tests for daily JSONL events, daily rollups, and secret/chat-id redaction.
- [ ] Implement `RUNTIME_ANALYTICS_DIR`, daily event streams, rollup updates, URL fingerprint helpers, and safe payload normalization.
- [ ] Run `python -m unittest tests.test_analytics_events`.
- [ ] Commit only analytics helper files.

### Task 2: Existing Run And Service Analytics

**Files:**
- Modify: `helpers/scraper_stats.py`
- Modify: `helpers/service_health.py`
- Test: `tests/test_scraper_stats.py`
- Test: `tests/test_service_health.py`

- [ ] Add failing tests proving `RunStatsCollector.write_jsonl()` updates analytics rollups and `ServiceHealthReporter.record_success/failure()` records operation analytics.
- [ ] Wire the helper with comments explaining this is structured telemetry, not human logging.
- [ ] Run targeted tests.
- [ ] Commit only run/service analytics changes.

### Task 3: Marketplace Item And Image Analytics

**Files:**
- Modify: `helpers/marketplace_pipeline.py`
- Modify: `helpers/image_pipeline.py`
- Test: `tests/test_marketplace_pipeline.py`
- Test: `tests/test_marketplace_upscale_and_chunks.py`

- [ ] Add tests for new/persist/send/fail item lifecycle events.
- [ ] Add tests for image events with dimensions, input/output byte counts, upscaled flag, and fallback reason.
- [ ] Wire events with comments explaining why detailed events are only for state transitions and sent images.
- [ ] Run targeted tests.
- [ ] Commit only marketplace/image analytics changes.

### Task 4: Bot-Specific Analytics

**Files:**
- Modify: `useful_bot/exchange_rate_helper.py`
- Modify: `svitlo_bot.py`
- Modify: `helpers/auto_ria/runtime.py`
- Test: existing relevant tests plus small focused tests if needed.

- [ ] Record exchange-rate fetch/send/no-change reasons.
- [ ] Record Svitlo check latency, transitions, and short-outage suppression.
- [ ] Record Auto RIA send/image fallback outcomes beyond the existing run summary.
- [ ] Run targeted tests.
- [ ] Commit only bot-specific analytics changes.

### Task 5: Final Verification

**Files:**
- No new production files expected beyond previous tasks.

- [ ] Run `python -m unittest discover -s tests`.
- [ ] Inspect `git diff --stat` and ensure unrelated `AGENTS.md` is not staged unless explicitly requested.
- [ ] Commit remaining analytics changes if any.
