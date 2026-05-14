# Second Brain YouTube Transcripts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the Second Brain bot turn a sent YouTube link into cleaned transcript Markdown plus AI-distilled theme notes in the Obsidian vault.

**Architecture:** Add a focused `second_brain_bot/youtube.py` module for URL detection, transcript fetching, and timestamp cleanup. Extend `SecondBrainService` with a YouTube capture path that writes a source transcript note first, then asks AI for theme distillations and writes zero or more linked theme notes. Telegram capture routes YouTube links into that flow and formats all created notes in the confirmation.

**Tech Stack:** Python `unittest`, `youtube-transcript-api` for public captions, existing Gemini/AI orchestrator, existing Markdown vault/index/MOC code.

---

### Task 1: Transcript Utilities

**Files:**
- Create: `second_brain_bot/youtube.py`
- Test: `tests/test_second_brain_youtube.py`

- [ ] **Step 1: Write failing tests for URL detection and timestamp cleanup.**
- [ ] **Step 2: Run `python -m unittest tests.test_second_brain_youtube` and verify import/function failures.**
- [ ] **Step 3: Implement `extract_youtube_video_id`, `is_youtube_url`, `clean_transcript_text`, and a public transcript fetcher wrapper.**
- [ ] **Step 4: Run the YouTube utility tests and verify they pass.**

### Task 2: Vault Writers

**Files:**
- Modify: `second_brain_bot/vault.py`
- Test: `tests/test_second_brain_service.py`

- [ ] **Step 1: Write failing service tests for transcript note creation and MOC linking.**
- [ ] **Step 2: Add `write_youtube_transcript_note` and `write_youtube_theme_note` to `SecondBrainVault`.**
- [ ] **Step 3: Run the targeted service tests and verify they pass.**

### Task 3: Service Flow

**Files:**
- Modify: `second_brain_bot/service.py`
- Test: `tests/test_second_brain_service.py`

- [ ] **Step 1: Write failing tests for successful YouTube capture, AI distillation, and transcript-unavailable fallback.**
- [ ] **Step 2: Implement `capture_youtube_url`, AI theme parsing, indexing, analytics, and fallback behavior.**
- [ ] **Step 3: Run targeted service tests and verify they pass.**

### Task 4: Telegram Wiring

**Files:**
- Modify: `second_brain_bot/bot.py`
- Test: `tests/test_second_brain_bot.py`

- [ ] **Step 1: Write failing tests for multi-note capture confirmation formatting.**
- [ ] **Step 2: Route text captures containing YouTube URLs through the service YouTube path.**
- [ ] **Step 3: Format confirmation with the transcript note plus distillation notes.**
- [ ] **Step 4: Run bot tests and verify they pass.**

### Task 5: Dependency, Full Tests, Deploy

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add `youtube-transcript-api` dependency.**
- [ ] **Step 2: Run `python -m unittest tests.test_second_brain_youtube tests.test_second_brain_service tests.test_second_brain_bot`.**
- [ ] **Step 3: Run `python -m unittest discover -s tests`.**
- [ ] **Step 4: Commit, push, pull on instance, install requirements if needed, run Second Brain tests, and restart only `second-brain-bot.service`.**

### Self-Review

- Spec coverage: URL detection, transcript fetch, timestamp cleanup, transcript note, theme notes, Telegram UX, unavailable transcript fallback, and deployment are covered.
- Placeholder scan: no unresolved placeholders or deferred tasks remain.
- Type consistency: service returns a dedicated result object with a transcript note and a list of theme notes; bot formatting consumes that object directly.
