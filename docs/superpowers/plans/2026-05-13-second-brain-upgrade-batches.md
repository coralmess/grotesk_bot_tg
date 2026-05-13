# Second Brain Upgrade Batches Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the Second Brain bot into a more reliable knowledge, learning, task, retrieval, and maintenance system.

**Architecture:** Keep the existing Telegram/service/vault/index/AI split. Add retrieval and routing helpers behind `SecondBrainService`, extend SQLite with task/provider-health/query support, and keep Telegram output readable through existing formatting helpers.

**Tech Stack:** Python, python-telegram-bot, SQLite FTS5, Obsidian-compatible Markdown, OpenAI-compatible AI providers, `unittest`.

---

## Batch 1: Retrieval, Answer Quality, Provider Stability

**Files:**
- Modify: `second_brain_bot/index.py`
- Modify: `second_brain_bot/models.py`
- Modify: `second_brain_bot/ai.py`
- Modify: `second_brain_bot/service.py`
- Test: `tests/test_second_brain_index.py`
- Test: `tests/test_second_brain_ai.py`
- Test: `tests/test_second_brain_service.py`

- [x] Add a multi-stage retrieval API that combines FTS, exact title/path/entity/tag matches, relation expansion, and deduped ranking.
- [x] Add grounded answer prompt structure requiring answer, evidence, assumptions, confidence, and next actions.
- [x] Add provider health cooldown after repeated provider failures while preserving fallback order.
- [x] Verify with focused tests and full test suite.

## Batch 2: Learning, Actions, Telegram Readability

**Files:**
- Modify: `second_brain_bot/models.py`
- Modify: `second_brain_bot/index.py`
- Modify: `second_brain_bot/service.py`
- Modify: `second_brain_bot/vault.py`
- Modify: `second_brain_bot/bot.py`
- Test: `tests/test_second_brain_index.py`
- Test: `tests/test_second_brain_service.py`
- Test: `tests/test_second_brain_bot.py`

- [x] Store extracted action items as first-class indexed records with source note, status, and priority.
- [x] Make `/ask` include indexed action records for task-style questions.
- [x] Upgrade `/learn` to save a structured learning session with explanation, examples, misconceptions, recall questions, practice, and next steps.
- [x] Split long Telegram messages into readable numbered parts instead of cutting mid-response.
- [x] Verify with focused tests and full test suite.

## Batch 3: Vault Health And Consolidation

**Files:**
- Modify: `second_brain_bot/service.py`
- Modify: `second_brain_bot/vault.py`
- Modify: `second_brain_bot/bot.py`
- Test: `tests/test_second_brain_service.py`
- Test: `tests/test_second_brain_bot.py`

- [x] Add vault health audit for orphan notes, missing MOC links, weak titles, weak metadata, duplicate-looking titles, and non-English metadata.
- [x] Add `/review` command that returns a readable vault health report.
- [x] Add consolidation command that creates a periodic distilled vault review note without mutating old notes.
- [x] Verify with focused tests and full test suite.

## Deployment

- [x] Run `python -m unittest discover -s tests`.
- [ ] Commit the completed batches.
- [ ] Push, pull on instance, restart `second-brain-bot.service`, and verify service health.
