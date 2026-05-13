# Second Brain Accuracy And Learning Upgrade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve Second Brain accuracy, reduce hallucinations and token waste, make retrieval smarter, make learning cards more useful in Telegram, and add vault quality scores.

**Architecture:** Keep the existing `ai.py`, `index.py`, `service.py`, `bot.py`, and `vault.py` boundaries. Add task-specific prompts in `ai.py`, reranking inside `SecondBrainIndex`, learning-card formatting in the Telegram layer, and vault scoring in `SecondBrainService`.

**Tech Stack:** Python, SQLite FTS5, python-telegram-bot, Obsidian Markdown, `unittest`.

---

## Batch 1: Task-Specific Prompts And Evidence-Required Answers

- [x] Add tests proving enrichment, relation, ask, and learning prompts use task-specific instructions.
- [x] Add tests proving `/ask` does not call AI when no notes/actions support the question.
- [x] Implement prompt builders that avoid duplicating the full system prompt inside user content.
- [x] Make factual `/ask` responses require evidence from notes or an explicit "not found in vault" answer.

## Batch 2: Smarter Retrieval Reranking

- [x] Add tests proving exact title/entity matches rank above broad body matches.
- [x] Add tests proving high-confidence related notes are included but do not outrank direct hits.
- [x] Implement deterministic relevance scoring in `SecondBrainIndex.deep_search`.

## Batch 3: Learning Cards With Spoiler Answers

- [x] Add tests proving learning prompts request clear explanation, examples, misconceptions, recall questions, flashcards, and practice.
- [x] Add tests proving Telegram learning output renders flashcard answers as spoiler blocks.
- [x] Implement learning result formatting with HTML spoilers for card answers.

## Batch 4: Vault Quality Scores

- [x] Add tests proving `vault_health()` reports an overall score and per-category scores.
- [x] Implement scoring for title quality, MOC links, metadata language, duplicate-looking titles, and note structure.

## Verification

- [x] Run focused Second Brain tests.
- [x] Run `python -m unittest discover -s tests`.
- [ ] Commit, push, pull on instance, restart `second-brain-bot.service`, and verify service health.
