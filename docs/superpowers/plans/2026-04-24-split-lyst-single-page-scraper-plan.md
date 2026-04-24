# Split LYST Single-Page Scraper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move LYST single-page scrape/parse behavior out of `GroteskBotTg.py` while preserving the existing pagination, resume, Cloudflare, and run-accounting behavior.

**Architecture:** Add `helpers/lyst/page_scraper.py` as the owner of one page scrape: fetch soup/content, map fetch exceptions to the existing status strings, extract product cards, build the JSON-LD image fallback map, and return `(shoes, content, status)`. Keep `GroteskBotTg.scrape_page()` as a compatibility wrapper that injects runtime dependencies, and keep `scrape_all_pages()` in `GroteskBotTg.py` for this phase because it still owns global resume state, Cloudflare backoff, stop-too-early dumps, retry-mode switching, and source-success accounting.

**Tech Stack:** Python 3.10+, `unittest`, `unittest.mock`, `bs4`, existing `helpers.lyst` modules, no new dependencies.

---

## Reviewed Design

### Recommended Approach

Extract only the single-page scraper now. The helper should receive dependencies explicitly: `get_soup_and_content`, `extract_ldjson_image_map`, `extract_shoe_data`, `mark_issue`, and the three exception classes used by the runtime fetch path. This avoids importing `GroteskBotTg.py` from the helper and prevents circular imports.

### Alternatives Rejected

A full `scrape_all_pages()` move was rejected for this phase because it would move too many production-sensitive responsibilities at once: resume-state mutation, Cloudflare cooldown updates, terminal-page recovery, stop-too-early diagnostic dumps, recursive retry without pagination, and success/failure accounting. A pure copy-paste move with module globals was rejected because it would make the helper hard to test and couple it back to the monolith. A typed enum return was rejected for this phase because callers and tests already use status strings, so changing the contract would add behavior-risk without improving the split.

### Self-Review Iteration 1

Weakness found: the first version still moved pagination. That would make a refactor look larger than it is and increase the chance of breaking LYST resume behavior. Fix: only move `scrape_page()` internals.

### Self-Review Iteration 2

Weakness found: the helper could accidentally depend on `GroteskBotTg` globals. Fix: make all runtime hooks constructor/function arguments and add tests proving the wrapper passes them.

### Self-Review Iteration 3

Weakness found: exception mapping is easy to regress because statuses are plain strings. Fix: add focused tests for `cloudflare`, `aborted`, `terminal`, `failed`, and `ok` paths.

### Self-Review Iteration 4

Weakness found: no-shoes pages are not failures in existing runtime; they return `ok` with an empty list so `scrape_all_pages()` decides whether it is terminal, too-early, or retryable. Fix: explicitly test that empty product-card pages return `([], content, "ok")`, not `failed`.

## File Structure

Create:
- `helpers/lyst/page_scraper.py`: focused single-page scrape helper with injected runtime dependencies.
- `tests/test_lyst_page_scraper.py`: direct helper tests for all statuses and empty-page behavior.

Modify:
- `GroteskBotTg.py`: import `helpers.lyst.page_scraper` and replace `scrape_page()` body with a compatibility wrapper.
- `tests/test_lyst_runtime.py`: add one wrapper-delegation test verifying `GroteskBotTg.scrape_page()` injects runtime dependencies.

Do not modify:
- `GroteskBotTg.scrape_all_pages()`: pagination, resume, Cloudflare cooldown, and retry behavior remain unchanged in this phase.
- `helpers/lyst/fetch.py`: fetch transport behavior is not part of this split.

## Task 1: Add Failing Helper Tests

- [ ] Create `tests/test_lyst_page_scraper.py` with async `unittest.IsolatedAsyncioTestCase` tests.
- [ ] Add `test_scrape_page_returns_ok_shoes_and_content` using a fake `get_soup_and_content` that returns one `div._693owt3`; assert the parser receives the fallback map and the result is `([shoe], content, "ok")`.
- [ ] Add `test_scrape_page_maps_fetch_exceptions_to_runtime_statuses` covering cloudflare, aborted, and terminal exceptions.
- [ ] Add `test_scrape_page_marks_failed_when_soup_missing` covering `(None, content)` and asserting `mark_issue("Failed to get soup")`.
- [ ] Add `test_scrape_page_keeps_empty_product_page_as_ok` asserting no product cards returns `([], content, "ok")`.
- [ ] Run `python -m unittest tests.test_lyst_page_scraper -v`; expected result before implementation: import failure for `helpers.lyst.page_scraper`.

## Task 2: Implement Page Scraper Helper

- [ ] Create `helpers/lyst/page_scraper.py` with `async def scrape_page(...)`.
- [ ] Keep the return contract exactly `(list[dict], str | None, str)` and statuses exactly `"ok"`, `"cloudflare"`, `"aborted"`, `"terminal"`, `"failed"`.
- [ ] Catch the injected exception classes and map them to the same statuses as the old runtime body.
- [ ] Add a concise comment explaining why dependencies are injected: to keep the helper independent from the service entrypoint and avoid circular imports.
- [ ] Run `python -m unittest tests.test_lyst_page_scraper -v`; expected result: pass.

## Task 3: Replace Runtime Body With Wrapper

- [ ] Modify `GroteskBotTg.py` to import `helpers.lyst.page_scraper as lyst_page_scraper`.
- [ ] Replace `scrape_page()` body with a call to `lyst_page_scraper.scrape_page(...)` passing runtime hooks: `get_soup_and_content`, `extract_ldjson_image_map`, `extract_shoe_data`, `_mark_lyst_issue`, and the exception classes.
- [ ] Add a concise comment explaining the wrapper preserves the public runtime name while the helper owns page parsing.
- [ ] Add `tests/test_lyst_runtime.py::test_scrape_page_wrapper_delegates_runtime_dependencies` with `mock.AsyncMock` to assert wrapper dependency injection.
- [ ] Run `python -m unittest tests.test_lyst_runtime tests.test_lyst_page_scraper -v`; expected result: pass.

## Task 4: Regression Verification And Review

- [ ] Run `python -m unittest tests.test_lyst_runtime tests.test_lyst_page_scraper tests.test_lyst_parsing tests.test_lyst_fetch tests.test_lyst_cycle -v`.
- [ ] Run `python -m py_compile GroteskBotTg.py helpers\lyst\page_scraper.py tests\test_lyst_page_scraper.py tests\test_lyst_runtime.py`.
- [ ] Run full regression suite: `python -m unittest discover -s tests -v`.
- [ ] Review `git diff --stat` and `git diff -- GroteskBotTg.py helpers/lyst/page_scraper.py tests/test_lyst_page_scraper.py tests/test_lyst_runtime.py`.
- [ ] If review finds behavior drift, fix it before reporting.
- [ ] Do not commit or deploy unless the user explicitly asks after seeing the local result.

## Later Split Phase Not Included

The next phase after this should extract `scrape_all_pages()` into a stateful page-runner object only after adding tests for resume-start page, Cloudflare cooldown, terminal-only resume, stop-too-early retry, abort handling, and source-success accounting. That is intentionally excluded here because it is the highest-risk part of the LYST runtime.
