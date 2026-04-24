# Split GroteskBotTg.py Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce `GroteskBotTg.py` by moving already-isolated LYST parsing, pricing, and image URL helpers behind focused helper modules without changing LYST runtime behavior.

**Architecture:** Do not rewrite the LYST service loop in one step. First remove duplicated pure helper logic from the monolith and make `GroteskBotTg.py` delegate to `helpers/lyst/parsing.py` and `helpers/lyst/pricing.py`; later phases can extract page scraping and service orchestration after this stable seam exists. This keeps Playwright, resume state, Telegram sending, and scheduler behavior untouched during the first split.

**Tech Stack:** Python 3.10+, `unittest`, `unittest.mock`, existing `helpers.lyst` modules, no new dependencies.

---

## Reviewed Design

### Recommended Approach

Use an incremental strangler split. `GroteskBotTg.py` already imports `helpers.lyst.parsing` and `helpers.lyst.pricing`, but it still carries duplicate implementations for price token parsing, image URL normalization, JSON-LD image extraction, and full shoe-card parsing. Replacing those bodies with thin wrappers removes hundreds of lines while preserving old function names for compatibility.

### Alternatives Rejected

A full service-class rewrite was rejected for this pass because it would mix parsing, Playwright, resume state, status, and Telegram behavior in one high-risk change. A pure file move was rejected because it can create import cycles and does not prove behavior stayed identical. The chosen split is best because the target helpers already exist and are covered by tests.

### Self-Review Iteration 1

Weakness found: the first design moved too much, including page-fetch orchestration. That would risk Cloudflare/resume behavior. Fix: constrain implementation to pure parsing/pricing/image-url helpers only.

### Self-Review Iteration 2

Weakness found: delegating wrappers could silently stop passing required runtime context such as `logger`, `SKIPPED_ITEMS`, and `_normalize_lyst_product_link`. Fix: add tests that patch helper functions and assert the wrapper passes these arguments.

### Self-Review Iteration 3

Weakness found: removing helper names outright could break other modules/tests importing `GroteskBotTg._upgrade_lyst_image_url` or `GroteskBotTg.extract_shoe_data`. Fix: preserve all old function names as compatibility wrappers and remove only duplicate bodies.

## File Structure

Modify:
- `GroteskBotTg.py`: replace duplicate pure helper bodies with delegation wrappers.
- `tests/test_lyst_runtime.py`: add wrapper-delegation tests.

Do not modify:
- `helpers/lyst/parsing.py`: it already owns parsing logic and has tests.
- `helpers/lyst/pricing.py`: it already owns pricing logic and has tests.
- `helpers/lyst/fetch.py`, `helpers/lyst/cycle.py`, `helpers/lyst/processing.py`: page and run orchestration are later split phases.

## Task 1: Add Wrapper Delegation Tests

- [ ] Add tests to `tests/test_lyst_runtime.py` that patch `GroteskBotTg.lyst_parsing_helpers.upgrade_lyst_image_url`, `GroteskBotTg.lyst_parsing_helpers.image_url_candidates`, and `GroteskBotTg.lyst_parsing_helpers.extract_shoe_data`.
- [ ] Verify tests fail because `GroteskBotTg.py` still uses local duplicate implementations.

## Task 2: Replace Duplicate Helper Bodies

- [ ] In `GroteskBotTg.py`, change `_normalize_currency_token`, `extract_price_tokens`, `_extract_price_tokens_enhanced`, and `_parse_price_amount` to delegate to `helpers.lyst.pricing`.
- [ ] Change `_normalize_image_url`, `_pick_src_from_srcset`, `_extract_image_url_from_tag`, `_upgrade_lyst_image_url`, `_image_url_candidates`, `_dedupe_preserve`, `find_price_strings`, `extract_ldjson_image_map`, and `extract_shoe_data` to delegate to `helpers.lyst.parsing`.
- [ ] Keep compatibility wrapper names so existing runtime call sites and tests still work.
- [ ] Add comments explaining that the wrappers exist to preserve public runtime names while the real implementation lives in the focused helper package.

## Task 3: Verify Behavior

- [ ] Run `python -m unittest tests.test_lyst_runtime tests.test_lyst_parsing tests.test_lyst_pricing -v`.
- [ ] Run `python -m py_compile GroteskBotTg.py helpers\lyst\parsing.py helpers\lyst\pricing.py tests\test_lyst_runtime.py`.
- [ ] Run full local regression suite: `python -m unittest discover -s tests -v`.

## Task 4: Review And Cleanup

- [ ] Review `git diff --stat` and `git diff -- GroteskBotTg.py tests/test_lyst_runtime.py`.
- [ ] If the diff reveals behavior drift, fix it before final reporting.
- [ ] Do not commit unless the user explicitly asks; project rules require asking after local implementation.

## Later Split Phases Not Included In This Pass

After this safe split is verified, the next best phases are: extract `scrape_page`/`scrape_all_pages` into a `helpers/lyst/page_scraper.py` object with injected callbacks; extract `run_lyst_cycle_impl` into `helpers/lyst/service.py`; then turn `GroteskBotTg.py` into a thin service entrypoint. Those phases should each get their own tests and review because they touch Cloudflare, resume, and Telegram behavior.
