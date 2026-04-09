# Project Skills

## Available project-local skills

- `instance-ops`: Connect to the Ubuntu bot instance, inspect service status and logs, restart one or all bots, and move files between local and remote for safe edits. Use when work requires checking the instance state, reading `journalctl` logs, restarting `grotesk-market.service` / `grotesk-lyst.service` / `tsekbot.service` / `usefulbot.service` / `svitlobot.service`, or editing, adding, downloading, uploading, or deleting files on the instance. File: `skills/instance-ops/SKILL.md`
- `instance-storage`: Check disk usage on the Ubuntu instance, including overall free/used space and the biggest directories under the main server and bot paths. Use when work requires a fast storage-capacity check, finding what is occupying disk, or confirming remaining free space before uploads, backups, or game/server data changes. File: `skills/instance-storage/SKILL.md`
- `minecraft-skins`: Sync local Minecraft skin PNGs to the instance-hosted authoritative skin library, inspect remote skin-library status, and trigger server-side skin-library reloads after uploads. Use when work requires uploading or validating player skin collections for the Fabric Minecraft server. File: `skills/minecraft-skins/SKILL.md`

## Project Context

- Main runtime target is one Oracle Cloud Ampere A1 Flex PAYG instance with `4 OCPU`, `24 GB RAM`, and `100 GB` storage.
- Cost constraint is strict: prefer designs and operational decisions that keep the setup within Oracle Always Free usage. Do not assume paid upgrades, paid proxies, or extra paid infrastructure unless the user explicitly approves it. The user does not want to pay a cent.
- `GroteskBotTg`, `olx_scraper`, `shafa_scraper`, `svitlo_bot`, and `useful_bot` are effectively personal tools for one person only: the owner chat configured by `DANYLO_DEFAULT_CHAT_ID` (the user's DANYLO chat id).
- `tsek_bot` is used rarely, but when it is used the audience is broader, lower-skill, mixed-age, and Ukrainian-speaking only. Changes there should bias toward clarity, forgiving UX, and Ukrainian text.
- The LYST part of `GroteskBotTg` is the least stable subsystem. On the instance it regularly runs into Cloudflare challenges, so LYST changes should be evaluated with extra care for request rate, retry policy, resume behavior, and false-success status reporting.
- On the instance, OLX and SHAFA listing notifications are sent via `TELEGRAM_OLX_BOT_TOKEN`, while the Lyst/control bot uses `TELEGRAM_BOT_TOKEN`. Command features that act on marketplace messages, such as reply-based unsubscribe, must be wired to both bot identities or they will not see the relevant Telegram updates.
- The intended steady-state runtime split is `grotesk-market.service` for OLX + SHAFA + marketplace commands, and `grotesk-lyst.service` for Lyst + central status/control. Operational changes should preserve that boundary.

## Observed Runtime Scale

- `olx_scraper` usually scraping about `640-642` items per completed run with `0` missing images.
- `shafa_scraper` usually scanning `217` sources per run; many recent runs finished with `0 new / 0 sent`, so SHAFA activity is often sparse even when the scraper is healthy.
- LYST runs are mixed: successful runs process multiple brand/country pages and may still end with very few new items, while failing runs commonly abort early on Cloudflare and report `0 items scraped`.

## How to use project-local skills

- If the user names a project-local skill explicitly, open its `SKILL.md` and follow it.
- Resolve relative paths inside a project-local skill from that skill's folder first.
- Prefer bundled scripts from the skill over retyping SSH/SCP/systemctl commands by hand when they cover the task.
- For normal project work, update code locally first and test locally before any remote deployment.
- Do not use git branches in this project unless the user explicitly asks for them.
- After local changes and tests are done, ask the user whether they want the change committed, merged, and deployed to the instance.
- If the user approves the production path, perform it in this order: commit locally, merge into the production-bound state, then pull it on instance and deploy it on  instance. Do not skip the merge step in wording or execution.
- Only skip the local-first flow when the user explicitly asks for an instance hotfix or when local execution is impossible.
- When work reveals a stable operational fact, workflow rule, infrastructure constraint, or recurring failure mode that future agents would benefit from, consider asking the user whether it should be added to `AGENTS.md`.
