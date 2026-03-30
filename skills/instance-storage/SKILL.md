---
name: instance-storage
description: Check disk usage on the Ubuntu instance, including overall free/used space and the biggest directories under the main server and bot paths. Use when work requires a fast storage-capacity check, finding what is occupying disk, or confirming remaining free space before uploads, backups, or game/server data changes.
---

# Instance Storage

Use this skill when the task is about disk/storage on the Ubuntu instance.

## Preferred workflow

1. Run the bundled script for the standard storage report.
2. If needed, rerun with `--deep` for a larger directory breakdown.
3. Summarize:
   - total disk size
   - used space
   - free space
   - largest relevant directories

## Commands

Quick report:

```powershell
python skills\instance-storage\scripts\check_instance_storage.py
```

Deeper report:

```powershell
python skills\instance-storage\scripts\check_instance_storage.py --deep
```

## Notes

- The script uses the existing `instance-ops` helper; do not hand-write SSH unless the helper is broken.
- The quick report focuses on the most relevant paths:
  - `/`
  - `/home/ubuntu`
  - `/srv`
  - `/var`
  - `/srv/minecraft-fabric-1.21.11`
  - `/home/ubuntu/LystTgFirefox`
- Use `--deep` when the user wants to know what is consuming space inside those roots.
