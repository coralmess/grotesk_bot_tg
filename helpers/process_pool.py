from __future__ import annotations

import asyncio
import os
import pickle
from concurrent.futures import ProcessPoolExecutor
from functools import partial

_PROCESS_POOL: ProcessPoolExecutor | None = None


def recommended_process_workers() -> int:
    cpu_count = os.cpu_count() or 1
    return max(1, min(4, cpu_count))


def get_process_pool() -> ProcessPoolExecutor:
    global _PROCESS_POOL
    if _PROCESS_POOL is None:
        _PROCESS_POOL = ProcessPoolExecutor(max_workers=recommended_process_workers())
    return _PROCESS_POOL


async def run_cpu_bound(func, /, *args, **kwargs):
    loop = asyncio.get_running_loop()
    call = partial(func, *args, **kwargs)
    try:
        pickle.dumps(call)
    except Exception:
        return await asyncio.to_thread(call)
    return await loop.run_in_executor(get_process_pool(), call)
