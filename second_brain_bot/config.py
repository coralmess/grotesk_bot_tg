from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from helpers.runtime_paths import SECOND_BRAIN_VAULT_DIR

DEFAULT_VAULT_DIR = SECOND_BRAIN_VAULT_DIR


@dataclass(frozen=True)
class SecondBrainConfig:
    bot_token: str
    owner_chat_id: int
    vault_dir: Path
    digest_hour: int
    digest_tz: str
    modal_glm_api_key: str
    cerebras_api_key: str
    groq_api_key: str
    modal_glm_base_url: str = "https://api.us-west-2.modal.direct/v1"
    modal_glm_model: str = "zai-org/GLM-5.1-FP8"
    cerebras_base_url: str = "https://api.cerebras.ai/v1"
    cerebras_model: str = "qwen-3-235b-a22b-instruct-2507"
    groq_base_url: str = "https://api.groq.com/openai/v1"
    groq_model: str = "qwen/qwen3-32b"


def load_config() -> SecondBrainConfig:
    load_dotenv()
    token = os.getenv("SECOND_BRAIN_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing SECOND_BRAIN_BOT_TOKEN in .env")
    chat_id_raw = os.getenv("DANYLO_DEFAULT_CHAT_ID", "").strip()
    if not chat_id_raw:
        raise RuntimeError("Missing DANYLO_DEFAULT_CHAT_ID in .env")
    try:
        owner_chat_id = int(chat_id_raw)
    except ValueError as exc:
        raise RuntimeError("DANYLO_DEFAULT_CHAT_ID must be an integer") from exc
    return SecondBrainConfig(
        bot_token=token,
        owner_chat_id=owner_chat_id,
        vault_dir=Path(os.getenv("SECOND_BRAIN_VAULT_DIR", str(DEFAULT_VAULT_DIR))).expanduser(),
        digest_hour=_int_env("SECOND_BRAIN_DIGEST_HOUR", 13, min_value=0, max_value=23),
        digest_tz=os.getenv("SECOND_BRAIN_DIGEST_TZ", "Europe/Kyiv").strip() or "Europe/Kyiv",
        modal_glm_api_key=os.getenv("MODAL_GLM_API_KEY", "").strip(),
        cerebras_api_key=os.getenv("CEREBRAS_API_KEY", "").strip(),
        groq_api_key=os.getenv("GROQ_API_KEY", "").strip(),
        modal_glm_base_url=os.getenv("MODAL_GLM_BASE_URL", "https://api.us-west-2.modal.direct/v1").strip(),
        modal_glm_model=os.getenv("MODAL_GLM_MODEL", "zai-org/GLM-5.1-FP8").strip(),
        cerebras_base_url=os.getenv("CEREBRAS_BASE_URL", "https://api.cerebras.ai/v1").strip(),
        cerebras_model=os.getenv("CEREBRAS_MODEL", "qwen-3-235b-a22b-instruct-2507").strip(),
        groq_base_url=os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1").strip(),
        groq_model=os.getenv("GROQ_MODEL", "qwen/qwen3-32b").strip(),
    )


def _int_env(name: str, default: int, *, min_value: int, max_value: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        return default
    return min(max(value, min_value), max_value)
