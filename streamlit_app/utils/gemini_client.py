"""
MeterFlow IQ - Gemini Developer API helper.

Gemini is optional and invoked only from an explicit Streamlit button click.
The default RCA workflow remains rules-based and read-only.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None  # type: ignore[assignment]

try:
    from google import genai
    from google.genai import types
except ImportError:  # pragma: no cover - keeps the app read-only if SDK is absent
    genai = None  # type: ignore[assignment]
    types = None  # type: ignore[assignment]


REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = REPO_ROOT / ".env"

DEFAULT_GEMINI_MODEL = "gemini-3.5-flash"
DEFAULT_MAX_OUTPUT_TOKENS = 800
DEFAULT_TEMPERATURE = 0.2


def _load_local_env() -> None:
    """
    Load local .env for developer mode.
    """
    if load_dotenv is not None and ENV_PATH.exists():
        load_dotenv(ENV_PATH, override=True)


def _env(name: str, default: str | None = None) -> str | None:
    _load_local_env()
    value = os.getenv(name, default)

    if value is None:
        return None

    value = str(value).strip()
    return value if value else None


def _truthy(value: str | None) -> bool:
    if value is None:
        return False

    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _gemini_api_key_present() -> bool:
    return bool(_env("GEMINI_API_KEY"))


def gemini_enabled() -> bool:
    """
    Return True only when Gemini is explicitly enabled and configured.
    """
    if not _truthy(_env("GEMINI_ENABLED", "false")):
        return False

    if genai is None:
        return False

    return _gemini_api_key_present()


def gemini_demo_pin_configured() -> bool:
    """
    Return True when a local demo PIN gate is configured.
    """
    return bool(_env("GEMINI_DEMO_PIN"))


def gemini_demo_pin_matches(pin_value: str) -> bool:
    """
    Check a user-entered demo PIN without exposing the configured value.
    """
    configured_pin = _env("GEMINI_DEMO_PIN")

    if not configured_pin:
        return True

    return str(pin_value or "").strip() == configured_pin


def get_gemini_caption() -> str:
    """
    Return a safe Streamlit caption for Gemini configuration state.
    """
    if not _truthy(_env("GEMINI_ENABLED", "false")):
        return "Gemini Developer API: disabled. Rules-based RCA remains the default."

    if genai is None:
        return "Gemini Developer API: enabled but google-genai is not installed."

    if not _gemini_api_key_present():
        return "Gemini Developer API: enabled but GEMINI_API_KEY is not configured."

    model_name = _env("GEMINI_MODEL", DEFAULT_GEMINI_MODEL) or DEFAULT_GEMINI_MODEL
    return f"Gemini Developer API: enabled for explicit analyst-triggered calls using {model_name}."


def _get_generation_config() -> Any:
    if types is not None and hasattr(types, "GenerateContentConfig"):
        return types.GenerateContentConfig(
            temperature=DEFAULT_TEMPERATURE,
            max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
        )

    return {
        "temperature": DEFAULT_TEMPERATURE,
        "max_output_tokens": DEFAULT_MAX_OUTPUT_TOKENS,
    }


def _extract_response_text(response: Any) -> str:
    try:
        text = getattr(response, "text", None)
        if text:
            return str(text).strip()
    except Exception:
        pass

    try:
        candidates = getattr(response, "candidates", None) or []
        parts = candidates[0].content.parts
        text_parts = [
            str(getattr(part, "text", "")).strip()
            for part in parts
            if getattr(part, "text", None)
        ]
        return "\n".join(text_parts).strip()
    except Exception:
        return ""


def generate_gemini_rca(prompt_text: str) -> dict[str, Any]:
    """
    Generate an optional Gemini RCA response from a facts-only prompt.

    This function never exposes the API key and returns safe error messages.
    """
    if not gemini_enabled():
        return {
            "ok": False,
            "response_text": "",
            "message": get_gemini_caption(),
        }

    clean_prompt = str(prompt_text or "").strip()
    if not clean_prompt:
        return {
            "ok": False,
            "response_text": "",
            "message": "Gemini prompt is empty.",
        }

    api_key = _env("GEMINI_API_KEY")
    model_name = _env("GEMINI_MODEL", DEFAULT_GEMINI_MODEL) or DEFAULT_GEMINI_MODEL

    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=model_name,
            contents=clean_prompt,
            config=_get_generation_config(),
        )

        response_text = _extract_response_text(response)
        if not response_text:
            return {
                "ok": False,
                "response_text": "",
                "message": "Gemini returned an empty response.",
                "model": model_name,
            }

        return {
            "ok": True,
            "response_text": response_text,
            "message": "Gemini RCA summary generated.",
            "model": model_name,
        }
    except Exception:
        return {
            "ok": False,
            "response_text": "",
            "message": "Gemini request failed. Check API configuration and quota.",
            "model": model_name,
        }
