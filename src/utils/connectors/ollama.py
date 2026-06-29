from __future__ import annotations
import os
from typing import Any
import requests
from src.utils.logger import logger


class Ollama:
    """Singleton wrapper around a local Ollama chat endpoint."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return

        self.base_url = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434").rstrip("/")
        self.model = os.getenv("OLLAMA_MODEL", "llama3.2:latest")
        self.timeout = float(os.getenv("OLLAMA_TIMEOUT", "120"))

        logger.announcement("Initializing Ollama Client...", "info")
        logger.info(f"Ollama host={self.base_url} model={self.model}")

        self._initialized = True
        logger.announcement("Ollama Client initialized", "success")

    def chat(self, messages: list[dict[str, Any]]):
        """Send a list of chat messages to Ollama and return the assistant response."""
        try:
            logger.info(f"User is sending messages to Ollama: {messages}")
            response = requests.post(
                f"{self.base_url}/api/chat",
                json={
                    "model": self.model,
                    "messages": messages,
                    "stream": False,
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = response.json()
            message = ((data.get("message") or {}).get("content") or "").strip()
            result = {
                "model": self.model,
                "messages": [
                    {
                        "message": message,
                        "role": "AIMessage",
                    }
                ],
            }
            logger.info(f"Ollama responded with: {result}")
            return result
        except Exception as e:
            logger.error(f"Error in Ollama chat: {e}")
            raise Exception(f"Error in Ollama chat: {e}")
