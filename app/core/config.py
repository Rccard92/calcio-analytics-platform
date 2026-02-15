"""Application configuration. Load from environment."""

import os

from dotenv import load_dotenv

load_dotenv()


def get_database_url() -> str:
    """Return DATABASE_URL from environment. Raises if missing."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable is required")
    return url
