"""Project-level configuration helpers for the Quarto template.

NOTE: This file is provided as an EXAMPLE for ADVANCED USERS ONLY.
Most students will NOT need to modify this file for their projects.

This module centralises environment configuration so users can adjust
Python imports, logging, plotting defaults, etc., in a single place.
It is automatically imported and executed during report rendering.

Common use cases for advanced users:
- Setting random seeds for reproducibility (numpy, random)
- Loading environment variables from .env files
- Configuring custom plotting styles (matplotlib rcParams)
- Setting up database connections or API credentials

Functions defined here are imported from `report/report.qmd` during the
setup chunk. Feel free to extend this file with project-specific
helpers if needed.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path


def configure_environment(project_root: Path | None = None) -> None:
    """Configure the Python runtime for rendering the report.

    This function is intentionally lightweight. It sets up logging,
    ensures expected directories exist, and can be extended with
    additional project-specific configuration. Students may adapt this
    function to register custom plotting styles, load environment
    variables, or perform other startup routines.
    """

    if project_root is None:
        project_root = Path.cwd()

    _configure_logging()
    _ensure_data_directories(project_root)

    # Example: set a deterministic random seed if the project uses RNG.
    # import random
    # import numpy as np
    # random.seed(42)
    # np.random.seed(42)


def _configure_logging() -> None:
    """Ensure a basic logging configuration is available."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def _ensure_data_directories(project_root: Path) -> None:
    """Create processed data directory if it does not exist."""

    processed_dir = project_root / "data" / "processed"
    try:
        processed_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logging.getLogger(__name__).warning(
            "Unable to create processed data directory %s: %s",
            processed_dir,
            exc,
        )

    # Example: load environment variables from a `.env` file if present.
    dotenv_path = project_root / ".env"
    if dotenv_path.is_file():
        _load_dotenv(dotenv_path)


def _load_dotenv(dotenv_path: Path) -> None:
    """Load key-value pairs from a `.env` file into the environment."""

    with dotenv_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            if key:
                os.environ.setdefault(key.strip(), value.strip())$


