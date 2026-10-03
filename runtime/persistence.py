"""
runtime/persistence.py

Durable Task Checkpoint Store (Phase 7 — Memory & Persistence).

Provides CheckpointStore abstract base class and FileCheckpointStore implementation.
Serializes task checkpoints to JSON files under a configurable directory.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("ccs.persistence")


class CheckpointStore(ABC):
    """Abstract interface for task checkpoint persistence."""

    @abstractmethod
    def save(self, task_id: str, checkpoint_dict: dict[str, Any]) -> None:
        """Save a task checkpoint dict."""
        pass

    @abstractmethod
    def load(self, task_id: str) -> Optional[dict[str, Any]]:
        """Load a task checkpoint dict by task_id. Returns None if not found."""
        pass

    @abstractmethod
    def delete(self, task_id: str) -> None:
        """Delete a task checkpoint by task_id."""
        pass


class FileCheckpointStore(CheckpointStore):
    """
    File-based JSON checkpoint store under a configurable directory.
    Atomic writes prevent corrupted reads.
    """

    def __init__(self, dir_path: str | Path) -> None:
        self.dir_path = Path(dir_path)
        self.dir_path.mkdir(parents=True, exist_ok=True)

    def _get_path(self, task_id: str) -> Path:
        safe_id = "".join(c for c in task_id if c.isalnum() or c in ("-", "_"))
        return self.dir_path / f"{safe_id}.json"

    def save(self, task_id: str, checkpoint_dict: dict[str, Any]) -> None:
        file_path = self._get_path(task_id)
        temp_path = file_path.with_suffix(".json.tmp")
        try:
            with temp_path.open("w", encoding="utf-8") as f:
                json.dump(checkpoint_dict, f, indent=2)
            temp_path.replace(file_path)
            logger.debug("saved checkpoint for task %s to %s", task_id, file_path)
        except Exception as exc:
            logger.error("failed to save checkpoint for task %s: %s", task_id, exc)
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass
            raise

    def load(self, task_id: str) -> Optional[dict[str, Any]]:
        file_path = self._get_path(task_id)
        if not file_path.exists():
            return None
        try:
            with file_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            logger.debug("loaded checkpoint for task %s from %s", task_id, file_path)
            return data
        except Exception as exc:
            logger.error("failed to load checkpoint for task %s: %s", task_id, exc)
            return None

    def delete(self, task_id: str) -> None:
        file_path = self._get_path(task_id)
        if file_path.exists():
            try:
                file_path.unlink()
                logger.debug("deleted checkpoint for task %s", task_id)
            except OSError as exc:
                logger.error("failed to delete checkpoint for task %s: %s", task_id, exc)
