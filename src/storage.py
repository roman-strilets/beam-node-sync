"""JSON-lines output helpers for UTXO export."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import TextIO


class JsonLineWriter:
    """Write one JSON object per line to stdout or a file."""

    def __init__(self, output_path: str | None):
        self._should_close = output_path is not None
        self.path = output_path
        if output_path is None:
            self._stream: TextIO = sys.stdout
            return

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._stream = path.open("w", encoding="utf-8")

    def __enter__(self) -> "JsonLineWriter":
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def write(self, record: object) -> None:
        """Serialize ``record`` as a JSON line and flush immediately."""
        if hasattr(record, "as_dict"):
            payload = record.as_dict()  # type: ignore[assignment]
        elif isinstance(record, dict):
            payload = record
        else:
            raise TypeError("record must be a dict or expose as_dict()")
        json.dump(payload, self._stream, separators=(",", ":"), sort_keys=True)
        self._stream.write("\n")
        self._stream.flush()

    def close(self) -> None:
        """Close the underlying stream when it is file-backed."""
        if self._should_close:
            self._stream.close()