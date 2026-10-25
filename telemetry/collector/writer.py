"""Append-only JSONL output for one run."""
from __future__ import annotations

import json
import os


class RunWriter:
    def __init__(self, directory: str):
        os.makedirs(directory, exist_ok=True)
        self.directory = directory
        self._handles = {}

    def append(self, name: str, record: dict) -> None:
        handle = self._handles.get(name)
        if handle is None:
            handle = open(os.path.join(self.directory, name), "a", encoding="utf-8")
            self._handles[name] = handle
        handle.write(json.dumps(record, separators=(",", ":")) + "\n")
        handle.flush()

    def write_json(self, name: str, record: dict) -> None:
        path = os.path.join(self.directory, name)
        temp = path + ".tmp"
        with open(temp, "w", encoding="utf-8") as handle:
            json.dump(record, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp, path)

    def close(self) -> None:
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()
