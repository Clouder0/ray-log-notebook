from __future__ import annotations

import io
import os
import threading
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional, Set


@dataclass
class _TailState:
    file: io.TextIOBase
    position: int


class DirectoryTailer:
    """Poll a directory and forward appended text from matching files.

    The tailer keeps each matching file open and periodically checks for new
    data. Whenever it finds appended content it forwards it to ``callback``
    together with the absolute ``Path`` of the file.
    """

    def __init__(
        self,
        directory: Path,
        callback: Callable[[Path, str], None],
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        poll_interval: float = 0.25,
        encoding: str = "utf-8",
    ) -> None:
        self._directory = Path(directory)
        self._callback = callback
        self._include = tuple(include) if include is not None else tuple()
        self._exclude = tuple(exclude) if exclude is not None else tuple()
        self._poll_interval = max(0.05, float(poll_interval))
        self._encoding = encoding

        self._states: Dict[Path, _TailState] = {}
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._thread is not None:
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="ray-log-tail",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        thread: Optional[threading.Thread]
        with self._lock:
            thread = self._thread
            self._thread = None
            self._stop_event.set()

        if thread is not None:
            thread.join(timeout=self._poll_interval * 4)

        for state in list(self._states.values()):
            try:
                state.file.close()
            except OSError:
                pass
        self._states.clear()

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._scan_directory()
                self._drain_files()
            except Exception:
                # We do not want background logging to crash the notebook.
                # If something goes wrong we simply retry on the next tick.
                # (Optional: add debug printing here in the future.)
                pass
            self._stop_event.wait(self._poll_interval)

    # Internal helpers -------------------------------------------------
    def _scan_directory(self) -> None:
        if not self._directory.exists():
            return

        seen: Set[Path] = set()
        for entry in self._directory.iterdir():
            if not entry.is_file():
                continue
            if self._include and not any(
                fnmatch(entry.name, pattern) for pattern in self._include
            ):
                continue
            if self._exclude and any(
                fnmatch(entry.name, pattern) for pattern in self._exclude
            ):
                continue

            seen.add(entry)
            if entry not in self._states:
                self._states[entry] = self._open_state(entry)

        # Clean up states that disappeared.
        for path in list(self._states):
            if path not in seen or not path.exists():
                state = self._states.pop(path)
                try:
                    state.file.close()
                except OSError:
                    pass

    def _open_state(self, path: Path) -> _TailState:
        # Open file in text mode. We want to ignore decode errors to avoid
        # crashing if Ray emits mixed encodings.
        file_obj = path.open("r", encoding=self._encoding, errors="replace")
        file_obj.seek(0, os.SEEK_END)
        return _TailState(file=file_obj, position=file_obj.tell())

    def _drain_files(self) -> None:
        for path, state in list(self._states.items()):
            file_obj = state.file
            try:
                current_size = path.stat().st_size
            except OSError:
                current_size = state.position

            if current_size < state.position:
                # File was truncated or rotated; reopen and start from end.
                try:
                    state.file.close()
                except OSError:
                    pass
                self._states[path] = self._open_state(path)
                continue

            try:
                file_obj.seek(state.position)
                chunk = file_obj.read()
                new_pos = file_obj.tell()
            except OSError:
                continue

            if chunk:
                self._callback(path, chunk)
            state.position = new_pos
