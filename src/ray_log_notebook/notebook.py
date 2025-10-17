from __future__ import annotations

import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Optional, Sequence

from ._tail import DirectoryTailer

try:
    from ipywidgets import Output
    from IPython.display import display
    from IPython import get_ipython
except ImportError:  # pragma: no cover - optional dependency
    Output = None
    display = None

    def get_ipython():  # type: ignore
        return None


def _ensure_notebook_deps() -> None:
    if Output is None or display is None:
        raise ImportError(
            "ipywidgets and IPython are required for notebook log streaming. "
            "Install the 'note' dependency group or run inside Jupyter."
        )


def _find_ray_log_dir() -> Path:
    """Return the Ray session log directory for the current driver.

    We rely on the public ``ray._private`` namespace because Ray does not expose
    a stable API for this. The code is defensive and raises a clear error if
    Ray is not initialised yet.
    """

    try:
        import ray
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise ImportError("Ray must be installed to stream Ray logs.") from exc

    if not ray.is_initialized():
        raise RuntimeError(
            "Ray is not initialised. Call ray.init() before streaming logs."
        )

    # Access global_worker to obtain the CoreWorker reference. The attribute is
    # lazily defined once Ray is initialised, hence the getattr guard.
    from ray._private import worker  # type: ignore

    global_node = getattr(worker, "_global_node", None)
    if global_node and "session_dir" in global_node.address_info:
        return Path(global_node.address_info["session_dir"]) / "logs"

    global_worker = getattr(worker, "global_worker", None)
    if global_worker and hasattr(global_worker, "core_worker"):
        core_worker = global_worker.core_worker
        if core_worker is not None:
            session_path = Path(core_worker.get_session_dir_path())
            return session_path / "logs"

    raise RuntimeError(
        "Ray does not appear to be initialised. Call ray.init() before enabling log streaming."
    )


class _NotebookTailer:
    """Glue between ``DirectoryTailer`` and a notebook ``Output`` widget."""

    def __init__(
        self,
        include: Optional[Iterable[str]],
        exclude: Optional[Iterable[str]],
        poll_interval: float,
        show_filename: bool,
    ) -> None:
        _ensure_notebook_deps()
        self.output = Output()
        self._show_filename = show_filename
        self._lock = threading.Lock()
        self._tailer = DirectoryTailer(
            directory=_find_ray_log_dir(),
            include=include,
            exclude=exclude,
            poll_interval=poll_interval,
            callback=self._emit,
        )

    def display(self) -> None:
        display(self.output)
        self._tailer.start()

    def stop(self) -> None:
        self._tailer.stop()

    def _emit(self, path: Path, chunk: str) -> None:
        text = chunk.replace("\r\n", "\n")
        lines = text.splitlines()
        if not lines:
            return

        with self._lock:
            for idx, line in enumerate(lines):
                prefix = ""
                if self._show_filename:
                    prefix = f"[{path.name}] "
                # Preserve trailing newlines Ray provides by adding them back.
                suffix = "\n" if idx < len(lines) - 1 or chunk.endswith("\n") else ""
                self.output.append_stdout(f"{prefix}{line}{suffix}")


class _AutoCellManager:
    """Register IPython hooks to automatically attach log outputs per cell."""

    def __init__(
        self,
        include: Optional[Sequence[str]],
        exclude: Optional[Sequence[str]],
        poll_interval: float,
        show_filename: bool,
    ) -> None:
        self._include = include
        self._exclude = exclude
        self._poll_interval = poll_interval
        self._show_filename = show_filename
        self._current: Optional[_NotebookTailer] = None

    # IPython hook callbacks -------------------------------------------------
    def pre_run_cell(self) -> None:
        # Shut down the previous tailer (if any) before displaying output for
        # the current cell. This ensures upcoming logs attach to the new cell.
        self._stop_current()
        self._current = _NotebookTailer(
            include=self._include,
            exclude=self._exclude,
            poll_interval=self._poll_interval,
            show_filename=self._show_filename,
        )
        self._current.display()

    def post_run_cell(self) -> None:
        # Keep streaming worker logs to the output created for the cell that
        # just finished. We leave the tailer running until the next cell starts.
        pass

    def stop(self) -> None:
        self._stop_current()

    # Internal helpers -------------------------------------------------------
    def _stop_current(self) -> None:
        if self._current is not None:
            self._current.stop()
            self._current = None


_STATE: dict[str, object] = {}


def enable_notebook_logs(
    include: Optional[Sequence[str]] = ("worker-*.out", "worker-*.err"),
    exclude: Optional[Sequence[str]] = None,
    poll_interval: float = 0.25,
    show_filename: bool = True,
) -> None:
    """Start streaming Ray worker logs into the currently executed notebook cell.

    Once enabled the next executed cell spawns a dedicated output widget that
    tails the Ray session logs. Each subsequent cell gets its own output widget
    so worker log lines always appear underneath the cell that triggered them.
    """

    _ensure_notebook_deps()

    ip = get_ipython()
    if ip is None or not hasattr(ip, "events"):
        raise RuntimeError(
            "enable_notebook_logs() must be called from a Jupyter/IPython environment."
        )

    disable_notebook_logs()

    manager = _AutoCellManager(
        include=include,
        exclude=exclude,
        poll_interval=poll_interval,
        show_filename=show_filename,
    )
    ip.events.register("pre_run_cell", manager.pre_run_cell)
    ip.events.register("post_run_cell", manager.post_run_cell)

    _STATE["manager"] = manager
    _STATE["ip"] = ip


def disable_notebook_logs() -> None:
    """Remove hooks installed by :func:`enable_notebook_logs` and stop tailing."""

    manager = _STATE.pop("manager", None)
    ip = _STATE.pop("ip", None)
    if manager is not None:
        manager.stop()
    if ip is not None and hasattr(ip, "events"):
        for event, handler in (
            ("pre_run_cell", getattr(manager, "pre_run_cell", None)),
            ("post_run_cell", getattr(manager, "post_run_cell", None)),
        ):
            if handler is not None:
                try:
                    ip.events.unregister(event, handler)
                except ValueError:
                    # Already unregistered
                    pass


@contextmanager
def stream_logs(
    include: Optional[Sequence[str]] = ("worker-*.out", "worker-*.err"),
    exclude: Optional[Sequence[str]] = None,
    poll_interval: float = 0.25,
    show_filename: bool = True,
):
    """Context manager to tail Ray logs within a single notebook cell.

    Example
    -------
    ```python
    with stream_logs():
        ray.get(actor.run.remote())
    ```
    """

    _ensure_notebook_deps()

    tailer = _NotebookTailer(
        include=include,
        exclude=exclude,
        poll_interval=poll_interval,
        show_filename=show_filename,
    )
    tailer.display()
    try:
        yield
    finally:
        tailer.stop()
