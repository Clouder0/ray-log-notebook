import asyncio
import sys
from io import StringIO
from typing import Optional

import ray
import ray.util.client
from IPython.core.getipython import get_ipython
from ray._private.ray_logging import stderr_deduplicator, stdout_deduplicator

_capture_instance: Optional["RayLogCapture"] = None


async def logging_worker(stdout: asyncio.Queue, stderr: asyncio.Queue):
    async def stdout_logger():
        while True:
            once = await stdout.get()
            sys.stdout.write(once)

    async def stderr_logger():
        while True:
            once = await stderr.get()
            sys.stderr.write(once)

    tasks = [asyncio.create_task(stdout_logger()), asyncio.create_task(stderr_logger())]
    await asyncio.gather(*tasks)


class RayLogCapture:
    def __init__(self, jupyter_loop: asyncio.AbstractEventLoop):
        self._stdout: asyncio.Queue[str] = asyncio.Queue()
        self._stderr: asyncio.Queue[str] = asyncio.Queue()
        self._loop = jupyter_loop

    async def put_stdout(self, log: str):
        await self._stdout.put(log)

    async def put_stderr(self, log: str):
        await self._stderr.put(log)

    def pre_execute(self):
        # print("Pre Execute")
        if hasattr(self, "_logging"):
            if not self._logging.done():
                self._logging.cancel()
        self._logging = asyncio.run_coroutine_threadsafe(
            logging_worker(self._stdout, self._stderr), self._loop
        )

    def register_ray(self):
        """Infer Client or Local mode and register corresponding ray hooks."""
        client_worker = ray.util.client.ray.get_context().client_worker
        if client_worker is None:
            print("Ray runs in local mode. Register hooks.")
            self.register_ray_local()
        else:
            print("Ray runs in Client mode. Register hooks.")
            self.register_ray_client(client_worker)

    def register_ray_client(self, client_worker):
        loop = self._loop

        def mock_stdstream(level: int, msg: str):
            """Log the stdout/stderr entry from the log stream.
            By default, calls print but this can be overridden.

            Args:
                level: The loglevel of the received log message
                msg: The content of the message
            """
            if len(msg) == 0:
                return
            if level == -2:
                # stderr
                asyncio.run_coroutine_threadsafe(self.put_stderr(msg), loop)
            else:
                # stdout
                asyncio.run_coroutine_threadsafe(self.put_stdout(msg), loop)

        # replace the LogStreamClient stdstream method, note it runs on a separate thread
        client_worker.log_client.stdstream = mock_stdstream

    def register_ray_local(self):
        from ray._private.worker import (
            global_worker_stdstream_dispatcher,  # type: ignore
            print_worker_logs,
        )

        # remove original print logs
        global_worker_stdstream_dispatcher.remove_handler("ray_print_logs")
        loop = self._loop

        # this function is copied from ray._private, we replace the entire event handler
        def ray_print_logs(data):
            should_dedup = data.get("pid") not in ["autoscaler"]

            out_stdout = StringIO()
            out_stderr = StringIO()
            if data["is_err"]:
                if should_dedup:
                    batches = stderr_deduplicator.deduplicate(data)
                else:
                    batches = [data]
                sink = out_stderr
            else:
                if should_dedup:
                    batches = stdout_deduplicator.deduplicate(data)
                else:
                    batches = [data]
                sink = out_stdout
            for batch in batches:
                print_worker_logs(batch, sink)
            out_stdout_str = out_stdout.getvalue()
            out_stderr_str = out_stderr.getvalue()
            if len(out_stdout_str) > 0:
                asyncio.run_coroutine_threadsafe(
                    self.put_stdout(out_stdout_str), loop=loop
                )
            if len(out_stderr_str) > 0:
                asyncio.run_coroutine_threadsafe(
                    self.put_stdout(out_stderr_str), loop=loop
                )

        global_worker_stdstream_dispatcher.add_handler("ray_print_logs", ray_print_logs)

    def register_ipython(self):
        ip = get_ipython()
        assert ip is not None
        ip.events.register("pre_execute", self.pre_execute)


def enable():
    """Enable ray log capture in jupyter notebook."""
    global _capture_instance
    if _capture_instance is not None:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        print("Jupyter event loop not found. Try to run in a notebook.")
        return
    _capture_instance = RayLogCapture(loop)
    _capture_instance.register_ray()
    _capture_instance.register_ipython()
