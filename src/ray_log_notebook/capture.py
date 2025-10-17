import asyncio
import sys
from asyncio import Queue
from io import StringIO

import ray
import ray.util.client
from IPython.core.getipython import get_ipython
from ray._private.ray_logging import stderr_deduplicator, stdout_deduplicator

_capture_instance: "RayLogCapture | None" = None


async def logging_worker(q_stdout: Queue, q_stderr: Queue):
    async def stdout_logger():
        while True:
            once = await q_stdout.get()
            sys.stdout.write(once)

    async def stderr_logger():
        while True:
            once = await q_stderr.get()
            sys.stderr.write(once)

    await asyncio.gather(stdout_logger(), stderr_logger())  # run forever


class RayLogCapture:
    def __init__(self, jupyter_loop: asyncio.AbstractEventLoop):
        self._q_stdout: Queue[str] = Queue()
        self._q_stderr: Queue[str] = Queue()
        self._loop = jupyter_loop
        self._loop.set_task_factory(asyncio.eager_task_factory)

    async def put_stdout(self, log: str):
        await self._q_stdout.put(log)

    async def put_stderr(self, log: str):
        await self._q_stderr.put(log)

    def pre_run_cell(self, _):
        # single thread, we don't care much about thread safety here
        if hasattr(self, "logger"):
            self.logger.cancel()
        self.logger = self._loop.create_task(
            logging_worker(
                self._q_stdout,
                self._q_stderr,
            )
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

        # replace the LogStreamClient stdstream method,
        # note it runs on a separate thread
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
        def ray_log_to_notebook(data):
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
                    self.put_stderr(out_stderr_str), loop=loop
                )

        global_worker_stdstream_dispatcher.add_handler(
            "ray_print_logs", ray_log_to_notebook
        )

    def register_ipython(self):
        ip = get_ipython()
        assert ip is not None
        ip.events.register("pre_run_cell", self.pre_run_cell)


def enable():
    """Enable ray log capture in jupyter notebook. Must be called after ray.init()."""
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
