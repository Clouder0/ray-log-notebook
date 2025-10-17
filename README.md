# Ray Log Notebook

This is a tiny **FIX** for Ray to display logs under the **last executed cell** instead of **ray.init()** cell.

Q: Why would this happen?\
A: Ipykernel captures thread output and redir to the cell where thread is created.
This is desired in some scenarios, but definitely not for Ray, whose logging thread
is created upon `ray.init()` initialization.

Q: How can you fix this?\
A: Utilizing IPython Events and mock Ray's internal log event handler.
**This may be unstable because we depend on Ray's non-public API.**
In normal mode, Ray would log by emitting logs to `global_worker_stdstream_dispatcher`,
replacing the original handler with ours just works.
For client mode, things get trickier. `client_worker`'s LogStreamClient `log_client`
would receive remote log stream by gRPC and print to stdout/stderr.
We directly **mock replace** the `stdstream` method of that LogStreamClient instance.
There may be more robust and non-intrusive ways to do it,
for example capturing the `ray.init()`
cell output.

Q: How can I use this?
A: As simple as:

```python
% cell 1
import ray
ray.init()

% cell 2
import ray_log_notebook
ray_log_notebook.enable()

% cell 3
@ray.remote
def test_print():
    print("Woola!")

await test_print.remote()
```

![before](./assets/before-ray.png)
![after](./assets/after-ray.png)

Logs will always go to the last executed cell, instead of where the Ray Tasks are created.

Tested on Python 3.13 and Ray 2.50.0, generally should work but I don't have much time to test.
