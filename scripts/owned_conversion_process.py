"""Linux-only acceptance helper: pause a real hictk child of an owned test JVM.

No executable, process output or job state is replaced. A stopped child makes the
tiny fixture's otherwise brief conversion stage observable before UI cancellation.
The guard owns only a direct child identified by PID, parent and kernel start time.
"""

import os
from pathlib import Path
import signal
import sys
import threading
import time


class PausedHictkChild:
    def __init__(self, parent):
        if not sys.platform.startswith("linux"):
            raise RuntimeError("Live cancellation acceptance requires Linux /proc")
        self.parent = parent
        self.identity = None
        self.ready = threading.Event()
        self.stop = threading.Event()
        self.error = None
        self.thread = None

    @staticmethod
    def process_state(pid):
        # comm can contain spaces and parentheses; fields after the final ')' are fixed.
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return fields[0], int(fields[1]), fields[19]

    def alive(self):
        if not self.identity:
            return False
        try:
            state, parent, started = self.process_state(self.identity["pid"])
            return state != "Z" and started == self.identity["startTicks"]
        except (FileNotFoundError, ProcessLookupError):
            return False

    def monitor(self):
        try:
            while not self.stop.is_set():
                tasks = Path(f"/proc/{self.parent}/task")
                children = set()
                for task in tasks.iterdir():
                    try:
                        children.update(
                            map(int, (task / "children").read_text().split())
                        )
                    except FileNotFoundError:
                        continue
                for pid in children:
                    try:
                        proc = Path(f"/proc/{pid}")
                        command = proc.joinpath("cmdline").read_bytes().split(b"\0")
                        executable = Path(os.readlink(proc / "exe")).name
                        if executable != "hictk" or not any(
                            part in (b"zoomify", b"balance") for part in command
                        ):
                            continue
                        _, parent, started = self.process_state(pid)
                        if parent != self.parent:
                            continue
                        self.identity = {
                            "pid": pid,
                            "parent": parent,
                            "startTicks": started,
                            "command": [part.decode() for part in command if part],
                        }
                        os.kill(pid, signal.SIGSTOP)
                        state, parent_after, started_after = self.process_state(pid)
                        if (parent_after, started_after) != (parent, started):
                            raise RuntimeError("Process identity changed during pause")
                        deadline = time.monotonic() + 2
                        while state not in ("T", "t"):
                            if time.monotonic() > deadline:
                                raise TimeoutError("Owned hictk child did not stop")
                            time.sleep(0.001)
                            state, _, _ = self.process_state(pid)
                        self.ready.set()
                        return
                    except (FileNotFoundError, ProcessLookupError):
                        self.identity = None
                self.stop.wait(0.002)
        except BaseException as error:
            self.error = repr(error)
            self.ready.set()

    def __enter__(self):
        self.thread = threading.Thread(target=self.monitor, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=5)
        if self.alive():
            # Failure cleanup never leaves the process paused for server shutdown.
            try:
                os.kill(self.identity["pid"], signal.SIGCONT)
            except ProcessLookupError:
                pass
