"""Run `cdcctl.sh` on the two nodes: through SSM Run Command, or locally for tests.

AWS mode reads the node ids, bucket and cluster id from scripts/tf.sh output
(override with CDC_NODES_JSON, a file holding the same JSON).
Local mode: set CDC_LOCAL=<dir made by local_e2e_setup.sh>.

CLI:  python cdc_remote.py <a|b> <cdcctl command...>
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
BASH = os.getenv("BASH_EXE") or ("C:/Program Files/Git/bin/bash.exe" if os.name == "nt" else "bash")
PROFILE = os.getenv("AWS_PROFILE", "cdc-cache-poc")
TERMINAL = {"Success", "Failed", "Cancelled", "TimedOut", "Undeliverable", "Terminated",
            "DeliveryTimedOut", "ExecutionTimedOut"}


class RemoteError(RuntimeError):
    pass


def aws(region: str, *args: str) -> str:
    cmd = ["aws", *args, "--profile", PROFILE, "--region", region]
    # the CLI otherwise fails on non-cp1252 characters in node output on Windows
    env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
    for attempt in range(4):
        p = subprocess.run(cmd, capture_output=True, encoding="utf-8", errors="replace", env=env)
        # local credential refresh can fail transiently; the request was never sent
        if p.returncode == 0 or "AUTH_CREDENTIALS" not in p.stderr:
            break
        time.sleep(5 * (attempt + 1))
    if p.returncode != 0:
        raise RemoteError(f"{' '.join(args[:2])}: {p.stderr.strip()}")
    return p.stdout


def load_nodes() -> dict:
    """{"region_a", "region_b", "instance_a", "instance_b", "bucket", "cluster_id"}"""
    if f := os.getenv("CDC_NODES_JSON"):
        return json.loads(Path(f).read_text(encoding="utf-8"))
    p = subprocess.run([BASH, (HERE / "tf.sh").as_posix(), "output", "-json", "nodes"],
                       capture_output=True, text=True)
    if p.returncode != 0:
        raise RemoteError(f"terraform output failed: {p.stderr.strip()}")
    return json.loads(p.stdout)


class Ssm:
    def __init__(self, nodes: dict | None = None, poll_s: float = 3.0) -> None:
        self.n = nodes or load_nodes()
        self.poll_s = poll_s

    def start(self, side: str, args: list[str], timeout_s: int = 1200) -> tuple[str, str]:
        script = "/opt/cdc/cdcctl.sh " + " ".join(shlex.quote(a) for a in args)
        params = json.dumps({"commands": [script], "executionTimeout": [str(timeout_s)]})
        out = aws(self.n[f"region_{side}"], "ssm", "send-command",
                  "--document-name", "AWS-RunShellScript",
                  "--instance-ids", self.n[f"instance_{side}"],
                  "--parameters", params, "--query", "Command.CommandId", "--output", "text")
        return side, out.strip()

    def wait(self, handle: tuple[str, str], timeout_s: int = 1200) -> tuple[int, str, str]:
        side, cid = handle
        deadline = time.time() + timeout_s + 60
        while time.time() < deadline:
            time.sleep(self.poll_s)
            try:
                inv = json.loads(aws(self.n[f"region_{side}"], "ssm", "get-command-invocation",
                                     "--command-id", cid, "--instance-id", self.n[f"instance_{side}"],
                                     "--output", "json"))
            except RemoteError as e:
                if "InvocationDoesNotExist" in str(e):   # not registered yet
                    continue
                raise
            if inv["Status"] in TERMINAL:
                rc = inv["ResponseCode"] if inv["Status"] in ("Success", "Failed") else 255
                return rc, inv["StandardOutputContent"], inv["StandardErrorContent"]
        raise RemoteError(f"command {cid} on {side} did not finish")

    def fetch(self, name: str) -> str:
        return aws(self.n["region_a"], "s3", "cp", f"s3://{self.n['bucket']}/runs/{name}", "-")


class Local:
    """Runs the same cdcctl.sh against the local two-sided stack."""

    def __init__(self, root: str) -> None:
        self.root = Path(root)

    def start(self, side: str, args: list[str], timeout_s: int = 1200):
        env = {**os.environ, "CDC_HOME": (self.root / side).as_posix(), "MSYS_NO_PATHCONV": "1",
               "TMPDIR": self.root.as_posix()}
        return subprocess.Popen([BASH, "cdcctl.sh", *args], cwd=self.root / side, env=env, encoding="utf-8", errors="replace",
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def wait(self, handle, timeout_s: int = 1200) -> tuple[int, str, str]:
        out, err = handle.communicate(timeout=timeout_s)
        return handle.returncode, out, err

    def fetch(self, name: str) -> str:
        return (self.root / "store" / name).read_text(encoding="utf-8")


def transport():
    return Local(os.environ["CDC_LOCAL"]) if os.getenv("CDC_LOCAL") else Ssm()


def run(t, side: str, args: list[str], timeout_s: int = 1200) -> tuple[int, str, str]:
    return t.wait(t.start(side, args, timeout_s), timeout_s)


def main() -> int:
    if len(sys.argv) < 3 or sys.argv[1] not in ("a", "b"):
        print(__doc__, file=sys.stderr)
        return 2
    rc, out, err = run(transport(), sys.argv[1], sys.argv[2:])
    sys.stdout.write(out)
    if rc != 0:
        sys.stderr.write(err)
    return rc


if __name__ == "__main__":
    sys.exit(main())
