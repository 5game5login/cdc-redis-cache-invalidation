"""Roll the current S3 bundle out to both nodes (run `tf.sh apply` first to upload it).

Rebuilds the image, re-registers the connector and restarts only what changed.
"""
from __future__ import annotations

import json
import sys
import time

from cdc_remote import Ssm, aws

SCRIPT = ["set -e", "export HOME=/root", "/opt/cdc/update.sh",
          "/opt/cdc/cdcctl.sh up > /var/log/cdc-up.log 2>&1 || { tail -15 /var/log/cdc-up.log; exit 1; }",
          "/opt/cdc/cdcctl.sh status 2> /dev/null"]


def main() -> int:
    t = Ssm()
    handles = []
    for side in "ab":
        out = aws(t.n[f"region_{side}"], "ssm", "send-command", "--document-name", "AWS-RunShellScript",
                  "--instance-ids", t.n[f"instance_{side}"], "--parameters",
                  json.dumps({"commands": SCRIPT, "executionTimeout": ["900"]}),
                  "--query", "Command.CommandId", "--output", "text")
        handles.append((side, out.strip()))
    t0, bad = time.time(), 0
    for h in handles:
        rc, out, err = t.wait(h, 900)
        bad |= rc != 0
        print(f"[{h[0]}] rc={rc} {time.time() - t0:.0f}s {out.strip()[-500:]}" + (f"\n{err[-800:]}" if rc else ""), flush=True)
    return int(bad)


if __name__ == "__main__":
    sys.exit(main())
