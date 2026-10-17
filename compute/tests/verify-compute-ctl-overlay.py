#!/usr/bin/env python3
"""Exercise the pooled-Compute startup path, not merely compute_ctl --help.

Uses only a temporary local Docker container and fake unauthenticated config.
No production connection, published ports, or real credentials are needed.
"""
import argparse
import json
import subprocess
import tempfile
import time
from pathlib import Path


def run(*args, **kwargs):
    return subprocess.run(args, capture_output=True, text=True, errors="replace", timeout=kwargs.pop("timeout", 30), **kwargs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("image")
    parser.add_argument("--build-tag", required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="lbfs-warm-smoke-") as directory:
        config = Path(directory) / "config.json"
        config.write_text(json.dumps({"spec": None, "compute_ctl_config": {"jwks": {"keys": []}, "tls": None}}))
        config.chmod(0o644)  # Public fake config, readable by image's non-root UID.
        started = run("docker", "run", "-d", "--platform", "linux/amd64", "--network", "none",
                      "--tmpfs", "/tmp:size=768m,mode=1777", "-v", str(config) + ":/config.json:ro",
                      "--entrypoint", "/usr/local/bin/compute_ctl", args.image,
                      "-b", "/usr/local/bin/postgres", "--pgdata", "/tmp/compute",
                      "--connstr", "postgresql://localhost/postgres", "--compute-id", "warm-smoke",
                      "--config", "/config.json", "--dev", check=True)
        container = started.stdout.strip()
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                state = run("docker", "inspect", "--format", "{{.State.Running}}", container, check=True)
                if state.stdout.strip() != "true":
                    raise RuntimeError("pooled Compute exited before becoming available")
                # The retained runtime has Bash, but intentionally no curl.
                # HTTP/1.0 + Connection: close gives a complete public /metrics
                # response over loopback without adding runtime dependencies.
                request = "exec 3<>/dev/tcp/127.0.0.1/3080; printf 'GET /metrics HTTP/1.0\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n' >&3; cat <&3"
                response = run("docker", "exec", container, "bash", "-c", request, timeout=3)
                if response.returncode == 0:
                    samples = [line for line in response.stdout.splitlines()
                               if line.startswith("compute_ctl_up{") and 'status="empty"' in line
                               and line.rstrip().endswith(" 1")]
                    if samples:
                        logs = run("docker", "logs", container, check=True)
                        assert "compute build_tag: " + args.build_tag in logs.stdout + logs.stderr
                        assert "done prewarming vm memory" in logs.stdout + logs.stderr
                        print("PASS: real warm initdb/Postgres prewarm and HTTP empty-state up metric; compiled source tag exact")
                        return
                time.sleep(0.2)
            raise RuntimeError("pooled Compute did not return HTTP empty-state up metric within 30s")
        finally:
            run("docker", "rm", "-f", container, check=True)


if __name__ == "__main__":
    main()
