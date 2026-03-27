from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


def main() -> int:
    target = sys.argv[1] if len(sys.argv) > 1 else "ready"
    host = os.getenv("WORKER_HEALTH_HOST", "127.0.0.1")
    port = int(os.getenv("WORKER_HEALTH_PORT", "8081"))
    endpoint = "/livez" if target == "live" else "/readyz"
    url = f"http://{host}:{port}{endpoint}"
    try:
        with urllib.request.urlopen(url, timeout=2.0) as response:
            payload = json.loads(response.read().decode("utf-8"))
            if response.status != 200:
                print(payload)
                return 1
            return 0
    except urllib.error.URLError as exc:
        print(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
