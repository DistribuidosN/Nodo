"""Compatibility wrapper for the old worker entrypoint.

Use ``worker.server`` as the canonical module from now on.
"""

from worker.server import main, run_worker_server

run_worker = run_worker_server


if __name__ == "__main__":
    main()
