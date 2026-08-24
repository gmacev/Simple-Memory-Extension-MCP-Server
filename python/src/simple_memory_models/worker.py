from __future__ import annotations

import json
import os
import sys
import traceback

from .runtime import ModelRuntime, RuntimeConfig
from .worker_protocol import handle_request


def _write(payload: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def main() -> None:
    runtime = ModelRuntime(RuntimeConfig.from_environment())
    print(
        f"simple-memory model worker ready pid={os.getpid()} device={runtime.config.device}",
        file=sys.stderr,
        flush=True,
    )
    for line in sys.stdin:
        request_id: object = None
        try:
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError("Request must be a JSON object")
            request_id = payload.get("id")
            result = handle_request(runtime, payload)
            _write({"id": request_id, "ok": True, "result": result})
            if payload.get("operation") == "shutdown":
                return
        except Exception as error:
            print(traceback.format_exc(), file=sys.stderr, flush=True)
            _write(
                {
                    "id": request_id,
                    "ok": False,
                    "error": {"type": type(error).__name__, "message": str(error)},
                }
            )


if __name__ == "__main__":
    main()
