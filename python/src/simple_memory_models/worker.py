from __future__ import annotations

import json
import os
import sys
import traceback
from typing import Any

from .runtime import ModelRuntime, RuntimeConfig


def _write(payload: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _require_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _require_strings(payload: dict[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{key} must be an array of strings")
    return value


def _handle(runtime: ModelRuntime, payload: dict[str, Any]) -> object:
    operation = _require_string(payload, "operation")
    if operation == "health":
        return {"status": "ok", "pid": os.getpid(), **runtime.model_info()}
    if operation == "model_info":
        return runtime.model_info()
    if operation == "embed_documents":
        return {"vectors": runtime.embed_documents(_require_strings(payload, "texts"))}
    if operation == "count_tokens":
        return {"counts": runtime.count_tokens(_require_strings(payload, "texts"))}
    if operation == "embed_query":
        return {"vector": runtime.embed_query(_require_string(payload, "text"))}
    if operation == "rerank":
        return {
            "scores": runtime.rerank(
                _require_string(payload, "query"), _require_strings(payload, "documents")
            )
        }
    if operation == "shutdown":
        return {"status": "shutting_down"}
    raise ValueError(f"Unknown operation: {operation}")


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
            result = _handle(runtime, payload)
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
